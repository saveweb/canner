package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofrs/flock"
	"github.com/saveweb/go2internetarchive/pkg/upload"
	"github.com/tus/tusd/v2/pkg/filelocker"
	"github.com/tus/tusd/v2/pkg/filestore"
	_ "modernc.org/sqlite"
)

const (
	deliveryPollInterval      = time.Second
	deliveryReconcileInterval = time.Hour
)

type deliveryStore struct {
	db *sql.DB
}

func openDeliveryStore(dataDir string) (*deliveryStore, error) {
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dataDir, "delivery.sqlite"))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA busy_timeout=5000`); err != nil {
		db.Close()
		return nil, fmt.Errorf("configure delivery database: %w", err)
	}
	if err := dropLegacyDeliveries(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate legacy delivery index: %w", err)
	}
	for _, statement := range []string{
		`PRAGMA journal_mode=WAL`,
		`CREATE TABLE IF NOT EXISTS artifacts (
            object_id TEXT PRIMARY KEY,
            project TEXT NOT NULL,
            filename TEXT NOT NULL,
            checksum TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            accepted_at INTEGER NOT NULL,
            package_id TEXT,
            packaging_error TEXT,
            source_purged_at INTEGER,
            next_source_purge_at INTEGER NOT NULL DEFAULT 0,
            source_purge_error TEXT
        ) STRICT`,
		`CREATE INDEX IF NOT EXISTS artifacts_package_idx ON artifacts(project,package_id,packaging_error,accepted_at,object_id)`,
		`CREATE INDEX IF NOT EXISTS artifacts_source_purge_idx ON artifacts(source_purged_at,next_source_purge_at,accepted_at,object_id)`,
		`CREATE TABLE IF NOT EXISTS packaging_projects (
            project TEXT PRIMARY KEY,
            draining INTEGER NOT NULL DEFAULT 0 CHECK (draining IN (0,1)),
            updated_at INTEGER NOT NULL
        ) STRICT`,
		`CREATE TABLE IF NOT EXISTS packages (
            package_id TEXT PRIMARY KEY,
            project TEXT NOT NULL,
			packager TEXT NOT NULL CHECK (packager IN ('identity','mergewarc')),
            filename TEXT NOT NULL,
            manifest_filename TEXT NOT NULL,
			state TEXT NOT NULL CHECK (state IN ('building','sealed','blocked')),
            size_bytes INTEGER,
            checksum TEXT,
            manifest_checksum TEXT,
            member_count INTEGER NOT NULL,
			build_attempts INTEGER NOT NULL DEFAULT 0,
			next_build_at INTEGER NOT NULL,
			build_error TEXT,
            updated_at INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            sealed_at INTEGER,
            purge_after INTEGER,
            next_purge_attempt_at INTEGER,
			purge_error TEXT,
            purged_at INTEGER
        ) STRICT`,
		`CREATE INDEX IF NOT EXISTS packages_build_idx ON packages(state,next_build_at,created_at,package_id)`,
		`CREATE INDEX IF NOT EXISTS packages_purge_idx ON packages(purged_at,next_purge_attempt_at,purge_after,package_id)`,
		`CREATE TABLE IF NOT EXISTS package_members (
            package_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            object_id TEXT NOT NULL UNIQUE,
            offset_bytes INTEGER NOT NULL,
            size_bytes INTEGER NOT NULL,
            checksum TEXT NOT NULL,
            PRIMARY KEY(package_id,ordinal),
            FOREIGN KEY(package_id) REFERENCES packages(package_id)
        ) STRICT`,
		`CREATE TABLE IF NOT EXISTS deliveries (
            package_id TEXT NOT NULL,
            sink_id TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('pending','delivering','retry_wait','delivered')),
            plan TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at INTEGER NOT NULL,
            last_error TEXT,
            remote_id TEXT,
			progress TEXT,
            updated_at INTEGER NOT NULL,
            delivered_at INTEGER,
            PRIMARY KEY(package_id,sink_id),
            FOREIGN KEY(package_id) REFERENCES packages(package_id)
        ) STRICT`,
		`CREATE INDEX IF NOT EXISTS deliveries_due_idx ON deliveries(state,next_attempt_at,package_id,sink_id)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("initialize delivery database: %w", err)
		}
	}
	if err := ensureDeliveryProgressColumn(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate delivery progress: %w", err)
	}
	return &deliveryStore{db: db}, nil
}

func ensureDeliveryProgressColumn(db *sql.DB) error {
	hasColumn, err := deliveryProgressColumnExists(db)
	if err != nil || hasColumn {
		return err
	}
	if _, err := db.Exec(`ALTER TABLE deliveries ADD COLUMN progress TEXT`); err != nil {
		// Receiver and deliver can start together against the same database.
		hasColumn, checkErr := deliveryProgressColumnExists(db)
		if checkErr == nil && hasColumn {
			return nil
		}
		return err
	}
	return nil
}

func deliveryProgressColumnExists(db *sql.DB) (bool, error) {
	rows, err := db.Query(`PRAGMA table_info(deliveries)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, dataType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, err
		}
		if name == "progress" {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func dropLegacyDeliveries(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(deliveries)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	legacy := false
	current := false
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, dataType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			return err
		}
		legacy = legacy || name == "object_id"
		current = current || name == "package_id"
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if legacy && !current {
		_, err = db.Exec(`DROP TABLE deliveries`)
	}
	return err
}

func (s *deliveryStore) close() error { return s.db.Close() }

func exactlyOne(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("delivery state update affected %d rows", count)
	}
	return nil
}

type rowScanner interface {
	Scan(...any) error
}

type deliveryWorker struct {
	cfg            runtimeConfig
	store          *deliveryStore
	now            func() time.Time
	deliverPackage func(context.Context, packageDeliveryPlan, string, chan<- upload.Progress) error
	uploadsDir     string
	receiptsDir    string
}

func newDeliveryWorker(cfg runtimeConfig, store *deliveryStore) *deliveryWorker {
	return &deliveryWorker{
		cfg: cfg, store: store, now: time.Now,
		deliverPackage: deliverPackageToInternetArchive,
		uploadsDir:     filepath.Join(cfg.DataDir, "uploads"), receiptsDir: filepath.Join(cfg.DataDir, "receipts"),
	}
}

func runDelivery(ctx context.Context, cfg runtimeConfig) error {
	lock, err := acquireDeliveryLock(cfg.DataDir)
	if err != nil {
		return err
	}
	defer lock.Unlock()
	store, err := openDeliveryStore(cfg.DataDir)
	if err != nil {
		return err
	}
	defer store.close()
	worker := newDeliveryWorker(cfg, store)
	now := worker.now().Unix()
	if err := store.recoverInterruptedDeliveries(ctx, now); err != nil {
		return fmt.Errorf("recover interrupted package deliveries: %w", err)
	}
	if err := worker.discoverAccepted(ctx); err != nil {
		return fmt.Errorf("discover accepted artifacts: %w", err)
	}
	nextReconcile := worker.now().Add(deliveryReconcileInterval)
	for {
		if !worker.now().Before(nextReconcile) {
			if err := worker.discoverAccepted(ctx); err != nil {
				return fmt.Errorf("reconcile accepted artifacts: %w", err)
			}
			nextReconcile = worker.now().Add(deliveryReconcileInterval)
		}
		worked, err := worker.runPackageBuildCycle(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if worked {
			continue
		}
		worked, err = worker.runPackagedSourcePurgeCycle(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if worked {
			continue
		}
		worked, err = worker.runPackagePurgeCycle(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if worked {
			continue
		}
		worked, err = worker.runPackageDeliveryCycle(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if worked {
			continue
		}
		timer := time.NewTimer(deliveryPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func acquireDeliveryLock(dataDir string) (*flock.Flock, error) {
	path, err := filepath.Abs(filepath.Join(dataDir, "delivery.lock"))
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	lock := flock.New(path)
	locked, err := lock.TryLock()
	if err != nil {
		return nil, err
	}
	if !locked {
		return nil, fmt.Errorf("delivery process lock is held by another process")
	}
	return lock, nil
}

func purgeUpload(ctx context.Context, uploadsDir, objectID string) (err error) {
	if !identifierPattern.MatchString(objectID) {
		return fmt.Errorf("invalid artifact object id %q", objectID)
	}
	lock, err := filelocker.New(uploadsDir).NewLock(objectID)
	if err != nil {
		return err
	}
	if err := lock.Lock(ctx, func() {}); err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, lock.Unlock())
	}()
	for _, suffix := range []string{"", ".info", ".stop"} {
		if removeErr := os.Remove(filepath.Join(uploadsDir, objectID+suffix)); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return removeErr
		}
	}
	return syncDirectory(uploadsDir)
}

func unixCeil(value time.Time) int64 {
	seconds := value.Unix()
	if value.Nanosecond() != 0 {
		seconds++
	}
	return seconds
}

func retryDelay(attempt int) time.Duration {
	delay := time.Minute
	for i := 1; i < attempt && delay < time.Hour; i++ {
		delay *= 2
	}
	if delay > time.Hour {
		return time.Hour
	}
	return delay
}

func (w *deliveryWorker) discoverAccepted(ctx context.Context) error {
	entries, err := os.ReadDir(w.receiptsDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	fileStore := filestore.New(w.uploadsDir)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(w.receiptsDir, entry.Name()))
		if err != nil {
			slog.Error("read receipt for delivery", "receipt", entry.Name(), "err", err)
			continue
		}
		var receipt artifactReceipt
		if err := json.Unmarshal(raw, &receipt); err != nil {
			slog.Error("decode receipt for delivery", "receipt", entry.Name(), "err", err)
			continue
		}
		if receipt.ObjectID == "" || entry.Name() != receipt.ObjectID+".json" {
			slog.Error("receipt has mismatched object id", "receipt", entry.Name(), "object_id", receipt.ObjectID)
			continue
		}
		exists, err := w.store.hasArtifact(ctx, receipt.ObjectID)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		upload, err := fileStore.GetUpload(ctx, receipt.ObjectID)
		if err != nil {
			slog.Error("load accepted upload for delivery", "object_id", receipt.ObjectID, "err", err)
			continue
		}
		info, err := upload.GetInfo(ctx)
		if err != nil {
			slog.Error("read accepted upload metadata for delivery", "object_id", receipt.ObjectID, "err", err)
			continue
		}
		filename := safeFilename(info.MetaData["filename"], receipt.ObjectID)
		project := info.MetaData["project"]
		if !identifierPattern.MatchString(project) {
			slog.Error("accepted upload has invalid project metadata", "object_id", receipt.ObjectID, "project", project)
			continue
		}
		if err := w.store.addArtifact(ctx, artifactRecord{ObjectID: receipt.ObjectID, Project: project, Filename: filename, Checksum: receipt.Checksum, SizeBytes: receipt.SizeBytes, AcceptedAt: receipt.AcceptedAt}); err != nil {
			return err
		}
	}
	return nil
}

func safeFilename(filename, objectID string) string {
	if filename == "" || filename == "." || filename == ".." || len(filename) > 255 || filepath.Base(filename) != filename || strings.Contains(filename, `\`) {
		return objectID
	}
	return filename
}
