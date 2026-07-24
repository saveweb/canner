package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofrs/flock"
	"github.com/tus/tusd/v2/pkg/filelocker"
	"github.com/tus/tusd/v2/pkg/filestore"
	_ "modernc.org/sqlite"
)

const (
	deliveryPollInterval      = time.Second
	deliveryReconcileInterval = time.Hour
)

type deliveryJob struct {
	ObjectID    string  `json:"object_id"`
	Project     string  `json:"project"`
	Filename    string  `json:"filename"`
	AcceptedAt  int64   `json:"accepted_at"`
	State       string  `json:"state"`
	Attempts    int     `json:"attempts"`
	NextAttempt int64   `json:"next_attempt_at"`
	LastError   *string `json:"last_error"`
	RemoteID    *string `json:"remote_id"`
	RemoteName  *string `json:"remote_name"`
	UpdatedAt   int64   `json:"updated_at"`
	DeliveredAt *int64  `json:"delivered_at"`
	PurgeAfter  *int64  `json:"purge_after"`
	NextPurgeAt *int64  `json:"next_purge_attempt_at"`
	PurgedAt    *int64  `json:"purged_at"`
}

const deliveryColumns = `object_id,project,filename,accepted_at,state,attempts,next_attempt_at,last_error,remote_id,remote_name,updated_at,delivered_at,purge_after,next_purge_attempt_at,purged_at`

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
	for _, statement := range []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS deliveries (
            object_id TEXT PRIMARY KEY,
            project TEXT NOT NULL,
            filename TEXT NOT NULL,
            accepted_at INTEGER NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('pending','delivering','retry_wait','delivered')),
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at INTEGER NOT NULL,
            last_error TEXT,
            remote_id TEXT,
            remote_name TEXT,
            updated_at INTEGER NOT NULL,
            delivered_at INTEGER,
            purge_after INTEGER,
            next_purge_attempt_at INTEGER,
            purged_at INTEGER
        ) STRICT`,
		`CREATE INDEX IF NOT EXISTS deliveries_due_idx ON deliveries(state,next_attempt_at,accepted_at,object_id)`,
		`CREATE INDEX IF NOT EXISTS deliveries_purge_idx ON deliveries(purged_at,next_purge_attempt_at,purge_after,object_id)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("initialize delivery database: %w", err)
		}
	}
	return &deliveryStore{db: db}, nil
}

func (s *deliveryStore) close() error { return s.db.Close() }

func (s *deliveryStore) recoverInterrupted(ctx context.Context, now int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE deliveries SET state='retry_wait',next_attempt_at=?,last_error='delivery process stopped before recording an outcome',updated_at=? WHERE state='delivering'`, now, now)
	return err
}

func (s *deliveryStore) addAccepted(ctx context.Context, job deliveryJob) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO deliveries(object_id,project,filename,accepted_at,state,next_attempt_at,updated_at) VALUES(?,?,?,?,'pending',?,?) ON CONFLICT(object_id) DO NOTHING`, job.ObjectID, job.Project, job.Filename, job.AcceptedAt, job.AcceptedAt, job.AcceptedAt)
	return err
}

func (s *deliveryStore) claim(ctx context.Context, now int64) (deliveryJob, bool, error) {
	row := s.db.QueryRowContext(ctx, `UPDATE deliveries SET state='delivering',attempts=attempts+1,updated_at=? WHERE object_id=(SELECT object_id FROM deliveries WHERE state IN ('pending','retry_wait') AND next_attempt_at<=? ORDER BY accepted_at,object_id LIMIT 1) RETURNING `+deliveryColumns, now, now)
	job, err := scanDelivery(row)
	if errors.Is(err, sql.ErrNoRows) {
		return deliveryJob{}, false, nil
	}
	return job, err == nil, err
}

func (s *deliveryStore) markDelivered(ctx context.Context, objectID string, result deliveryResult, deliveredAt, purgeAfter int64) error {
	dbResult, err := s.db.ExecContext(ctx, `UPDATE deliveries SET state='delivered',next_attempt_at=0,remote_id=?,remote_name=?,last_error=NULL,updated_at=?,delivered_at=?,purge_after=?,next_purge_attempt_at=? WHERE object_id=? AND state='delivering'`, result.RemoteID, result.RemoteName, deliveredAt, deliveredAt, purgeAfter, purgeAfter, objectID)
	return exactlyOne(dbResult, err)
}

func (s *deliveryStore) nextPurge(ctx context.Context, now int64) (deliveryJob, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries WHERE state='delivered' AND purged_at IS NULL AND next_purge_attempt_at<=? ORDER BY next_purge_attempt_at,object_id LIMIT 1`, now)
	job, err := scanDelivery(row)
	if errors.Is(err, sql.ErrNoRows) {
		return deliveryJob{}, false, nil
	}
	return job, err == nil, err
}

func (s *deliveryStore) markPurged(ctx context.Context, objectID string, now int64) error {
	result, err := s.db.ExecContext(ctx, `UPDATE deliveries SET purged_at=?,next_purge_attempt_at=NULL,last_error=NULL,updated_at=? WHERE object_id=? AND state='delivered' AND purged_at IS NULL`, now, now, objectID)
	return exactlyOne(result, err)
}

func (s *deliveryStore) markPurgeRetry(ctx context.Context, objectID, message string, retryAt, now int64) error {
	if len(message) > 4096 {
		message = message[:4096]
	}
	result, err := s.db.ExecContext(ctx, `UPDATE deliveries SET next_purge_attempt_at=?,last_error=?,updated_at=? WHERE object_id=? AND state='delivered' AND purged_at IS NULL`, retryAt, message, now, objectID)
	return exactlyOne(result, err)
}

func (s *deliveryStore) hasObject(ctx context.Context, objectID string) (bool, error) {
	var exists int
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM deliveries WHERE object_id=?)`, objectID).Scan(&exists)
	return exists == 1, err
}

func (s *deliveryStore) markRetry(ctx context.Context, objectID, message string, nextAttempt, now int64) error {
	if len(message) > 4096 {
		message = message[:4096]
	}
	result, err := s.db.ExecContext(ctx, `UPDATE deliveries SET state='retry_wait',next_attempt_at=?,last_error=?,updated_at=? WHERE object_id=? AND state='delivering'`, nextAttempt, message, now, objectID)
	return exactlyOne(result, err)
}

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

func scanDelivery(row rowScanner) (deliveryJob, error) {
	var job deliveryJob
	err := row.Scan(&job.ObjectID, &job.Project, &job.Filename, &job.AcceptedAt, &job.State, &job.Attempts, &job.NextAttempt, &job.LastError, &job.RemoteID, &job.RemoteName, &job.UpdatedAt, &job.DeliveredAt, &job.PurgeAfter, &job.NextPurgeAt, &job.PurgedAt)
	return job, err
}

func (s *deliveryStore) list(ctx context.Context) ([]deliveryJob, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries ORDER BY accepted_at,object_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []deliveryJob
	for rows.Next() {
		job, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

type artifactSink interface {
	deliver(context.Context, deliveryJob, string) (deliveryResult, error)
}

type sinkFactory func(project string, cfg deliveryConfig) (artifactSink, error)

type deliveryWorker struct {
	cfg         runtimeConfig
	store       *deliveryStore
	now         func() time.Time
	newSink     sinkFactory
	uploadsDir  string
	receiptsDir string
}

func newDeliveryWorker(cfg runtimeConfig, store *deliveryStore) *deliveryWorker {
	return &deliveryWorker{
		cfg: cfg, store: store, now: time.Now, newSink: newInternetArchiveSink,
		uploadsDir: filepath.Join(cfg.DataDir, "uploads"), receiptsDir: filepath.Join(cfg.DataDir, "receipts"),
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
	if err := store.recoverInterrupted(ctx, now); err != nil {
		return fmt.Errorf("recover interrupted deliveries: %w", err)
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
		worked, err := worker.runPurgeCycle(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if worked {
			continue
		}
		worked, err = worker.runCycle(ctx)
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

func (w *deliveryWorker) runCycle(ctx context.Context) (bool, error) {
	now := w.now()
	job, ok, err := w.store.claim(ctx, now.Unix())
	if err != nil || !ok {
		return false, err
	}
	projectCfg, exists := w.cfg.Projects[job.Project]
	if !exists || projectCfg.Delivery == nil {
		err = fmt.Errorf("project %q has no delivery configuration", job.Project)
	} else {
		var sink artifactSink
		sink, err = w.newSink(job.Project, *projectCfg.Delivery)
		if err == nil {
			var result deliveryResult
			result, err = sink.deliver(ctx, job, filepath.Join(w.uploadsDir, job.ObjectID))
			if err == nil {
				deliveredAt := w.now()
				purgeAfter := deliveredAt.Add(projectCfg.Delivery.localArtifactRetention)
				purgeAfterUnix := unixCeil(purgeAfter)
				if markErr := w.store.markDelivered(ctx, job.ObjectID, result, deliveredAt.Unix(), purgeAfterUnix); markErr != nil {
					return true, markErr
				}
				slog.Info("artifact delivered", "object_id", job.ObjectID, "project", job.Project, "remote_id", result.RemoteID, "remote_name", result.RemoteName, "purge_after", purgeAfterUnix)
				return true, nil
			}
		}
	}
	next := w.now().Add(retryDelay(job.Attempts)).Unix()
	if markErr := w.store.markRetry(ctx, job.ObjectID, err.Error(), next, w.now().Unix()); markErr != nil {
		return true, markErr
	}
	slog.Error("artifact delivery failed", "object_id", job.ObjectID, "project", job.Project, "attempt", job.Attempts, "retry_at", next, "err", err)
	return true, nil
}

func (w *deliveryWorker) runPurgeCycle(ctx context.Context) (bool, error) {
	now := w.now()
	job, ok, err := w.store.nextPurge(ctx, now.Unix())
	if err != nil || !ok {
		return false, err
	}
	if err := purgeUpload(ctx, w.uploadsDir, job.ObjectID); err != nil {
		retryAt := unixCeil(w.now().Add(time.Minute))
		if markErr := w.store.markPurgeRetry(ctx, job.ObjectID, err.Error(), retryAt, w.now().Unix()); markErr != nil {
			return true, markErr
		}
		slog.Error("local artifact purge failed", "object_id", job.ObjectID, "project", job.Project, "retry_at", retryAt, "err", err)
		return true, nil
	}
	if err := w.store.markPurged(ctx, job.ObjectID, w.now().Unix()); err != nil {
		return true, err
	}
	slog.Info("local artifact purged", "object_id", job.ObjectID, "project", job.Project)
	return true, nil
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
		exists, err := w.store.hasObject(ctx, receipt.ObjectID)
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
		job := deliveryJob{ObjectID: receipt.ObjectID, Project: info.MetaData["project"], Filename: filename, AcceptedAt: receipt.AcceptedAt}
		if !identifierPattern.MatchString(job.Project) {
			slog.Error("accepted upload has invalid project metadata", "object_id", receipt.ObjectID, "project", job.Project)
			continue
		}
		if err := w.store.addAccepted(ctx, job); err != nil {
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

func printDeliveries(ctx context.Context, cfg runtimeConfig, output io.Writer) error {
	store, err := openDeliveryStore(cfg.DataDir)
	if err != nil {
		return err
	}
	defer store.close()
	worker := newDeliveryWorker(cfg, store)
	if err := worker.discoverAccepted(ctx); err != nil {
		return err
	}
	jobs, err := store.list(ctx)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	for _, job := range jobs {
		if err := encoder.Encode(job); err != nil {
			return err
		}
	}
	return nil
}
