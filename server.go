package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tus/tusd/v2/pkg/filelocker"
	"github.com/tus/tusd/v2/pkg/filestore"
	tusd "github.com/tus/tusd/v2/pkg/handler"
	"github.com/zeebo/blake3"
	"golang.org/x/sys/unix"
)

const (
	receiptHeader         = "Artifact-Receipt"
	backpressureRetrySecs = "60"
)

type server struct {
	cfg           runtimeConfig
	store         filestore.FileStore
	deliveryStore *deliveryStore
	uploadsDir    string
	receiptDir    string
	handler       http.Handler
	now           func() time.Time
	available     func(string) (uint64, error)
}

func newServer(cfg runtimeConfig) (*server, error) {
	uploadsDir := filepath.Join(cfg.DataDir, "uploads")
	receiptDir := filepath.Join(cfg.DataDir, "receipts")
	for _, dir := range []string{uploadsDir, receiptDir} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("create data directory: %w", err)
		}
	}
	store := filestore.New(uploadsDir)
	store.DirModePerm = 0o750
	store.FileModePerm = 0o640
	locker := filelocker.New(uploadsDir)
	composer := tusd.NewStoreComposer()
	store.UseIn(composer)
	locker.UseIn(composer)

	s := &server{cfg: cfg, store: store, uploadsDir: uploadsDir, receiptDir: receiptDir, now: time.Now, available: availableBytes}
	if cfg.hasDelivery() {
		deliveryStore, deliveryErr := openDeliveryStore(cfg.DataDir)
		if deliveryErr != nil {
			slog.Error("open delivery index; startup reconciliation will recover receipts", "err", deliveryErr)
		} else {
			s.deliveryStore = deliveryStore
		}
	}
	uploadHandler, err := tusd.NewHandler(tusd.Config{
		BasePath:             "/files/",
		StoreComposer:        composer,
		MaxSize:              cfg.MaxUploadBytes,
		DisableDownload:      true,
		DisableTermination:   true,
		DisableConcatenation: true,
		PreUploadCreateCallback: func(hook tusd.HookEvent) (tusd.HTTPResponse, tusd.FileInfoChanges, error) {
			return s.beforeCreate(hook)
		},
		PreFinishResponseCallback: func(hook tusd.HookEvent) (tusd.HTTPResponse, error) {
			return s.beforeFinish(hook)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create upload handler: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /api/v1/receipts/{object_id}", s.getReceipt)
	files := s.storageBackpressure(http.StripPrefix("/files", uploadHandler))
	mux.Handle("/files", files)
	mux.Handle("/files/", files)
	s.handler = mux
	return s, nil
}

func (s *server) beforeCreate(hook tusd.HookEvent) (tusd.HTTPResponse, tusd.FileInfoChanges, error) {
	project := hook.Upload.MetaData["project"]
	if !identifierPattern.MatchString(project) {
		return tusd.HTTPResponse{}, tusd.FileInfoChanges{}, tusd.NewError("ERR_INVALID_PROJECT", "Upload-Metadata project is invalid", http.StatusBadRequest)
	}
	if _, ok := s.cfg.Projects[project]; !ok {
		return tusd.HTTPResponse{}, tusd.FileInfoChanges{}, tusd.NewError("ERR_INVALID_PROJECT", "Upload-Metadata project is not configured", http.StatusBadRequest)
	}
	if hook.Upload.SizeIsDeferred || hook.Upload.Size < 1 {
		return tusd.HTTPResponse{}, tusd.FileInfoChanges{}, tusd.NewError("ERR_INVALID_SIZE", "a positive Upload-Length is required", http.StatusBadRequest)
	}
	checksum := hook.Upload.MetaData["checksum"]
	const prefix = "blake3:"
	if !checksumPattern.MatchString(checksum) || !strings.HasPrefix(checksum, prefix) || !digestPattern.MatchString(strings.TrimPrefix(checksum, prefix)) {
		return tusd.HTTPResponse{}, tusd.FileInfoChanges{}, tusd.NewError("ERR_INVALID_CHECKSUM", "Upload-Metadata checksum must be blake3:<lowercase-hex>", http.StatusBadRequest)
	}
	metadata := tusd.MetaData{"project": project, "checksum": checksum}
	if filename := hook.Upload.MetaData["filename"]; filename != "" && len(filename) <= 1024 {
		metadata["filename"] = filename
	}
	return tusd.HTTPResponse{}, tusd.FileInfoChanges{MetaData: metadata}, nil
}

func (s *server) beforeFinish(hook tusd.HookEvent) (tusd.HTTPResponse, error) {
	if _, ok := s.cfg.Projects[hook.Upload.MetaData["project"]]; !ok {
		return tusd.HTTPResponse{}, tusd.NewError("ERR_INVALID_PROJECT", "upload project is no longer configured", http.StatusBadRequest)
	}
	receipt, err := s.accept(hook.Upload)
	if err != nil {
		var hashErr *hashMismatchError
		if errors.As(err, &hashErr) {
			return tusd.HTTPResponse{}, tusd.NewError("ERR_CHECKSUM_MISMATCH", err.Error(), http.StatusUnprocessableEntity)
		}
		slog.Error("accept upload", "object_id", hook.Upload.ID, "err", err)
		return tusd.HTTPResponse{}, tusd.NewError("ERR_ACCEPT_FAILED", "could not durably accept upload", http.StatusInternalServerError)
	}
	raw, _ := json.Marshal(receipt)
	return tusd.HTTPResponse{Header: tusd.HTTPHeader{
		receiptHeader: base64.RawURLEncoding.EncodeToString(raw),
	}}, nil
}

type hashMismatchError struct {
	want string
	got  string
}

func (e *hashMismatchError) Error() string {
	return fmt.Sprintf("checksum mismatch: expected %s, got %s", e.want, e.got)
}

func (s *server) accept(info tusd.FileInfo) (artifactReceipt, error) {
	if receipt, err := s.readReceipt(info.ID); err == nil {
		s.enqueueDelivery(info, receipt)
		return receipt, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return artifactReceipt{}, err
	}
	path := info.Storage[filestore.StorageKeyPath]
	file, err := os.Open(path)
	if err != nil {
		return artifactReceipt{}, fmt.Errorf("open upload: %w", err)
	}
	defer file.Close()
	if err := file.Sync(); err != nil {
		return artifactReceipt{}, fmt.Errorf("sync upload: %w", err)
	}
	infoFile, err := os.Open(info.Storage[filestore.StorageKeyInfoPath])
	if err != nil {
		return artifactReceipt{}, fmt.Errorf("open upload metadata: %w", err)
	}
	if err := infoFile.Sync(); err != nil {
		infoFile.Close()
		return artifactReceipt{}, fmt.Errorf("sync upload metadata: %w", err)
	}
	if err := infoFile.Close(); err != nil {
		return artifactReceipt{}, fmt.Errorf("close upload metadata: %w", err)
	}
	if err := syncDirectory(s.uploadsDir); err != nil {
		return artifactReceipt{}, fmt.Errorf("sync uploads directory: %w", err)
	}
	hash := blake3.New()
	n, err := io.Copy(hash, file)
	if err != nil {
		return artifactReceipt{}, fmt.Errorf("hash upload: %w", err)
	}
	if n != info.Size {
		return artifactReceipt{}, fmt.Errorf("stored size %d differs from declared size %d", n, info.Size)
	}
	actualChecksum := "blake3:" + hex.EncodeToString(hash.Sum(nil))
	if expectedChecksum := info.MetaData["checksum"]; actualChecksum != expectedChecksum {
		return artifactReceipt{}, &hashMismatchError{want: expectedChecksum, got: actualChecksum}
	}
	acceptedAt := s.now().UTC().Unix()
	receipt := artifactReceipt{
		ID: "receipt:" + info.ID, Issuer: s.cfg.Issuer, ObjectID: info.ID,
		Checksum: actualChecksum, SizeBytes: n, AcceptedAt: acceptedAt,
	}
	if err := s.writeReceipt(receipt); err != nil {
		return artifactReceipt{}, err
	}
	s.enqueueDelivery(info, receipt)
	return receipt, nil
}

func (s *server) enqueueDelivery(info tusd.FileInfo, receipt artifactReceipt) {
	if s.deliveryStore == nil {
		return
	}
	job := deliveryJob{
		ObjectID:   receipt.ObjectID,
		Project:    info.MetaData["project"],
		Filename:   safeFilename(info.MetaData["filename"], receipt.ObjectID),
		AcceptedAt: receipt.AcceptedAt,
	}
	if err := s.deliveryStore.addAccepted(context.Background(), job); err != nil {
		slog.Error("index accepted artifact for delivery; reconciliation will retry", "object_id", receipt.ObjectID, "err", err)
	}
}

func (s *server) writeReceipt(receipt artifactReceipt) error {
	raw, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("encode receipt: %w", err)
	}
	path := s.receiptPath(receipt.ObjectID)
	temp, err := os.CreateTemp(s.receiptDir, ".receipt-*")
	if err != nil {
		return fmt.Errorf("create receipt: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o640); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(raw); err != nil {
		temp.Close()
		return fmt.Errorf("write receipt: %w", err)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return fmt.Errorf("sync receipt: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close receipt: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("publish receipt: %w", err)
	}
	return syncDirectory(s.receiptDir)
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (s *server) readReceipt(objectID string) (artifactReceipt, error) {
	if !identifierPattern.MatchString(objectID) {
		return artifactReceipt{}, os.ErrNotExist
	}
	raw, err := os.ReadFile(s.receiptPath(objectID))
	if err != nil {
		return artifactReceipt{}, err
	}
	var receipt artifactReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return artifactReceipt{}, fmt.Errorf("decode stored receipt: %w", err)
	}
	return receipt, nil
}

func (s *server) receiptPath(objectID string) string {
	return filepath.Join(s.receiptDir, objectID+".json")
}

func (s *server) storageBackpressure(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost || r.Method == http.MethodPatch {
			available, err := s.available(s.cfg.DataDir)
			if err != nil {
				writeBackpressure(w, r, http.StatusServiceUnavailable, "could not determine available storage")
				return
			}
			if available < s.cfg.MinFreeBytes {
				writeBackpressure(w, r, http.StatusTooManyRequests, "insufficient free storage; retry later")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func availableBytes(path string) (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, err
	}
	if stat.Bsize <= 0 {
		return 0, fmt.Errorf("filesystem reported invalid block size %d", stat.Bsize)
	}
	blockSize := uint64(stat.Bsize)
	if stat.Bavail > ^uint64(0)/blockSize {
		return ^uint64(0), nil
	}
	return stat.Bavail * blockSize, nil
}

func writeBackpressure(w http.ResponseWriter, r *http.Request, status int, message string) {
	w.Header().Set("Retry-After", backpressureRetrySecs)
	if version := r.Header.Get("Tus-Resumable"); version != "" {
		w.Header().Set("Tus-Resumable", version)
	}
	writeError(w, status, message)
}

func (s *server) getReceipt(w http.ResponseWriter, r *http.Request) {
	objectID := r.PathValue("object_id")
	receipt, err := s.readReceipt(objectID)
	if errors.Is(err, os.ErrNotExist) {
		if _, uploadErr := s.store.GetUpload(r.Context(), objectID); uploadErr == nil {
			writeError(w, http.StatusConflict, "upload has not been accepted")
		} else {
			writeError(w, http.StatusNotFound, "receipt not found")
		}
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not read receipt")
		return
	}
	writeJSON(w, http.StatusOK, receipt)
}

func (s *server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"message": message}})
}
