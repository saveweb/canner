package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const uploadAttemptSuffix = ".attempt"

const partialUploadLockTimeout = 100 * time.Millisecond

type partialUpload struct {
	ObjectID string
	Project  string
	Received int64
	Size     int64
	Stale    bool
}

type staleUploadStats struct {
	Count      int64
	Received   int64
	TotalBytes int64
}

type staleUploadSnapshot struct {
	ScannedAt time.Time
	Projects  map[string]staleUploadStats
}

func (s *server) trackUploadAttempts(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		objectID, ok := uploadObjectID(request)
		if !ok || request.Method != http.MethodPatch {
			next.ServeHTTP(response, request)
			return
		}
		s.setUploadAttemptActive(objectID, 1)
		defer func() {
			if err := s.recordUploadAttemptEnd(objectID, s.now()); err != nil {
				slog.Error("record upload attempt end", "object_id", objectID, "err", err)
			}
			s.setUploadAttemptActive(objectID, -1)
		}()
		next.ServeHTTP(response, request)
	})
}

func uploadObjectID(request *http.Request) (string, bool) {
	objectID := strings.TrimPrefix(request.URL.Path, "/files/")
	if objectID == request.URL.Path || strings.Contains(objectID, "/") || !identifierPattern.MatchString(objectID) {
		return "", false
	}
	return objectID, true
}

func (s *server) setUploadAttemptActive(objectID string, delta int) {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	s.activeAttempts[objectID] += delta
	if s.activeAttempts[objectID] <= 0 {
		delete(s.activeAttempts, objectID)
	}
}

func (s *server) uploadAttemptActive(objectID string) bool {
	s.activeMu.RLock()
	defer s.activeMu.RUnlock()
	return s.activeAttempts[objectID] > 0
}

func (s *server) activeUploadIDs() []string {
	s.activeMu.RLock()
	defer s.activeMu.RUnlock()
	ids := make([]string, 0, len(s.activeAttempts))
	for objectID := range s.activeAttempts {
		ids = append(ids, objectID)
	}
	return ids
}

func (s *server) recordUploadAttemptEnd(objectID string, endedAt time.Time) error {
	if _, err := os.Stat(s.receiptPath(objectID)); err == nil {
		if err := os.Remove(s.uploadAttemptPath(objectID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if _, err := os.Stat(filepath.Join(s.uploadsDir, objectID+".info")); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	temp, err := os.CreateTemp(s.uploadsDir, ".attempt-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o640); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Chtimes(tempPath, endedAt, endedAt); err != nil {
		return err
	}
	if err := os.Rename(tempPath, s.uploadAttemptPath(objectID)); err != nil {
		return err
	}
	return syncDirectory(s.uploadsDir)
}

func (s *server) uploadAttemptPath(objectID string) string {
	return filepath.Join(s.uploadsDir, objectID+uploadAttemptSuffix)
}

func (s *server) partialUploadLastAttemptEnd(objectID string) (time.Time, error) {
	var latest time.Time
	for _, path := range []string{s.uploadAttemptPath(objectID), filepath.Join(s.uploadsDir, objectID), filepath.Join(s.uploadsDir, objectID+".info")} {
		stat, err := os.Stat(path)
		if err == nil {
			if stat.ModTime().After(latest) {
				latest = stat.ModTime()
			}
			continue
		}
		if !errors.Is(err, os.ErrNotExist) {
			return time.Time{}, err
		}
	}
	if !latest.IsZero() {
		return latest, nil
	}
	return time.Time{}, os.ErrNotExist
}

func (s *server) startPartialUploadCleanup() {
	go s.runPartialUploadCleanup()
}

func (s *server) runPartialUploadCleanup() {
	defer close(s.cleanupDone)
	interval := s.cfg.partialUploadRetention / 2
	if interval > time.Minute {
		interval = time.Minute
	}
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if count, err := s.cleanupStalePartialUploads(context.Background()); err != nil {
			slog.Error("clean stale partial uploads", "err", err)
		} else if count > 0 {
			slog.Info("cleaned stale partial uploads", "count", count)
		}
		select {
		case <-ticker.C:
		case <-s.cleanupStop:
			return
		}
	}
}

func (s *server) cleanupStalePartialUploads(ctx context.Context) (int, error) {
	uploads, err := s.partialUploads()
	if err != nil {
		return 0, err
	}
	removed := 0
	stats := make(map[string]staleUploadStats)
	var cleanupErr error
	for _, upload := range uploads {
		if !upload.Stale {
			continue
		}
		wasRemoved, remainsStale, err := s.cleanupStalePartialUpload(ctx, upload.ObjectID)
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		}
		if wasRemoved {
			removed++
		}
		if !remainsStale {
			continue
		}
		project := stats[upload.Project]
		project.Count++
		project.Received += upload.Received
		project.TotalBytes += upload.Size
		stats[upload.Project] = project
	}
	s.staleUploads.Store(&staleUploadSnapshot{ScannedAt: s.now(), Projects: stats})
	return removed, cleanupErr
}

func (s *server) cleanupStalePartialUpload(ctx context.Context, objectID string) (removed, remainsStale bool, err error) {
	if s.uploadAttemptActive(objectID) {
		return false, false, nil
	}
	lock, err := s.locker.NewLock(objectID)
	if err != nil {
		return false, true, err
	}
	lockCtx, cancel := context.WithTimeout(ctx, partialUploadLockTimeout)
	defer cancel()
	if err := lock.Lock(lockCtx, func() {}); err != nil {
		if lockCtx.Err() != nil {
			return false, !s.uploadAttemptActive(objectID), nil
		}
		return false, true, err
	}
	defer lock.Unlock()
	if s.uploadAttemptActive(objectID) {
		return false, false, nil
	}
	if _, err := os.Stat(s.receiptPath(objectID)); err == nil {
		return false, false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, true, err
	}
	lastAttemptEnd, err := s.partialUploadLastAttemptEnd(objectID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, false, nil
		}
		return false, true, err
	}
	if lastAttemptEnd.Add(s.cfg.partialUploadRetention).After(s.now()) {
		return false, false, nil
	}
	for _, suffix := range []string{"", ".info", uploadAttemptSuffix} {
		if err := os.Remove(filepath.Join(s.uploadsDir, objectID+suffix)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, true, fmt.Errorf("remove partial upload %s: %w", objectID, err)
		}
	}
	if err := syncDirectory(s.uploadsDir); err != nil {
		return false, false, err
	}
	return true, false, nil
}

func (s *server) partialUploads() ([]partialUpload, error) {
	entries, err := os.ReadDir(s.uploadsDir)
	if err != nil {
		return nil, err
	}
	var uploads []partialUpload
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".info") {
			continue
		}
		objectID := strings.TrimSuffix(entry.Name(), ".info")
		upload, ok, err := s.partialUpload(objectID)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		stale, err := s.partialUploadStale(objectID)
		if err != nil {
			return nil, err
		}
		upload.Stale = stale
		uploads = append(uploads, upload)
	}
	return uploads, nil
}

func (s *server) partialUploadStale(objectID string) (bool, error) {
	if s.uploadAttemptActive(objectID) {
		return false, nil
	}
	lastAttemptEnd, err := s.partialUploadLastAttemptEnd(objectID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return !lastAttemptEnd.Add(s.cfg.partialUploadRetention).After(s.now()), nil
}

func (s *server) partialUpload(objectID string) (partialUpload, bool, error) {
	if _, err := os.Stat(s.receiptPath(objectID)); err == nil {
		return partialUpload{}, false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return partialUpload{}, false, err
	}
	raw, err := os.ReadFile(filepath.Join(s.uploadsDir, objectID+".info"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return partialUpload{}, false, nil
		}
		return partialUpload{}, false, err
	}
	var info struct {
		Size     int64             `json:"Size"`
		MetaData map[string]string `json:"MetaData"`
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		return partialUpload{}, false, fmt.Errorf("parse upload info %s: %w", objectID, err)
	}
	var received int64
	if stat, err := os.Stat(filepath.Join(s.uploadsDir, objectID)); err == nil {
		received = stat.Size()
	} else if !errors.Is(err, os.ErrNotExist) {
		return partialUpload{}, false, err
	}
	return partialUpload{ObjectID: objectID, Project: info.MetaData["project"], Received: received, Size: info.Size}, true, nil
}
