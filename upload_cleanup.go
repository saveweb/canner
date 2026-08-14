package main

import (
	"context"
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
		select {
		case <-ticker.C:
			if count, err := s.cleanupStalePartialUploads(context.Background()); err != nil {
				slog.Error("clean stale partial uploads", "err", err)
			} else if count > 0 {
				slog.Info("cleaned stale partial uploads", "count", count)
			}
		case <-s.cleanupStop:
			return
		}
	}
}

func (s *server) cleanupStalePartialUploads(ctx context.Context) (int, error) {
	uploads, err := s.activeUploads()
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, upload := range uploads {
		if !upload.Stale {
			continue
		}
		ok, err := s.cleanupStalePartialUpload(ctx, upload.ObjectID)
		if err != nil {
			return removed, err
		}
		if ok {
			removed++
		}
	}
	return removed, nil
}

func (s *server) cleanupStalePartialUpload(ctx context.Context, objectID string) (bool, error) {
	s.activeMu.Lock()
	if s.activeAttempts[objectID] > 0 {
		s.activeMu.Unlock()
		return false, nil
	}
	lock, err := s.locker.NewLock(objectID)
	if err != nil {
		s.activeMu.Unlock()
		return false, err
	}
	if err := lock.Lock(ctx, func() {}); err != nil {
		s.activeMu.Unlock()
		return false, err
	}
	s.activeMu.Unlock()
	defer lock.Unlock()
	if _, err := os.Stat(s.receiptPath(objectID)); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	lastAttemptEnd, err := s.partialUploadLastAttemptEnd(objectID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if lastAttemptEnd.Add(s.cfg.partialUploadRetention).After(s.now()) {
		return false, nil
	}
	for _, suffix := range []string{"", ".info", uploadAttemptSuffix} {
		if err := os.Remove(filepath.Join(s.uploadsDir, objectID+suffix)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("remove partial upload %s: %w", objectID, err)
		}
	}
	if err := syncDirectory(s.uploadsDir); err != nil {
		return false, err
	}
	return true, nil
}
