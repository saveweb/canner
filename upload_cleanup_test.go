package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPartialUploadStalenessStartsWhenAttemptEnds(t *testing.T) {
	s := testServer(t)
	s.cfg.partialUploadRetention = 30 * time.Minute
	endedAt := time.Now().Add(time.Hour).Truncate(time.Second)
	s.now = func() time.Time { return endedAt }
	location := createUpload(t, s, "test", 10, blake3Checksum([]byte("0123456789")))

	request := httptest.NewRequest(http.MethodPatch, location, bytes.NewReader([]byte("012")))
	request.Header.Set("Tus-Resumable", "1.0.0")
	request.Header.Set("Upload-Offset", "0")
	request.Header.Set("Content-Type", "application/offset+octet-stream")
	response := httptest.NewRecorder()
	s.handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("PATCH status = %d, body = %s", response.Code, response.Body.String())
	}

	objectID := strings.TrimPrefix(location, "/files/")
	stat, err := os.Stat(s.uploadAttemptPath(objectID))
	if err != nil {
		t.Fatal(err)
	}
	if !stat.ModTime().Equal(endedAt) {
		t.Fatalf("attempt end = %s, want %s", stat.ModTime(), endedAt)
	}

	s.now = func() time.Time { return endedAt.Add(29 * time.Minute) }
	uploads, err := s.partialUploads()
	if err != nil || len(uploads) != 1 || uploads[0].Stale {
		t.Fatalf("upload before retention = %+v, %v", uploads, err)
	}
	s.now = func() time.Time { return endedAt.Add(30 * time.Minute) }
	uploads, err = s.partialUploads()
	if err != nil || len(uploads) != 1 || !uploads[0].Stale {
		t.Fatalf("upload at retention = %+v, %v", uploads, err)
	}
}

func TestActiveAttemptIsNotStale(t *testing.T) {
	s := testServer(t)
	s.cfg.partialUploadRetention = 30 * time.Minute
	location := createUpload(t, s, "test", 10, blake3Checksum([]byte("0123456789")))
	objectID := strings.TrimPrefix(location, "/files/")
	old := time.Unix(1_700_000_000, 0)
	if err := os.Chtimes(filepath.Join(s.uploadsDir, objectID+".info"), old, old); err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return old.Add(time.Hour) }
	s.setUploadAttemptActive(objectID, 1)
	defer s.setUploadAttemptActive(objectID, -1)
	uploads, err := s.partialUploads()
	if err != nil || len(uploads) != 1 || uploads[0].Stale {
		t.Fatalf("active upload = %+v, %v", uploads, err)
	}
}

func TestCleanupRemovesOnlyStalePartialUploads(t *testing.T) {
	s := testServer(t)
	s.cfg.partialUploadRetention = 30 * time.Minute
	now := time.Unix(1_700_000_000, 0)
	s.now = func() time.Time { return now }

	staleLocation := createUpload(t, s, "test", 10, blake3Checksum([]byte("0123456789")))
	freshLocation := createUpload(t, s, "test", 10, blake3Checksum([]byte("abcdefghij")))
	staleID := strings.TrimPrefix(staleLocation, "/files/")
	freshID := strings.TrimPrefix(freshLocation, "/files/")
	old := now.Add(-31 * time.Minute)
	for _, suffix := range []string{"", ".info"} {
		if err := os.Chtimes(filepath.Join(s.uploadsDir, staleID+suffix), old, old); err != nil {
			t.Fatal(err)
		}
	}

	removed, err := s.cleanupStalePartialUploads(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d", removed)
	}
	for _, suffix := range []string{"", ".info", uploadAttemptSuffix} {
		if _, err := os.Stat(filepath.Join(s.uploadsDir, staleID+suffix)); !os.IsNotExist(err) {
			t.Fatalf("stale upload suffix %q still exists: %v", suffix, err)
		}
	}
	if _, err := os.Stat(filepath.Join(s.uploadsDir, freshID+".info")); err != nil {
		t.Fatalf("fresh upload was removed: %v", err)
	}
	snapshot := s.staleUploads.Load()
	if snapshot == nil || !snapshot.ScannedAt.Equal(now) || len(snapshot.Projects) != 0 {
		t.Fatalf("stale snapshot after cleanup = %+v", snapshot)
	}
}

func TestCleanupDoesNotHoldActiveMutexWhileWaitingForUploadLock(t *testing.T) {
	s := testServer(t)
	s.cfg.partialUploadRetention = 30 * time.Minute
	now := time.Unix(1_700_000_000, 0)
	s.now = func() time.Time { return now }
	location := createUpload(t, s, "test", 10, blake3Checksum([]byte("0123456789")))
	objectID := strings.TrimPrefix(location, "/files/")
	old := now.Add(-time.Hour)
	for _, suffix := range []string{"", ".info"} {
		if err := os.Chtimes(filepath.Join(s.uploadsDir, objectID+suffix), old, old); err != nil {
			t.Fatal(err)
		}
	}

	lock, err := s.locker.NewLock(objectID)
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Lock(t.Context(), func() {}); err != nil {
		t.Fatal(err)
	}
	defer lock.Unlock()

	done := make(chan error, 1)
	go func() {
		_, err := s.cleanupStalePartialUploads(context.Background())
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	activeUpdated := make(chan struct{})
	go func() {
		s.setUploadAttemptActive(objectID, 1)
		close(activeUpdated)
	}()
	select {
	case <-activeUpdated:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("active upload update blocked behind cleanup file lock")
	}
	defer s.setUploadAttemptActive(objectID, -1)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup did not stop waiting for upload lock")
	}
	if _, err := os.Stat(filepath.Join(s.uploadsDir, objectID+".info")); err != nil {
		t.Fatalf("active upload was removed: %v", err)
	}
	if snapshot := s.staleUploads.Load(); snapshot == nil || len(snapshot.Projects) != 0 {
		t.Fatalf("active upload reported as stale: %+v", snapshot)
	}
}

func TestCleanupPublishesLockedStaleUpload(t *testing.T) {
	s := testServer(t)
	s.cfg.partialUploadRetention = 30 * time.Minute
	now := time.Unix(1_700_000_000, 0)
	s.now = func() time.Time { return now }
	location := createUpload(t, s, "test", 10, blake3Checksum([]byte("0123456789")))
	objectID := strings.TrimPrefix(location, "/files/")
	if err := os.WriteFile(filepath.Join(s.uploadsDir, objectID), []byte("012"), 0o640); err != nil {
		t.Fatal(err)
	}
	old := now.Add(-time.Hour)
	for _, suffix := range []string{"", ".info"} {
		if err := os.Chtimes(filepath.Join(s.uploadsDir, objectID+suffix), old, old); err != nil {
			t.Fatal(err)
		}
	}
	lock, err := s.locker.NewLock(objectID)
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Lock(t.Context(), func() {}); err != nil {
		t.Fatal(err)
	}
	defer lock.Unlock()

	removed, err := s.cleanupStalePartialUploads(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatalf("removed = %d", removed)
	}
	snapshot := s.staleUploads.Load()
	want := staleUploadStats{Count: 1, Received: 3, TotalBytes: 10}
	if snapshot == nil || snapshot.Projects["test"] != want {
		t.Fatalf("stale snapshot = %+v, want %+v", snapshot, want)
	}
}

func TestDashboardShowsStaleIncompleteSeparately(t *testing.T) {
	view := dashboardView{
		UploadingCount: 1, StaleUploadCount: 2, StaleStatsReady: true,
		Projects: []dashboardProject{{Name: "test", UploadingArtifacts: 1, UploadingBytes: 3, UploadingTotalBytes: 10, StaleUploads: 2, StaleUploadBytes: 4, StaleUploadTotalBytes: 20}},
	}
	response := httptest.NewRecorder()
	if err := dashboardStatusTemplate.Execute(response, view); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"1 active", "2 stale/incomplete", "3 B / 10 B", "4 B / 20 B"} {
		if !strings.Contains(response.Body.String(), want) {
			t.Errorf("dashboard does not contain %q: %s", want, response.Body.String())
		}
	}
}

func TestDashboardShowsPendingStaleScan(t *testing.T) {
	response := httptest.NewRecorder()
	if err := dashboardStatusTemplate.Execute(response, dashboardView{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(response.Body.String(), "Scanning...") {
		t.Fatalf("dashboard does not show pending stale scan: %s", response.Body.String())
	}
}
