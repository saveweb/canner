package main

import (
	"bytes"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenDeliveryStoreReplacesLegacyArtifactDeliveryIndex(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "delivery.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE deliveries(object_id TEXT PRIMARY KEY,state TEXT NOT NULL) STRICT`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := openDeliveryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	rows, err := store.db.Query(`PRAGMA table_info(deliveries)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns := make(map[string]bool)
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, dataType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		columns[name] = true
	}
	if !columns["package_id"] || !columns["sink_id"] || columns["object_id"] {
		t.Fatalf("delivery columns after migration = %v", columns)
	}
}

func TestDeliveryProcessLockRejectsSecondOwner(t *testing.T) {
	dir := t.TempDir()
	first, err := acquireDeliveryLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Unlock()
	if _, err := acquireDeliveryLock(dir); err == nil {
		t.Fatal("second delivery process lock succeeded")
	}
}

func TestRetryDelayCapsAtOneHour(t *testing.T) {
	if got := retryDelay(1); got != time.Minute {
		t.Fatalf("first retry delay = %s", got)
	}
	if got := retryDelay(100); got != time.Hour {
		t.Fatalf("capped retry delay = %s", got)
	}
}

func TestUnixCeilDoesNotShortenRetention(t *testing.T) {
	if got := unixCeil(time.Unix(10, 1)); got != 11 {
		t.Fatalf("unixCeil with fractional second = %d", got)
	}
	if got := unixCeil(time.Unix(10, 0)); got != 10 {
		t.Fatalf("unixCeil on second boundary = %d", got)
	}
}

func TestUnsafeFilenameFallsBackToObjectID(t *testing.T) {
	for _, filename := range []string{"", ".", "..", "../artifact", `dir\artifact`, "/artifact"} {
		if got := safeFilename(filename, "object"); got != "object" {
			t.Errorf("safeFilename(%q) = %q", filename, got)
		}
	}
	if got := safeFilename("artifact.warc.zst", "object"); got != "artifact.warc.zst" {
		t.Fatalf("safe filename changed to %q", got)
	}
}

func acceptTestArtifact(t *testing.T, s *server, body []byte) {
	t.Helper()
	location := createUpload(t, s, "test", int64(len(body)), blake3Checksum(body))
	request := httptest.NewRequest(http.MethodPatch, location, bytes.NewReader(body))
	request.Header.Set("Tus-Resumable", "1.0.0")
	request.Header.Set("Upload-Offset", "0")
	request.Header.Set("Content-Type", "application/offset+octet-stream")
	response := httptest.NewRecorder()
	s.handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("PATCH status = %d, body = %s", response.Code, response.Body.String())
	}
	if matches, err := filepath.Glob(filepath.Join(s.receiptDir, "*.json")); err != nil || len(matches) < 1 {
		t.Fatalf("receipts = %v, %v", matches, err)
	}
}
