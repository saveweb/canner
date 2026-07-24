package main

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/zeebo/blake3"
)

func TestUploadReturnsAndPersistsReceipt(t *testing.T) {
	s := testServer(t)
	body := []byte("a small artifact\n")
	checksum := blake3Checksum(body)
	location := createUpload(t, s, "test", int64(len(body)), checksum)

	request := httptest.NewRequest(http.MethodPatch, location, bytes.NewReader(body))
	request.Header.Set("Tus-Resumable", "1.0.0")
	request.Header.Set("Upload-Offset", "0")
	request.Header.Set("Content-Type", "application/offset+octet-stream")
	response := httptest.NewRecorder()
	s.handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("PATCH status = %d, body = %s", response.Code, response.Body.String())
	}
	header := response.Header().Get(receiptHeader)
	if header == "" {
		t.Fatal("completion response has no receipt header")
	}
	raw, err := base64.RawURLEncoding.DecodeString(header)
	if err != nil {
		t.Fatal(err)
	}
	var fromHeader artifactReceipt
	if err := json.Unmarshal(raw, &fromHeader); err != nil {
		t.Fatal(err)
	}
	if fromHeader.Checksum != checksum || fromHeader.SizeBytes != int64(len(body)) {
		t.Fatalf("invalid receipt: %+v", fromHeader)
	}
	if bytes.Contains(raw, []byte(`"signature"`)) {
		t.Fatal("receipt unexpectedly contains a signature")
	}

	objectID := strings.TrimPrefix(location, "/files/")
	request = httptest.NewRequest(http.MethodGet, "/api/v1/receipts/"+objectID, nil)
	response = httptest.NewRecorder()
	s.handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET receipt status = %d, body = %s", response.Code, response.Body.String())
	}
	var stored artifactReceipt
	if err := json.Unmarshal(response.Body.Bytes(), &stored); err != nil {
		t.Fatal(err)
	}
	if stored != fromHeader {
		t.Fatalf("stored receipt differs: got %+v, want %+v", stored, fromHeader)
	}

	restarted, err := newServer(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodGet, "/api/v1/receipts/"+objectID, nil)
	response = httptest.NewRecorder()
	restarted.handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET receipt after restart status = %d, body = %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodDelete, location, nil)
	request.Header.Set("Tus-Resumable", "1.0.0")
	response = httptest.NewRecorder()
	restarted.handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE accepted upload status = %d, want 405", response.Code)
	}
}

func TestChecksumMismatchDoesNotIssueReceipt(t *testing.T) {
	s := testServer(t)
	body := []byte("content")
	location := createUpload(t, s, "test", int64(len(body)), "blake3:"+strings.Repeat("0", 64))
	request := httptest.NewRequest(http.MethodPatch, location, bytes.NewReader(body))
	request.Header.Set("Tus-Resumable", "1.0.0")
	request.Header.Set("Upload-Offset", "0")
	request.Header.Set("Content-Type", "application/offset+octet-stream")
	response := httptest.NewRecorder()
	s.handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("PATCH status = %d, body = %s", response.Code, response.Body.String())
	}
	if response.Header().Get(receiptHeader) != "" {
		t.Fatal("mismatched upload received a receipt")
	}
}

func TestUnknownProjectIsRejected(t *testing.T) {
	s := testServer(t)
	body := []byte("content")
	metadata := "project " + base64.StdEncoding.EncodeToString([]byte("unknown")) + ",checksum " + base64.StdEncoding.EncodeToString([]byte(blake3Checksum(body)))
	request := httptest.NewRequest(http.MethodPost, "/files/", nil)
	request.Header.Set("Tus-Resumable", "1.0.0")
	request.Header.Set("Upload-Length", fmtInt(int64(len(body))))
	request.Header.Set("Upload-Metadata", metadata)
	response := httptest.NewRecorder()
	s.handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("POST status = %d, want 400; body = %s", response.Code, response.Body.String())
	}
}

func TestStorageBackpressureRejectsCreateAndPatch(t *testing.T) {
	s := testServer(t)
	s.available = func(string) (uint64, error) { return s.cfg.MinFreeBytes - 1, nil }
	body := []byte("content")
	metadata := "project " + base64.StdEncoding.EncodeToString([]byte("test")) + ",checksum " + base64.StdEncoding.EncodeToString([]byte(blake3Checksum(body)))
	request := httptest.NewRequest(http.MethodPost, "/files/", nil)
	request.Header.Set("Tus-Resumable", "1.0.0")
	request.Header.Set("Upload-Length", fmtInt(int64(len(body))))
	request.Header.Set("Upload-Metadata", metadata)
	response := httptest.NewRecorder()
	s.handler.ServeHTTP(response, request)
	if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != backpressureRetrySecs || response.Header().Get("Tus-Resumable") != "1.0.0" {
		t.Fatalf("backpressured POST = %d, headers = %v", response.Code, response.Header())
	}

	s.available = func(string) (uint64, error) { return s.cfg.MinFreeBytes, nil }
	location := createUpload(t, s, "test", int64(len(body)), blake3Checksum(body))
	s.available = func(string) (uint64, error) { return s.cfg.MinFreeBytes - 1, nil }
	request = httptest.NewRequest(http.MethodPatch, location, bytes.NewReader(body))
	request.Header.Set("Tus-Resumable", "1.0.0")
	request.Header.Set("Upload-Offset", "0")
	request.Header.Set("Content-Type", "application/offset+octet-stream")
	response = httptest.NewRecorder()
	s.handler.ServeHTTP(response, request)
	if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != backpressureRetrySecs {
		t.Fatalf("backpressured PATCH = %d, headers = %v", response.Code, response.Header())
	}

	request = httptest.NewRequest(http.MethodHead, location, nil)
	request.Header.Set("Tus-Resumable", "1.0.0")
	response = httptest.NewRecorder()
	s.handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Upload-Offset") != "0" {
		t.Fatalf("HEAD during backpressure = %d, offset = %q", response.Code, response.Header().Get("Upload-Offset"))
	}
}

func TestStorageBackpressureFailsClosedWhenSpaceIsUnknown(t *testing.T) {
	s := testServer(t)
	s.available = func(string) (uint64, error) { return 0, errors.New("statfs failed") }
	request := httptest.NewRequest(http.MethodPost, "/files/", nil)
	request.Header.Set("Tus-Resumable", "1.0.0")
	response := httptest.NewRecorder()
	s.handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") != backpressureRetrySecs {
		t.Fatalf("POST with unknown free space = %d, headers = %v", response.Code, response.Header())
	}
}

func testServer(t *testing.T) *server {
	t.Helper()
	cfg := testConfig(t)
	s, err := newServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return time.Unix(123456789, 0) }
	return s
}

func testConfig(t *testing.T) runtimeConfig {
	t.Helper()
	return runtimeConfig{config: config{
		Issuer: "https://canner.example", DataDir: t.TempDir(), MaxUploadBytes: 1 << 20, MinFreeBytes: 1,
		Projects: map[string]projectConfig{"test": {}},
	}}
}

func createUpload(t *testing.T, s *server, project string, size int64, checksum string) string {
	t.Helper()
	metadata := "project " + base64.StdEncoding.EncodeToString([]byte(project)) + ",checksum " + base64.StdEncoding.EncodeToString([]byte(checksum))
	request := httptest.NewRequest(http.MethodPost, "/files/", nil)
	request.Header.Set("Tus-Resumable", "1.0.0")
	request.Header.Set("Upload-Length", fmtInt(size))
	request.Header.Set("Upload-Metadata", metadata)
	response := httptest.NewRecorder()
	s.handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		body, _ := io.ReadAll(response.Result().Body)
		t.Fatalf("POST status = %d, body = %s", response.Code, body)
	}
	location := response.Header().Get("Location")
	parsed, err := url.Parse(location)
	if err != nil || parsed.Path == "" {
		t.Fatalf("invalid Location %q", location)
	}
	return parsed.Path
}

func fmtInt(value int64) string {
	return strconv.FormatInt(value, 10)
}

func blake3Checksum(value []byte) string {
	sum := blake3.Sum256(value)
	return "blake3:" + hex.EncodeToString(sum[:])
}
