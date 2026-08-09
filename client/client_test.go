package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewUsesDefaultBaseURL(t *testing.T) {
	client, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	if got := client.baseURL.String(); got != DefaultBaseURL {
		t.Fatalf("base URL = %q, want %q", got, DefaultBaseURL)
	}
}

func TestUploadCompletesAndReturnsHeaderReceipt(t *testing.T) {
	content := []byte("artifact content")
	_, checksum, err := inspectContent(bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	receipt := Receipt{ID: "receipt:object", Issuer: "https://canner.example", ObjectID: "object", Checksum: checksum, SizeBytes: int64(len(content)), AcceptedAt: 123}
	var offset int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			if r.URL.Path != "/files/" || r.Header.Get("Upload-Length") != strconv.Itoa(len(content)) {
				t.Errorf("unexpected create request: %s %s, length %q", r.Method, r.URL.Path, r.Header.Get("Upload-Length"))
			}
			metadata := decodeMetadata(t, r.Header.Get("Upload-Metadata"))
			if metadata["project"] != "demo" || metadata["filename"] != "artifact.warc.gz" || metadata["checksum"] != checksum {
				t.Errorf("metadata = %#v", metadata)
			}
			w.Header().Set("Location", "/files/object")
			w.WriteHeader(http.StatusCreated)
		case http.MethodHead:
			w.Header().Set("Upload-Length", strconv.Itoa(len(content)))
			w.Header().Set("Upload-Offset", strconv.FormatInt(offset, 10))
			w.WriteHeader(http.StatusOK)
		case http.MethodPatch:
			body, _ := io.ReadAll(r.Body)
			if r.Header.Get("Upload-Offset") != "0" || !bytes.Equal(body, content) {
				t.Errorf("PATCH offset/body = %q/%q", r.Header.Get("Upload-Offset"), body)
			}
			offset = int64(len(body))
			raw, _ := json.Marshal(receipt)
			w.Header().Set(receiptHeader, base64.RawURLEncoding.EncodeToString(raw))
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.Upload(t.Context(), "demo", "artifact.warc.gz", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if got != receipt {
		t.Fatalf("receipt = %+v, want %+v", got, receipt)
	}
}

func TestResumeUsesRemoteOffsetAndReceiptFallback(t *testing.T) {
	content := []byte("abcdef")
	_, checksum, _ := inspectContent(bytes.NewReader(content))
	receipt := Receipt{ID: "receipt:object", ObjectID: "object", Checksum: checksum, SizeBytes: int64(len(content)), AcceptedAt: 123}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Upload-Length", "6")
			w.Header().Set("Upload-Offset", "3")
			w.WriteHeader(http.StatusOK)
		case http.MethodPatch:
			body, _ := io.ReadAll(r.Body)
			if r.Header.Get("Upload-Offset") != "3" || string(body) != "def" {
				t.Errorf("PATCH offset/body = %q/%q", r.Header.Get("Upload-Offset"), body)
			}
			w.WriteHeader(http.StatusNoContent)
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(receipt)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	client, _ := New(server.URL)
	session := Session{UploadURL: server.URL + "/files/object", ObjectID: "object", Project: "demo", Checksum: checksum, Size: 6}

	got, err := client.Resume(t.Context(), session, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if got != receipt {
		t.Fatalf("receipt = %+v, want %+v", got, receipt)
	}
}

func TestCreateRetriesBackpressure(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Location", "/files/object")
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()
	client, _ := New(server.URL)
	client.RetryDelay = time.Millisecond

	session, err := client.Create(t.Context(), "demo", "artifact", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || session.ObjectID != "object" {
		t.Fatalf("calls/session = %d/%+v", calls.Load(), session)
	}
}

func TestCreateDoesNotRetryAmbiguousTransportFailure(t *testing.T) {
	transport := &failingTransport{}
	client, _ := New("https://canner.example")
	client.HTTPClient = &http.Client{Transport: transport}
	_, err := client.Create(t.Context(), "demo", "artifact", bytes.NewReader([]byte("data")))
	if err == nil || transport.calls.Load() != 1 {
		t.Fatalf("error/calls = %v/%d", err, transport.calls.Load())
	}
}

func TestResumeRejectsCrossOriginSession(t *testing.T) {
	client, _ := New("https://canner.example")
	_, checksum, _ := inspectContent(bytes.NewReader([]byte("data")))
	session := Session{UploadURL: "https://other.example/files/object", ObjectID: "object", Checksum: checksum, Size: 4}
	_, err := client.Resume(t.Context(), session, bytes.NewReader([]byte("data")))
	if err == nil || !strings.Contains(err.Error(), "invalid upload session") {
		t.Fatalf("Resume error = %v", err)
	}
}

func TestResumeRejectsChangedArtifact(t *testing.T) {
	client, _ := New("https://canner.example")
	_, checksum, _ := inspectContent(bytes.NewReader([]byte("data")))
	session := Session{UploadURL: "https://canner.example/files/object", ObjectID: "object", Checksum: checksum, Size: 4}
	_, err := client.Resume(t.Context(), session, bytes.NewReader([]byte("edit")))
	if err == nil || !strings.Contains(err.Error(), "artifact changed") {
		t.Fatalf("Resume error = %v", err)
	}
}

func TestBackpressureWaitHonorsContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	client, _ := New(server.URL)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := client.Create(ctx, "demo", "artifact", bytes.NewReader([]byte("data")))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Create error = %v", err)
	}
}

type failingTransport struct {
	calls atomic.Int32
}

func (t *failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	t.calls.Add(1)
	return nil, errors.New("connection lost")
}

func decodeMetadata(t *testing.T, value string) map[string]string {
	t.Helper()
	metadata := make(map[string]string)
	for _, item := range strings.Split(value, ",") {
		parts := strings.SplitN(item, " ", 2)
		if len(parts) != 2 {
			t.Fatalf("invalid metadata item %q", item)
		}
		raw, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			t.Fatal(err)
		}
		metadata[parts[0]] = string(raw)
	}
	return metadata
}
