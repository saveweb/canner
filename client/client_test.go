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
	_, checksum, err := inspectContent(t.Context(), bytes.NewReader(content))
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

func TestUploadReportsHashingAndUploadingProgress(t *testing.T) {
	content := []byte("artifact content")
	_, checksum, err := inspectContent(t.Context(), bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	receipt := Receipt{ID: "receipt:object", ObjectID: "object", Checksum: checksum, SizeBytes: int64(len(content)), AcceptedAt: 123}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			w.Header().Set("Location", "/files/object")
			w.WriteHeader(http.StatusCreated)
		case http.MethodHead:
			w.Header().Set("Upload-Length", strconv.Itoa(len(content)))
			w.Header().Set("Upload-Offset", "0")
			w.WriteHeader(http.StatusOK)
		case http.MethodPatch:
			_, _ = io.Copy(io.Discard, r.Body)
			raw, _ := json.Marshal(receipt)
			w.Header().Set(receiptHeader, base64.RawURLEncoding.EncodeToString(raw))
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	client, _ := New(server.URL)

	var progress []UploadProgress
	got, err := client.UploadWithProgress(t.Context(), "demo", "artifact", bytes.NewReader(content), func(snapshot UploadProgress) {
		progress = append(progress, snapshot)
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != receipt {
		t.Fatalf("receipt = %+v, want %+v", got, receipt)
	}
	wantStart := UploadProgress{Phase: ProgressHashing, BytesDone: 0, BytesTotal: int64(len(content))}
	if len(progress) < 4 || progress[0] != wantStart {
		t.Fatalf("progress starts with %+v, want %+v; all = %+v", progress[0], wantStart, progress)
	}
	assertProgressContains(t, progress, UploadProgress{Phase: ProgressHashing, BytesDone: int64(len(content)), BytesTotal: int64(len(content))})
	assertProgressContains(t, progress, UploadProgress{Phase: ProgressUploading, BytesDone: 0, BytesTotal: int64(len(content))})
	if got := progress[len(progress)-1]; got != (UploadProgress{Phase: ProgressUploading, BytesDone: int64(len(content)), BytesTotal: int64(len(content))}) {
		t.Fatalf("final progress = %+v", got)
	}
}

func TestReportPeriodicProgressUntilStopped(t *testing.T) {
	var latest atomic.Pointer[UploadProgress]
	progress := &UploadProgress{Phase: ProgressUploading, BytesDone: 1, BytesTotal: 4}
	latest.Store(progress)
	done := make(chan struct{})
	stopped := make(chan struct{})
	output := &channelWriter{lines: make(chan string, 2)}
	go func() {
		reportPeriodicProgress(done, &latest, time.Millisecond, output, "demo", "artifact.warc.zst")
		close(stopped)
	}()

	var got string
	select {
	case got = <-output.lines:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for progress output")
	}
	close(done)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("progress reporter did not stop")
	}
	want := "canner upload progress: project=demo file=artifact.warc.zst phase=uploading bytes=1/4 percent=25.0%\n"
	if got != want {
		t.Fatalf("progress output = %q, want line %q", got, want)
	}
}

func TestUploadFileWithProgressToStdoutRejectsInvalidInterval(t *testing.T) {
	client, _ := New("https://canner.example")
	_, err := client.uploadFileWithPeriodicProgress(t.Context(), "demo", "missing", 0, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "interval must be positive") {
		t.Fatalf("error = %v", err)
	}
}

func TestResumeUsesRemoteOffsetAndReceiptFallback(t *testing.T) {
	content := []byte("abcdef")
	_, checksum, _ := inspectContent(t.Context(), bytes.NewReader(content))
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
	_, checksum, _ := inspectContent(t.Context(), bytes.NewReader([]byte("data")))
	session := Session{UploadURL: "https://other.example/files/object", ObjectID: "object", Checksum: checksum, Size: 4}
	_, err := client.Resume(t.Context(), session, bytes.NewReader([]byte("data")))
	if err == nil || !strings.Contains(err.Error(), "invalid upload session") {
		t.Fatalf("Resume error = %v", err)
	}
}

func TestResumeRejectsChangedArtifact(t *testing.T) {
	client, _ := New("https://canner.example")
	_, checksum, _ := inspectContent(t.Context(), bytes.NewReader([]byte("data")))
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

func TestCreateHashingHonorsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	content := &cancelingReadSeeker{reader: bytes.NewReader(make([]byte, 128<<10)), cancel: cancel}
	client, _ := New("https://canner.example")

	_, err := client.Create(ctx, "demo", "artifact", content)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Create error = %v, want context.Canceled", err)
	}
}

type cancelingReadSeeker struct {
	reader *bytes.Reader
	cancel context.CancelFunc
	reads  int
}

func (r *cancelingReadSeeker) Read(p []byte) (int, error) {
	r.reads++
	if r.reads == 1 {
		defer r.cancel()
	}
	return r.reader.Read(p)
}

func (r *cancelingReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return r.reader.Seek(offset, whence)
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

func assertProgressContains(t *testing.T, snapshots []UploadProgress, want UploadProgress) {
	t.Helper()
	for _, snapshot := range snapshots {
		if snapshot == want {
			return
		}
	}
	t.Fatalf("progress does not contain %+v: %+v", want, snapshots)
}

type channelWriter struct {
	lines chan string
}

func (w *channelWriter) Write(p []byte) (int, error) {
	w.lines <- string(p)
	return len(p), nil
}
