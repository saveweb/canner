package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type fakeArtifactSink struct {
	remoteID string
	err      error
	calls    int
}

func (s *fakeArtifactSink) deliver(context.Context, deliveryJob, string) (deliveryResult, error) {
	s.calls++
	return deliveryResult{RemoteID: s.remoteID, RemoteName: "artifact.warc.gz"}, s.err
}

func TestDeliveryWorkerDiscoversAndDeliversAcceptedArtifact(t *testing.T) {
	s := testServer(t)
	acceptTestArtifact(t, s, []byte("artifact"))
	project := s.cfg.Projects["test"]
	project.Delivery = &deliveryConfig{
		Sink: "internet_archive", CredentialsFile: "unused", Identifier: "unused", RemoteName: "unused",
	}
	s.cfg.Projects["test"] = project
	store, err := openDeliveryStore(s.cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	sink := &fakeArtifactSink{remoteID: "ia-item"}
	worker := newDeliveryWorker(s.cfg, store)
	worker.now = func() time.Time { return time.Unix(123456800, 0) }
	worker.newSink = func(string, deliveryConfig) (artifactSink, error) { return sink, nil }
	if err := worker.discoverAccepted(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := worker.discoverAccepted(t.Context()); err != nil {
		t.Fatal(err)
	}
	worked, err := worker.runCycle(t.Context())
	if err != nil || !worked {
		t.Fatalf("runCycle = %v, %v", worked, err)
	}
	jobs, err := store.list(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].State != "delivered" || jobs[0].Attempts != 1 || jobs[0].NextAttempt != 0 || jobs[0].RemoteID == nil || *jobs[0].RemoteID != "ia-item" || jobs[0].RemoteName == nil || *jobs[0].RemoteName != "artifact.warc.gz" {
		t.Fatalf("deliveries = %+v", jobs)
	}
	if sink.calls != 1 {
		t.Fatalf("sink calls = %d, want 1", sink.calls)
	}
}

func TestDeliveredArtifactIsPurgedAfterRetention(t *testing.T) {
	cfg := testConfig(t)
	project := cfg.Projects["test"]
	project.Delivery = &deliveryConfig{
		Sink: "internet_archive", CredentialsFile: "unused", Identifier: "unused", RemoteName: "unused",
		localArtifactRetention: 2 * time.Second,
	}
	cfg.Projects["test"] = project
	s, err := newServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.deliveryStore.close()
	now := time.Unix(123456800, 0)
	s.now = func() time.Time { return now }
	acceptTestArtifact(t, s, []byte("artifact"))

	worker := newDeliveryWorker(cfg, s.deliveryStore)
	worker.now = func() time.Time { return now }
	worker.newSink = func(string, deliveryConfig) (artifactSink, error) {
		return &fakeArtifactSink{remoteID: "ia-item"}, nil
	}
	if worked, err := worker.runCycle(t.Context()); err != nil || !worked {
		t.Fatalf("delivery cycle = %v, %v", worked, err)
	}
	jobs, err := s.deliveryStore.list(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	objectID := jobs[0].ObjectID
	if jobs[0].PurgeAfter == nil || *jobs[0].PurgeAfter != now.Add(2*time.Second).Unix() {
		t.Fatalf("delivery after upload = %+v", jobs[0])
	}
	if worked, err := worker.runPurgeCycle(t.Context()); err != nil || worked {
		t.Fatalf("early purge cycle = %v, %v", worked, err)
	}
	for _, suffix := range []string{"", ".info"} {
		if _, err := os.Stat(filepath.Join(s.uploadsDir, objectID+suffix)); err != nil {
			t.Fatalf("upload%s removed before retention: %v", suffix, err)
		}
	}

	now = now.Add(2 * time.Second)
	if worked, err := worker.runPurgeCycle(t.Context()); err != nil || !worked {
		t.Fatalf("due purge cycle = %v, %v", worked, err)
	}
	for _, suffix := range []string{"", ".info"} {
		if _, err := os.Stat(filepath.Join(s.uploadsDir, objectID+suffix)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("upload%s still exists after purge: %v", suffix, err)
		}
	}
	jobs, err = s.deliveryStore.list(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "delivered" || jobs[0].PurgedAt == nil || *jobs[0].PurgedAt != now.Unix() {
		t.Fatalf("delivery after purge = %+v", jobs[0])
	}

	request := httptest.NewRequest(http.MethodGet, "/api/v1/receipts/"+objectID, nil)
	response := httptest.NewRecorder()
	s.handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET purged receipt status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestDuePurgeSurvivesDeliveryRestart(t *testing.T) {
	dir := t.TempDir()
	uploadsDir := filepath.Join(dir, "uploads")
	if err := os.MkdirAll(uploadsDir, 0o750); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", ".info"} {
		if err := os.WriteFile(filepath.Join(uploadsDir, "object"+suffix), []byte("data"), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	store, err := openDeliveryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	job := deliveryJob{ObjectID: "object", Project: "test", Filename: "artifact", AcceptedAt: 10}
	if err := store.addAccepted(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.claim(t.Context(), 10); err != nil || !ok {
		t.Fatalf("claim = %v, %v", ok, err)
	}
	if err := store.markDelivered(t.Context(), "object", deliveryResult{RemoteID: "item", RemoteName: "artifact"}, 20, 21); err != nil {
		t.Fatal(err)
	}
	if err := store.close(); err != nil {
		t.Fatal(err)
	}

	store, err = openDeliveryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	worker := newDeliveryWorker(runtimeConfig{config: config{DataDir: dir}}, store)
	worker.now = func() time.Time { return time.Unix(21, 0) }
	if worked, err := worker.runPurgeCycle(t.Context()); err != nil || !worked {
		t.Fatalf("purge after restart = %v, %v", worked, err)
	}
	for _, suffix := range []string{"", ".info"} {
		if _, err := os.Stat(filepath.Join(uploadsDir, "object"+suffix)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("upload%s still exists after restart purge: %v", suffix, err)
		}
	}
}

func TestReceiverIndexesAcceptedArtifactWithoutReconciliation(t *testing.T) {
	cfg := testConfig(t)
	project := cfg.Projects["test"]
	project.Delivery = &deliveryConfig{Sink: "internet_archive", CredentialsFile: "unused", Identifier: "unused"}
	cfg.Projects["test"] = project
	s, err := newServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.deliveryStore.close()
	s.now = func() time.Time { return time.Unix(123456789, 0) }
	acceptTestArtifact(t, s, []byte("artifact"))

	jobs, err := s.deliveryStore.list(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].State != "pending" || jobs[0].Project != "test" {
		t.Fatalf("deliveries = %+v", jobs)
	}
}

func TestDeliveryFailureIsPersistedForRetry(t *testing.T) {
	s := testServer(t)
	acceptTestArtifact(t, s, []byte("artifact"))
	project := s.cfg.Projects["test"]
	project.Delivery = &deliveryConfig{
		Sink: "internet_archive", CredentialsFile: "unused", Identifier: "unused", RemoteName: "unused",
	}
	s.cfg.Projects["test"] = project
	store, err := openDeliveryStore(s.cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	worker := newDeliveryWorker(s.cfg, store)
	now := time.Unix(123456800, 0)
	worker.now = func() time.Time { return now }
	wantErr := errors.New("sink unavailable")
	worker.newSink = func(string, deliveryConfig) (artifactSink, error) {
		return &fakeArtifactSink{err: wantErr}, nil
	}
	if err := worker.discoverAccepted(t.Context()); err != nil {
		t.Fatal(err)
	}
	if worked, err := worker.runCycle(t.Context()); err != nil || !worked {
		t.Fatalf("runCycle = %v, %v", worked, err)
	}
	jobs, err := store.list(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].State != "retry_wait" || jobs[0].Attempts != 1 || jobs[0].NextAttempt != now.Add(time.Minute).Unix() || jobs[0].LastError == nil || *jobs[0].LastError != wantErr.Error() {
		t.Fatalf("deliveries = %+v", jobs)
	}
	if _, err := s.readReceipt(jobs[0].ObjectID); err != nil {
		t.Fatalf("receipt changed after delivery failure: %v", err)
	}
}

func TestDeliveryStoreRecoversInterruptedAttempt(t *testing.T) {
	store, err := openDeliveryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	job := deliveryJob{ObjectID: "object", Project: "test", Filename: "artifact", AcceptedAt: 10}
	if err := store.addAccepted(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.claim(t.Context(), 10); err != nil || !ok {
		t.Fatalf("claim = %v, %v", ok, err)
	}
	if err := store.recoverInterrupted(t.Context(), 20); err != nil {
		t.Fatal(err)
	}
	jobs, err := store.list(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].State != "retry_wait" || jobs[0].NextAttempt != 20 {
		t.Fatalf("deliveries = %+v", jobs)
	}
}

func TestDeliveryTemplateIsDeterministic(t *testing.T) {
	values := map[string]string{
		"{{PROJECT}}": "project", "{{OBJECT_ID}}": "object", "{{FILENAME}}": "file.warc.gz", "{{DATE}}": "20260723010203",
	}
	got := resolveDeliveryTemplate("{{PROJECT}}-{{DATE}}-{{OBJECT_ID}}/{{FILENAME}}", values)
	if got != "project-20260723010203-object/file.warc.gz" {
		t.Fatalf("resolved template = %q", got)
	}
	values["{{FILENAME}}"] = "literal-{{DATE}}"
	if got := resolveDeliveryTemplate("{{FILENAME}}", values); got != "literal-{{DATE}}" {
		t.Fatalf("template recursively expanded replacement to %q", got)
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
	if got := safeFilename("artifact.warc.gz", "object"); got != "artifact.warc.gz" {
		t.Fatalf("safe filename changed to %q", got)
	}
}

func TestInternetArchiveSinkRejectsUnsafeRemoteName(t *testing.T) {
	sink := &internetArchiveSink{
		project: "test",
		cfg:     deliveryConfig{Identifier: "valid-identifier", RemoteName: "../artifact"},
	}
	if _, err := sink.deliver(t.Context(), deliveryJob{ObjectID: "object", Filename: "artifact", AcceptedAt: 1}, "/unused"); err == nil {
		t.Fatal("unsafe remote name was accepted")
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
	if matches, err := filepath.Glob(filepath.Join(s.receiptDir, "*.json")); err != nil || len(matches) != 1 {
		t.Fatalf("receipts = %v, %v", matches, err)
	}
}
