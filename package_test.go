package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/saveweb/go2internetarchive/pkg/upload"
	"github.com/saveweb/mergewarc"
)

func TestPackagingConfig(t *testing.T) {
	path := writePackagingTestConfig(t, 1024, 512, "24h")
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	packaging := cfg.Projects["test"].Packaging
	if packaging.TriggerBytes != 1024 || packaging.TargetPackageBytes != 512 || packaging.maxWait != 24*time.Hour {
		t.Fatalf("packaging = %+v", packaging)
	}
	for _, test := range []struct {
		trigger int64
		target  int64
		wait    string
	}{
		{511, 512, "24h"}, {1024, 0, "24h"}, {1024, 512, "500ms"},
	} {
		if _, err := loadConfig(writePackagingTestConfig(t, test.trigger, test.target, test.wait)); err == nil {
			t.Fatalf("accepted packaging trigger=%d target=%d wait=%q", test.trigger, test.target, test.wait)
		}
	}
}

func TestIdentityPackagingConfigRequiresNoThresholds(t *testing.T) {
	raw := `{
"listen_addr":":8080","issuer":"https://canner.example","data_dir":"` + t.TempDir() + `",
"max_upload_bytes":1024,"min_free_bytes":1,
"projects":{"test":{"packaging":{"type":"identity"},"delivery":{
"sink":"internet_archive","credentials_file":"unused","identifier":"{{PACKAGE_ID}}",
"remote_name":"{{PACKAGE_FILENAME}}","local_artifact_retention":"1h"}}}}`
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err != nil {
		t.Fatal(err)
	}
}

func TestPackageDeliveryPlanUsesShortPackageID(t *testing.T) {
	packageID := strings.Repeat("a", 64)
	plan, err := makePackageDeliveryPlan("sinavideo", packageID, "package.warc.zst", "package.manifest.jsonl", time.Date(2026, 8, 12, 0, 50, 2, 0, time.UTC), deliveryConfig{
		Sink: "internet_archive", Identifier: "saveweb_sinavideo_{{DATE}}_{{PACKAGE_ID_SHORT}}", RemoteName: "{{PACKAGE_FILENAME}}",
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := "saveweb_sinavideo_20260812005002_" + packageID[:24]; plan.Identifier != want {
		t.Fatalf("identifier = %q, want %q", plan.Identifier, want)
	}
}

func TestPackageDeliveryPlanRejectsInvalidIAIdentifier(t *testing.T) {
	for _, identifier := range []string{"four", "-starts-with-dash", "contains:colon", strings.Repeat("a", 101)} {
		_, err := makePackageDeliveryPlan("test", strings.Repeat("a", 64), "package", "manifest", time.Unix(0, 0), deliveryConfig{
			Sink: "internet_archive", Identifier: identifier, RemoteName: "{{PACKAGE_FILENAME}}",
		})
		if err == nil {
			t.Fatalf("accepted invalid IA identifier %q", identifier)
		}
	}
}

func TestIdentityPackageUsesOriginalArtifactBytes(t *testing.T) {
	cfg := testConfig(t)
	project := cfg.Projects["test"]
	project.Packaging = packagingConfig{Type: "identity"}
	project.Delivery = deliveryConfig{
		Sink: "internet_archive", CredentialsFile: "unused", Identifier: "identity-{{PACKAGE_ID}}",
		RemoteName: "{{PACKAGE_FILENAME}}", localArtifactRetention: time.Hour,
	}
	cfg.Projects["test"] = project
	s, err := newServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.deliveryStore.close()
	s.now = func() time.Time { return time.Unix(50, 0) }
	body := []byte("an arbitrary non-WARC artifact")
	acceptTestArtifact(t, s, body)
	worker := newDeliveryWorker(cfg, s.deliveryStore)
	worker.now = s.now
	if worked, err := worker.runPackageBuildCycle(t.Context()); err != nil || !worked {
		t.Fatalf("identity build = %v, %v", worked, err)
	}
	packages, err := s.deliveryStore.listPackages(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(packages) != 1 || packages[0].Packager != "identity" || packages[0].State != "sealed" || packages[0].MemberCount != 1 {
		t.Fatalf("packages = %+v", packages)
	}
	pkg := packages[0]
	members, err := s.deliveryStore.packageMembers(t.Context(), pkg.PackageID)
	if err != nil || len(members) != 1 {
		t.Fatalf("members = %+v, %v", members, err)
	}
	sourcePath := filepath.Join(cfg.DataDir, "uploads", members[0].ObjectID)
	packagePath := filepath.Join(cfg.DataDir, "packages", pkg.Filename)
	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	packageInfo, err := os.Stat(packagePath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(sourceInfo, packageInfo) {
		t.Fatal("identity package copied bytes instead of using the original artifact inode")
	}
	manifestRaw, err := os.ReadFile(filepath.Join(cfg.DataDir, "package-manifests", pkg.ManifestFilename))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(manifestRaw, []byte(`"checksums":`)) || bytes.Contains(manifestRaw, []byte(`"checksum":`)) {
		t.Fatalf("identity manifest uses obsolete checksum schema: %s", manifestRaw)
	}
	if worked, err := worker.runPackagedSourcePurgeCycle(t.Context()); err != nil || !worked {
		t.Fatalf("identity source purge = %v, %v", worked, err)
	}
	if _, err := os.Stat(sourcePath); !os.IsNotExist(err) {
		t.Fatalf("identity source still exists: %v", err)
	}
	got, err := os.ReadFile(packagePath)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("identity package bytes = %q, %v", got, err)
	}
}

func TestPackageDeliveryFailureAndRestartAreRetryable(t *testing.T) {
	dir := t.TempDir()
	store, err := openDeliveryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	plan, err := json.Marshal(packageDeliveryPlan{
		Version: 1, Sink: "internet_archive", CredentialsFile: "unused", Identifier: "item",
		Files: map[string]string{"package": "package"}, RetentionNanos: int64(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `INSERT INTO packages(package_id,project,packager,filename,manifest_filename,state,size_bytes,checksum,manifest_checksum,member_count,next_build_at,updated_at,created_at,sealed_at) VALUES('package','test','identity','package','package.manifest.jsonl','sealed',1,'blake3:00','blake3:00',1,0,100,100,100)`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `INSERT INTO deliveries(package_id,sink_id,state,plan,next_attempt_at,updated_at) VALUES('package','internet_archive','pending',?,100,100)`, string(plan)); err != nil {
		t.Fatal(err)
	}
	worker := newDeliveryWorker(runtimeConfig{config: config{DataDir: dir}}, store)
	worker.now = func() time.Time { return time.Unix(100, 0) }
	wantErr := errors.New("sink unavailable")
	worker.deliverPackage = func(context.Context, packageDeliveryPlan, string, chan<- upload.Progress) error { return wantErr }
	if worked, err := worker.runPackageDeliveryCycle(t.Context()); err != nil || !worked {
		t.Fatalf("delivery failure = %v, %v", worked, err)
	}
	deliveries, err := store.listDeliveries(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 1 || deliveries[0].State != "retry_wait" || deliveries[0].Attempts != 1 || deliveries[0].NextAttempt != 160 || deliveries[0].LastError == nil || *deliveries[0].LastError != wantErr.Error() {
		t.Fatalf("delivery after failure = %+v", deliveries)
	}
	if _, err := store.db.ExecContext(t.Context(), `UPDATE deliveries SET state='delivering' WHERE package_id='package'`); err != nil {
		t.Fatal(err)
	}
	if err := store.recoverInterruptedDeliveries(t.Context(), 200); err != nil {
		t.Fatal(err)
	}
	deliveries, err = store.listDeliveries(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if deliveries[0].State != "retry_wait" || deliveries[0].NextAttempt != 200 {
		t.Fatalf("delivery after restart = %+v", deliveries[0])
	}
}

func TestClaimPackageDeliveryPrefersPendingOverRetry(t *testing.T) {
	store, err := openDeliveryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	for _, delivery := range []struct {
		packageID string
		state     string
		createdAt int
	}{
		{packageID: "old-retry", state: "retry_wait", createdAt: 1},
		{packageID: "new-pending", state: "pending", createdAt: 2},
	} {
		if _, err := store.db.ExecContext(t.Context(), `INSERT INTO packages(package_id,project,packager,filename,manifest_filename,state,size_bytes,checksum,manifest_checksum,member_count,next_build_at,updated_at,created_at,sealed_at) VALUES(?,'test','identity',?,?,'sealed',1,'blake3:00','blake3:00',1,0,?,?,?)`, delivery.packageID, delivery.packageID, delivery.packageID+".manifest", delivery.createdAt, delivery.createdAt, delivery.createdAt); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.ExecContext(t.Context(), `INSERT INTO deliveries(package_id,sink_id,state,plan,next_attempt_at,updated_at) VALUES(?,'internet_archive',?, '{}',0,?)`, delivery.packageID, delivery.state, delivery.createdAt); err != nil {
			t.Fatal(err)
		}
	}

	delivery, ok, err := store.claimPackageDelivery(t.Context(), 100)
	if err != nil || !ok {
		t.Fatalf("claim delivery = %+v, %v, %v", delivery, ok, err)
	}
	if delivery.PackageID != "new-pending" {
		t.Fatalf("claimed %q before fresh pending delivery", delivery.PackageID)
	}
}

func TestPackageDeliveryPersistsProgressWhileRunning(t *testing.T) {
	dir := t.TempDir()
	store, err := openDeliveryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	plan, err := json.Marshal(packageDeliveryPlan{
		Version: 1, Sink: "internet_archive", CredentialsFile: "unused", Identifier: "item",
		Files: map[string]string{"package": "package"}, RetentionNanos: int64(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `INSERT INTO packages(package_id,project,packager,filename,manifest_filename,state,size_bytes,checksum,manifest_checksum,member_count,next_build_at,updated_at,created_at,sealed_at) VALUES('package','test','identity','package','manifest','sealed',100,'blake3:00','blake3:00',1,0,100,100,100)`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `INSERT INTO deliveries(package_id,sink_id,state,plan,next_attempt_at,updated_at) VALUES('package','internet_archive','pending',?,100,100)`, string(plan)); err != nil {
		t.Fatal(err)
	}
	worker := newDeliveryWorker(runtimeConfig{config: config{DataDir: dir}}, store)
	worker.now = func() time.Time { return time.Unix(100, 0) }
	progressRecorded := make(chan struct{})
	finish := make(chan struct{})
	worker.deliverPackage = func(_ context.Context, _ packageDeliveryPlan, _ string, progress chan<- upload.Progress) error {
		progress <- upload.Progress{BytesUploaded: 40, TotalBytes: 100, BytesPerSecond: 20, CurrentFile: "package"}
		close(progressRecorded)
		<-finish
		return errors.New("stop after snapshot")
	}
	result := make(chan error, 1)
	go func() {
		_, err := worker.runPackageDeliveryCycle(t.Context())
		result <- err
	}()
	<-progressRecorded
	var raw *string
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if err := store.db.QueryRow(`SELECT progress FROM deliveries WHERE package_id='package'`).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if raw != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if raw == nil || !strings.Contains(*raw, `"BytesUploaded":40`) {
		t.Fatalf("progress while running = %v", raw)
	}
	close(finish)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT progress FROM deliveries WHERE package_id='package'`).Scan(&raw); err != nil || raw != nil {
		t.Fatalf("progress after completion = %v, %v", raw, err)
	}
}

func TestDeliverySchedulerBoundsConcurrencyAndRecoversCancellation(t *testing.T) {
	dir := t.TempDir()
	store, err := openDeliveryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	for i, identifier := range []string{"item-a", "item-b", "item-c"} {
		packageID := "package-" + identifier
		plan, err := json.Marshal(packageDeliveryPlan{
			Version: 1, Sink: "internet_archive", CredentialsFile: "unused", Identifier: identifier,
			Files: map[string]string{packageID: packageID}, RetentionNanos: int64(time.Hour),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.ExecContext(t.Context(), `INSERT INTO packages(package_id,project,packager,filename,manifest_filename,state,size_bytes,checksum,manifest_checksum,member_count,next_build_at,updated_at,created_at,sealed_at) VALUES(?, 'test','identity',?,?,'sealed',1,'blake3:00','blake3:00',1,0,?,?,?)`, packageID, packageID, packageID+".manifest", 100+i, 100+i, 100+i); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.ExecContext(t.Context(), `INSERT INTO deliveries(package_id,sink_id,state,plan,next_attempt_at,updated_at) VALUES(?,'internet_archive','pending',?,100,100)`, packageID, string(plan)); err != nil {
			t.Fatal(err)
		}
	}

	cfg := runtimeConfig{config: config{DataDir: dir, DeliveryConcurrency: 2}}
	worker := newDeliveryWorker(cfg, store)
	worker.now = func() time.Time { return time.Unix(100, 0) }
	started := make(chan string, 3)
	release := map[string]chan struct{}{"item-a": make(chan struct{}), "item-b": make(chan struct{}), "item-c": make(chan struct{})}
	worker.deliverPackage = func(ctx context.Context, plan packageDeliveryPlan, _ string, _ chan<- upload.Progress) error {
		started <- plan.Identifier
		select {
		case <-release[plan.Identifier]:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- worker.runScheduler(ctx) }()

	first := waitForStartedDelivery(t, started)
	second := waitForStartedDelivery(t, started)
	select {
	case third := <-started:
		t.Fatalf("started %q while %q and %q were active", third, first, second)
	case <-time.After(50 * time.Millisecond):
	}
	close(release[first])
	third := waitForStartedDelivery(t, started)
	if third == first || third == second {
		t.Fatalf("third delivery = %q after starting %q and %q", third, first, second)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}

	var delivered, interrupted int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM deliveries WHERE state='delivered'`).Scan(&delivered); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM deliveries WHERE state='delivering'`).Scan(&interrupted); err != nil {
		t.Fatal(err)
	}
	if delivered != 1 || interrupted != 2 {
		t.Fatalf("after cancellation: delivered=%d delivering=%d", delivered, interrupted)
	}
	if err := store.recoverInterruptedDeliveries(t.Context(), 200); err != nil {
		t.Fatal(err)
	}
	var retrying int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM deliveries WHERE state='retry_wait' AND next_attempt_at=200`).Scan(&retrying); err != nil {
		t.Fatal(err)
	}
	if retrying != 2 {
		t.Fatalf("recovered deliveries = %d", retrying)
	}
}

func TestDeliverySchedulerBuildsWhileUploadIsActive(t *testing.T) {
	cfg := testConfig(t)
	cfg.DeliveryConcurrency = 1
	project := cfg.Projects["test"]
	project.Packaging = packagingConfig{Type: "identity"}
	project.Delivery = deliveryConfig{
		Sink: "internet_archive", CredentialsFile: "unused", Identifier: "identity-{{PACKAGE_ID}}",
		RemoteName: "{{PACKAGE_FILENAME}}", localArtifactRetention: time.Hour,
	}
	cfg.Projects["test"] = project
	s, err := newServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.deliveryStore.close()
	s.now = func() time.Time { return time.Unix(100, 0) }

	acceptTestArtifact(t, s, []byte("first"))
	worker := newDeliveryWorker(cfg, s.deliveryStore)
	worker.now = s.now
	if worked, err := worker.runPackageBuildCycle(t.Context()); err != nil || !worked {
		t.Fatalf("initial package build = %v, %v", worked, err)
	}

	started := make(chan chan<- upload.Progress, 1)
	worker.deliverPackage = func(ctx context.Context, _ packageDeliveryPlan, _ string, progress chan<- upload.Progress) error {
		started <- progress
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- worker.runScheduler(ctx) }()
	progress := <-started

	acceptTestArtifact(t, s, []byte("second"))
	progress <- upload.Progress{}
	deadline := time.Now().Add(time.Second)
	for {
		var packages int
		if err := s.deliveryStore.db.QueryRow(`SELECT COUNT(*) FROM packages WHERE state='sealed'`).Scan(&packages); err != nil {
			t.Fatal(err)
		}
		if packages == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("new package was not built while an upload was active")
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestDeliverySchedulerPackagesWithoutSinkDeliveryAtZeroConcurrency(t *testing.T) {
	cfg := testConfig(t)
	cfg.DeliveryConcurrency = 0
	project := cfg.Projects["test"]
	project.Packaging = packagingConfig{Type: "identity"}
	project.Delivery = deliveryConfig{
		Sink: "internet_archive", CredentialsFile: "unused", Identifier: "identity-{{PACKAGE_ID}}",
		RemoteName: "{{PACKAGE_FILENAME}}", localArtifactRetention: time.Hour,
	}
	cfg.Projects["test"] = project
	s, err := newServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.deliveryStore.close()
	s.now = func() time.Time { return time.Unix(100, 0) }
	acceptTestArtifact(t, s, []byte("artifact"))

	worker := newDeliveryWorker(cfg, s.deliveryStore)
	worker.now = s.now
	deliveryStarted := make(chan struct{}, 1)
	worker.deliverPackage = func(context.Context, packageDeliveryPlan, string, chan<- upload.Progress) error {
		deliveryStarted <- struct{}{}
		return nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- worker.runScheduler(ctx) }()

	deadline := time.Now().Add(time.Second)
	for {
		var sealed, pending int
		if err := s.deliveryStore.db.QueryRow(`SELECT COUNT(*) FROM packages WHERE state='sealed'`).Scan(&sealed); err != nil {
			t.Fatal(err)
		}
		if err := s.deliveryStore.db.QueryRow(`SELECT COUNT(*) FROM deliveries WHERE state='pending'`).Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if sealed == 1 && pending == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("packaging with delivery disabled: sealed=%d pending=%d", sealed, pending)
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-deliveryStarted:
		t.Fatal("sink delivery started with zero concurrency")
	default:
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func waitForStartedDelivery(t *testing.T, started <-chan string) string {
	t.Helper()
	select {
	case identifier := <-started:
		return identifier
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delivery to start")
		return ""
	}
}

func TestPackagePurgeWaitsForEveryDelivery(t *testing.T) {
	store, err := openDeliveryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if _, err := store.db.ExecContext(t.Context(), `INSERT INTO packages(package_id,project,packager,filename,manifest_filename,state,size_bytes,checksum,manifest_checksum,member_count,next_build_at,updated_at,created_at,sealed_at) VALUES('package','test','identity','package','manifest','sealed',1,'blake3:00','blake3:00',1,0,100,100,100)`); err != nil {
		t.Fatal(err)
	}
	for _, sink := range []string{"archive-a", "archive-b"} {
		if _, err := store.db.ExecContext(t.Context(), `INSERT INTO deliveries(package_id,sink_id,state,plan,next_attempt_at,updated_at) VALUES('package',?,'pending','{}',0,100)`, sink); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.db.ExecContext(t.Context(), `UPDATE deliveries SET state='delivering' WHERE package_id='package' AND sink_id='archive-a'`); err != nil {
		t.Fatal(err)
	}
	if err := store.markPackageDelivered(t.Context(), "package", "archive-a", "remote-a", 101, 110); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.nextPackagePurge(t.Context(), 200); err != nil || ok {
		t.Fatalf("purge with pending delivery = %v, %v", ok, err)
	}
	if _, err := store.db.ExecContext(t.Context(), `UPDATE deliveries SET state='delivering' WHERE package_id='package' AND sink_id='archive-b'`); err != nil {
		t.Fatal(err)
	}
	if err := store.markPackageDelivered(t.Context(), "package", "archive-b", "remote-b", 102, 120); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.nextPackagePurge(t.Context(), 119); err != nil || ok {
		t.Fatalf("early multi-sink purge = %v, %v", ok, err)
	}
	pkg, ok, err := store.nextPackagePurge(t.Context(), 120)
	if err != nil || !ok || pkg.PurgeAfter == nil || *pkg.PurgeAfter != 120 {
		t.Fatalf("due multi-sink purge = %+v, %v, %v", pkg, ok, err)
	}
}

func TestPackageLifecycle(t *testing.T) {
	cfg := testConfig(t)
	project := cfg.Projects["test"]
	project.Packaging = packagingConfig{Type: "mergewarc", TriggerBytes: 1 << 20, TargetPackageBytes: 1 << 20, MaxWait: "1s", maxWait: time.Second}
	project.Delivery = deliveryConfig{
		Sink: "internet_archive", CredentialsFile: "unused", Identifier: "sinavideo-{{PACKAGE_ID}}",
		RemoteName: "{{PACKAGE_FILENAME}}", LocalArtifactRetention: "2s", localArtifactRetention: 2 * time.Second,
	}
	cfg.Projects["test"] = project
	s, err := newServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.deliveryStore.close()
	now := time.Unix(123456800, 0)
	s.now = func() time.Time { return now }
	first := testWARCZstd(t, "00000000-0000-0000-0000-000000000001", "one")
	second := testWARCZstd(t, "00000000-0000-0000-0000-000000000002", "two")
	acceptTestArtifact(t, s, first)
	acceptTestArtifact(t, s, second)

	worker := newDeliveryWorker(cfg, s.deliveryStore)
	worker.now = func() time.Time { return now.Add(time.Second) }
	if worked, err := worker.runPackageBuildCycle(t.Context()); err != nil || !worked {
		t.Fatalf("build cycle = %v, %v", worked, err)
	}
	packages, err := s.deliveryStore.listPackages(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(packages) != 1 || packages[0].State != "sealed" || packages[0].MemberCount != 2 {
		t.Fatalf("packages = %+v", packages)
	}
	pkg := packages[0]
	merged, err := os.ReadFile(filepath.Join(cfg.DataDir, "packages", pkg.Filename))
	if err != nil {
		t.Fatal(err)
	}
	manifestFile, err := os.Open(filepath.Join(cfg.DataDir, "package-manifests", pkg.ManifestFilename))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := mergewarc.ReadJSONL(manifestFile)
	manifestFile.Close()
	if err != nil || len(manifest.Entries) != 2 {
		t.Fatalf("manifest = %+v, %v", manifest, err)
	}
	if len(manifest.Output.Checksums) != 3 {
		t.Fatalf("output checksums = %v", manifest.Output.Checksums)
	}
	blake3Output, err := checksumForAlgorithm(manifest.Output.Checksums, mergewarc.ChecksumBLAKE3)
	if err != nil || pkg.Checksum == nil || *pkg.Checksum != blake3Output {
		t.Fatalf("stored package checksum = %v, manifest = %v, err = %v", pkg.Checksum, manifest.Output.Checksums, err)
	}
	remaining := [][]byte{first, second}
	for _, entry := range manifest.Entries {
		if len(entry.Checksums) != 3 {
			t.Fatalf("entry checksums = %v", entry.Checksums)
		}
		var extracted bytes.Buffer
		if err := mergewarc.Extract(t.Context(), bytes.NewReader(merged), entry, &extracted); err != nil {
			t.Fatal(err)
		}
		matched := -1
		for i, want := range remaining {
			if bytes.Equal(extracted.Bytes(), want) {
				matched = i
				break
			}
		}
		if matched < 0 {
			t.Fatal("manifest entry does not recover an original artifact")
		}
		remaining = append(remaining[:matched], remaining[matched+1:]...)
	}

	for range 2 {
		if worked, err := worker.runPackagedSourcePurgeCycle(t.Context()); err != nil || !worked {
			t.Fatalf("source purge = %v, %v", worked, err)
		}
	}
	for _, entry := range manifest.Entries {
		objectID := entry.Metadata["object_id"]
		if _, err := os.Stat(filepath.Join(cfg.DataDir, "uploads", objectID)); !os.IsNotExist(err) {
			t.Fatalf("source artifact %s still exists: %v", objectID, err)
		}
	}

	var deliveredPlan packageDeliveryPlan
	worker.deliverPackage = func(_ context.Context, plan packageDeliveryPlan, _ string, _ chan<- upload.Progress) error {
		deliveredPlan = plan
		return nil
	}
	// A later configuration edit must not change the already sealed plan.
	changed := worker.cfg.Projects["test"]
	changed.Delivery.Identifier = "changed-{{PACKAGE_ID}}"
	worker.cfg.Projects["test"] = changed
	if worked, err := worker.runPackageDeliveryCycle(t.Context()); err != nil || !worked {
		t.Fatalf("delivery cycle = %v, %v", worked, err)
	}
	if deliveredPlan.Identifier != "sinavideo-"+pkg.PackageID || len(deliveredPlan.Files) != 2 {
		t.Fatalf("delivery plan = %+v", deliveredPlan)
	}
	packages, _ = s.deliveryStore.listPackages(t.Context())
	if packages[0].State != "sealed" || packages[0].PurgeAfter == nil || *packages[0].PurgeAfter != now.Add(3*time.Second).Unix() {
		t.Fatalf("delivered package = %+v", packages[0])
	}
	now = now.Add(2 * time.Second)
	if worked, err := worker.runPackagePurgeCycle(t.Context()); err != nil || !worked {
		t.Fatalf("package purge = %v, %v", worked, err)
	}
	for _, path := range []string{filepath.Join(cfg.DataDir, "packages", pkg.Filename), filepath.Join(cfg.DataDir, "package-manifests", pkg.ManifestFilename)} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("package payload still exists at %s: %v", path, err)
		}
	}
}

func TestPackageWaitTriggerAndTargetBoundary(t *testing.T) {
	store, err := openDeliveryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	for i, size := range []int64{6, 6, 6} {
		id := "object-" + strconv.Itoa(i)
		if err := store.addArtifact(t.Context(), artifactRecord{ObjectID: id, Project: "test", Filename: id + ".warc.zst", Checksum: "blake3:" + string(make([]byte, 64)), SizeBytes: size, AcceptedAt: int64(10 + i)}); err != nil {
			t.Fatal(err)
		}
	}
	packaging := packagingConfig{Type: "mergewarc", TriggerBytes: 100, TargetPackageBytes: 10, maxWait: time.Hour}
	delivery := deliveryConfig{Sink: "internet_archive", CredentialsFile: "keys", Identifier: "{{PACKAGE_ID}}", RemoteName: "{{PACKAGE_FILENAME}}"}
	if _, ok, err := store.reservePackage(t.Context(), "test", packaging, delivery, time.Unix(20, 0)); err != nil || ok {
		t.Fatalf("early reserve = %v, %v", ok, err)
	}
	pkg, ok, err := store.reservePackage(t.Context(), "test", packaging, delivery, time.Unix(3610, 0))
	if err != nil || !ok || pkg.MemberCount != 1 {
		t.Fatalf("aged reserve = %+v, %v, %v", pkg, ok, err)
	}
}

func TestThresholdStartsPersistentDrainingRound(t *testing.T) {
	store, err := openDeliveryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	for i := range 4 {
		id := "drain-" + strconv.Itoa(i)
		if err := store.addArtifact(t.Context(), artifactRecord{ObjectID: id, Project: "test", Filename: id + ".warc.zst", Checksum: "blake3:00", SizeBytes: 6, AcceptedAt: int64(10 + i)}); err != nil {
			t.Fatal(err)
		}
	}
	packaging := packagingConfig{Type: "mergewarc", TriggerBytes: 18, TargetPackageBytes: 6, maxWait: time.Hour}
	delivery := deliveryConfig{Sink: "internet_archive", CredentialsFile: "keys", Identifier: "{{PACKAGE_ID}}", RemoteName: "{{PACKAGE_FILENAME}}"}
	for i := range 4 {
		pkg, ok, err := store.reservePackage(t.Context(), "test", packaging, delivery, time.Unix(20, 0))
		if err != nil || !ok || pkg.MemberCount != 1 {
			t.Fatalf("reserve %d = %+v, %v, %v", i, pkg, ok, err)
		}
	}
	if _, ok, err := store.reservePackage(t.Context(), "test", packaging, delivery, time.Unix(20, 0)); err != nil || ok {
		t.Fatalf("empty draining round = %v, %v", ok, err)
	}
	if err := store.addArtifact(t.Context(), artifactRecord{ObjectID: "tail", Project: "test", Filename: "tail.warc.zst", Checksum: "blake3:00", SizeBytes: 6, AcceptedAt: 21}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.reservePackage(t.Context(), "test", packaging, delivery, time.Unix(22, 0)); err != nil || ok {
		t.Fatalf("new sub-threshold tail = %v, %v", ok, err)
	}
}

func TestInvalidWARCIsIsolatedWithoutBlockingValidArtifacts(t *testing.T) {
	cfg := testConfig(t)
	project := cfg.Projects["test"]
	project.Packaging = packagingConfig{Type: "mergewarc", TriggerBytes: 1 << 20, TargetPackageBytes: 1 << 20, MaxWait: "1s", maxWait: time.Second}
	project.Delivery = deliveryConfig{Sink: "internet_archive", CredentialsFile: "unused", Identifier: "{{PACKAGE_ID}}", RemoteName: "{{PACKAGE_FILENAME}}", localArtifactRetention: time.Hour}
	cfg.Projects["test"] = project
	s, err := newServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.deliveryStore.close()
	s.now = func() time.Time { return time.Unix(100, 0) }
	acceptTestArtifact(t, s, []byte("not a WARC-Zstd file"))
	acceptTestArtifact(t, s, testWARCZstd(t, "00000000-0000-0000-0000-000000000003", "valid"))

	worker := newDeliveryWorker(cfg, s.deliveryStore)
	worker.now = func() time.Time { return s.now().Add(time.Second) }
	if worked, err := worker.runPackageBuildCycle(t.Context()); err != nil || !worked {
		t.Fatalf("invalid build cycle = %v, %v", worked, err)
	}
	packages, err := s.deliveryStore.listPackages(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(packages) != 1 || packages[0].State != "blocked" || packages[0].MemberCount != 1 {
		t.Fatalf("packages after invalid input = %+v", packages)
	}
	var invalid, available int
	rows, err := s.deliveryStore.db.QueryContext(t.Context(), `SELECT packaging_error,package_id FROM artifacts`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var packagingError, packageID *string
		if err := rows.Scan(&packagingError, &packageID); err != nil {
			t.Fatal(err)
		}
		if packagingError != nil {
			invalid++
		} else if packageID == nil {
			available++
		}
	}
	rows.Close()
	if invalid != 1 || available != 1 {
		t.Fatalf("invalid artifacts = %d, available artifacts = %d", invalid, available)
	}
	if worked, err := worker.runPackageBuildCycle(t.Context()); err != nil || !worked {
		t.Fatalf("valid build cycle = %v, %v", worked, err)
	}
	packages, err = s.deliveryStore.listPackages(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var sealed int
	for _, pkg := range packages {
		if pkg.State == "sealed" && pkg.MemberCount == 1 {
			sealed++
		}
	}
	if len(packages) != 2 || sealed != 1 {
		t.Fatalf("packages after valid retry = %+v", packages)
	}
}

func writePackagingTestConfig(t *testing.T, trigger, target int64, wait string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"listen_addr": ":8080", "issuer": "https://canner.example", "data_dir": t.TempDir(),
		"max_upload_bytes": 2048, "min_free_bytes": 1,
		"projects": map[string]any{"test": map[string]any{
			"packaging": map[string]any{"type": "mergewarc", "trigger_bytes": trigger, "target_package_bytes": target, "max_wait": wait},
			"delivery":  map[string]any{"sink": "internet_archive", "credentials_file": "unused", "identifier": "{{PACKAGE_ID}}", "remote_name": "{{PACKAGE_FILENAME}}", "local_artifact_retention": "24h"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func testWARCZstd(t *testing.T, id, block string) []byte {
	t.Helper()
	record := []byte("WARC/1.1\r\n" +
		"WARC-Type: metadata\r\n" +
		"WARC-Record-ID: <urn:uuid:" + id + ">\r\n" +
		"Content-Type: text/plain\r\n" +
		"Content-Length: " + strconv.Itoa(len(block)) + "\r\n\r\n" + block + "\r\n\r\n")
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderCRC(true), zstd.WithSingleSegment(true))
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	return encoder.EncodeAll(record, nil)
}
