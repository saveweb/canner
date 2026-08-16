package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/saveweb/go2internetarchive/pkg/upload"
)

func TestDashboardProjectStatus(t *testing.T) {
	s := testServer(t)
	s.cfg.Projects["test"] = projectConfig{
		Packaging: packagingConfig{Type: "mergewarc", TriggerBytes: 1000, TargetPackageBytes: 500, maxWait: time.Hour},
	}
	if err := s.deliveryStore.addArtifact(t.Context(), artifactRecord{ObjectID: "pending", Project: "test", Filename: "pending.warc.zst", Checksum: "blake3:00", SizeBytes: 400, AcceptedAt: s.now().Add(-30 * time.Minute).Unix()}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.deliveryStore.db.Exec(`INSERT INTO packages(package_id,project,packager,filename,manifest_filename,state,member_count,build_attempts,next_build_at,build_error,updated_at,created_at) VALUES('building-package-id','test','mergewarc','package','manifest','building',1,2,?,'no space left on device',1,1)`, s.now().Add(100*time.Second).Unix()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.deliveryStore.db.Exec(`INSERT INTO packages(package_id,project,packager,filename,manifest_filename,state,member_count,next_build_at,updated_at,created_at) VALUES('active-package-id','test','mergewarc','active','active-manifest','building',1,0,1,2),('blocked-package-id','test','mergewarc','blocked','blocked-manifest','blocked',1,0,1,3)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.deliveryStore.db.Exec(`INSERT INTO deliveries(package_id,sink_id,state,plan,next_attempt_at,progress,updated_at) VALUES('building-package-id','internet_archive','delivering','{}',0,'{"BytesUploaded":400,"TotalBytes":1000,"BytesPerSecond":200,"FilesUploaded":1,"TotalFiles":2,"CurrentFile":"package.warc.zst"}',1)`); err != nil {
		t.Fatal(err)
	}

	view, err := s.dashboardView(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Projects) != 1 {
		t.Fatalf("projects = %+v", view.Projects)
	}
	project := view.Projects[0]
	if project.PendingArtifacts != 1 || project.PendingBytes != 400 || project.BytesUntilTrigger != 600 || project.TriggerPercent != 40 || project.SecondsUntilMaxWait != 1800 {
		t.Fatalf("backlog = %+v", project)
	}
	if project.BuildingPackages != 1 || project.RetryingPackages != 1 || project.BlockedPackages != 1 || project.DeliveriesActive != 1 || view.BuildingCount != 1 || view.DeliveringCount != 1 {
		t.Fatalf("activity = project %+v, view %+v", project, view)
	}
	if len(project.PackageBuildErrors) != 1 || project.PackageBuildErrors[0].PackageID != "building-pac" || project.PackageBuildErrors[0].Message != "no space left on device" || project.PackageBuildErrors[0].RetryIn != 100 || project.NextPackageRetryIn != 100 {
		t.Fatalf("package errors = %+v", project)
	}
	if !project.DeliveryProgress || project.DeliveryBytes != 400 || project.DeliveryTotalBytes != 1000 || project.DeliveryBytesPerSec != 200 || project.DeliveryFiles != 1 || project.DeliveryTotalFiles != 2 || project.DeliveryCurrentFile != "package.warc.zst" || project.DeliveryPercent != 40 {
		t.Fatalf("delivery progress = %+v", project)
	}
	response := httptest.NewRecorder()
	if err := dashboardStatusTemplate.Execute(response, view); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"packages building", "1 building", "1 waiting to retry", "retry in 1m40s", "1 blocked", "1 package errors", "building-pac", "no space left on device", "40", "400 B / 1000 B", "200 B/s", "1 / 2 files", "package.warc.zst"} {
		if !strings.Contains(response.Body.String(), want) {
			t.Errorf("dashboard does not contain %q: %s", want, response.Body.String())
		}
	}
}

func TestDeliveryProgressLifecycle(t *testing.T) {
	store, err := openDeliveryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if _, err := store.db.Exec(`INSERT INTO packages(package_id,project,packager,filename,manifest_filename,state,member_count,next_build_at,updated_at,created_at) VALUES('package','test','identity','package','manifest','sealed',1,0,1,1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO deliveries(package_id,sink_id,state,plan,next_attempt_at,updated_at) VALUES('package','internet_archive','delivering','{}',0,1)`); err != nil {
		t.Fatal(err)
	}
	snapshot := upload.Progress{BytesUploaded: 25, TotalBytes: 100, BytesPerSecond: 10, FilesUploaded: 1, TotalFiles: 2}
	if err := store.markPackageDeliveryProgress(t.Context(), "package", "internet_archive", snapshot, 2); err != nil {
		t.Fatal(err)
	}
	var raw *string
	if err := store.db.QueryRow(`SELECT progress FROM deliveries WHERE package_id='package'`).Scan(&raw); err != nil || raw == nil {
		t.Fatalf("stored progress = %v, %v", raw, err)
	}
	if err := store.markPackageRetry(t.Context(), "package", "internet_archive", "failed", 60, 3); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT progress FROM deliveries WHERE package_id='package'`).Scan(&raw); err != nil || raw != nil {
		t.Fatalf("progress after retry = %v, %v", raw, err)
	}
}

func TestDashboardRoutes(t *testing.T) {
	s := testServer(t)
	for _, path := range []string{"/", "/dashboard/status"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		s.handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s = %d, body = %s", path, response.Code, response.Body.String())
		}
		if !strings.Contains(response.Header().Get("Content-Type"), "text/html") {
			t.Fatalf("GET %s content type = %q", path, response.Header().Get("Content-Type"))
		}
		if path == "/" && !strings.Contains(response.Body.String(), `hx-get="/dashboard/status"`) {
			t.Fatal("dashboard page does not load the status fragment")
		}
		if path == "/dashboard/status" && !strings.Contains(response.Body.String(), "test") {
			t.Fatal("dashboard status does not contain the configured project")
		}
	}
}
