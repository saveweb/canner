package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigParsesLocalArtifactRetention(t *testing.T) {
	path := writeTestConfig(t, `"local_artifact_retention":"24h"`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Projects["test"].Delivery.localArtifactRetention; got != 24*time.Hour {
		t.Fatalf("local artifact retention = %s", got)
	}
}

func TestLoadConfigDefaultsDeliveryConcurrencyToDisabled(t *testing.T) {
	cfg, err := loadConfig(writeTestConfig(t, `"local_artifact_retention":"24h"`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DeliveryConcurrency != 0 {
		t.Fatalf("delivery concurrency = %d", cfg.DeliveryConcurrency)
	}
}

func TestLoadConfigDefaultsPartialUploadRetention(t *testing.T) {
	cfg, err := loadConfig(writeTestConfig(t, `"local_artifact_retention":"24h"`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.partialUploadRetention != 30*time.Minute {
		t.Fatalf("partial upload retention = %s", cfg.partialUploadRetention)
	}
}

func TestLoadConfigParsesPartialUploadRetention(t *testing.T) {
	path := writeTestConfig(t, `"local_artifact_retention":"24h"`)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.Replace(string(raw), `"min_free_bytes":1`, `"min_free_bytes":1,"partial_upload_retention":"2h"`, 1))
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.partialUploadRetention != 2*time.Hour {
		t.Fatalf("partial upload retention = %s", cfg.partialUploadRetention)
	}

	for _, value := range []string{"500ms", "later"} {
		invalid := []byte(strings.Replace(string(raw), `"partial_upload_retention":"2h"`, `"partial_upload_retention":"`+value+`"`, 1))
		if err := os.WriteFile(path, invalid, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadConfig(path); err == nil {
			t.Fatalf("loadConfig accepted partial_upload_retention %q", value)
		}
	}
}

func TestLoadConfigParsesDeliveryConcurrency(t *testing.T) {
	path := writeTestConfig(t, `"local_artifact_retention":"24h"`)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.Replace(string(raw), `"min_free_bytes":1`, `"min_free_bytes":1,"delivery_concurrency":3`, 1))
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DeliveryConcurrency != 3 {
		t.Fatalf("delivery concurrency = %d", cfg.DeliveryConcurrency)
	}

	raw = []byte(strings.Replace(string(raw), `"delivery_concurrency":3`, `"delivery_concurrency":-1`, 1))
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil {
		t.Fatal("loadConfig accepted negative delivery_concurrency")
	}
}

func TestLoadConfigRequiresLocalArtifactRetention(t *testing.T) {
	for _, field := range []string{"", `"local_artifact_retention":"500ms"`, `"local_artifact_retention":"later"`} {
		if _, err := loadConfig(writeTestConfig(t, field)); err == nil {
			t.Fatalf("loadConfig accepted retention field %q", field)
		}
	}
}

func writeTestConfig(t *testing.T, retentionField string) string {
	t.Helper()
	if retentionField != "" {
		retentionField = "," + retentionField
	}
	raw := fmt.Sprintf(`{
"listen_addr":":8080",
"issuer":"https://canner.example",
"data_dir":%q,
"max_upload_bytes":1024,
"min_free_bytes":1,
"projects":{"test":{"packaging":{"type":"identity"},"delivery":{
"sink":"internet_archive","credentials_file":"unused","identifier":"test"%s
}}}}`, t.TempDir(), retentionField)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
