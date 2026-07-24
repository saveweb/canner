package main

import (
	"fmt"
	"os"
	"path/filepath"
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
"projects":{"test":{"delivery":{
"sink":"internet_archive","credentials_file":"unused","identifier":"test"%s
}}}}`, t.TempDir(), retentionField)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
