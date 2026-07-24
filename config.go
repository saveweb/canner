package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"time"
)

var (
	identifierPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	digestPattern     = regexp.MustCompile(`^[0-9a-f]{64}$`)
	checksumPattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,31}:([0-9a-f]{2}){1,64}$`)
)

type config struct {
	ListenAddr     string                   `json:"listen_addr"`
	Issuer         string                   `json:"issuer"`
	DataDir        string                   `json:"data_dir"`
	MaxUploadBytes int64                    `json:"max_upload_bytes"`
	MinFreeBytes   uint64                   `json:"min_free_bytes"`
	Projects       map[string]projectConfig `json:"projects"`
}

type projectConfig struct {
	Delivery *deliveryConfig `json:"delivery,omitempty"`
}

type deliveryConfig struct {
	Sink                   string              `json:"sink"`
	CredentialsFile        string              `json:"credentials_file"`
	Identifier             string              `json:"identifier"`
	RemoteName             string              `json:"remote_name,omitempty"`
	LocalArtifactRetention string              `json:"local_artifact_retention"`
	Metadata               map[string][]string `json:"metadata"`
	localArtifactRetention time.Duration
}

type runtimeConfig struct {
	config
}

func (cfg runtimeConfig) hasDelivery() bool {
	for _, project := range cfg.Projects {
		if project.Delivery != nil {
			return true
		}
	}
	return false
}

func loadConfig(path string) (runtimeConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return runtimeConfig{}, fmt.Errorf("read config: %w", err)
	}
	var cfg config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return runtimeConfig{}, fmt.Errorf("parse config: %w", err)
	}
	if cfg.ListenAddr == "" || cfg.DataDir == "" || cfg.MaxUploadBytes < 1 || cfg.MinFreeBytes < 1 {
		return runtimeConfig{}, fmt.Errorf("listen_addr, data_dir, positive max_upload_bytes, and positive min_free_bytes are required")
	}
	issuerURL, err := url.Parse(cfg.Issuer)
	if err != nil || issuerURL.Scheme == "" || issuerURL.Host == "" {
		return runtimeConfig{}, fmt.Errorf("issuer must be an absolute URL")
	}
	if len(cfg.Projects) == 0 {
		return runtimeConfig{}, fmt.Errorf("at least one project is required")
	}
	for project, projectCfg := range cfg.Projects {
		if !identifierPattern.MatchString(project) {
			return runtimeConfig{}, fmt.Errorf("project %q has an invalid id", project)
		}
		if delivery := projectCfg.Delivery; delivery != nil {
			if delivery.Sink != "internet_archive" || delivery.CredentialsFile == "" || delivery.Identifier == "" || delivery.LocalArtifactRetention == "" {
				return runtimeConfig{}, fmt.Errorf("project %q delivery requires sink internet_archive, credentials_file, identifier, and local_artifact_retention", project)
			}
			if delivery.RemoteName == "" {
				delivery.RemoteName = "{{FILENAME}}"
			}
			retention, err := time.ParseDuration(delivery.LocalArtifactRetention)
			if err != nil || retention < time.Second {
				return runtimeConfig{}, fmt.Errorf("project %q local_artifact_retention must be a duration of at least 1s", project)
			}
			delivery.localArtifactRetention = retention
		}
	}
	return runtimeConfig{config: cfg}, nil
}
