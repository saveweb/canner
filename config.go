package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
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
	Projects       map[string]projectConfig `json:"projects"`
}

type projectConfig struct {
	TokenSHA256 string          `json:"token_sha256"`
	Delivery    *deliveryConfig `json:"delivery,omitempty"`
}

type deliveryConfig struct {
	Sink            string              `json:"sink"`
	CredentialsFile string              `json:"credentials_file"`
	Identifier      string              `json:"identifier"`
	RemoteName      string              `json:"remote_name,omitempty"`
	Metadata        map[string][]string `json:"metadata"`
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
	if cfg.ListenAddr == "" || cfg.DataDir == "" || cfg.MaxUploadBytes < 1 {
		return runtimeConfig{}, fmt.Errorf("listen_addr, data_dir, and positive max_upload_bytes are required")
	}
	issuerURL, err := url.Parse(cfg.Issuer)
	if err != nil || issuerURL.Scheme == "" || issuerURL.Host == "" {
		return runtimeConfig{}, fmt.Errorf("issuer must be an absolute URL")
	}
	if len(cfg.Projects) == 0 {
		return runtimeConfig{}, fmt.Errorf("at least one project is required")
	}
	for project, projectCfg := range cfg.Projects {
		if !identifierPattern.MatchString(project) || !digestPattern.MatchString(projectCfg.TokenSHA256) {
			return runtimeConfig{}, fmt.Errorf("project %q has an invalid id or token_sha256", project)
		}
		if delivery := projectCfg.Delivery; delivery != nil {
			if delivery.Sink != "internet_archive" || delivery.CredentialsFile == "" || delivery.Identifier == "" {
				return runtimeConfig{}, fmt.Errorf("project %q delivery requires sink internet_archive, credentials_file, and identifier", project)
			}
			if delivery.RemoteName == "" {
				delivery.RemoteName = "{{FILENAME}}"
			}
		}
	}
	return runtimeConfig{config: cfg}, nil
}

func (cfg runtimeConfig) authenticate(token string) (string, bool) {
	sum := sha256.Sum256([]byte(token))
	for project, projectCfg := range cfg.Projects {
		expected, _ := hex.DecodeString(projectCfg.TokenSHA256)
		if subtle.ConstantTimeCompare(sum[:], expected) == 1 {
			return project, true
		}
	}
	return "", false
}
