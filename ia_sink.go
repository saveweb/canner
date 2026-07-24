package main

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/saveweb/go2internetarchive/pkg/upload"
	"github.com/saveweb/go2internetarchive/pkg/utils"
)

const deliveryAttemptTimeout = 24 * time.Hour

type internetArchiveSink struct {
	project   string
	cfg       deliveryConfig
	accessKey string
	secretKey string
}

func newInternetArchiveSink(project string, cfg deliveryConfig) (artifactSink, error) {
	accessKey, secretKey, err := utils.ReadKeysFromFile(cfg.CredentialsFile)
	if err != nil {
		return nil, fmt.Errorf("read Internet Archive credentials: %w", err)
	}
	return &internetArchiveSink{project: project, cfg: cfg, accessKey: accessKey, secretKey: secretKey}, nil
}

func (s *internetArchiveSink) deliver(ctx context.Context, job deliveryJob, localPath string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	values := map[string]string{
		"{{PROJECT}}":   s.project,
		"{{OBJECT_ID}}": job.ObjectID,
		"{{FILENAME}}":  job.Filename,
		"{{DATE}}":      time.Unix(job.AcceptedAt, 0).UTC().Format("20060102150405"),
	}
	identifier := resolveDeliveryTemplate(s.cfg.Identifier, values)
	remoteName := resolveDeliveryTemplate(s.cfg.RemoteName, values)
	if remoteName == "" || remoteName == "." || strings.Contains(remoteName, `\`) || strings.HasPrefix(remoteName, "/") || strings.HasSuffix(remoteName, "/") || path.Clean(remoteName) != remoteName || strings.HasPrefix(remoteName, "../") {
		return "", fmt.Errorf("resolved remote name %q is invalid", remoteName)
	}
	metadata := make(map[string][]string, len(s.cfg.Metadata))
	for key, input := range s.cfg.Metadata {
		output := make([]string, len(input))
		for i, value := range input {
			output[i] = resolveDeliveryTemplate(value, values)
		}
		metadata[key] = output
	}
	files := map[string]string{remoteName: localPath}
	uploadCtx, cancel := context.WithTimeout(ctx, deliveryAttemptTimeout)
	defer cancel()
	client := &http.Client{Timeout: deliveryAttemptTimeout}
	if err := uploadToInternetArchive(uploadCtx, client, identifier, files, metadata, s.accessKey, s.secretKey); err != nil {
		return "", err
	}
	return identifier, nil
}

func uploadToInternetArchive(ctx context.Context, client *http.Client, identifier string, files map[string]string, metadata map[string][]string, accessKey, secretKey string) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("Internet Archive uploader panicked: %v", value)
		}
	}()
	return upload.UploadContext(ctx, client, identifier, files, metadata, accessKey, secretKey)
}

func resolveDeliveryTemplate(value string, replacements map[string]string) string {
	return strings.NewReplacer(
		"{{PROJECT}}", replacements["{{PROJECT}}"],
		"{{OBJECT_ID}}", replacements["{{OBJECT_ID}}"],
		"{{FILENAME}}", replacements["{{FILENAME}}"],
		"{{DATE}}", replacements["{{DATE}}"],
	).Replace(value)
}
