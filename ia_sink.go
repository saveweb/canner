package main

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/saveweb/go2internetarchive/pkg/upload"
	"github.com/saveweb/go2internetarchive/pkg/utils"
)

const deliveryAttemptTimeout = 24 * time.Hour

func deliverPackageToInternetArchive(ctx context.Context, plan packageDeliveryPlan, dataDir string, progress chan<- upload.Progress) error {
	if plan.Version != 1 || plan.Sink != "internet_archive" || plan.Identifier == "" || len(plan.Files) == 0 || plan.RetentionNanos < int64(time.Second) {
		return fmt.Errorf("invalid package delivery plan")
	}
	accessKey, secretKey, err := utils.ReadKeysFromFile(plan.CredentialsFile)
	if err != nil {
		return fmt.Errorf("read Internet Archive credentials: %w", err)
	}
	files := make(map[string]string, len(plan.Files))
	for remoteName, relativePath := range plan.Files {
		if err := validateRemoteName(remoteName); err != nil {
			return err
		}
		base := filepath.Base(relativePath)
		if base != relativePath {
			return fmt.Errorf("invalid package delivery source %q", relativePath)
		}
		dir := "packages"
		if strings.HasSuffix(relativePath, ".manifest.jsonl") {
			dir = "package-manifests"
		}
		files[remoteName] = filepath.Join(dataDir, dir, relativePath)
	}
	uploadCtx, cancel := context.WithTimeout(ctx, deliveryAttemptTimeout)
	defer cancel()
	client := &http.Client{Timeout: deliveryAttemptTimeout}
	return uploadToInternetArchive(uploadCtx, client, plan.Identifier, files, plan.Metadata, accessKey, secretKey, progress)
}

func uploadToInternetArchive(ctx context.Context, client *http.Client, identifier string, files map[string]string, metadata map[string][]string, accessKey, secretKey string, progress chan<- upload.Progress) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("Internet Archive uploader panicked: %v", value)
		}
	}()
	return upload.UploadContextWithProgress(ctx, client, identifier, files, metadata, accessKey, secretKey, progress)
}
