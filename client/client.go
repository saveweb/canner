// Package client uploads artifacts to a canner receiver using tus.
package client

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/zeebo/blake3"
)

const (
	// DefaultBaseURL is the public Saveweb Canner receiver.
	DefaultBaseURL = "https://canner.saveweb.org/"

	tusVersion    = "1.0.0"
	receiptHeader = "Artifact-Receipt"
	maxErrorBody  = 64 << 10
)

// Receipt is the durable acceptance receipt returned by canner and accepted by
// SavewebHQ.
type Receipt struct {
	ID         string `json:"id"`
	Issuer     string `json:"issuer"`
	ObjectID   string `json:"object_id"`
	Checksum   string `json:"checksum"`
	SizeBytes  int64  `json:"size_bytes"`
	AcceptedAt int64  `json:"accepted_at"`
}

// Session contains everything a worker must persist to resume an upload after
// restarting. The artifact content must remain unchanged.
type Session struct {
	UploadURL string `json:"upload_url"`
	ObjectID  string `json:"object_id"`
	Project   string `json:"project"`
	Filename  string `json:"filename,omitempty"`
	Checksum  string `json:"checksum"`
	Size      int64  `json:"size_bytes"`
}

// HTTPError describes a non-successful response from canner.
type HTTPError struct {
	StatusCode int
	Body       string
	RetryAfter time.Duration
}

type transportError struct {
	err error
}

func (e *transportError) Error() string { return e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

func (e *HTTPError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("canner HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("canner HTTP %d: %s", e.StatusCode, e.Body)
}

// Client is safe for concurrent use when its exported fields are not mutated.
type Client struct {
	HTTPClient *http.Client
	RetryDelay time.Duration

	baseURL *url.URL
}

// New creates a client for a canner receiver. An empty base URL uses
// DefaultBaseURL.
func New(baseURL string) (*Client, error) {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("canner base URL must be an absolute HTTP URL")
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return &Client{baseURL: parsed, RetryDelay: time.Second}, nil
}

// UploadFile opens path and uploads it using its base name as metadata.
func (c *Client) UploadFile(ctx context.Context, project, path string) (Receipt, error) {
	file, err := os.Open(path)
	if err != nil {
		return Receipt{}, fmt.Errorf("open artifact: %w", err)
	}
	defer file.Close()
	return c.Upload(ctx, project, filepath.Base(path), file)
}

// Upload creates and completes an upload. For recovery across process restarts,
// call Create, persist the returned Session, and then call Resume.
func (c *Client) Upload(ctx context.Context, project, filename string, content io.ReadSeeker) (Receipt, error) {
	session, err := c.Create(ctx, project, filename, content)
	if err != nil {
		return Receipt{}, err
	}
	return c.resume(ctx, session, content, false)
}

// Create hashes the complete content and creates an empty resumable upload.
func (c *Client) Create(ctx context.Context, project, filename string, content io.ReadSeeker) (Session, error) {
	size, checksum, err := inspectContent(content)
	if err != nil {
		return Session{}, err
	}
	metadata := []string{metadataValue("project", project), metadataValue("checksum", checksum)}
	if filename != "" {
		metadata = append(metadata, metadataValue("filename", filename))
	}

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("files/"), nil)
		if err != nil {
			return Session{}, err
		}
		req.Header.Set("Tus-Resumable", tusVersion)
		req.Header.Set("Upload-Length", strconv.FormatInt(size, 10))
		req.Header.Set("Upload-Metadata", strings.Join(metadata, ","))
		response, err := c.httpClient().Do(req)
		if err != nil {
			return Session{}, fmt.Errorf("create upload: %w", err)
		}
		if response.StatusCode == http.StatusCreated {
			location := response.Header.Get("Location")
			response.Body.Close()
			uploadURL, objectID, err := resolveUploadLocation(c.baseURL, location)
			if err != nil {
				return Session{}, err
			}
			return Session{UploadURL: uploadURL, ObjectID: objectID, Project: project, Filename: filename, Checksum: checksum, Size: size}, nil
		}
		httpErr := responseError(response)
		if !retryable(httpErr.StatusCode) {
			return Session{}, httpErr
		}
		if err := c.wait(ctx, httpErr.RetryAfter); err != nil {
			return Session{}, err
		}
	}
}

// Resume verifies content against session and resumes it from the receiver's
// current offset until a receipt is available.
func (c *Client) Resume(ctx context.Context, session Session, content io.ReadSeeker) (Receipt, error) {
	return c.resume(ctx, session, content, true)
}

func (c *Client) resume(ctx context.Context, session Session, content io.ReadSeeker, verifyContent bool) (Receipt, error) {
	if err := validateSession(session, c.baseURL); err != nil {
		return Receipt{}, err
	}
	if verifyContent {
		size, checksum, err := inspectContent(content)
		if err != nil {
			return Receipt{}, err
		}
		if size != session.Size || checksum != session.Checksum {
			return Receipt{}, fmt.Errorf("artifact changed since upload creation")
		}
	}

	for {
		offset, err := c.offset(ctx, session)
		if err != nil {
			var httpErr *HTTPError
			if errors.As(err, &httpErr) && retryable(httpErr.StatusCode) {
				if err := c.wait(ctx, httpErr.RetryAfter); err != nil {
					return Receipt{}, err
				}
				continue
			}
			if isTransportError(err) {
				if err := c.wait(ctx, 0); err != nil {
					return Receipt{}, err
				}
				continue
			}
			return Receipt{}, err
		}
		if offset == session.Size {
			receipt, err := c.Receipt(ctx, session.ObjectID)
			if err != nil {
				return Receipt{}, fmt.Errorf("upload is complete but receipt is unavailable: %w", err)
			}
			return receipt, validateReceipt(receipt, session)
		}

		if _, err := content.Seek(offset, io.SeekStart); err != nil {
			return Receipt{}, fmt.Errorf("seek artifact to upload offset: %w", err)
		}
		body := io.NopCloser(io.LimitReader(content, session.Size-offset))
		req, err := http.NewRequestWithContext(ctx, http.MethodPatch, session.UploadURL, body)
		if err != nil {
			return Receipt{}, err
		}
		req.ContentLength = session.Size - offset
		req.Header.Set("Tus-Resumable", tusVersion)
		req.Header.Set("Upload-Offset", strconv.FormatInt(offset, 10))
		req.Header.Set("Content-Type", "application/offset+octet-stream")
		response, err := c.httpClient().Do(req)
		if err != nil {
			if err := c.wait(ctx, 0); err != nil {
				return Receipt{}, err
			}
			continue
		}
		if response.StatusCode == http.StatusNoContent {
			header := response.Header.Get(receiptHeader)
			response.Body.Close()
			if header == "" {
				receipt, err := c.Receipt(ctx, session.ObjectID)
				if err != nil {
					return Receipt{}, fmt.Errorf("completion response omitted receipt: %w", err)
				}
				return receipt, validateReceipt(receipt, session)
			}
			receipt, err := decodeReceipt(header)
			if err != nil {
				return Receipt{}, err
			}
			return receipt, validateReceipt(receipt, session)
		}
		httpErr := responseError(response)
		if retryable(httpErr.StatusCode) || httpErr.StatusCode == http.StatusConflict {
			if err := c.wait(ctx, httpErr.RetryAfter); err != nil {
				return Receipt{}, err
			}
			continue
		}
		return Receipt{}, httpErr
	}
}

// Receipt retrieves an accepted upload receipt by object ID.
func (c *Client) Receipt(ctx context.Context, objectID string) (Receipt, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint("api/v1/receipts/")+url.PathEscape(objectID), nil)
	if err != nil {
		return Receipt{}, err
	}
	response, err := c.httpClient().Do(req)
	if err != nil {
		return Receipt{}, fmt.Errorf("get receipt: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return Receipt{}, responseError(response)
	}
	defer response.Body.Close()
	var receipt Receipt
	if err := json.NewDecoder(io.LimitReader(response.Body, maxErrorBody)).Decode(&receipt); err != nil {
		return Receipt{}, fmt.Errorf("decode receipt: %w", err)
	}
	return receipt, nil
}

func (c *Client) offset(ctx context.Context, session Session) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, session.UploadURL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Tus-Resumable", tusVersion)
	response, err := c.httpClient().Do(req)
	if err != nil {
		return 0, &transportError{err: fmt.Errorf("inspect upload: %w", err)}
	}
	if response.StatusCode != http.StatusOK {
		return 0, responseError(response)
	}
	response.Body.Close()
	length, err := strconv.ParseInt(response.Header.Get("Upload-Length"), 10, 64)
	if err != nil || length != session.Size {
		return 0, fmt.Errorf("receiver reported unexpected upload length %q", response.Header.Get("Upload-Length"))
	}
	offset, err := strconv.ParseInt(response.Header.Get("Upload-Offset"), 10, 64)
	if err != nil || offset < 0 || offset > session.Size {
		return 0, fmt.Errorf("receiver reported invalid upload offset %q", response.Header.Get("Upload-Offset"))
	}
	return offset, nil
}

func inspectContent(content io.ReadSeeker) (int64, string, error) {
	size, err := content.Seek(0, io.SeekEnd)
	if err != nil || size < 1 {
		return 0, "", fmt.Errorf("artifact must be a non-empty seekable stream")
	}
	if _, err := content.Seek(0, io.SeekStart); err != nil {
		return 0, "", fmt.Errorf("seek artifact: %w", err)
	}
	hash := blake3.New()
	n, err := io.Copy(hash, content)
	if err != nil {
		return 0, "", fmt.Errorf("hash artifact: %w", err)
	}
	if n != size {
		return 0, "", fmt.Errorf("artifact size changed while hashing")
	}
	if _, err := content.Seek(0, io.SeekStart); err != nil {
		return 0, "", fmt.Errorf("rewind artifact: %w", err)
	}
	return size, "blake3:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func validateSession(session Session, base *url.URL) error {
	_, objectID, err := resolveUploadLocation(base, session.UploadURL)
	if err != nil || objectID != session.ObjectID || session.Size < 1 {
		return fmt.Errorf("invalid upload session")
	}
	const prefix = "blake3:"
	digest, err := hex.DecodeString(strings.TrimPrefix(session.Checksum, prefix))
	if !strings.HasPrefix(session.Checksum, prefix) || err != nil || len(digest) != 32 {
		return fmt.Errorf("invalid upload session checksum")
	}
	return nil
}

func validateReceipt(receipt Receipt, session Session) error {
	if receipt.ObjectID != session.ObjectID || receipt.Checksum != session.Checksum || receipt.SizeBytes != session.Size {
		return fmt.Errorf("receipt does not match upload session")
	}
	return nil
}

func decodeReceipt(value string) (Receipt, error) {
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return Receipt{}, fmt.Errorf("decode receipt header: %w", err)
	}
	var receipt Receipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return Receipt{}, fmt.Errorf("decode receipt JSON: %w", err)
	}
	return receipt, nil
}

func metadataValue(key, value string) string {
	return key + " " + base64.StdEncoding.EncodeToString([]byte(value))
}

func resolveUploadLocation(base *url.URL, location string) (string, string, error) {
	reference, err := url.Parse(location)
	if err != nil || location == "" {
		return "", "", fmt.Errorf("canner returned invalid upload location %q", location)
	}
	resolved := base.ResolveReference(reference)
	if resolved.Scheme != base.Scheme || resolved.Host != base.Host {
		return "", "", fmt.Errorf("canner returned cross-origin upload location")
	}
	parts := strings.Split(strings.Trim(resolved.EscapedPath(), "/"), "/")
	if len(parts) == 0 {
		return "", "", fmt.Errorf("canner returned invalid upload location %q", location)
	}
	objectID, err := url.PathUnescape(parts[len(parts)-1])
	if err != nil || objectID == "" || strings.Contains(objectID, "/") {
		return "", "", fmt.Errorf("canner returned invalid upload location %q", location)
	}
	return resolved.String(), objectID, nil
}

func responseError(response *http.Response) *HTTPError {
	defer response.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(response.Body, maxErrorBody))
	return &HTTPError{StatusCode: response.StatusCode, Body: strings.TrimSpace(string(raw)), RetryAfter: parseRetryAfter(response.Header.Get("Retry-After"), time.Now())}
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if date, err := http.ParseTime(value); err == nil && date.After(now) {
		return date.Sub(now)
	}
	return 0
}

func retryable(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable
}

func isTransportError(err error) bool {
	var transportErr *transportError
	return errors.As(err, &transportErr)
}

func (c *Client) endpoint(path string) string {
	base := *c.baseURL
	base.Path = strings.TrimRight(base.Path, "/") + "/" + strings.TrimLeft(path, "/")
	base.RawPath = ""
	return base.String()
}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

func (c *Client) wait(ctx context.Context, suggested time.Duration) error {
	delay := suggested
	if delay <= 0 {
		delay = c.RetryDelay
	}
	if delay <= 0 {
		delay = time.Second
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
