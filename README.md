# canner

`canner` accepts artifacts from Saveweb workers, returns receipts for SavewebHQ
job completions, packages accepted artifacts, and delivers packages to their
final sink. Upload receiving and packaging/delivery are separate processes
sharing one persistent data directory.

A receipt means only that canner has synced the file, checked its declared
content checksum, and durably stored the receipt. It does not mean that
the file is a valid WARC or that a downstream archive accepted it.

## Run

Create a config based on `config.example.json`, then run:

```sh
go run . serve config.json
go run . deliver config.json
```

Production deployments should mount `data_dir` from persistent storage.

## Upload protocol

Uploads are anonymous. Create a tus upload at `/files/` with `Upload-Length`
and these base64-encoded metadata values:

```text
project <configured project id>,checksum blake3:<64 lowercase hex characters>
```

Before accepting a create or patch request, canner checks the filesystem
containing `data_dir`. If available space is below the configurable
`min_free_bytes`, it returns `429 Too Many Requests` with `Retry-After: 60`.
The example configuration uses 100 GiB. Status (`HEAD`) and receipt requests
remain available while uploads are paused.

The final successful `PATCH` includes `Artifact-Receipt`, containing the
base64url-encoded JSON receipt. If that response is lost, retrieve the same
receipt with:

```text
GET /api/v1/receipts/{object_id}
```

Receipt checksums use the self-describing `algorithm:lowercase-hex` form. HQ is
algorithm-neutral; this canner version accepts `blake3` for uploaded content.

`GET /healthz` is unauthenticated and returns process health.

## Go client

Workers can use the public `github.com/saveweb/canner/client` package instead
of implementing tus directly:

```go
canner, err := client.New("") // defaults to https://canner.saveweb.org/
if err != nil {
	return err
}
receipt, err := canner.UploadFile(ctx, "project-id", "artifact.warc.gz")
if err != nil {
	return err
}
```

`UploadFile` computes BLAKE3, creates the tus upload, resumes from the server's
current offset, honors `429`/`503` `Retry-After`, and returns the decoded
receipt. Its retry lifetime is controlled by `ctx`.

Use `UploadFileWithProgress` when the worker needs progress reporting. Its
callback receives synchronous `hashing` and `uploading` snapshots with completed
and total byte counts. The callback must return quickly; callers can retain the
latest snapshot and publish it at their own logging interval. Upload progress
may move backwards when recovery discovers a lower durable receiver offset.
For simple command-line workers, `UploadFileWithProgressToStdout` manages the
sampling loop itself and prints the latest snapshot at the supplied interval.

Pass an absolute HTTP URL to `New` to override the default receiver, for example
when using a self-hosted canner instance or a test server.

For recovery across worker restarts, call `Create`, persist the returned
JSON-compatible `client.Session`, and call `Resume` with the unchanged artifact.
`Resume` verifies the artifact against the session before sending data. The
client also exposes `Receipt` for explicit receipt recovery by object ID.

## Delivery

Each project configures an `internet_archive` delivery sink. The credentials
file uses the go2internetarchive format: access key on the first line and secret
key on the second. Canner expands these deterministic templates in the IA
identifier, remote name, and metadata:

- `{{PROJECT}}`: worker-declared configured canner project;
- `{{PACKAGE_ID}}` (and `{{OBJECT_ID}}`): stable package ID;
- `{{PACKAGE_ID_SHORT}}`: first 24 hexadecimal characters (96 bits) of the stable package ID;
- `{{PACKAGE_FILENAME}}` (and `{{FILENAME}}`): package filename;
- `{{DATE}}`: package creation time in UTC `YYYYMMDDhhmmss` form.

Resolved Internet Archive identifiers must be 5-100 characters, start with an
ASCII letter or digit, and contain only ASCII letters, digits, periods,
underscores, or dashes.

The receiver root (`/`) provides a read-only status dashboard. Its htmx fragment
at `/dashboard/status` refreshes every two seconds and reports active tus
uploads, unpackaged artifact bytes, package trigger progress, package builds,
delivery states, live Internet Archive upload bytes, throughput and file
progress, and packaging errors for each configured project. The `deliver`
process stores its latest progress snapshot in `delivery.sqlite`, so the
separate receiver process can render it.

Every project explicitly selects a packager. `identity` creates a one-member
package whose payload is the original artifact, without transforming or copying
its bytes:

```json
"packaging": { "type": "identity" }
```

Because uploads and packages share `data_dir`, the identity packager publishes
the payload with a hard link. The normal package lifecycle then applies without
a separate direct-delivery path.

`mergewarc` aggregates dictionary-free WARC-Zstd artifacts:

```json
"packaging": {
  "type": "mergewarc",
  "trigger_bytes": 10737418240,
  "target_package_bytes": 1073741824,
  "max_wait": "24h"
}
```

Once unpackaged accepted data reaches `trigger_bytes`, canner starts a durable
draining round and creates packages no larger than approximately
`target_package_bytes` until less than one target remains. The tail joins the
next round; `max_wait` seals an older partial package so low-volume data cannot
wait forever. An artifact larger than the target forms a package by itself.

Package inputs are ordered by acceptance time and object ID. Canner uses
`mergewarc` to strictly validate and concatenate their original compressed
bytes without recompression. It writes and syncs the package and its JSONL
manifest through temporary files, publishes each with an atomic rename, and
only seals the package after both are durable. The manifest records each original object ID, receipt ID,
SHA-1, SHA-256 and BLAKE3 checksums, encoded-byte range, and record count.

The resolved IA identifier, remote names, metadata, sink driver, and
credential-file reference are persisted as an immutable plan before the first
attempt. Each IA item receives both the package payload and its manifest.
Changing project templates affects only packages created afterward.

Acceptance remains checksum-only: a packaging format error does not invalidate
an artifact receipt or reopen its HQ job. Canner marks the bad artifact and
continues packaging valid artifacts. Transient build failures retry with the
same one-minute-to-one-hour backoff used by delivery.

The receiver records accepted artifacts in `data/delivery.sqlite`; `deliver`
also reconciles immutable receipt sidecars at startup and once per hour. SQLite
persists package membership and immutable delivery plans and must be backed up
with the rest of `data_dir`. It processes one package at a time. A failed attempt enters
`retry_wait`; retries start after one minute and cap at one hour. A restart
returns an interrupted `delivering` item to `retry_wait`. Delivery never changes
the receipt or reopens the HQ job.

Each Internet Archive attempt is cancellable and limited to 24 hours. Remote
file names are URL-encoded while preserving `/` path separators.

`local_artifact_retention` is required for every delivery configuration. It is
a Go duration of at least one second, such as `"30s"` or `"24h"`, with no fixed
maximum. After a successful delivery, canner persists the IA identifier,
delivery time, and purge deadline. Once the deadline passes, it idempotently
removes the local package payload and manifest while retaining the receipt,
membership, and delivery record. Failed purges retry after one minute.

Sealing provides a durable, byte-exact package representation, so canner removes
member upload paths and tus metadata after the package and manifest are synced.
For `identity`, the package hard link keeps the same inode alive. Canner retains
the original receipts and package membership.

Run exactly one `deliver` process for a data directory. Inspect delivery state
as JSONL with:

```sh
go run . packages config.json
```

The JSONL output has separate `package` and `delivery` records, including build,
delivery and purge retry state, blocked format errors, checksums, final IA
identifier, `delivered_at`, and `purged_at`.

## Verify

```sh
go test ./...
```
