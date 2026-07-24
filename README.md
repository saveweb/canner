# canner

`canner` accepts artifacts from Saveweb workers, returns receipts for SavewebHQ
job completions, and delivers accepted artifacts to their final sink. Upload
receiving and delivery are separate processes sharing one persistent data
directory.

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
canner, err := client.New("https://canner.example")
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

For recovery across worker restarts, call `Create`, persist the returned
JSON-compatible `client.Session`, and call `Resume` with the unchanged artifact.
`Resume` verifies the artifact against the session before sending data. The
client also exposes `Receipt` for explicit receipt recovery by object ID.

## Delivery

Each project may configure an `internet_archive` delivery sink. The credentials
file uses the go2internetarchive format: access key on the first line and secret
key on the second. Canner expands these deterministic templates in the IA
identifier, remote name, and metadata:

- `{{PROJECT}}`: worker-declared configured canner project;
- `{{OBJECT_ID}}`: stable tus object ID;
- `{{FILENAME}}`: safe upload filename, or the object ID when it is unsafe;
- `{{DATE}}`: receipt acceptance time in UTC `YYYYMMDDhhmmss` form.

The receiver records accepted artifacts in `data/delivery.sqlite`; `deliver`
also reconciles immutable receipt sidecars at startup and once per hour so the
database can recover missing accepted objects before their local files are
purged. It processes one artifact at a time. A failed attempt enters
`retry_wait`; retries start after one minute and cap at one hour. A restart
returns an interrupted `delivering` item to `retry_wait`. Delivery never changes
the receipt or reopens the HQ job.

Each Internet Archive attempt is cancellable and limited to 24 hours. Remote
file names are URL-encoded while preserving `/` path separators.

`local_artifact_retention` is required for every delivery configuration. It is
a Go duration of at least one second, such as `"30s"` or `"24h"`, with no fixed
maximum. After a successful delivery, canner persists the IA identifier, remote
name, delivery time, and purge deadline. Once the deadline passes, it
idempotently removes the artifact and tus metadata while retaining the receipt
and delivery record. Failed purges retry after one minute.

Run exactly one `deliver` process for a data directory. Inspect delivery state
as JSONL with:

```sh
go run . deliveries config.json
```

The output includes attempts, delivery and purge retry times, last error, final
IA identifier and remote name, `delivered_at`, and `purged_at`.

## Verify

```sh
go test ./...
```
