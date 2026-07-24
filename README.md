# canner

`canner` accepts artifacts from Saveweb workers, returns receipts for SavewebHQ
job completions, and delivers accepted artifacts to their final sink. Upload
receiving and delivery are separate processes sharing one persistent data
directory.

A receipt means only that canner has synced the file, checked its declared
content checksum, and durably stored the receipt. It does not mean that
the file is a valid WARC or that a downstream archive accepted it.

## Run

Hash a project upload token:

```sh
go run . hash-token 'replace-with-a-random-token'
```

Put the token hash in a config based on `config.example.json`, then run:

```sh
go run . serve config.json
go run . deliver config.json
```

Production deployments should mount `data_dir` from persistent storage.
Clear-text project tokens must not be committed.

## Upload protocol

Create a tus upload at `/files/` with a bearer token, `Upload-Length`, and this
base64-encoded metadata value:

```text
checksum blake3:<64 lowercase hex characters>
```

The final successful `PATCH` includes `Artifact-Receipt`, containing the
base64url-encoded JSON receipt. If that response is lost, retrieve the same
receipt with:

```text
GET /api/v1/receipts/{object_id}
Authorization: Bearer <project token>
```

Receipt checksums use the self-describing `algorithm:lowercase-hex` form. HQ is
algorithm-neutral; this canner version accepts `blake3` for uploaded content.

`GET /healthz` is unauthenticated and returns process health.

## Delivery

Each project may configure an `internet_archive` delivery sink. The credentials
file uses the go2internetarchive format: access key on the first line and secret
key on the second. Canner expands these deterministic templates in the IA
identifier, remote name, and metadata:

- `{{PROJECT}}`: authenticated canner project;
- `{{OBJECT_ID}}`: stable tus object ID;
- `{{FILENAME}}`: safe upload filename, or the object ID when it is unsafe;
- `{{DATE}}`: receipt acceptance time in UTC `YYYYMMDDhhmmss` form.

The receiver records accepted artifacts in `data/delivery.sqlite`; `deliver`
also reconciles immutable receipt sidecars at startup and once per hour so the
database remains rebuildable. It processes one artifact at a time. A failed
attempt enters `retry_wait`; retries start after one minute and cap at one hour.
A restart returns an interrupted `delivering` item to `retry_wait`. Delivery
never changes the receipt or reopens the HQ job.

Each Internet Archive attempt is cancellable and limited to 24 hours. Remote
file names are URL-encoded while preserving `/` path separators.

Run exactly one `deliver` process for a data directory. Inspect delivery state
as JSONL with:

```sh
go run . deliveries config.json
```

The output includes attempts, the next retry time, last error, and the final IA
identifier in `remote_id` after success.

## Verify

```sh
go test ./...
```
