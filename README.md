# canner

`canner` accepts artifacts from Saveweb workers and returns receipts that
can be attached to SavewebHQ job completions. It uses resumable tus uploads and
stores files on a local persistent volume.

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

## Verify

```sh
GOCACHE=/tmp/canner-go-cache go test ./...
```
