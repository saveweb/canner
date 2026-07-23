# Canner design

## Scope

Canner is the durable handoff between volunteer workers and artifact sinks:

```text
worker -> resumable upload -> canner persistent volume -> receipt -> HQ
                              |
                              +-> later sink processing
```

Version 1 owns upload authentication, resumable storage, content checksum
verification, receipt persistence, and receipt recovery. It does not parse or
validate WARC/media formats, upload to Internet Archive, or expose a generic
processing queue.

## Acceptance boundary

An artifact is accepted only after all of these steps succeed in the final tus
request:

1. tus has written exactly the declared number of bytes;
2. the upload file, tus metadata, and uploads directory are synced to the
   persistent volume;
3. canner computes BLAKE3 over the complete stored file and it matches the
   worker-declared `blake3:<hex>` checksum;
4. the receipt is atomically written and synced.

No structural file validation is performed. This keeps the receiver's CPU and
memory cost proportional to the one content-hash pass required for integrity.

Once a receipt exists, acceptance is final. A later sink failure belongs to
canner's delivery state and never reopens the HQ job.

## Identity and authorization

Each configured project has one SHA-256 hash of an upload bearer token. The
clear token is held by workers, not by canner configuration. Token comparison
is constant-time.

The authenticated project is written into server-controlled tus metadata.
Every operation on an existing upload checks that metadata, so one project's
token cannot inspect, resume, terminate, or retrieve another project's upload.

## Storage

The first implementation deliberately has no database:

- `data/uploads/{object_id}` is the artifact;
- tus stores resumable upload metadata beside the artifact;
- `data/receipts/{object_id}.json` is the immutable acceptance receipt;
- file locking serializes concurrent operations on one upload.

The receipt sidecar is written to a temporary file, synced, renamed, and then
the receipt directory is synced. Its existence is the acceptance commit point.
Repeated acceptance returns the existing receipt unchanged.

Operators must back up the whole data directory. Removing accepted artifacts or
receipts is outside the online API and requires an explicit retention policy.

## Receipt contract

The JSON fields match SavewebHQ's `ArtifactReceipt`:

```json
{
  "id": "receipt:<object_id>",
  "issuer": "https://canner.example",
  "object_id": "<tus upload id>",
  "checksum": "blake3:<lowercase hex>",
  "size_bytes": 123,
  "accepted_at": 1784764800
}
```

`checksum` is algorithm-neutral and uses `algorithm:lowercase-hex`.

HQ validates only the checksum envelope syntax because it stores receipts but
does not verify artifact contents or receipt authenticity. The receiver decides
which algorithms are strong enough to issue a receipt; version 1 permits only
BLAKE3.

## Failure behavior

- An interrupted upload remains resumable through tus.
- A checksum mismatch returns `422` and never writes a receipt.
- A storage or sync failure returns `500` and never writes a receipt.
- Losing the final HTTP response is harmless because the worker can retrieve
  the immutable receipt by object ID.
- Canner restart recovery uses tus metadata and receipt sidecars; no in-memory
  acceptance state is authoritative.
