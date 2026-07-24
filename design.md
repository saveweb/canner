# Canner design

## Scope

Canner is the durable handoff between volunteer workers and artifact sinks:

```text
worker -> receiver -> canner persistent volume -> receipt -> HQ
                         |
                         +-> delivery worker -> final sink
```

Version 1 owns upload authentication, resumable storage, content checksum
verification, receipt persistence, receipt recovery, and reliable delivery to
Internet Archive. It does not parse or validate WARC/media formats or expose a
generic remote queue.

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

The acceptance path deliberately does not depend on a database:

- `data/uploads/{object_id}` is the artifact;
- tus stores resumable upload metadata beside the artifact;
- `data/receipts/{object_id}.json` is the immutable acceptance receipt;
- file locking serializes concurrent operations on one upload.

The receipt sidecar is written to a temporary file, synced, renamed, and then
the receipt directory is synced. Its existence is the acceptance commit point.
Repeated acceptance returns the existing receipt unchanged.

Operators must back up the whole data directory. Removing accepted artifacts or
receipts is outside the online API and requires an explicit retention policy.

## Delivery queue

Delivery is a separate `canner deliver` process. The receiver inserts accepted
objects into `data/delivery.sqlite` after publishing their receipts. The
delivery process also scans receipt sidecars at startup and once per hour,
inserting any missing objects. The database is therefore an operational index,
not the source of acceptance truth: deleting and rebuilding it from receipts
plus tus metadata cannot lose an accepted artifact before local payload cleanup.

The states are:

```text
pending -> delivering -> delivered
             |
             +-> retry_wait -> delivering
```

Attempts are sequential. Failures retry indefinitely with exponential backoff
from one minute to one hour, so a temporary sink or credential outage cannot
silently abandon an artifact. On process startup, any `delivering` row is moved
to `retry_wait`; the Internet Archive adapter uses deterministic identifiers and
remote names, while go2internetarchive checks an existing remote file before
uploading it again. Every sink attempt is cancellable and has a 24-hour upper
bound.

Only one delivery process may own a data directory; a process lock rejects a
second instance. SQLite uses WAL and stores the attempt count, next retry time,
last error, final remote identifier, and delivery time for operator inspection.
Delivery success or failure never edits the immutable receipt.

Internet Archive configuration is project-scoped. Template expansion uses only
the stable project, object ID, accepted filename, and receipt time, so a retry
resolves to the same destination. IA credentials are mounted only into the
delivery process.

## 计划中的清理生命周期

本节描述尚未实现的清理机制。目标是让本地 artifact 占用保持在存储容量以内，
同时在 HQ 仍然引用 receipt 时保留可查询的 delivery 证据。Delivery 是否成功、
本地 payload 是否存在，以及 HQ 是否仍然引用 receipt，是三个相互独立的状态，
不能合并成一个状态字段。

完整时序如下：

```text
 Worker             Receiver              Deliver             Internet Archive          HQ
   |                    |                    |                         |                    |
   |  上传 artifact     |                    |                         |                    |
   |------------------->|                    |                         |                    |
   |                    | 校验 checksum      |                         |                    |
   |                    | 持久化 artifact    |                         |                    |
   |                    | 写入 receipt       |                         |                    |
   |                    | 登记 delivery DB   |                         |                    |
   |   返回 receipt     |                    |                         |                    |
   |<-------------------|                    |                         |                    |
   |-------------------------------------------------------------- receipt ------------->|
   |                    |                    |                         |                    |
   |                    |                    | 读取本地 artifact       |                    |
   |                    |                    |------------------------>|                    |
   |                    |                    |       上传 artifact     |                    |
   |                    |                    |------------------------>|                    |
   |                    |                    |       上传成功          |                    |
   |                    |                    |<------------------------|                    |
   |                    |                    |                         |                    |
   |                    |                    | 先持久化：                                   |
   |                    |                    | - state = delivered                          |
   |                    |                    | - IA identifier                               |
   |                    |                    | - IA remote filename                          |
   |                    |                    | - delivered_at                                |
   |                    |                    | - purge_after                                 |
   |                    |                    |                         |                    |
   |                    |                    | 等待可配置的本地 retention                   |
   |                    |                    |                         |                    |
   |                    |                    | 再删除：                                      |
   |                    |                    | - artifact                                    |
   |                    |                    | - tus .info                                   |
   |                    |                    | - tus 临时状态                                |
   |                    |                    |                         |                    |
   |                    |                    | 写入 purged_at                                |
   |                    |                    |                         |                    |
   |                    |                    | 此时仍保留：                                  |
   |                    |                    | - receipt                                     |
   |                    |                    | - delivery DB 记录                            |
   |                    |                    | - IA 远端位置                                 |
   |                    |                    |                         |                    |
   |                    |                    |< - - 查询 delivery 状态 - - - - - - - - - -|
   |                    |                    |- - delivered + IA 位置 - - - - - - - - - ->|
   |                    |                    |                         |                    |
   |                    |                    |                         | HQ 删除或归档 Job
   |                    |                    |                         | 并通过 outbox 通知
   |                    |                    |                         |                    |
   |                    |                    |< - - release receipt - - - - - - - - - - - |
   |                    |                    | 写入 released_at                              |
   |                    |                    |                         |                    |
```

核心状态可以概括为：

```text
accepted
   |
   | artifact 必须存在，供 delivery 和失败重试使用
   v
delivered
   |
   | 等待可配置的本地 retention
   v
purged_at
   |
   | artifact 已删除，但 receipt 和 delivery 结果仍可查询
   v
released_at
   |
   | HQ 已不再引用 receipt，可以归档 metadata
   v
archived
```

本地 payload 清理必须按以下顺序执行：先确认 Internet Archive 上传成功；再将
远端 identifier、远端文件名、`delivered_at` 和 `purge_after` 持久化；等待
retention 到期；最后幂等删除 artifact 和 tus metadata，并写入 `purged_at`。
若进程在删除中途退出，重启后应继续清理，已经不存在的文件视为删除成功。
`delivery_state` 仍然是 `delivered`，不能因为本地文件已删除而改变最终送达结果。

Payload 清理不需要等待 HQ。HQ 只参与 receipt/metadata 的最终释放：HQ 确认
artifact 已 delivered 后，在删除或归档 Job 的同一个事务中写入 outbox 事件；
outbox 幂等通知 canner，canner 再写入 `released_at`。在此之前，canner 必须
继续提供 receipt 和 delivery 状态查询。

Receipt 和 delivery 记录本身也不能永久堆积。如果要求不损失历史信息，canner
应将已 release 的最小记录按日或按月写入压缩的 append-only ledger，例如
`JSONL.zst`。只有 ledger 已上传到持久存储并完成 checksum 校验后，才能删除
本地 receipt sidecar 和 SQLite 历史行：

```text
 Canner delivery DB           Archive ledger             Local storage
         |                           |                          |
         | 收集 released 记录       |                          |
         |-------------------------->|                          |
         | 上传并校验 JSONL.zst      |                          |
         |<------------------------->|                          |
         |                           |                          |
         | 归档确认成功              |                          |
         |----------------------------------------------------->|
         |                           | 删除 receipt sidecar     |
         |                           | 删除 SQLite 历史行       |
```

在实现 payload 清理之前，receipt 查询必须先解除对 tus `.info` 的依赖，认证所需
的 project 应来自保留下来的 receipt/delivery index。清理策略的配置名称、默认值、
最小 retention 和最大 retention 仍待确定，本设计不预设具体秒数范围。

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
- Delivery database loss is recoverable by rescanning receipt sidecars.
- Sink failures persist their error and next retry time without affecting HQ.
