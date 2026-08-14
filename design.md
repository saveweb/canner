# Canner 设计

## 范围

Canner 接收志愿者 worker 上传的 artifact，并独立负责 accepted object 到配置
sink 的 delivery：

```text
                                      +-- receipt --> worker -- complete + receipt --> HQ
worker -- artifact --> receiver ------+
                                      +--> delivery worker --> final sink
```

版本 1 负责 anonymous upload admission、resumable storage、内容 checksum
校验、receipt persistence、receipt recovery、可选的 WARC-Zstd packaging，
以及 reliable delivery 到 Internet Archive。Acceptance 不解析或校验
WARC/媒体格式；只有启用 `mergewarc` packager 的 project 会在 packaging 阶段
执行格式校验。

HQ 与 Canner 永不直接通信。Canner 不知道 HQ job ID、generation、attempt ID、
outcome 或 job 状态。Worker 是唯一桥梁：它向 Canner 上传 artifact、取得
receipt，再在完成 HQ attempt 时提交该 receipt。多次执行或多个 generation
可能产生多个相互独立的 Canner object ID；Canner 分别处理每个 object，不进行
job 级去重或协调。

## Acceptance 边界

只有最终 tus 请求中的以下步骤全部成功，artifact 才算被接收：

1. tus 已写入与声明大小完全一致的字节数；
2. 上传文件、tus metadata 和 uploads 目录均已同步到持久卷；
3. Canner 对完整存储文件计算 BLAKE3，且结果与 worker 声明的
   `blake3:<hex>` checksum 一致；
4. receipt 已经原子写入并同步。

Canner 不执行文件结构校验。这样 receiver 的 CPU 和内存成本只与完整性校验所需
的一次内容哈希扫描成正比。

receipt 一旦存在，acceptance 即为 final。后续 sink failure 属于 Canner 的
delivery state，不会重新打开 HQ job。

## 匿名选择 project

上传和 receipt 查询均为匿名操作。Worker 在 tus `Upload-Metadata` 中声明一个已配置的
project ID；Canner 在创建上传前拒绝未知或格式错误的 project ID。Canner 将该
project 与上传一同保存，并用它选择 delivery 配置。

任何知道上传 object ID 的人都可以查看其 tus 状态、继续上传或取回 receipt。这是
匿名版本 1 协议的有意属性；project ID 是路由标签，不是凭据。

## 存储背压

每次创建上传（`POST`）和追加数据（`PATCH`）之前，receiver 都会检查 `data_dir`
所在文件系统的可用字节数。可用空间小于全局 `min_free_bytes` 时返回
`429 Too Many Requests`，并带上 `Retry-After: 60`；恰好相等时允许请求。
`HEAD`、健康检查和 receipt 查询仍然可用，使 worker 可以观察已有上传。

如果无法确定可用空间，修改上传状态的请求会收到 `503 Service Unavailable`，
并带有相同的重试提示。这是一个刻意保持简单的请求边界保护，而不是空间预留系统：
并发接受的请求仍可能消耗额外空间，因此运维人员应根据预期上传大小和并发量为阈值
保留足够余量。示例配置使用 100 GiB；实际部署可以选择其他正字节数。

## 存储

接收路径有意不依赖数据库：

- `data/uploads/{object_id}` 是 artifact；
- tus 将断点续传 metadata 保存在 artifact 旁边；
- `data/receipts/{object_id}.json` 是 immutable acceptance receipt；
- 文件锁串行化针对同一上传的并发操作。

receipt sidecar 先写入临时文件并同步，再重命名，最后同步 receipt 目录。Sidecar
的存在是 acceptance commit point。重复执行 acceptance 会原样返回已有 receipt。

运维人员必须备份整个数据目录。

## Delivery 队列

Delivery 由独立的 `canner deliver` 进程执行。Receiver 发布 receipt 后，将
accepted object 写入 `data/delivery.sqlite`。Delivery 进程还会在启动时和此后
每小时扫描一次 receipt sidecar，补入缺失 object。因此该数据库是运维索引，
而不是 acceptance truth：只要 receipt 和 tus metadata 仍然存在，就可以通过
它们重建该数据库，而不会丢失 accepted artifact。

状态如下：

```text
pending -> delivering -> delivered
             |
             +-> retry_wait -> delivering
```

Delivery attempt 串行执行。失败会以指数退避无限重试，间隔从一分钟增长到一小时，
因此临时 sink 或凭据故障不会使 artifact 被静默放弃。进程启动时，所有
`delivering` 行都会回到 `retry_wait`。Internet Archive adapter 使用确定性的
identifier 和远端文件名，go2internetarchive 会在上传前检查远端已有文件。每个
sink attempt 均可取消，最长执行 24 小时。

`deliver` 进程每秒从 go2internetarchive 接收一次 progress snapshot，并将最新值
写入 `delivery.sqlite`。独立的 receiver 进程通过同一 SQLite 在 Web UI 展示 IA
上传字节数、总量、吞吐量、文件数和当前文件；attempt 结束或进程恢复时清除快照，
避免旧进度被误认为仍在运行。

同一数据目录只能由一个 delivery 进程持有；第二个进程会被进程锁拒绝。SQLite
使用 WAL，并保存 attempt 次数、下次重试时间、last error、final remote
identifier 和 delivery 时间，供运维检查。Delivery 成功或失败都不会修改
immutable receipt。

Internet Archive 配置以 project 为作用域。Package reserve 时一次性展开模板并
持久化 immutable delivery plan，retry 不再读取可变配置。IA 凭据文件只由
delivery 进程读取。

## Artifact、Package 与 Delivery

Canner 对所有 project 使用三个独立层次，不存在绕过 Package 的 direct-delivery
分支：

```text
Artifact -- ordered membership --> Package -- immutable plan --> Delivery
   |                                  |
   +-- original receipt               +-- payload + JSONL manifest
```

`Artifact` 是 Worker 上传并取得 receipt 的原始对象。`Package` 是 Canner 从一个
或多个 Artifact 形成的 delivery unit，不签发新的 HQ receipt。`Delivery` 只消费
已经 sealed 的 Package。HQ 继续保存每个原始 Artifact 的 receipt，不知道 Package。

每个 project 必须显式选择 packager：

- `identity`：一个 Artifact 自身就是一个 Package，payload 字节不做任何处理；
- `mergewarc`：多个 dictionary-free WARC-Zstd Artifact 合并成一个 Package。

`identity` 不维护另一套 delivery 状态机。它在同一 `data_dir` 内为 accepted artifact
建立 hard link，原子发布单成员 Package 和 provenance manifest，随后完全复用
Package delivery、retry 和 retention。删除 upload link 后，Package link 仍引用同一
inode，因此不复制 payload，也不会提前释放内容。

`mergewarc` project 另外配置：

- `trigger_bytes`：开始一次 draining round 的未打包总字节数；
- `target_package_bytes`：单个 Package 的目标上限；
- `max_wait`：尾部 Artifact 可以等待的最长时间。

达到 `trigger_bytes` 后，draining 状态持久化在 SQLite 中。Canner 持续按
`(accepted_at, object_id)` 选择输入并生成 Package，直到剩余数据不足一个 target；
进程重启不会中断这一轮。尾部留给下一轮，超过 `max_wait` 时允许生成不足 target
的 Package。单个超大 Artifact 独立成包，不在 WARC record 中间切割。

Package ID 由 packager version、project 以及 ordered member 的 object ID、checksum
和 size 确定性计算。成员 reserve、顺序和 IA delivery plan 在 materialization 前
持久化。`mergewarc` 严格校验 dictionary-free WARC-Zstd，然后原样复制压缩字节，
生成包含 offset、size、record count 和 receipt provenance 的 JSONL manifest。
每个 input 和 output 都记录 SHA-1、SHA-256 和 BLAKE3
checksums。Package 与 manifest 分别经过 temporary file、`fsync`、atomic
rename 和 directory `fsync`；两者均持久化后 Package 才进入 `sealed`。

IA plan 包含 resolved identifier、两个 remote filename、metadata、sink driver 和
credentials-file reference。Plan 一经写入不再读取可变模板，因此 crash 后 retry
仍指向同一 IA item。一个 Package 对应一个 IA item，item 同时包含 payload 和
`.manifest.jsonl`；`mergewarc` payload 使用 `.warc.zst`。

`deliver` 由单个 scheduler 串行执行 package claim、SQLite progress/state 更新、
packaging 和 cleanup，同时以 bounded goroutine 并发执行 sink 网络上传；并发上限由
顶层 `delivery_concurrency` 配置，缺省为 `2`。shutdown 取消所有 active upload，
未完成的 `delivering` row 在下次启动时恢复为 `retry_wait`。

Package sealed 后，manifest 的 byte range 可以精确恢复每个原始 Artifact，Canner
因此可以删除 member payload 和 tus metadata，同时保留原始 receipt、Artifact row
和 package membership。Package delivery 成功并经过 `local_artifact_retention` 后，
本地 Package 和 manifest 才被删除；SQLite 中的 delivery 结果继续保留。

格式错误只影响 packaging，不回滚 acceptance。Canner 单独标记无法打包的 Artifact，
将同组合法 Artifact 释放回候选队列继续处理。I/O 等 transient build failure 保持
原 membership 并退避重试。

## Receipt 契约

JSON 字段与 SavewebHQ 的 `ArtifactReceipt` 一致：

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

`checksum` 与具体算法无关，格式为 `algorithm:lowercase-hex`。

HQ 只校验 checksum 封装格式，因为它仅保存 receipt，不验证 artifact 内容或
receipt authenticity。Receiver 决定哪些算法可用于签发 receipt；版本 1 只允许
BLAKE3。

## 失败行为

- 中断的上传仍可通过 tus 继续；
- 可用存储空间过低时，创建和继续上传返回 `429`；worker 应遵守
  `Retry-After` 并稍后重试；
- checksum 不匹配返回 `422`，且绝不写入 receipt；
- 存储或同步失败返回 `500`，且绝不写入 receipt；
- final HTTP response 丢失不会造成问题，因为 worker 可以按 object ID 取回
  immutable receipt；
- Canner 重启恢复使用 tus metadata 和 receipt sidecar，内存状态均不具权威性；
- sink failure 会持久化 error 和 next retry time，但不影响 HQ。
