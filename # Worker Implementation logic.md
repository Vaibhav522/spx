# Worker Implementation logic


This whole system doesn't handle external logic, 

1. Fetch jobs from server in batches
2. Enqueue fetched jobs
    1. Pop one job at a time
    2. Transcode and write to disk
3. Mark job completed to db, if no error else provide the erorr faced to db


Job fetch result:

``` js
[
    {
        file_sha: str,
        file_name: str,
        file_path: str
    }
]
```


Constructing destination_file_path

``` pseudocode
1. Given file_name of type string
2. And  knowing output format of .pcm
3. Call bucket allocator to allocate a bucket_id

    file_destination = <output_directory>/<bucket_id>/<filename>.pcm
```



Bucket Allocator: Given a `max_content_count` bucket allocators job is to allocate a folder such that `total_file_or_folder_count <= max_content_count`. To do this, we need to track list of all 1st level folders inside `output_directory`. Since, we have single node, we don't need external source of truth of this. 

1. Scan the output directory for all the first level directories
2. Filter anything that has `total_file_or_folder_count > max_content_count`
3. Mark pick one of the directories and allocate the directory.


In-memory allocator state → authoritative during normal operation.
The Standard Approach: `Arc<Mutex<T>>` A Mutex `<T>` (Mutual Exclusion) ensures that exactly one thread can access and mutate the underlying data at any given second. To share this mutex among multiple threads, you must wrap it in an Arc<T> (Atomic Reference Counted) pointer, which allows safe, shared ownership across thread boundaries. 


1. Scan the output directory and create an in-memory table



#### Processed update to DB: 

``` js
{
    file_sha: str,
    file_destination: str,
    output_file_size: int (bytes),
    transcoder_error_faced: {
        error_type: int,
        error_body: str
    },
    processed_at: datetime,
    time_took_to_proces: float (sec)
}
```



##  When workers fail and we have to manage temp files.

In linux when `PrivateTmp=true`. When this is enabled, the system creates a unique, isolated, hidden temporary directory specifically for that process. To the process, it looks like it's writing to /tmp, but no other process on the machine can see it.


A single transaction pull from the server. 




a single transactional data pull from the db.

keep attempt count to 2.


``` sql



```


Create an shared db pool size of 16.

db config is sourced from .env file present at the "." folder of execution.



intitalize db connection config, once complete start a connection pool.



The objective is to keep the db fetch and queue functions to be seperated, meaning the queue should only have access to fetch job's and nothing else.
This keeps thing clean as we want. But how can we do that? 

Now, given access to 



The queue is intialized externally, which is consumed by multiple threads, meaning following:

initalized queue object shared with thread so we can do `Arc::new(Queue::new(*args));` with atomic reference counter we clone for each thread, such that each thread can safely consume the queue. I am using `crossbeam-queue` which is thread safe, and non-blocking ensuring workers aren't kept idle. Now, fields that will be actively called by threads are as follow: `requested_job_pull: Atomic Boolean` this is to ensure only one thread can change it, next we have source field, depends upon `requeusted_job_pull` to be false to be called, but since this instance can be called from any thread we want to wrap that with `Arc` too such that it's a thread safe call.


Now, to call job_pull, the `requested_job_pull` value should be false, and the `max_job_fetch_attempt > failed_attempt` and `queue should have 30 % left`, now when such conditions are met, we set the atomic bool to true, and make a fetch request, if failed 
whatever the condition is we know the 


the fetcher is a external thread, meaning we have to pass mutable references to the data fields, allowing it to mutate it. 



Job update type can be categorized of two types:

1. Failed 
2. Success

So, two states of updates. 

``` rust 

enum PreProcessingError {
    
}


struct TranscodingError {
    error_type: String,
    error_message: String,
    error_origination: String
}
struct JobUpdate {
    file_sha: String,
    output_file_path: Option<String>,
    is_transcoded: bool, // true for success else false for error faced
    error_faced: Option<TranscodingError>,
    processed_at: DateTime<Utc>,
    output_file_size: usize,
    time_to_process: Time,
}

```


multiple threads sharing the 



Since you are already using `tracing` macros and need **contextual errors** alongside `#[instrument(err)]`, you have two incredibly powerful ways to handle this without falling back into the `match` boilerplate trap.

Here is how you can inject dynamic, runtime context into your errors and traces cleanly.

---

## 1. The `tracing` Way: `#[instrument(fields(...))]` + `Record`

If you want your context to be tied to the **trace/span** itself (so every log message inside that function inherits the context), you can dynamically record fields inside the function.

`#[instrument]` allows you to declare empty placeholders that you fill with context later using `tracing::Span::current().record()`.

```rust
use tracing::{instrument, Span};

#[instrument(skip(data), fields(user_id = %data.user_id, order_id = tracing::field::Empty))]
fn process_payment(data: &PaymentPayload) -> Result<Receipt, PaymentError> {
    // 1. Do some work to find the order ID
    let order_id = db::get_order_id_for_user(data.user_id)?;
    
    // 2. Inject context dynamically into the current span!
    Span::current().record("order_id", order_id);

    // 3. If this fails, the automatic `err` log will include BOTH user_id AND order_id
    let receipt = gateway::charge_card(order_id, data.amount)?;
    
    Ok(receipt)
}

```

### Why this is great:

If `gateway::charge_card` fails, `#[instrument(err)]` will automatically emit an error log. Because you recorded `order_id` into the span context, structured logging targets (like Datadog, AWS CloudWatch, or Grafana Loki) will automatically index that error with `user_id` and `order_id` attached.

---

## 2. The Error-Centric Way: `anyhow::Context` or `eyre`

If you want the context embedded inside the **returned error object itself** (so that whatever catches it downstream knows exactly what failed), you should use the `.context()` or `.with_context()` extension traits provided by `anyhow` or `eyre`.

When combined with `#[instrument(err)]`, the macro will automatically print the full error chain, including your custom context strings.

```rust
use anyhow::Context;
use tracing::instrument;

#[instrument(err)] // <-- Will log the full context chain if an error bubbles out
fn process_invoice(invoice_id: u64) -> anyhow::Result<()> {
    
    // Static context string
    let user = db::fetch_user(invoice_id)
        .context("Failed to fetch user associated with invoice")?;

    // Dynamic context string (lazily evaluated via closure)
    let balances = gateway::get_balances(user.id)
        .with_context(|| format!("Failed to retrieve balances for user_id: {}", user.id))?;

    Ok(())
}

```

### What the log output looks like:

If `gateway::get_balances` fails, `#[instrument(err)]` will dump the error out to your log collector looking like this:

```text
2026-07-15T00:01:13Z ERROR process_invoice: invoice_id=12345, error=Failed to retrieve balances for user_id: 9988: Connection timed out

```

---

## 3. Combining Both for Production Workers

For heavy-duty production workers (e.g., consuming from Kafka, SQS, or RabbitMQ), the gold standard pattern is to combine both approaches.

Use **`fields`** for structural tracing data (IDs, metrics, indexing) and **`context()`** for semantic code errors.

```rust
#[instrument(
    name = "worker.process_message",
    skip(msg), 
    fields(msg.id = %msg.id, msg.retry_count = msg.retry)
)]
async fn handle_worker_message(msg: Message) -> anyhow::Result<()> {
    let payload = deserialize(&msg.body)
        .context("Worker dropped message due to corrupted JSON payload")?;

    execute_business_logic(payload)
        .context("Business logic execution failed during worker loop")?;

    Ok(())
}

```


``` rust 

use tracing::{info, warn};
use tracing_subscriber::{fmt, prelude::*, Registry};

fn main() {
    // 1. Configure the file appender (creates 'app.log' in the './logs' directory)
    // The '_guard' must remain in scope for the entire main function to flush pending logs
    let file_appender = tracing_appender::rolling::never("./logs", "app.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    // 2. Create a formatting layer that writes to the file
    let file_layer = fmt::layer()
        .with_ansi(false) // Disable colors since text files cannot render them
        .with_writer(non_blocking);

    // 3. R
    egister the layer with the Subscriber
    Registry::default()
        .with(file_layer)
        .init();

    // 4. Generate logs
    info!("The application has started successfully.");
    warn!(user_id = 42, "Suspicious activity detected.");
}

```

With this pattern, you don't have a single `match` statement or explicit `error!` macro polluting your code, yet your logs will be perfectly traceable and packed with contextual information.

Which of these two approaches fits better with how your current worker code is structured?


How file destination path is generated?

given filesha, and known extension of files, being .pcm

output_path = <output_directory>/<allocated_bucket>/<file-sha>.<extension>

file-sha is of type UUID4, an example would be like this: 12345678-1234-4567-89ab-1234567890ab

128 bit unique_id's for each files.

Both the input and output files are named after their assigned sha, reasoning being this way we can know which output and input file's corresponds. And sha based tracking is possible. Such, that if workers fail to update db, we can scan directories to infer the processed file, by thier sha.



The error is detailed into logs, but certains errors like related to file transcoding is transfered to the db. 

using json for 



Task type: IO

    Given an job processed state of type in `[failed, success]`, prepare update data pack, serialize and write it to messagepack
    storage file.

An worker atomic usize counter tracks total insert counts in messagepack file, when reaching sync state, a function loads file into memory and sync complete data to database.

Problem to solve are following:

1. How each workers are protected from breaking and stopping, under unkown condition?


2. What eventually causes worker to halt?
Atomic state of queue_completed is the decision maker for whether to stop the queue or not.


3. How db updates generated during the file db sync's are handled?
Given an n numbers of updates available to sync to the db, and x number of new updates waiting to be written into file. 

What we can do is load the file into memory, deserialize into json, sync to db. And then remove from the file by acquiring lock. It will not be possible in our case since mutex won't allow parallel updates, leading to performance penalty.

Using, a header based tracking where header of file handles the binary range of last db sync, latest addition to file, latest file read request range. Is also good choice but need a very delicate dance, cause multi-threading will lead to race conditon. So won't use this too. But we can do is load the details into atomic vals and then process files. Here file represent a cold source of truth, and memory loaded data is what we make mutations to.

Or we can use new file creation at each limit reach, this introduce other problems such as curr tracking, allocation tracking etc. Not suitable too. 

Or we can do hot cold data buffer, processing system doesn't know anything about the file, and file becomes only only push based. Hot memory is what we handle. 




A complete fault tolerance file sync architecture:









Yes. Below is the detailed technical report on the **two-file messagepack hot-cold architecture**, with explicit sections for failure recovery and fault tolerance.

---

# Technical Report: Hot-Cold Messagepack Sync Architecture with Dual-File Durability

## 1. Architecture Overview

The system separates **mutable runtime state** (hot memory) from **durable cold storage** (two messagepack files).

| Component | Type | Mutation Rule | Purpose |
|---|---|---|---|
| **Hot Buffer** (`Hot_A` / `Hot_B`) | In-memory concurrent structure | Swapped atomically; never edited in place by sync | Holds active update batches; serves parallel writers |
| **Atomic Counter** (`usize`) | In-memory atomic | `fetch_add` / `load` | Runtime tracking of total inserts; triggers sync threshold |
| **`updates.msgpack`** | Disk file, messagepack stream | **Append-only** | Durable log of serialized job-state updates (`success` / `failed`) |
| **`watermark.msgpack`** | Disk file, single messagepack object | **Atomic overwrite** (`write` → `rename`) | Durable checkpoint of `synced_offset` and `synced_count` |
| **DB** | External relational/store | Transactional commit | Final persistence target |

The processing system (workers) knows **nothing** about file I/O. They serialize job packs and push them to the hot buffer. A flusher drains batches to `updates.msgpack`. A sync worker reads the file range defined by `watermark.msgpack`, pushes to DB, and updates the watermark only upon DB commit confirmation.

---

## 2. File Design & Serialization

### 2.1 `updates.msgpack` (Cold Data Log)

- **Format**: Stream of messagepack-encoded maps, appended sequentially.
- **Record schema** (example):

```msgpack
{
  "job_id": "uuid-string",
  "state": "success" | "failed",
  "payload": { ... },
  "ts_utc_ns": 1718000000000
}
```

- **Write semantics**: Only `O_APPEND` (or equivalent) writes. No seeks, no truncation, no in-place updates.
- **Durability guarantee**: Once a record is appended and the OS buffer is flushed (`fsync` or equivalent), it survives process crashes.

### 2.2 `watermark.msgpack` (Durable Checkpoint)

- **Format**: Single messagepack map, fully rewritten on every successful sync.
- **Schema**:

```msgpack
{
  "synced_offset": 1024,      // byte offset or record index in updates.msgpack
  "synced_count": 42,         // total number of records confirmed in DB
  "last_sync_ts": 1718000100,
  "checksum": "sha256:..."    // optional integrity hash of updates.msgpack up to offset
}
```

- **Write semantics**: Written to `watermark.tmp.msgpack`, then atomically renamed to `watermark.msgpack`. Readers never observe a partial file.
- **Read semantics**: Loaded into an atomic in-memory mirror (`AtomicUsize` for offset/count) for zero-latency reads during normal operation. The file is read only on startup (recovery).

---

## 3. Hot-Cold Buffer & Memory Management

### 3.1 Double-Buffer Swap (No In-Place Deletion)

When the atomic insert counter reaches the sync threshold (`n` updates ready):

1. **Swap**: `Hot_A` (current active buffer) is atomically exchanged with `Hot_B` (fresh empty buffer). Workers immediately begin writing new updates (`x`) into `Hot_B`.
2. **Snapshot ownership**: The sync thread takes exclusive ownership of `Hot_A`. No other thread holds references to it.
3. **Flush to cold**: The flusher may also append `Hot_A`’s contents to `updates.msgpack` before or during the swap, ensuring durability precedes the snapshot.

### 3.2 Memory Clearing (Leak Prevention)

- **Success path**: After DB commit and `watermark.msgpack` atomic rename, the sync thread drops `Hot_A` (e.g., `Arc::try_unwrap`, `Vec::clear`, or scope exit). Memory is reclaimed by the allocator. There is no lingering shared reference.
- **No partial retention**: Because the durable state lives in `updates.msgpack` and the commit point is in `watermark.msgpack`, memory does not need to retain old batches “just in case.”

---

## 4. Normal Operation Flow

```text
Workers → Serialize pack → Push to Hot_Buffer (B)
Flusher  → Batch drain  → Append to updates.msgpack
Counter  → Atomic fetch_add → When >= THRESHOLD, trigger Sync

Sync Thread:
  1. Load watermark.msgpack → get synced_offset/count (n)
  2. Load updates.msgpack up to n → build Hot_A snapshot
  3. Swap Hot_A ↔ Hot_B (new updates go to B)
  4. Push Hot_A (n updates) to DB (transactional)
  5. DB COMMIT succeeds
  6. Write new watermark.msgpack (atomic rename) with n + len(Hot_A)
  7. Drop Hot_A → memory freed
  8. Update in-memory atomic mirror
```

Concurrent `x` updates are never blocked; they accumulate in `Hot_B` and are appended to `updates.msgpack` independently.

---

## 5. Failure Recovery Scenarios

### 5.1 DB Sync Fails (Rollback Condition)

- **State**: `updates.msgpack` contains the `n` updates (durable). `watermark.msgpack` remains at the old value (unchanged). `Hot_A` is retained by the sync thread.
- **Action**:
  - Do **not** advance the in-memory atomic watermark.
  - Do **not** overwrite `watermark.msgpack`.
  - Retry `Hot_A` (same `n` updates) directly from memory, or reload from `updates.msgpack` using the unchanged watermark.
- **Concurrent `x` updates**: Unaffected. `Hot_B` and new file appends proceed normally. They will form the **next** sync batch after the retry succeeds.

### 5.2 Process Crash During DB Sync

- **Pre-commit crash**: DB transaction is not committed. `watermark.msgpack` is unchanged. On restart:
  1. Read `watermark.msgpack` → recover `synced_offset`.
  2. Load `updates.msgpack` up to that offset.
  3. Rebuild `Hot_A` snapshot.
  4. Re-attempt DB sync for the same `n` updates.
- **Post-commit, pre-watermark crash**: DB has the data, but `watermark.msgpack` was not yet renamed. This creates a **duplicate sync risk**.
  - **Mitigation**: The DB sync should be **idempotent** (e.g., upsert on `job_id`, or use DB-side transaction IDs). Alternatively, include a `sync_batch_id` in both the DB write and the watermark record, allowing the recovery logic to query the DB: “Was batch `B` committed?” If yes, write the watermark and proceed; if no, retry.

### 5.3 Crash During `updates.msgpack` Append

- **Partial append**: If the OS crashes mid-write, the last record in `updates.msgpack` may be truncated or corrupt.
- **Recovery**:
  - Scan `updates.msgpack` sequentially.
  - Validate each messagepack record (length prefix, CRC, or schema check).
  - Stop at the first corrupt/incomplete record; treat the file as valid up to that point.
  - Set `synced_offset` to the last valid record boundary (which should match `watermark.msgpack` if the crash occurred during write, not during sync).
  - Truncate or mark the corrupt tail (optional: use a separate `updates.msgpack.tmp` + rename for batch flushes, though append-only streams usually rely on fsync + validation).

### 5.4 Corruption of `watermark.msgpack`

- **Scenario**: The atomic rename fails or the file is corrupted on disk.
- **Recovery**:
  - If a `.tmp` file exists, ignore it; the old `watermark.msgpack` remains intact.
  - If both are corrupt, fall back to **DB reconciliation**:
    1. Query DB for the maximum `job_id` or `last_sync_ts` present.
    2. Scan `updates.msgpack` from the beginning.
    3. Rebuild `synced_offset` by matching records against DB contents.
  - Because `updates.msgpack` is append-only and never modified, it remains the authoritative cold source; the watermark is only an optimization.

### 5.5 Memory Pressure / OOM During Large Snapshot

- **Risk**: Loading a very large `n` (e.g., millions of updates) into `Hot_A` could exhaust RAM.
- **Fault tolerance**:
  - Cap `n` (threshold) to a bounded batch size (e.g., 10,000 records or 16 MB).
  - If the buffer exceeds the cap, trigger an early sync or split into multiple `Hot_A` batches.
  - The atomic counter triggers sync at the cap, preventing unbounded growth.

---

## 6. Fault Tolerance & Concurrent Safety

| Hazard | Protection Mechanism |
|---|---|
| **Concurrent writes during sync** | Double-buffer swap; writers never touch `Hot_A`. |
| **Concurrent reads of file** | `updates.msgpack` is append-only; readers can stream it without locks. |
| **Race on watermark update** | Only the sync thread writes `watermark.msgpack`; atomic rename ensures readers see either old or new, never partial. |
| **Rollback / retry** | Unchanged `watermark.msgpack` guarantees the same `n` updates are re-selected. |
| **DB partial failure** | Idempotent DB writes or `batch_id` tracking prevent duplicates on retry. |
| **File corruption** | Sequential validation of `updates.msgpack`; `watermark.msgpack` can be reconstructed from DB + file scan. |
| **Crash recovery** | On startup: load `watermark.msgpack` → validate `updates.msgpack` boundary → rebuild hot snapshot → resume. |

---

## 7. Recovery Procedures (Step-by-Step on Restart)

1. **Read checkpoint**  
   Load `watermark.msgpack` into the atomic in-memory mirror (`offset`, `count`).

2. **Validate data file boundary**  
   Open `updates.msgpack`. Scan records sequentially from byte 0 up to `synced_offset`. Confirm messagepack integrity (valid map, expected keys).

3. **Reconstruct hot state**  
   Load all records **after** `synced_offset` into a new `Hot_A` (these are the unsynced `x` updates, if any). If `synced_offset` equals file length, `Hot_A` is empty.

4. **Check DB consistency** (optional but recommended)  
   Query DB for the highest `batch_id` or `synced_offset`. If DB is ahead of `watermark.msgpack` (post-commit crash), update `watermark.msgpack` immediately and drop the redundant snapshot.

5. **Resume normal operation**  
   Initialize `Hot_B` (new empty buffer). Start workers and flusher. Monitor atomic counter for next sync trigger.

---

## 8. Trade-offs & Constraints

| Advantage | Constraint |
|---|---|
| Strict separation of mutable (memory) and durable (file) state | Two files require coordinated backup/restoration |
| Append-only data file eliminates lock contention on writes | `updates.msgpack` grows unbounded; requires periodic truncation/archiving (post-sync, after confirming DB + watermark) |
| Atomic watermark file gives durable rollback point | Must use atomic rename (`write` + `rename`) to prevent partial reads |
| Memory leak eliminated via double-buffer ownership transfer | Large `n` requires bounded batch caps to prevent OOM |
| Crash recovery is deterministic (file scan + DB reconcile) | DB must support idempotent sync or batch IDs to handle post-commit/pre-watermark crashes safely |

### Note on Truncation / Archiving

Because `updates.msgpack` is append-only, it will grow indefinitely. Once `synced_offset` is confirmed durable in both DB and `watermark.msgpack`, you may:

- **Archive**: Rotate the file (`updates.msgpack` → `updates.msgpack.2026-01-01`), start a new empty file, and reset the offset context.
- **Truncate**: In a maintenance window, copy valid post-offset data to a new file and atomically replace. This should **not** be done during active sync or writes.

---

## 9. Summary

The dual-file design (`updates.msgpack` + `watermark.msgpack`) satisfies all constraints from the original design review:

- **No file-header races**: The watermark is a separate object, not embedded in the data stream header.
- **No mutex penalty on parallel updates**: Writers append to one file; only the sync thread touches the other, via atomic rename.
- **Rollback is safe**: A failed DB sync leaves both files unchanged (old watermark, intact data log), allowing exact retry.
- **Memory is cleared safely**: The snapshot (`Hot_A`) is dropped after DB success, with durable state preserved in the file pair.
- **Crash recovery is deterministic**: Restart reads the watermark, validates the data boundary, and resumes without data loss or duplication.

This architecture achieves **fault-tolerant, lock-free concurrent updates** with durable, recoverable synchronization.


///////////////////////////////////////////////////////////////////////////////////

Yes — using **NATS JetStream with file persistence** can replace your entire manual file layer (`updates.msgpack` + `watermark.msgpack` and the atomic counter). It turns your custom append-only log and watermark tracking into a managed stream with durable consumers.

Here is how it maps to your architecture and what changes for failure recovery and fault tolerance.

---

## 1. What JetStream Replaces

| Your Custom Component | JetStream Equivalent | Behavior |
|---|---|---|
| `updates.msgpack` (append-only cold log) | **Stream** (`file` storage) | Durable, segmented, append-only message log. Configurable retention (`Limits` by count/size/age, or `WorkQueue` delete-after-ack). |
| `watermark.msgpack` (durable checkpoint) | **Durable Consumer** state | JetStream persists the consumer’s delivered/acknowledged offset server-side. No manual file needed. |
| Atomic `usize` insert counter | **Stream sequence / message count** | The server tracks message IDs and sequence numbers. |
| Hot buffer (`Hot_A` / `Hot_B`) swap | **Pull consumer batch** | Your sync worker pulls a batch (`n`) into local memory, processes, and acks. New messages are published independently. |
| Manual rollback (unadvanced watermark) | **Negative Ack / No Ack** | If DB sync fails, you do not `Ack` the message (or send `Nak`). JetStream redelivers it. |

---

## 2. Serialization Still Applies

JetStream does not replace your payload format — it carries it.

```msgpack
# Published payload (inside NATS message data field)
{
  "job_id": "...",
  "state": "success" | "failed",
  "ts_utc_ns": 1718000000000,
  ...
}
```

Workers serialize the pack and publish to a subject (e.g., `jobs.processed`). The stream stores the raw bytes.

---

## 3. Failure Recovery & Fault Tolerance with JetStream

### Normal Flow

1. **Publish**: Worker publishes `msgpack` payload to `jobs.processed` → Stream `JOB_LOG` (file-backed).
2. **Consume**: Durable pull consumer (`DB_SYNC`) fetches a batch of `n` messages into local memory (`Hot_A`).
3. **DB Sync**: Push batch to DB transactionally.
4. **Ack**: Only after DB `COMMIT`, the worker sends `Ack` back to JetStream.
5. **Retention**: If stream uses `WorkQueue`, acked messages are removed automatically. If `Limits`, they are evicted by size/age regardless of ack.

### DB Sync Fails (Rollback Condition)

- **Action**: Do not call `Ack`. Optionally send `Nak` with a delay.
- **Result**: Messages remain in the stream at their sequence position. The durable consumer’s delivered state does not advance.
- **Retry**: JetStream redelivers the same `n` messages to the consumer. Your `Hot_A` can be rebuilt from the redelivered batch.
- **Concurrent updates (`x`)**: Publishers continue writing to the stream independently. The consumer will see them only after the failed batch is acked.

### Crash During Sync (Pre-Ack)

- **Consumer crashes** before acking.
- **Recovery**: The durable consumer resumes from the last acked offset (persisted by JetStream). The unacked `n` messages are still in the stream and are redelivered.
- **No manual file scan required**: You do not need to read `updates.msgpack` or `watermark.msgpack` on restart.

### Crash During Sync (Post-DB-Commit, Pre-Ack)

- **DB committed**, but the ack was lost due to a network/consumer crash.
- **Risk**: Duplicate DB writes.
- **Mitigation**: Your DB sync must be **idempotent** (e.g., `INSERT ... ON CONFLICT` or `UPSERT` using `job_id`). The durable consumer will eventually redeliver; the DB ignores the duplicate.
- **Alternative**: Write a `batch_ack_token` to DB in the same transaction, then ack only if the token is new.

### Stream File Growth (The 1M Update Problem)

JetStream handles this internally:

- **File storage**: Uses internal block files and indexes (`blks/`). You do not manage a single 1GB `msgpack`.
- **Retention policy**: Configure `Limits` (`MaxMsgs: 100000`, `MaxBytes: ...`) or use `WorkQueue` so acked messages are deleted. The server manages compaction/deletion.
- **No manual rotation needed**: You do not need to rename `updates.msgpack` or reset offsets. The stream segments and purges transparently.

---

## 4. Hot-Cold Separation with JetStream

In this model, **JetStream is the cold layer**; your application keeps a minimal hot layer only for batching.

| Layer | Implementation |
|---|---|
| **Hot (mutable workset)** | The consumer’s pulled batch (`Hot_A`) in local memory. Not a long-lived queue — just the current `n` messages being processed. |
| **Cold (durable truth)** | The JetStream stream (file-backed). |

Because the stream is external, you no longer need to manage file descriptors, `fsync`, or file rotation. Your code only manages:

- Publishing (fire-and-forget or with confirmation).
- Pulling batches.
- DB transaction + ack/nak.

---

## 5. Trade-offs & Constraints

| Advantage | Cost / Constraint |
|---|---|
| **No manual file management** (`msgpack` rotation, watermark tracking, scanning) | Requires a running **NATS server with JetStream enabled** (`--jetstream`). |
| **Built-in replay and redelivery** | Network dependency; previously your storage was local disk-only. Even on `localhost`, it is a TCP (or Unix socket) hop. |
| **Durable consumer state** survives crashes | Must configure `Durable` consumer explicitly (`DurableName`). Non-durable consumers lose state on disconnect. |
| **Retention policies solve 1M-file bloat** | Must choose correct policy (`Limits` for audit/replay, `WorkQueue` for task-queue deletion). `WorkQueue` deletes after ack — if you need to keep history for DB reconciliation, use `Limits`. |
| **Idempotent DB writes become mandatory** | Because redelivery can happen after a crash or timeout, your DB updates must handle duplicates. |

### Persistence Configuration

For your use case, configure the stream with:

```go
// Conceptual config
StreamConfig: {
  Name: "JOB_UPDATES",
  Subjects: ["jobs.>"],
  Storage: "file",        // NOT memory; survives server restart
  Retention: "limits",    // or "workqueue"
  Discard: "old",
  MaxMsgs: 500000,        // keeps stream bounded; old messages evicted
  MaxAge: "72h",
}
```

If you use `WorkQueue`, the message is deleted **only after all consumers ack it**. If you have only one DB-sync consumer, this behaves like a task queue that cleans up after confirmation.

---

## 6. Revised Architecture Diagram

```
Workers (publish msgpack payloads)
     |
     v
NATS JetStream Stream (file storage, "JOB_UPDATES")
     |
     | Durable Pull Consumer ("DB_SYNC_CONSUMER")
     v
Local Memory Batch (Hot_A, size n)
     |
     v
DB Transaction (UPSERT / Idempotent)
     |
     | Success
     v
Ack -> JetStream (message removed/offset advanced)
```

No `updates.msgpack`. No `watermark.msgpack`. No atomic file counter. The stream and durable consumer replace all three.

---

## 7. When to Keep Your Manual File Approach Instead

Keep the two-file `messagepack` design if:

- You must run **without any external service** (pure local process, no NATS dependency).
- You need **offline portability** (moving a single file to another system without a NATS server).
- Your deployment is **embedded or edge-constrained** where running a JetStream server is too heavy.

Otherwise, JetStream with file persistence eliminates the delicate file-management dance (rotation, header races, watermark durability, recovery scans) and gives you fault-tolerant message delivery out of the box.