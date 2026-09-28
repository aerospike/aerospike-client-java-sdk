# Error handling

This is the contract for `execute()` / `executeAsync()`, `ErrorStrategy.IN_STREAM`, and
`ErrorHandler`. Per-record flags such as `includeMissingKeys` and `failOnFilteredOut` are
detailed in [error-filtering-behavior.md](error-filtering-behavior.md).

There are three channels. Mixing them is what produces “I have to catch in five places.”

## 1. Programming errors

Illegal arguments and builder misuse (null `ErrorStrategy` / `ErrorHandler`, no operations
specified, partition range used twice, …) throw **immediately** from both `execute()` and
`executeAsync()`. These are caller bugs, not database failures.

## 2. Terminal failures

The whole submitted operation cannot run. Examples:

- dataset/index query planning (missing hard-hinted index)
- cluster has no nodes
- query killed / server rejected the query as a whole
- **transaction namespace mismatch**: an explicit transaction or implicit MRT wrapping a
  batch write requires one namespace. `Txn.setNamespace` detects a second namespace
  **client-side**, before any per-key command is sent. The check is at the batch parent, so
  the entire submitted call fails. Result code is not `INVALID_NAMESPACE` (20); it is a
  transaction constraint (today, result code `-1` with a “Namespace must be the same…”
  message).

**Sync `execute()`:** throw `AerospikeException`. `ErrorHandler` and `ErrorStrategy` are
ignored.

**Async `executeAsync()`:** return a `RecordStream` immediately. Cheap argument checks stay
on the calling thread; planning and I/O run on a virtual thread. Failure is
`AsyncRecordStream.error(Throwable)` — a **terminal marker**, not a `RecordResult` row:

- `hasNext()` / `next()` throw
- `asPublisher()` gets `onError`
- `asCompletableFuture*()` **complete exceptionally**

`ErrorHandler` is not invoked. Completing the future with a list of one error record would
look like success.

Dataset/index queries currently produce only successful rows. A non-zero result from the
query protocol terminates the query; `IN_STREAM` must not invent a synthetic error row
(null key, index `-1`).

## 3. Per-key / per-row

Some keys or rows fail; others may succeed.

| How you executed | What you see |
|---|---|
| `execute(IN_STREAM)`, `executeAsync(IN_STREAM)`, default **batch** `execute()` | each key in the stream; check `isOk()` / `orThrow()` |
| `execute(ErrorHandler)` / `executeAsync(handler)` | success rows only; handler gets `(key, index, ex)` |
| single-key `execute()` | throw for **actionable** errors |

**Informational codes** (not `ErrorHandler` events, not throws):

- `KEY_NOT_FOUND` on a point/batch **read**: omitted unless `includeMissingKeys()`. With
  that flag it is always a stream row (single-key `execute()` does not throw; `ErrorHandler`
  is not invoked). Writes always report a per-key outcome, so write `KEY_NOT_FOUND` follows
  the normal per-key error channel.
  See [error-filtering-behavior.md](error-filtering-behavior.md).

**`INVALID_NAMESPACE` (20)** is per-key **only outside a transaction** (including
`.notInAnyTransaction()` and batches that never enter `TxnMonitor`). A mistyped namespace
on one key must not fail the other keys. **Inside** a transaction, mixed namespaces are
channel 2 (terminal), not per-key `INVALID_NAMESPACE`.

## Async return timing

`executeAsync` must not do explain/planning, batch I/O, or `validateNodes` on the caller
after the null/builder checks. The returned handle is the error channel for everything
that happens next.
