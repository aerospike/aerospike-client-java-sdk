# RecordStream Error Filtering Behavior

## Overview

This document specifies how `KEY_NOT_FOUND_ERROR` (result code 2) and `FILTERED_OUT` (result code 27) results are handled across different operation types, error disposition modes, and flag combinations.

## Flags

### `includeMissingKeys`

- **Applies to:** Query (read) operations only
- **Default:** `false`
- **Effect:** When `true`, a `RecordResult` with result code `KEY_NOT_FOUND_ERROR` (2) is included in the stream for keys that don't map to a record. When `false` (default), missing records are silently omitted from the stream.

### `failOnFilteredOut`

- **Applies to:** All operation types
- **Default:** `false`
- **This flag is purely client-side.** It is not sent over the wire protocol. The server *always* returns `FILTERED_OUT` (result code 27) when a filter expression evaluates to false. The `failOnFilteredOut` flag controls how the client handles that response.
- **For single-key operations:** When `false`, `FILTERED_OUT` results are silently dropped (no stream entry, no exception, no error handler invocation). When `true`, `FILTERED_OUT` results are routed through the error disposition.
- **For batch write operations** (upsert, insert, replace, update, UDF): `FILTERED_OUT` results are **always** included in the stream regardless of this flag, because the caller needs complete visibility into the outcome of each key in the batch. The flag has no effect on batch writes.
- **For batch read operations:** Follows the same behavior as single-key — when `false`, `FILTERED_OUT` results are dropped; when `true`, they are included in the stream.

## Error Disposition Modes

| Mode | Description |
|------|-------------|
| **Default (Throw)** | Single-key sync operations with no explicit error strategy. Actionable errors throw an exception. |
| **IN_STREAM** | Errors are embedded as `RecordResult` entries in the `RecordStream`. |
| **Error Handler** | Errors are dispatched to a user-supplied callback. Nothing is placed in the stream for errored records. |

For **batch** operations, the error disposition applies per-record within the batch. Batch operations with default error handling place errors in the stream (they do not throw per-record exceptions).

For **async** operations, errors are placed in the stream (equivalent to IN_STREAM behavior).

---

## Operation Behaviors

### 1. Query (Read)

#### Record does not exist

| Disposition | `includeMissingKeys` = false | `includeMissingKeys` = true |
|---|---|---|
| Default (Throw) | Nothing in stream | `RecordResult` in stream (result code 2) |
| IN_STREAM | Nothing in stream | `RecordResult` in stream (result code 2) |
| Error Handler | Nothing in stream; handler **not** invoked | `RecordResult` in stream (result code 2) |
| Batch | Nothing in stream | `RecordResult` in stream (result code 2) |
| Async | Nothing in stream | `RecordResult` in stream (result code 2) |

#### Record filtered out by expression

| Disposition | `failOnFilteredOut` = false | `failOnFilteredOut` = true |
|---|---|---|
| Default (Throw) | Nothing in stream; no exception | Exception thrown (FILTERED_OUT, code 27) |
| IN_STREAM | Nothing in stream | `RecordResult` in stream (result code 27) |
| Error Handler | Nothing in stream; handler **not** invoked | Handler invoked with FILTERED_OUT |
| Batch | Nothing in stream | `RecordResult` in stream (result code 27) |
| Async | Nothing in stream | `RecordResult` in stream (result code 27) |

---

### 2. Upsert / Insert / Replace / UDF

These operations create the record if it doesn't exist.

#### Record does not exist

The record is created (or, for UDF, executed). A successful `RecordResult` (result code OK) is placed in the stream. `KEY_NOT_FOUND_ERROR` does not arise.

#### Record filtered out by expression

| Disposition | `failOnFilteredOut` = false | `failOnFilteredOut` = true |
|---|---|---|
| Default (Throw) | Nothing in stream; no exception | Exception thrown (FILTERED_OUT, code 27) |
| IN_STREAM | Nothing in stream | `RecordResult` in stream (result code 27) |
| Error Handler | Nothing in stream; handler **not** invoked | Handler invoked with FILTERED_OUT |
| Batch | `RecordResult` in stream (result code 27) | `RecordResult` in stream (result code 27) |
| Async | Nothing in stream | `RecordResult` in stream (result code 27) |

> Note: For single-key operations, `failOnFilteredOut` controls whether the result is surfaced. For batch operations, `FILTERED_OUT` is **always** included in the stream so the caller has complete visibility into the outcome of each key.

---

### 3. Update / Replace-If-Exists

These operations require the record to already exist.

#### Record does not exist

| Disposition | Behavior |
|---|---|
| Default (Throw) | Exception thrown (`KEY_NOT_FOUND_ERROR`, code 2) |
| IN_STREAM | `RecordResult` in stream (result code 2) |
| Error Handler | Nothing in stream; handler invoked with code 2 |
| Batch | `RecordResult` in stream (result code 2) |
| Async | `RecordResult` in stream (result code 2) |

> `includeMissingKeys` has no effect. Missing record errors are always surfaced for these operations.

#### Record filtered out by expression

| Disposition | `failOnFilteredOut` = false | `failOnFilteredOut` = true |
|---|---|---|
| Default (Throw) | Nothing in stream; no exception | Exception thrown (FILTERED_OUT, code 27) |
| IN_STREAM | Nothing in stream | `RecordResult` in stream (result code 27) |
| Error Handler | Nothing in stream; handler **not** invoked | Handler invoked with FILTERED_OUT |
| Batch | `RecordResult` in stream (result code 27) | `RecordResult` in stream (result code 27) |
| Async | Nothing in stream | `RecordResult` in stream (result code 27) |

> Note: For batch operations, `FILTERED_OUT` is **always** included in the stream regardless of `failOnFilteredOut`.

---

### 4. Exists / Delete / Touch

These three operations all follow the same behavior.

#### Record does not exist

| Disposition | Behavior |
|---|---|
| All modes | `RecordResult` in stream with result code 2 (`KEY_NOT_FOUND_ERROR`) |

> `includeMissingKeys` has no effect. A result is **always** placed in the stream for missing records.

#### Record filtered out by expression

The server always returns `FILTERED_OUT` (code 27) when the filter expression doesn't match.

| Disposition | `failOnFilteredOut` = false | `failOnFilteredOut` = true |
|---|---|---|
| Default (Throw) | Nothing in stream; no exception | Exception thrown (FILTERED_OUT, code 27) |
| IN_STREAM | Nothing in stream | `RecordResult` in stream (result code 27) |
| Error Handler | Nothing in stream; handler **not** invoked | Handler invoked with FILTERED_OUT |
| Batch | `RecordResult` in stream (result code 27) | `RecordResult` in stream (result code 27) |
| Async | Nothing in stream | `RecordResult` in stream (result code 27) |

> Note: For batch operations, `FILTERED_OUT` is **always** included in the stream regardless of `failOnFilteredOut`. For single-key, the flag controls behavior as with other operation types.

---

## Operation Type Classification

| OpType | Category | KEY_NOT_FOUND behavior | FILTERED_OUT (single-key) | FILTERED_OUT (batch) |
|---|---|---|---|---|
| *(null / read)* | Query | Depends on `includeMissingKeys` | Depends on `failOnFilteredOut` | Depends on `failOnFilteredOut` |
| `UPSERT` | Upsert | N/A (record created) | Depends on `failOnFilteredOut` | Always included |
| `INSERT` | Upsert | N/A (record created) | Depends on `failOnFilteredOut` | Always included |
| `REPLACE` | Upsert | N/A (record created) | Depends on `failOnFilteredOut` | Always included |
| `UDF` | Upsert | N/A (record created) | Depends on `failOnFilteredOut` | Always included |
| `UPDATE` | Update | Always surfaced | Depends on `failOnFilteredOut` | Always included |
| `REPLACE_IF_EXISTS` | Update | Always surfaced | Depends on `failOnFilteredOut` | Always included |
| `EXISTS` | Exists/Delete/Touch | Always surfaced | Depends on `failOnFilteredOut` | Always included |
| `DELETE` | Exists/Delete/Touch | Always surfaced | Depends on `failOnFilteredOut` | Always included |
| `TOUCH` | Exists/Delete/Touch | Always surfaced | Depends on `failOnFilteredOut` | Always included |

---

## Flag Availability by Builder

| Flag | Query builders | Write builders (upsert, insert, update, etc.) | No-bins builders (exists, delete, touch) |
|---|---|---|---|
| `includeMissingKeys` | Yes | No | No |
| `failOnFilteredOut` | Yes | Yes | Yes |

---

## Filtering Logic Summary

The following pseudocode captures the core filtering decision:

```
shouldIncludeResult(resultCode, opType, includeMissingKeys, failOnFilteredOut, isBatch):
    if resultCode == OK:
        return true

    if resultCode == FILTERED_OUT:
        if isBatch and opType != null:    // Batch writes: always include
            return true
        return failOnFilteredOut          // Single-key or batch reads: flag controls

    if resultCode == KEY_NOT_FOUND_ERROR:
        if opType == null:                // Query (read)
            return includeMissingKeys
        else:                             // Any non-read operation
            return true                   // Always surface KEY_NOT_FOUND

    return true                           // All other errors always included
```

> **Note:** The server always returns `FILTERED_OUT` when a filter expression evaluates to false. The filter expression is only evaluated when the record exists — if the key doesn't exist, the server returns `KEY_NOT_FOUND_ERROR` regardless of any filter expression.

---

## Design Rationale

### Why `failOnFilteredOut` is Client-Side Only

The `failOnFilteredOut` flag is **not** sent over the wire protocol. The server always returns `FILTERED_OUT` (code 27) when a filter expression evaluates to false. The flag purely controls client-side handling: whether to throw an exception, silently drop the result, or include it in the stream. This is distinct from `respondAllKeys`, which *is* a protocol-level flag controlling server batch response behavior.

### Why Batch Writes Always Include `FILTERED_OUT`

For single-key operations, silently swallowing `FILTERED_OUT` (when `failOnFilteredOut=false`) is acceptable. There's only one key; the caller knows which key it was, and "no result" implicitly means "nothing happened."

For batch writes, silently dropping `FILTERED_OUT` is dangerous. The caller sent N keys and expects to understand the outcome of each. If filtered-out results are silently dropped, the result count doesn't match the input count, and the caller might assume the write succeeded when it was actually skipped by the filter. This is a data integrity concern. Therefore, batch writes always include `FILTERED_OUT` results in the stream.

For batch reads, dropping filtered-out results is benign — the caller asked to read records matching a filter and got back the ones that matched.

### Why `includeMissingKeys` Does Not Affect `FILTERED_OUT`

A natural question is whether `includeMissingKeys` should cause `FILTERED_OUT` results to appear in the stream (informatively, without throwing) when `failOnFilteredOut` is not set. We decided against it for two reasons:

1. **`includeMissingKeys` is the wrong semantic lever.** The name and intent of `includeMissingKeys` is about records that don't exist (`KEY_NOT_FOUND_ERROR`). A filtered-out record is not a missing key — the key exists, the record simply didn't pass the expression. Overloading `includeMissingKeys` to also mean "include filtered-out results informatively" would muddy the meaning of both flags.

2. **The existing tools already cover this use case.** If a caller wants to know which records were filtered out without getting exceptions, the combination already exists:
   - `failOnFilteredOut()` + `execute(ErrorStrategy.IN_STREAM)` — `FILTERED_OUT` results appear in the stream, no exceptions thrown.
   - `failOnFilteredOut()` + `execute(errorHandler)` — filtered-out records are dispatched to the handler, nothing placed in the stream.

   The only scenario with no non-throwing option is single-key sync with default (Throw) disposition — but that is the safe default: if you asked to be told about filtered-out records and didn't specify a non-throwing strategy, throwing is the correct behavior.
