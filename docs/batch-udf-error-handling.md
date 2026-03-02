# Batch UDF Error Handling Issue

## Problem

The `batchUDFError` and `batchUDFComplex` tests in `UdfTest.java` fail because
batch UDF operations throw an exception instead of embedding per-row errors in
the `RecordStream`.  Both tests already carry TODO comments describing the
expected (but not yet implemented) behaviour.

```java
// batchUDFComplex – third executeUdf is designed to fail
RecordStream rs = session
    .executeUdf(key1)
        .function("record_example", "writeBin")
        .passing(binName, "value1")
    .executeUdf(key2)
        .function("record_example", "writeWithValidation")
        .passing(binName, 5)
    .executeUdf(key2)
        .function("record_example", "writeWithValidation")
        .passing(binName, 999)   // ← fails by design
    .execute();                  // ← throws AerospikeException instead of returning a stream
```

The `defaultDisposition` for a multi-key / multi-spec batch is `IN_STREAM`, so
errors should be embedded in the stream rather than thrown.  The first two UDF
calls succeed; only the third should carry `UDF_BAD_RESPONSE`.

## Analysis

### How a batch is executed

1. `ChainableUdfBuilder.execute()` calls `OperationSpecExecutor.execute()`.
2. Individual `BatchUDF` records are built for each key/spec.
3. All records are grouped into `BatchNode`(s) and wrapped in a
   `Batch.OperateListSync` command.
4. `BatchExecutor.execute()` runs the command(s) synchronously.
5. After `BatchExecutor` returns, `OperationSpecExecutor` iterates the
   `BatchRecord` list and builds a `RecordStream`, respecting the error
   disposition (`IN_STREAM` / `THROW` / `HANDLER`).

### Where the exception originates

`OperateListSync` extends `BatchNodeExecutor` → `NodeExecutor` →
`SyncExecutor`.  The call chain is:

```
SyncExecutor.execute()
  → SyncExecutor.executeCommand()
      → NodeExecutor.parseResult()          // reads response from server
          → NodeExecutor.parseGroup()        // iterates rows in a response group
              → OperateListSync.parseRow()   // per-row dispatch
```

**`parseRow()` handles `UDF_BAD_RESPONSE` correctly** – it stores the error on
the `BatchRecord`, calls `status.setRowError()`, and returns `true` without
throwing.  So individual row results are populated correctly before the batch
end marker arrives.

**`parseGroup()` is where the exception is thrown.**  After all rows are parsed,
the server sends an end marker (`INFO3_LAST`).  The code in
`NodeExecutor.parseGroup()` (line ~208) does:

```java
if ((info3 & Command.INFO3_LAST) != 0) {
    if (resultCode != 0) {
        // The server returned a fatal error.
        throw new AerospikeException(resultCode);
    }
    return false;
}
```

**Hypothesis:** The Aerospike server sets a non-zero result code
(`UDF_BAD_RESPONSE`) in the batch end marker when *any* UDF in the batch fails,
even though the per-row responses already carry the error.  This causes
`parseGroup()` to throw.

The exception then propagates:

```
NodeExecutor.parseGroup()          throws AerospikeException(UDF_BAD_RESPONSE)
  → caught in SyncExecutor.executeCommand() catch(AerospikeException)
      → resultCode is not TIMEOUT / DEVICE_OVERLOAD / KEY_BUSY
      → re-thrown
  → caught in BatchExecutor.execute()
      → status.setException(ae)
  → status.checkException()                re-throws stored exception
  → escapes OperationSpecExecutor.execute() before the record-processing loop
```

The already-populated row results are never placed into the `RecordStream`.

### Why `setRowError()` alone doesn't cause a throw

The fluent client's `BatchStatus.checkException()` only checks
`this.exception != null`; it does **not** check the `error` flag set by
`setRowError()`:

```java
// Fluent client
public void checkException() {
    if (exception != null) {
        exception.setSubExceptions(subExceptions);
        throw exception;
    }
}
```

Compare with the standard Aerospike Java client, which *also* throws on
`error`:

```java
// Standard client
public void checkException() {
    if (exception != null) { ... throw BatchRecordArray(exception, records); }
    if (error) { throw new AerospikeException.BatchRecordArray(records); }
}
```

So in the fluent client, row-level errors from `setRowError()` would
**not** trigger an exception by themselves.  The exception must be coming
from `command.execute()` (i.e. the `parseGroup()` end-marker throw).

### Supporting evidence

- The `batchUDFError` test wraps the call in `assertThrows(AerospikeException.class, ...)`
  and asserts the result code is `UDF_BAD_RESPONSE` – consistent with the
  end-marker hypothesis.
- Non-UDF batch operations (writes, reads, deletes) with row-level errors
  (e.g. `KEY_EXISTS_ERROR`) do **not** exhibit this problem, suggesting
  the server only sets a non-zero end marker for UDF errors.

## Proposed Fix

Wrap `BatchExecutor.execute()` in a try-catch inside
`OperationSpecExecutor.execute()` (the multi-key batch path, ~line 324):

```java
try {
    BatchExecutor.execute(cluster, commands, status);
}
catch (AerospikeException ae) {
    // Populate any records still in NO_RESPONSE state with the exception info,
    // then fall through to the normal disposition logic.
    for (BatchRecord br : records) {
        if (br.resultCode == ResultCode.NO_RESPONSE) {
            br.setError(ae.getResultCode(), ae.getInDoubt());
        }
    }
}

// existing record-processing loop with disposition handling follows unchanged
```

This way:

- **Records already parsed by `parseRow()`** keep their individual result codes
  (OK, UDF_BAD_RESPONSE, etc.).  The `NO_RESPONSE` check finds nothing to
  update for these.
- **Records not yet parsed** (e.g. if a genuine transport error interrupted
  mid-batch) get the exception's result code applied.
- The existing disposition logic (`IN_STREAM` / `THROW` / `HANDLER`) then
  handles them as designed.

### Test changes required

`batchUDFError` would need to be rewritten: instead of `assertThrows`, it
should iterate the stream and assert `UDF_BAD_RESPONSE` on each row.

`batchUDFComplex` would work as-is – the TODO can be removed and the existing
assertions (first two rows OK, third row `UDF_BAD_RESPONSE`) should pass.

### Open questions

1. **Is the hypothesis correct?**  Does the server actually send
   `UDF_BAD_RESPONSE` in the batch end marker?  A packet capture or
   server-side trace would confirm this definitively.
2. **Should `parseGroup()` be changed instead?**  Rather than catching in
   `OperationSpecExecutor`, should `NodeExecutor.parseGroup()` (or
   `BatchNodeExecutor`) suppress the end-marker throw for row-level error
   codes like `UDF_BAD_RESPONSE`?  This would fix the problem closer to the
   source but requires knowing which result codes are "row-level" vs
   "batch-level".
3. **Should `ObjectBuilder` and `BinsValuesBuilder` get the same treatment?**
   They also call `BatchExecutor.execute()` without a try-catch, but they
   don't handle UDF operations so they're less likely to hit this.  For
   robustness they could be wrapped too.

## Files involved

| File | Role |
|---|---|
| `NodeExecutor.java` ~line 208 | `parseGroup()` throws on non-zero end-marker resultCode |
| `SyncExecutor.java` ~line 134 | catches and re-throws non-retryable `AerospikeException` |
| `BatchExecutor.java` ~line 26 | catches, stores via `status.setException()`, re-throws via `checkException()` |
| `Batch.java` `OperateListSync.parseRow()` | correctly handles `UDF_BAD_RESPONSE` per-row (no throw) |
| `OperationSpecExecutor.java` ~line 324 | **proposed fix location** – wrap `BatchExecutor.execute()` in try-catch |
| `UdfTest.java` `batchUDFError` / `batchUDFComplex` | tests documenting the issue with TODOs |
