# Exception Hierarchy Work - Saved State

## Branch
`exception-changes` (child of `init`)

## What's Done
1. **Exception hierarchy implemented** in `AerospikeException.java`:
   - 15 new exception inner classes with javadoc
   - `Commit` reparented under `TransactionException`
   - `Connection` reparented under `CapacityException`
   - `resultCodeToException` factory expanded to cover all mapped result codes
   - Hierarchy comment added to the factory method

2. **Documentation** created: `client/src/main/java/com/aerospike/client/fluent/ExceptionHierarchy.md`

3. **One commit exists** on the branch (`ad37edc`) but does NOT include the `Connection` reparenting -- that's still uncommitted.

## What's Pending
**Convert call sites to use `resultCodeToException`** -- analysis is complete, changes not yet made:

### Category 1: Variable result codes (highest priority)
These throw `new AerospikeException(rp.resultCode)` or similar with a runtime result code.
Should become `AerospikeException.resultCodeToException(code, message, inDoubt)`.

- `TouchExecutor.java` lines 81, 87
- `ExistsExecutor.java` lines 76, 82
- `DeleteExecutor.java` lines 81, 87
- `ReadExecutor.java` lines 76, 81
- `OperateWriteExecutor.java` lines 77, 82
- `OperateReadExecutor.java` lines 72, 77
- `SyncTxnAddKeysExecutor.java` line 52
- `TxnMarkRollForward.java` line 73
- `TxnClose.java` line 71
- `QueryNodeExecutor.java` line 77
- `NodeExecutor.java` line 211
- `BackgroundQueryNodeExecutor.java` line 66
- `Session.java` lines 1052, 1087, 1182
- `IndexTask.java` line 111
- `Info.java` lines 199, 541
- `AdminCommand.java` lines 146, 155, 187

### Category 2: Explicit constants with mapped subclasses
Could use factory or direct subclass instantiation.

- `OperationSpecExecutor.java` line 650 — `UDF_BAD_RESPONSE`
- `RecordResult.java` line 154 — `OP_NOT_APPLICABLE`
- `IndexQueryBuilderImpl.java` line 82 — `OP_NOT_APPLICABLE`
- `TransactionalSession.java` lines 257, 272 — `TXN_ALREADY_ABORTED`, `TXN_ALREADY_COMMITTED`

### Category 3: No change needed
- `Value.java` (16 sites) — `PARAMETER_ERROR` — unmapped
- `BatchRead.java`, `CommandBuffer.java`, `BatchWrite.java` — `PARAMETER_ERROR`
- `PartitionTracker.java` (5 sites) — `PARAMETER_ERROR`, `PARTITION_UNAVAILABLE`
- `Cluster.java` — `SERVER_NOT_AVAILABLE` — already handled by Connection subclass
- `Node.java` line 831 — already uses `AerospikeException.Connection(NO_MORE_CONNECTIONS, ...)`

## Git State
Branch `exception-changes` — all changes committed (hierarchy classes, Connection reparented under CapacityException, docs).
