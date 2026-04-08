# Read-Only Operations for Query Builders

## Overview

This document describes the implementation of read-only operations (CDT reads, expression-based reads) for query builders in the Aerospike Java SDK. Prior to this change, query operations could only select bins or all record data. Now, queries support the same read operations that update operations support, including:

- **CDT (Collection Data Type) read operations** - Map and list navigation with read-only terminal operations
- **Expression-based reads** - `selectFrom()` operations using AEL expressions
- **Simple bin reads** - `get()` operations

## Key Findings

### Existing Infrastructure Analysis

Before implementing, we analyzed the existing codebase and found several pieces of infrastructure that could be reused:

1. **`BatchRead` class** (`client/src/main/java/com/aerospike/client/sdk/command/BatchRead.java`)
   - Already has a constructor that accepts a `List<Operation> ops` (lines 88-94)
   - Validates that operations are read-only types
   - Perfect for batch key queries with user-specified operations

2. **`OperateReadCommand` class** (`client/src/main/java/com/aerospike/client/sdk/command/OperateReadCommand.java`)
   - Extends `ReadCommand` and takes a `List<Operation> ops` and `OperateArgs`
   - Suitable for single-key read operations with custom operations

3. **`CdtOperationParams` class** (`client/src/main/java/com/aerospike/client/sdk/CdtOperationParams.java`)
   - Already used by `CdtGetOrRemoveBuilder` to store CDT operation parameters
   - Can be reused for read-only CDT operations

4. **`QueryBuilder.operations` list**
   - Already collects operations via `addOperation(Operation op)` method
   - Was being ignored during execution (this was a bug we fixed)

### The Bug

The `selectFrom()` operations added to query builders were being collected in the `operations` list but **completely ignored** during execution by:
- `SingleKeyQueryBuilderImpl` - used `ReadCommand` which doesn't support operations
- `BatchKeyQueryBuilderImpl` - used `BatchRead` constructors that don't take operations
- `OperationSpecExecutor.createBatchRead()` - ignored `spec.getOperations()`

## Code Sharing Strategy

### The Challenge

The existing `CdtGetOrRemoveBuilder` class provides both read and write operations for CDT navigation. For query builders, we needed:
- All the same **read** operations (navigation + get/count/etc.)
- **None** of the write operations (remove, setTo, insert, etc.)
- Compile-time enforcement (not just runtime checks)

### The Solution: `CdtOperationAcceptor` Pattern

We created a functional interface that decouples operation generation from operation storage:

```java
public interface CdtOperationAcceptor<T> {
    void acceptOp(Operation op);
    T getParentBuilder();
}
```

This allows both `CdtGetOrRemoveBuilder` (read/write) and `CdtReadOnlyBuilder` (read-only) to:
1. Generate the same `Operation` objects using identical logic
2. Add them to different targets (operation builder vs query builder)
3. Return different builder types for method chaining

### Interface Hierarchy

```
CdtReadActionBuilder<T>
    └── CdtReadActionInvertableBuilder<T>
            └── CdtReadContextInvertableBuilder<T>
    
CdtReadActionBuilder<T>
    └── CdtReadContextBuilder<T>
            └── CdtReadContextInvertableBuilder<T>
```

Key differences from the read/write interfaces:
- `onMapKey()` returns `CdtReadContextBuilder` instead of a setter builder
- No `setTo()`, `remove()`, `insert()`, `update()`, `add()` methods
- Only `mapSize()` terminal operation (no `mapClear()`, `listAppend()`, etc.)

## Files Created

### 1. `CdtOperationAcceptor.java`
Interface for accepting CDT operations and returning a builder, enabling code sharing between read/write and read-only CDT builders.

### 2. `CdtReadActionBuilder.java`
Read-only terminal operations interface providing:
- `getValues()`, `getKeys()`, `count()`
- `getIndexes()`, `getReverseIndexes()`
- `getRanks()`, `getReverseRanks()`
- `getKeysAndValues()`

### 3. `CdtReadActionInvertableBuilder.java`
Extended interface with "all others" operations:
- `getAllOtherValues()`, `getAllOtherKeys()`, `countAllOthers()`
- `getAllOtherIndexes()`, `getAllOtherReverseIndexes()`
- `getAllOtherRanks()`, `getAllOtherReverseRanks()`
- `getAllOtherKeysAndValues()`

### 4. `CdtReadContextBuilder.java`
Read-only context navigation interface providing map/list navigation methods without any write operations:
- `onMapIndex()`, `onMapKey()`, `onMapRank()`, `onMapValue()`
- `onMapIndexRange()`, `onMapKeyRange()`, `onMapRankRange()`, `onMapValueRange()`
- `onListIndex()`, `onListRank()`, `onListValue()`
- `mapSize()` (read-only terminal)

### 5. `CdtReadContextInvertableBuilder.java`
Extended navigation interface that includes both context navigation and invertable operations.

### 6. `CdtReadOnlyBuilder.java`
Full implementation class (~1000 lines) that:
- Implements all read-only CDT interfaces
- Uses `CdtOperationAcceptor` for operation storage
- Mirrors the logic from `CdtGetOrRemoveBuilder` but excludes all write methods

### 7. `QueryOperationsTest.java`
Comprehensive test suite covering:
- Simple bin reads (`get()`)
- Expression reads (`selectFrom()`)
- CDT map reads (`onMapKey()`, `onMapIndex()`, etc.)
- CDT list reads (`onListIndex()`, `onListRank()`, etc.)
- Batch key queries with operations
- Dataset queries (verify exception is thrown)

## Files Modified

### 1. `CdtOperationParams.java`
- Changed from package-private to `public` for cross-package access

### 2. `CdtGetOrRemoveBuilder.java`
- Changed `CdtOperation` enum from `protected static` to `public static` for use by `CdtReadOnlyBuilder`

### 3. `QueryBinBuilder.java`
Added CDT navigation methods for `ChainableQueryBuilder`:
- Implements `CdtOperationAcceptor<ChainableQueryBuilder>`
- Added `onMapKey()`, `onMapIndex()`, `onMapRank()`, `onMapValue()` methods
- Added `onListIndex()`, `onListRank()`, `onListValue()` methods
- Added range methods (`onMapIndexRange()`, `onMapKeyRange()`, etc.)

### 4. `QueryBuilderBinBuilder.java`
Added CDT navigation methods for `QueryBuilder` (dataset queries):
- Implements `CdtOperationAcceptor<QueryBuilder>`
- Same methods as `QueryBinBuilder`
- Note: These will throw at runtime since server doesn't support operations on scan/index queries

### 5. `SingleKeyQueryBuilderImpl.java`
Modified `executeInternal()` to use `OperateReadExecutor` when operations are present:

```java
List<Operation> ops = qb.getOperations();
Record record;

if (ops != null && !ops.isEmpty()) {
    // Use OperateReadExecutor when there are operations (CDT reads, selectFrom, etc.)
    OperateArgs operateArgs = new OperateArgs(ops);
    OperateReadCommand opCmd = new OperateReadCommand(cluster, partitions, txn, key, ops, operateArgs,
        filterExp, failOnFilteredOut, policy, attr);
    OperateReadExecutor exec = new OperateReadExecutor(cluster, opCmd);
    exec.execute();
    record = exec.getRecord();
} else {
    // Use ReadExecutor for simple bin reads
    ReadCommand cmd = new ReadCommand(cluster, partitions, txn, key, qb.getBinNames(),
        qb.getWithNoBins(), filterExp, failOnFilteredOut, policy, attr);
    ReadExecutor exec = new ReadExecutor(cluster, cmd);
    exec.execute();
    record = exec.getRecord();
}
```

**Important**: The key fix is using `OperateReadExecutor` instead of `ReadExecutor`. The `OperateReadExecutor` calls `cb.setOperateRead(operate)` which properly serializes the operations to the wire protocol. Using `ReadExecutor` with `OperateReadCommand` would ignore the operations because `ReadExecutor` doesn't know how to handle them.

### 6. `BatchKeyQueryBuilderImpl.java`
Modified batch record creation to use operations constructor:

```java
List<Operation> ops = getQueryBuilder().getOperations();

if (ops != null && !ops.isEmpty()) {
    thisBatchRecord = new BatchRead(thisKey, filterExp, ops);
} else if (getQueryBuilder().getWithNoBins()) {
    thisBatchRecord = new BatchRead(thisKey, null, false);
} else if (getQueryBuilder().getBinNames() != null) {
    thisBatchRecord = new BatchRead(thisKey, null, getQueryBuilder().getBinNames());
} else {
    thisBatchRecord = new BatchRead(thisKey, null, true);
}
```

### 7. `IndexQueryBuilderImpl.java`
Added validation to throw clear exception if operations are used on dataset queries:

```java
if (qb.getOperations() != null && !qb.getOperations().isEmpty()) {
    throw new AerospikeException(ResultCode.PARAMETER_ERROR,
        "CDT read operations and expression operations are not currently supported on " +
        "dataset-based queries (scans and secondary index queries). " +
        "Use key-based queries instead: session.query(dataSet.id(key1, key2, ...))");
}
```

### 8. `OperationSpecExecutor.java`
Modified `createBatchRead()` to handle operations from `OperationSpec`:

```java
private static BatchRead createBatchRead(OperationSpec spec, Key key, Expression filterExp) {
    List<Operation> ops = spec.getOperations();
    
    if (ops != null && !ops.isEmpty()) {
        return new BatchRead(key, filterExp, ops);
    } else if (spec.getProjectedBins() != null && spec.getProjectedBins().length > 0) {
        return new BatchRead(key, spec.getWhereClause(), spec.getProjectedBins());
    } else {
        return new BatchRead(key, spec.getWhereClause(), true);
    }
}
```

## Example Usage

### CDT Read Operations on Single Key Query

```java
session.query(key)
    .bin("settings").onMapKey("theme").getValues()
    .bin("scores").onListIndex(0).count()
    .execute();
```

### Expression-Based Reads

```java
session.query(key)
    .bin("ageIn20Years").selectFrom("$.age + 20")
    .bin("doubleScore").selectFrom("$.score * 2")
    .execute();
```

### Mixed Operations

```java
session.query(key)
    .bin("name").get()
    .bin("settings").onMapKey("volume").getValues()
    .bin("computed").selectFrom("$.age + $.bonus")
    .execute();
```

### Batch Key Queries with Operations

```java
session.query(key1, key2, key3)
    .bin("settings").onMapKey("theme").getValues()
    .execute();
```

### Dataset Queries (Not Supported)

```java
// This will throw AerospikeException at execution time
session.query(dataSet)  // scan or index query
    .bin("x").onMapKey("y").getValues()
    .execute();
```

## Future Enhancements

1. **Code Sharing Refactor**: The `CdtReadOnlyBuilder` currently duplicates the operation generation logic from `CdtGetOrRemoveBuilder`. A future enhancement could extract this logic into a shared `CdtReadOperations` static helper class.

2. **Server Support for Dataset Operations**: When the Aerospike server adds support for CDT operations on scan/query results, the `IndexQueryBuilderImpl` restriction can be removed and the operations will work seamlessly.

## Testing

The `QueryOperationsTest` class provides comprehensive coverage:

| Test Category | Tests |
|---------------|-------|
| Simple Bin Reads | `getSingleBin`, `getMultipleBins` |
| Expression Reads | `selectFromSimpleExpression`, `selectFromMultipleExpressions`, `selectFromWithGet` |
| CDT Map Reads | `mapKeyGetValue`, `mapKeyGetCount`, `mapIndexRangeGetValues`, `mapRankGetValue` |
| CDT List Reads | `listIndexGetValue`, `listIndexGetCount`, `listRankGetValue` |
| Batch Key Queries | `batchQueryWithGet`, `batchQueryWithSelectFrom`, `batchQueryWithCdtRead` |
| Dataset Queries | `datasetQueryWithCdtReadThrowsException`, `datasetQueryWithSelectFromThrowsException` |
| Chained Queries | `chainedQueryWithOperations` |
