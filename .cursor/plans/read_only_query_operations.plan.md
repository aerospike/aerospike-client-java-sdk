# Read-Only Operations for Query Builders

## Executive Summary

Currently, query operations (`session.query()`) support only basic bin projection and `selectFrom()` expression reads. However, update operations (`session.update()`) support a rich set of operations including:
- Simple reads: `get()`
- Expression reads: `selectFrom()`
- CDT reads: `onMapKey().getValues()`, `onListIndex().count()`, etc.

**Problem 1 - Execution Bug**: Operations added via `QueryBuilderBinBuilder.selectFrom()` are collected in `QueryBuilder.operations` but **never executed**. The execution paths (`SingleKeyQueryBuilderImpl`, `BatchKeyQueryBuilderImpl`) ignore the operations list entirely.

**Problem 2 - Missing CDT Support**: Query builders have no CDT navigation methods (`onMapKey()`, `onListIndex()`, etc.).

This plan addresses both issues and enables queries to perform all read-only operations.

---

## Current Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           UPDATE PATH (Working)                              │
├─────────────────────────────────────────────────────────────────────────────┤
│  session.update(key)                                                         │
│       │                                                                      │
│       ▼                                                                      │
│  ChainableOperationBuilder                                                   │
│       │                                                                      │
│       ▼  .bin("x")                                                          │
│  BinBuilder ───────────────────────────────────────────────────────────────┐│
│       │  .get()           → Operation.get(binName)                         ││
│       │  .setTo(v)        → Operation.put(bin)                             ││
│       │  .selectFrom(dsl) → ExpOperation.read(...)                         ││
│       │  .onMapKey(k)     → CdtGetOrRemoveBuilder                          ││
│       │       │                                                             ││
│       │       ▼  .getValues()                                              ││
│       │     MapOperation.getByKey(...) → Operation added to OperationSpec  ││
│       └─────────────────────────────────────────────────────────────────────┘│
│       │                                                                      │
│       ▼  .execute()                                                         │
│  OperationSpecExecutor.createBatchWrite() ← uses spec.getOperations()       │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                           QUERY PATH (Broken)                                │
├─────────────────────────────────────────────────────────────────────────────┤
│  session.query(key)                                                          │
│       │                                                                      │
│       ▼                                                                      │
│  QueryBuilder                                                                │
│       │                                                                      │
│       ▼  .bin("x")                                                          │
│  QueryBuilderBinBuilder ─────────────────────────────────────────┐          │
│       │  .get()           → Operation.get(binName)              │ Operations│
│       │  .selectFrom(dsl) → ExpOperation.read(...)              │ are added │
│       │  (NO CDT methods)                                       │ to list   │
│       └──────────────────────────────────────────────────────────┘          │
│       │                                                                      │
│       ▼  .execute()                                                         │
│  SingleKeyQueryBuilderImpl.executeInternal()                                 │
│       │                                                                      │
│       ▼  ReadCommand(cluster, key, binNames, ...)                           │
│          ❌ operations list is IGNORED                                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Proposed Changes

### Part 1: Create Read-Only CDT Interfaces

Create new interfaces that mirror the existing CDT interfaces but expose **only read operations** (no `remove()`, `setTo()`, `insert()`, `update()`, `add()`, `mapClear()`, `listAppend()`, etc.).

#### New Files to Create:

1. **`CdtReadActionBuilder.java`** - Read-only action interface

```java
package com.aerospike.client.fluent;

/**
 * Read-only CDT action interface for query operations.
 * Provides get operations without any write/remove operations.
 */
public interface CdtReadActionBuilder<T> {
    T getValues();
    T getKeys();
    T count();
    T getIndexes();
    T getReverseIndexes();
    T getRanks();
    T getReverseRanks();
    T getKeysAndValues();
    // Note: NO remove() method
}
```

2. **`CdtReadActionInvertableBuilder.java`** - Invertable read-only action interface

```java
package com.aerospike.client.fluent;

/**
 * Read-only CDT action interface with invertable operations.
 */
public interface CdtReadActionInvertableBuilder<T> extends CdtReadActionBuilder<T> {
    T getAllOtherValues();
    T getAllOtherKeys();
    T countAllOthers();
    T getAllOtherIndexes();
    T getAllOtherReverseIndexes();
    T getAllOtherRanks();
    T getAllOtherReverseRanks();
    T getAllOtherKeysAndValues();
    // Note: NO removeAllOthers() method
}
```

3. **`CdtReadContextBuilder.java`** - Read-only context navigation interface

```java
package com.aerospike.client.fluent;

import java.util.List;
import java.util.Map;
import com.aerospike.client.fluent.cdt.ListOrder;

/**
 * Read-only CDT context navigation for query operations.
 * Returns read-only builders instead of setters.
 */
public interface CdtReadContextBuilder<T> extends CdtReadActionBuilder<T> {
    // Map navigation - returns read-only contexts, not setters
    CdtReadContextBuilder<T> onMapIndex(int index);
    CdtReadActionInvertableBuilder<T> onMapIndexRange(int index, int count);
    CdtReadActionInvertableBuilder<T> onMapIndexRange(int index);
    
    CdtReadContextBuilder<T> onMapKey(long key);
    CdtReadContextBuilder<T> onMapKey(String key);
    CdtReadContextBuilder<T> onMapKey(byte[] key);
    
    CdtReadActionInvertableBuilder<T> onMapKeyRange(String startIncl, String endExcl);
    // ... other key range overloads
    
    CdtReadContextBuilder<T> onMapRank(int index);
    CdtReadActionInvertableBuilder<T> onMapRankRange(int rank, int count);
    
    CdtReadContextInvertableBuilder<T> onMapValue(long value);
    CdtReadContextInvertableBuilder<T> onMapValue(String value);
    // ... other value overloads
    
    CdtReadActionInvertableBuilder<T> onMapValueRange(long startIncl, long endExcl);
    // ... other range overloads
    
    CdtReadContextInvertableBuilder<T> onMapKeyList(List<?> keys);
    CdtReadContextInvertableBuilder<T> onMapValueList(List<?> values);
    
    // List navigation
    CdtReadContextBuilder<T> onListIndex(int index);
    CdtReadContextBuilder<T> onListIndex(int index, ListOrder order, boolean pad);
    CdtReadContextBuilder<T> onListRank(int index);
    CdtReadContextInvertableBuilder<T> onListValue(long value);
    CdtReadContextInvertableBuilder<T> onListValue(String value);
    // ... other value overloads
    
    // Read-only operations
    T mapSize();
    // Note: NO mapClear(), listAppend(), listAdd(), etc.
}
```

4. **`CdtReadContextInvertableBuilder.java`** - Invertable version (mirrors CdtReadContextBuilder but extends CdtReadActionInvertableBuilder)

5. **`CdtReadOnlyBuilder.java`** - Implementation class

```java
package com.aerospike.client.fluent;

/**
 * Read-only CDT builder for query operations.
 * Provides all CDT read operations without any write operations.
 */
public class CdtReadOnlyBuilder<T> implements CdtReadContextBuilder<T>, 
                                              CdtReadContextInvertableBuilder<T>,
                                              CdtReadActionInvertableBuilder<T> {
    
    private final String binName;
    private final QueryOperationAcceptor<T> opAcceptor;
    private final CdtOperationParams params;
    
    // Interface to add operations to the query builder
    public interface QueryOperationAcceptor<T> {
        void addOperation(Operation op);
        T getBuilder();
    }
    
    public CdtReadOnlyBuilder(String binName, QueryOperationAcceptor<T> opAcceptor, 
                              CdtOperationParams params) {
        this.binName = binName;
        this.opAcceptor = opAcceptor;
        this.params = params;
    }
    
    @Override
    public T getValues() {
        // Same implementation as CdtGetOrRemoveBuilder.getValues()
        // but uses opAcceptor.addOperation() instead of opBuilder.addOp()
        switch (params.getOperation()) {
        case MAP_BY_KEY:
            opAcceptor.addOperation(MapOperation.getByKey(binName, 
                params.getVal1(), MapReturnType.VALUE, params.context()));
            return opAcceptor.getBuilder();
        // ... other cases
        }
    }
    
    // All other read methods...
    // NO remove(), setTo(), insert(), update(), add() methods
}
```

---

### Part 2: Update Query Bin Builders

Modify `QueryBinBuilder` and `QueryBuilderBinBuilder` to support CDT navigation methods.

#### Changes to `QueryBinBuilder.java`:

```java
public class QueryBinBuilder implements CdtReadOnlyBuilder.QueryOperationAcceptor<ChainableQueryBuilder> {
    private final ChainableQueryBuilder queryBuilder;
    private final String binName;

    // Existing methods: get(), selectFrom()
    
    // NEW: CDT navigation methods
    
    /**
     * Navigate to a map element by index.
     * @param index the index to access
     * @return read-only CDT builder for further navigation or terminal operations
     */
    public CdtReadContextBuilder<ChainableQueryBuilder> onMapIndex(int index) {
        return new CdtReadOnlyBuilder<>(binName, this, 
            new CdtOperationParams(CdtOperation.MAP_BY_INDEX, index));
    }
    
    /**
     * Navigate to a map element by key.
     * @param key the key to access
     * @return read-only CDT builder for further navigation or terminal operations
     */
    public CdtReadContextBuilder<ChainableQueryBuilder> onMapKey(String key) {
        return new CdtReadOnlyBuilder<>(binName, this,
            new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }
    
    public CdtReadContextBuilder<ChainableQueryBuilder> onMapKey(long key) {
        return new CdtReadOnlyBuilder<>(binName, this,
            new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }
    
    // ... all other CDT navigation methods (similar to BinBuilder)
    
    public CdtReadContextBuilder<ChainableQueryBuilder> onListIndex(int index) {
        return new CdtReadOnlyBuilder<>(binName, this,
            new CdtOperationParams(CdtOperation.LIST_BY_INDEX, index));
    }
    
    // QueryOperationAcceptor implementation
    @Override
    public void addOperation(Operation op) {
        queryBuilder.addOperation(op);
    }
    
    @Override
    public ChainableQueryBuilder getBuilder() {
        return queryBuilder;
    }
}
```

Similar changes for `QueryBuilderBinBuilder.java`.

---

### Part 3: Fix Execution Paths

#### 3.1 Fix `SingleKeyQueryBuilderImpl`

When operations exist, use `operate` instead of simple read:

```java
private RecordStream executeInternal() {
    // ... existing setup code ...
    
    List<Operation> operations = qb.getOperations();
    
    if (operations != null && !operations.isEmpty()) {
        // Use operate path when operations are present
        OperateCommand cmd = new OperateCommand(cluster, partitions, txn, key, 
            operations, filterExp, failOnFilteredOut, policy, attr);
        OperateExecutor exec = new OperateExecutor(cluster, cmd);
        exec.execute();
        Record record = exec.getRecord();
        // ... result handling
    } else {
        // Existing simple read path
        ReadCommand cmd = new ReadCommand(cluster, partitions, txn, key, qb.getBinNames(),
            qb.getWithNoBins(), filterExp, failOnFilteredOut, policy, attr);
        // ...
    }
}
```

#### 3.2 Fix `BatchKeyQueryBuilderImpl`

When operations exist, use `BatchOperate` instead of `BatchRead`:

```java
public RecordStream executeInternal() {
    // ... existing setup code ...
    
    List<Operation> operations = qb.getOperations();
    boolean hasOperations = operations != null && !operations.isEmpty();
    
    for (Key thisKey : keyList) {
        // ... partition filtering ...
        
        BatchRecord thisBatchRecord;
        if (hasOperations) {
            // Use BatchOperate with operations
            thisBatchRecord = new BatchOperate(thisKey, spec.getWhereClause(), 
                operations.toArray(new Operation[0]));
        } else if (getQueryBuilder().getWithNoBins()) {
            thisBatchRecord = new BatchRead(thisKey, null, false);
        } else if (getQueryBuilder().getBinNames() != null) {
            thisBatchRecord = new BatchRead(thisKey, null, getQueryBuilder().getBinNames());
        } else {
            thisBatchRecord = new BatchRead(thisKey, null, true);
        }
        batchRecordsForServer.add(thisBatchRecord);
    }
    // ... rest of execution
}
```

#### 3.3 Fix `IndexQueryBuilderImpl` (Dataset Queries)

For dataset queries, throw an exception until Aerospike supports operate on queries:

```java
private RecordStream executeInternal() {
    List<Operation> operations = qb.getOperations();
    
    if (operations != null && !operations.isEmpty()) {
        throw new AerospikeException(ResultCode.PARAMETER_ERROR,
            "Operations on dataset queries (scans/secondary index queries) are not yet " +
            "supported by the Aerospike server. This feature will be available in a future " +
            "release. For now, use key-based queries with operations, or use selectFrom() " +
            "expression reads which are supported on dataset queries.");
    }
    
    // ... existing query execution
}
```

#### 3.4 Fix `OperationSpecExecutor.createBatchRead()`

Update to use operations when present:

```java
private static BatchRecord createBatchRead(OperationSpec spec, Key key, Expression filterExp) {
    List<Operation> operations = spec.getOperations();
    
    if (operations != null && !operations.isEmpty()) {
        // Use BatchOperate when operations are present
        return new BatchOperate(key, spec.getWhereClause(), 
            operations.toArray(new Operation[0]));
    } else if (spec.getProjectedBins() != null && spec.getProjectedBins().length > 0) {
        return new BatchRead(key, spec.getWhereClause(), spec.getProjectedBins());
    } else {
        return new BatchRead(key, spec.getWhereClause(), true);
    }
}
```

---

### Part 4: Files to Modify/Create

| File | Action | Description |
|------|--------|-------------|
| `CdtReadActionBuilder.java` | CREATE | Read-only action interface |
| `CdtReadActionInvertableBuilder.java` | CREATE | Invertable read-only action interface |
| `CdtReadContextBuilder.java` | CREATE | Read-only context navigation interface |
| `CdtReadContextInvertableBuilder.java` | CREATE | Invertable context navigation interface |
| `CdtReadOnlyBuilder.java` | CREATE | Implementation of read-only CDT operations |
| `QueryBinBuilder.java` | MODIFY | Add CDT navigation methods |
| `QueryBuilderBinBuilder.java` | MODIFY | Add CDT navigation methods |
| `SingleKeyQueryBuilderImpl.java` | MODIFY | Use operate when operations present |
| `BatchKeyQueryBuilderImpl.java` | MODIFY | Use BatchOperate when operations present |
| `IndexQueryBuilderImpl.java` | MODIFY | Throw exception if operations present |
| `OperationSpecExecutor.java` | MODIFY | Handle operations in createBatchRead() |

---

## Example Code (Post-Implementation)

### Basic CDT Read Operations

```java
// Get a map value by key
RecordStream results = session.query(dataSet.id(1))
    .bin("settings").onMapKey("theme").getValues()
    .execute();

// Get multiple items from a list by index range
RecordStream results = session.query(dataSet.id(1))
    .bin("scores").onListIndex(0).getValues()
    .bin("scores").onMapIndexRange(0, 5).count()
    .execute();

// Nested map navigation
RecordStream results = session.query(dataSet.id(1))
    .bin("user").onMapKey("preferences").onMapKey("notifications").getValues()
    .execute();
```

### Combined Operations

```java
// Mix of simple reads, expression reads, and CDT reads
RecordStream results = session.query(dataSet.ids(1, 2, 3))
    .bin("name").get()
    .bin("ageIn20Years").selectFrom("$.age + 20")
    .bin("settings").onMapKey("theme").getValues()
    .bin("scores").onListIndex(0).getValues()
    .execute();
```

### Chainable Query Operations

```java
// Use CDT reads in chained queries
RecordStream results = session
    .upsert(dataSet.id(100))
        .bin("name").setTo("Alice")
    .query(dataSet.ids(1, 2, 3))
        .bin("settings").onMapKey("theme").getValues()
        .bin("scores").onMapIndexRange(0, 3).getValues()
    .execute();
```

### Dataset Query (Throws Exception Until Server Support)

```java
// This will throw an exception until Aerospike server supports it
try {
    RecordStream results = session.query(dataSet)  // Dataset query (scan)
        .where("$.age > 21")
        .bin("settings").onMapKey("theme").getValues()  // CDT operation
        .execute();
} catch (AerospikeException e) {
    // "Operations on dataset queries are not yet supported..."
}

// But expression reads work on dataset queries
RecordStream results = session.query(dataSet)
    .where("$.age > 21")
    .bin("computed").selectFrom("$.age + 20")  // This works
    .execute();
```

---

## Testing Strategy

1. **Unit Tests for New Interfaces**: Verify read-only interfaces don't expose write methods
2. **Integration Tests for Key-Based Queries**: Test CDT reads with single key, batch keys
3. **Integration Tests for Chainable Queries**: Test CDT reads in chained operations
4. **Exception Tests for Dataset Queries**: Verify exception is thrown with clear message
5. **Regression Tests**: Ensure existing functionality still works

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Breaking existing code | High | Extensive regression testing; interfaces are additive |
| Performance regression | Medium | Benchmark before/after; operations path may be slower |
| API complexity | Medium | Clear documentation; consistent naming with existing CDT interfaces |
| Future server compatibility | Low | Exception message indicates temporary; easy to enable later |

---

## Infrastructure Verification (Completed)

### Key Finding: All Required Infrastructure Already Exists!

**1. `BatchRead` already supports operations:**
```java
// BatchRead.java line 88-94
public BatchRead(Key key, Expression filterExp, List<Operation> ops) {
    super(key, false);
    this.filterExp = filterExp;
    this.binNames = null;
    this.ops = ops;
    this.readAllBins = false;
}
```

And it validates read-only at line 141-143:
```java
if (op.type.isWrite) {
    throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Write operations not allowed in batch read");
}
```

**2. Single-key operate command exists:**
- `OperateReadCommand` extends `ReadCommand` and takes `List<Operation> ops` and `OperateArgs`
- `OperateArgs` analyzes operations to set correct wire protocol flags

**3. `CdtOperationParams` and `CdtOperation` can be reused:**
- `CdtOperationParams` is package-private but in `com.aerospike.client.fluent` - same package as query builders
- `CdtOperation` enum is defined in `CdtGetOrRemoveBuilder` 

### Code Sharing Strategy

Instead of duplicating the CDT operation generation code, we'll use a **functional interface approach**:

1. Create a `CdtOperationAcceptor<T>` interface:
```java
public interface CdtOperationAcceptor<T> {
    void addOp(Operation op);
    T getBuilder();
}
```

2. Extract CDT read operation generation to a static helper class `CdtReadOperations`:
```java
public class CdtReadOperations {
    public static <T> T getValues(String binName, CdtOperationParams params, 
                                   CdtOperationAcceptor<T> acceptor) {
        switch (params.getOperation()) {
        case MAP_BY_KEY:
            acceptor.addOp(MapOperation.getByKey(binName, params.getVal1(), 
                MapReturnType.VALUE, params.context()));
            return acceptor.getBuilder();
        // ... other cases - same logic as CdtGetOrRemoveBuilder.getValues()
        }
    }
    
    public static <T> T getKeys(String binName, CdtOperationParams params,
                                 CdtOperationAcceptor<T> acceptor) { ... }
    public static <T> T count(String binName, CdtOperationParams params,
                               CdtOperationAcceptor<T> acceptor) { ... }
    // ... all other read operations
}
```

3. `CdtGetOrRemoveBuilder` implements `CdtOperationAcceptor<T>` and delegates to `CdtReadOperations`:
```java
public class CdtGetOrRemoveBuilder<T extends AbstractOperationBuilder<T>> 
    extends AbstractCdtBuilder<T> 
    implements CdtOperationAcceptor<T>, ... {
    
    @Override
    public void addOp(Operation op) {
        opBuilder.addOp(op);
    }
    
    @Override
    public T getBuilder() {
        return opBuilder;
    }
    
    @Override
    public T getValues() {
        return CdtReadOperations.getValues(binName, params, this);
    }
    // ... other methods delegate to CdtReadOperations
}
```

4. `CdtReadOnlyBuilder<T>` implements `CdtOperationAcceptor<T>` and delegates to same `CdtReadOperations`:
```java
public class CdtReadOnlyBuilder<T> implements CdtOperationAcceptor<T>,
                                               CdtReadContextBuilder<T>, ... {
    private final QueryOperationAcceptor<T> queryAcceptor;
    
    @Override
    public void addOp(Operation op) {
        queryAcceptor.addOperation(op);
    }
    
    @Override
    public T getBuilder() {
        return queryAcceptor.getBuilder();
    }
    
    @Override
    public T getValues() {
        return CdtReadOperations.getValues(binName, params, this);
    }
    // ... only read methods, NO remove/setTo/insert/update/add
}
```

This approach:
- **Zero code duplication** for read operations
- `CdtGetOrRemoveBuilder` keeps all its existing functionality
- `CdtReadOnlyBuilder` exposes only read methods
- Compile-time enforcement of read-only operations on queries

---

## Updated Files Impact

| File | Action | Description |
|------|--------|-------------|
| `CdtOperationAcceptor.java` | **CREATE** | Functional interface for operation acceptance |
| `CdtReadOperations.java` | **CREATE** | Static helper with all read operation implementations |
| `CdtReadActionBuilder.java` | **CREATE** | Read-only action interface |
| `CdtReadActionInvertableBuilder.java` | **CREATE** | Invertable read-only action interface |
| `CdtReadContextBuilder.java` | **CREATE** | Read-only context navigation interface |
| `CdtReadContextInvertableBuilder.java` | **CREATE** | Invertable context navigation interface |
| `CdtReadOnlyBuilder.java` | **CREATE** | Read-only CDT builder implementation |
| `CdtGetOrRemoveBuilder.java` | **MODIFY** | Implement `CdtOperationAcceptor`, delegate reads to `CdtReadOperations` |
| `QueryBinBuilder.java` | **MODIFY** | Add CDT navigation methods |
| `QueryBuilderBinBuilder.java` | **MODIFY** | Add CDT navigation methods |
| `SingleKeyQueryBuilderImpl.java` | **MODIFY** | Use `OperateReadCommand` when operations present |
| `BatchKeyQueryBuilderImpl.java` | **MODIFY** | Use `BatchRead(key, filterExp, ops)` when operations present |
| `IndexQueryBuilderImpl.java` | **MODIFY** | Throw exception if operations present |
| `OperationSpecExecutor.java` | **MODIFY** | Use `BatchRead` with ops in `createBatchRead()` |

---

## Execution Path Fixes (Simplified)

### Single-key queries with operations:
```java
// SingleKeyQueryBuilderImpl.executeInternal()
if (operations != null && !operations.isEmpty()) {
    OperateArgs args = new OperateArgs(operations);
    OperateReadCommand cmd = new OperateReadCommand(cluster, partitions, txn, key, 
        operations, args, filterExp, failOnFilteredOut, policy, attr);
    // ... execute and handle result
}
```

### Batch-key queries with operations:
```java
// BatchKeyQueryBuilderImpl.executeInternal()
if (hasOperations) {
    thisBatchRecord = new BatchRead(thisKey, spec.getWhereClause(), operations);
} else if (...) {
    // existing logic
}
```

### OperationSpecExecutor fix:
```java
// createBatchRead()
List<Operation> operations = spec.getOperations();
if (operations != null && !operations.isEmpty()) {
    return new BatchRead(key, spec.getWhereClause(), operations);
}
// ... existing logic
```
