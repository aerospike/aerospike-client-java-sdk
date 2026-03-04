# API Properties Analysis

This document catalogs all configurable properties available on Session methods and their builders, showing the path to each attribute and verifying property propagation.

**Last Updated:** All property propagation issues resolved

**Status:** ✅ All identified issues have been fixed

## Entry Points from Session

### Write Operations (Key-based)
| Method | Returns | Description |
|--------|---------|-------------|
| `session.insert(Key)` | `ChainableOperationBuilder` | Insert single key |
| `session.insert(List<Key>)` | `ChainableOperationBuilder` | Insert multiple keys |
| `session.insert(Key, Key, Key...)` | `ChainableOperationBuilder` | Insert varargs keys |
| `session.update(Key)` | `ChainableOperationBuilder` | Update single key |
| `session.update(List<Key>)` | `ChainableOperationBuilder` | Update multiple keys |
| `session.update(Key, Key, Key...)` | `ChainableOperationBuilder` | Update varargs keys |
| `session.upsert(Key)` | `ChainableOperationBuilder` | Upsert single key |
| `session.upsert(List<Key>)` | `ChainableOperationBuilder` | Upsert multiple keys |
| `session.upsert(Key, Key, Key...)` | `ChainableOperationBuilder` | Upsert varargs keys |
| `session.replace(Key)` | `ChainableOperationBuilder` | Replace single key |
| `session.replace(List<Key>)` | `ChainableOperationBuilder` | Replace multiple keys |
| `session.replace(Key, Key, Key...)` | `ChainableOperationBuilder` | Replace varargs keys |
| `session.replaceIfExists(Key)` | `ChainableOperationBuilder` | Replace if exists single key |
| `session.replaceIfExists(List<Key>)` | `ChainableOperationBuilder` | Replace if exists multiple keys |
| `session.replaceIfExists(Key, Key, Key...)` | `ChainableOperationBuilder` | Replace if exists varargs keys |

### No-Bins Operations (Key-based)
| Method | Returns | Description |
|--------|---------|-------------|
| `session.touch(Key)` | `ChainableNoBinsBuilder` | Touch single key |
| `session.touch(List<Key>)` | `ChainableNoBinsBuilder` | Touch multiple keys |
| `session.touch(Key, Key, Key...)` | `ChainableNoBinsBuilder` | Touch varargs keys |
| `session.exists(Key)` | `ChainableNoBinsBuilder` | Check exists single key |
| `session.exists(List<Key>)` | `ChainableNoBinsBuilder` | Check exists multiple keys |
| `session.exists(Key, Key, Key...)` | `ChainableNoBinsBuilder` | Check exists varargs keys |
| `session.delete(Key)` | `ChainableNoBinsBuilder` | Delete single key |
| `session.delete(List<Key>)` | `ChainableNoBinsBuilder` | Delete multiple keys |
| `session.delete(Key, Key, Key...)` | `ChainableNoBinsBuilder` | Delete varargs keys |

### Query Operations (Key-based)
| Method | Returns | Description |
|--------|---------|-------------|
| `session.query(Key)` | `ChainableQueryBuilder` | Query single key |
| `session.query(List<Key>)` | `ChainableQueryBuilder` | Query multiple keys |
| `session.query(Key, Key, Key...)` | `ChainableQueryBuilder` | Query varargs keys |

### Dataset Operations
| Method | Returns | Description |
|--------|---------|-------------|
| `session.query(DataSet)` | `QueryBuilder` | Scan/index query on dataset |
| `session.insert(DataSet)` | `OperationObjectBuilder` | Insert objects into dataset |
| `session.upsert(DataSet)` | `OperationObjectBuilder` | Upsert objects into dataset |
| `session.update(DataSet)` | `OperationObjectBuilder` | Update objects in dataset |
| `session.insert(TypeSafeDataSet<T>)` | `OperationObjectBuilder<T>` | Type-safe insert |
| `session.upsert(TypeSafeDataSet<T>)` | `OperationObjectBuilder<T>` | Type-safe upsert |
| `session.update(TypeSafeDataSet<T>)` | `OperationObjectBuilder<T>` | Type-safe update |

### Background Operations
| Method | Returns | Description |
|--------|---------|-------------|
| `BackgroundTaskSession.upsert(DataSet)` | `BackgroundOperationBuilder` | Background upsert |
| `BackgroundTaskSession.update(DataSet)` | `BackgroundOperationBuilder` | Background update |
| `BackgroundTaskSession.delete(DataSet)` | `BackgroundOperationBuilder` | Background delete |

---

## Properties by Builder Type

### ChainableOperationBuilder Properties

```
session.upsert(key)
    ├── .bin(name).setTo(value)          // Bin operations
    ├── .bins(name, ...).values(...)     // Bulk bin pattern → BinsValuesBuilder
    │
    ├── .expireRecordAfter(Duration)     // Per-record expiration
    ├── .expireRecordAfterSeconds(int)   // Per-record expiration
    ├── .expireRecordAt(Date)            // Per-record expiration
    ├── .expireRecordAt(LocalDateTime)   // Per-record expiration
    ├── .withNoChangeInExpiration()      // TTL = -2
    ├── .neverExpire()                   // TTL = -1
    ├── .expiryFromServerDefault()       // TTL = 0
    │
    ├── .ensureGenerationIs(int)         // Generation check
    │
    ├── .where(String, Object...)        // Filter expression
    ├── .where(BooleanExpression)        // Filter expression
    ├── .where(PreparedDsl, Object...)   // Filter expression
    ├── .where(Exp)                      // Filter expression
    ├── .where(Expression)               // Filter expression
    │
    ├── .failOnFilteredOut()             // Fail if filtered
    ├── .includeMissingKeys()                // Include all keys in response
    │
    ├── .notInAnyTransaction()           // Override session transaction
    ├── .inTransaction(Txn)              // Use specific transaction
    ├── .sendKey()                       // Send key to server
    │
    ├── .delete(key)                     // Chain → ChainableNoBinsBuilder
    ├── .touch(key)                      // Chain → ChainableNoBinsBuilder
    ├── .exists(key)                     // Chain → ChainableNoBinsBuilder
    ├── .query(key)                      // Chain → ChainableQueryBuilder
    ├── .upsert(key2)                    // Chain another write operation
    │
    └── .execute()                       // Execute operations
```

### ChainableNoBinsBuilder Properties

```
session.delete(key)
    ├── .expireRecordAfter(Duration)     // Per-record expiration (for touch)
    ├── .expireRecordAfterSeconds(int)   // Per-record expiration (for touch)
    ├── .expireRecordAt(Date)            // Per-record expiration (for touch)
    ├── .expireRecordAt(LocalDateTime)   // Per-record expiration (for touch)
    ├── .withNoChangeInExpiration()      // TTL = -2 (for touch)
    ├── .neverExpire()                   // TTL = -1 (for touch)
    │
    ├── .ensureGenerationIs(int)         // Generation check
    │
    ├── .where(String, Object...)        // Filter expression
    ├── .where(BooleanExpression)        // Filter expression
    ├── .where(PreparedDsl, Object...)   // Filter expression
    ├── .where(Exp)                      // Filter expression
    ├── .where(Expression)               // Filter expression
    │
    ├── .failOnFilteredOut()             // Fail if filtered
    ├── .includeMissingKeys()                // Include all keys in response
    │
    ├── .notInAnyTransaction()           // Override session transaction
    ├── .inTransaction(Txn)              // Use specific transaction
    │
    ├── .durablyDelete()                 // Durable delete (delete only)
    │
    ├── .delete(key2)                    // Chain another delete
    ├── .touch(key2)                     // Chain touch
    ├── .exists(key2)                    // Chain exists
    ├── .query(key2)                     // Chain → ChainableQueryBuilder
    ├── .upsert(key2)                    // Chain → ChainableOperationBuilder
    │
    └── .execute()                       // Execute operations
```

### ChainableQueryBuilder Properties

```
session.query(key)
    ├── .bins(name, ...)                 // Specify bins to read
    ├── .readingOnlyBins(name, ...)      // Alias for bins()
    ├── .withNoBins()                    // Header only
    ├── .bin(name).get()                 // Read specific bin
    ├── .bin(name).selectFrom(expr)      // DSL expression read
    │
    ├── .where(String, Object...)        // Filter expression
    ├── .where(BooleanExpression)        // Filter expression
    ├── .where(PreparedDsl, Object...)   // Filter expression
    ├── .where(Exp)                      // Filter expression
    ├── .where(Expression)               // Filter expression
    │
    ├── .failOnFilteredOut()             // Fail if filtered
    ├── .includeMissingKeys()                // Include all keys in response
    │
    ├── .notInAnyTransaction()           // Override session transaction
    ├── .inTransaction(Txn)              // Use specific transaction
    │
    ├── .limit(long)                     // Limit results
    ├── .onPartition(int)                // Filter by partition
    ├── .onPartitionRange(int, int)      // Filter by partition range
    ├── .chunkSize(int)                  // Batch chunk size (no-op for key-based)
    │
    ├── .query(key2)                     // Chain another query
    ├── .upsert(key2)                    // Chain → ChainableOperationBuilder
    ├── .delete(key2)                    // Chain → ChainableNoBinsBuilder
    │
    └── .execute()                       // Execute operations
```

### BinsValuesBuilder Properties (from .bins().values())

```
session.upsert(key).bins("a", "b").values(1, 2)
    ├── .values(...)                     // Add more values for next key
    │
    ├── .ensureGenerationIs(int)         // Per-record generation
    ├── .expireRecordAfter(Duration)     // Per-record expiration
    ├── .expireRecordAfterSeconds(int)   // Per-record expiration
    ├── .expireRecordAt(Date)            // Per-record expiration
    ├── .expireRecordAt(LocalDateTime)   // Per-record expiration
    ├── .withNoChangeInExpiration()      // Per-record TTL = -2
    ├── .neverExpire()                   // Per-record TTL = -1
    │
    ├── .expireAllRecordsAfter(Duration) // Batch expiration (multi-key only)
    ├── .expireAllRecordsAfterSeconds(long) // Batch expiration
    ├── .expireAllRecordsAt(LocalDateTime)  // Batch expiration
    ├── .expireAllRecordsAt(Date)        // Batch expiration
    ├── .neverExpireAllRecords()         // Batch TTL = -1
    ├── .withNoChangeInExpirationForAllRecords() // Batch TTL = -2
    ├── .expiryFromServerDefaultForAllRecords()  // Batch TTL = 0
    │
    ├── .where(String, Object...)        // Filter expression
    ├── .where(BooleanExpression)        // Filter expression
    ├── .where(PreparedDsl, Object...)   // Filter expression
    ├── .where(Exp)                      // Filter expression
    ├── .where(Expression)               // Filter expression
    │
    ├── .failOnFilteredOut()             // Fail if filtered
    ├── .includeMissingKeys()                // Include all keys in response
    │
    ├── .notInAnyTransaction()           // Override session transaction
    ├── .inTransaction(Txn)              // Use specific transaction
    │
    └── .execute()                       // Execute operations
```

### QueryBuilder Properties (Dataset queries)

```
session.query(dataSet)
    ├── .bins(name, ...)                 // Specify bins to read
    ├── .allBins()                       // Read all bins
    ├── .noBins()                        // Header only
    │
    ├── .where(String, Object...)        // Filter/index expression
    ├── .where(BooleanExpression)        // Filter expression
    ├── .where(Expression)               // Filter expression
    │
    ├── .failOnFilteredOut()             // Fail if filtered
    ├── .includeMissingKeys()                // Include all keys in response
    │
    ├── .recordsPerSecond(int)           // Throttle query
    ├── .maxRecordsPerSecond(int)        // Max throttle (index queries)
    ├── .expectedQueryDuration(Duration) // Expected duration hint
    │
    ├── .limit(long)                     // Limit results
    ├── .chunkSize(int)                  // Batch chunk size
    ├── .onPartition(int)                // Partition filter
    ├── .onPartitionRange(int, int)      // Partition range filter
    │
    ├── .notInAnyTransaction()           // Override session transaction
    ├── .inTransaction(Txn)              // Use specific transaction
    │
    └── .execute()                       // Execute query
```

### ObjectBuilder Properties

```
session.upsert(dataSet).object(obj)
    ├── .object(obj2)                    // Add more objects
    ├── .objects(List<T>)                // Add list of objects
    │
    ├── .ensureGenerationIs(int)         // Per-object generation
    ├── .expireRecordAfter(Duration)     // Per-object expiration
    ├── .expireRecordAfterSeconds(int)   // Per-object expiration
    ├── .expireRecordAt(Date)            // Per-object expiration
    ├── .expireRecordAt(LocalDateTime)   // Per-object expiration
    ├── .withNoChangeInExpiration()      // Per-object TTL = -2
    ├── .neverExpire()                   // Per-object TTL = -1
    ├── .expiryFromServerDefault()       // Per-object TTL = 0
    │
    ├── .expireAllRecordsAfter(Duration) // Batch expiration (multi-object only)
    ├── .expireAllRecordsAfterSeconds(long) // Batch expiration
    ├── .expireAllRecordsAt(LocalDateTime)  // Batch expiration
    ├── .expireAllRecordsAt(Date)        // Batch expiration
    ├── .neverExpireAllRecords()         // Batch TTL = -1
    ├── .withNoChangeInExpirationForAllRecords() // Batch TTL = -2
    │
    ├── .notInAnyTransaction()           // Override session transaction
    ├── .inTransaction(Txn)              // Use specific transaction
    │
    └── .execute()                       // Execute operations
```

### BackgroundOperationBuilder Properties

```
backgroundSession.upsert(dataSet)
    ├── .bin(name).setTo(value)          // Bin operations
    │
    ├── .where(String, Object...)        // Filter expression
    ├── .where(BooleanExpression)        // Filter expression
    ├── .where(PreparedDsl, Object...)   // Filter expression
    ├── .where(Exp)                      // Filter expression
    ├── .where(Expression)               // Filter expression
    │
    ├── .recordsPerSecond(int)           // Throttle operations
    │
    └── .execute()                       // Execute background operation
```

---

## Property Propagation Analysis

### Transitions Between Builders

When one builder creates another (e.g., `bins()` creating `BinsValuesBuilder`), properties set on the parent may or may not propagate.

#### ChainableOperationBuilder → BinsValuesBuilder (via `.bins()`)

| Property | Source | Propagated? | Notes |
|----------|--------|-------------|-------|
| `keys` | `currentSpec.getKeys()` | ✅ Yes | Passed to constructor |
| `expirationInSeconds` | `currentSpec.getExpirationInSeconds()` | ✅ Yes | Passed to constructor |
| `txnToUse` | `opBuilder.getTxnToUse()` | ✅ Yes | Retrieved via interface |
| `whereClause` | `currentSpec.getWhereClause()` | ✅ Yes | Passed via initFromParent() |
| `generation` | `currentSpec.getGeneration()` | ✅ Yes | Passed via initFromParent() |
| `failOnFilteredOut` | `currentSpec.isFailOnFilteredOut()` | ✅ Yes | Passed via initFromParent() |
| `includeMissingKeys` | `currentSpec.isIncludeMissingKeys()` | ✅ Yes | Passed via initFromParent() |

#### ChainableOperationBuilder → ChainableNoBinsBuilder (via `.delete()`, `.touch()`, `.exists()`)

| Property | Source | Propagated? | Notes |
|----------|--------|-------------|-------|
| `session` | `session` | ✅ Yes | Passed to constructor |
| `operationSpecs` | `operationSpecs` | ✅ Yes | Shared list |
| `defaultWhereClause` | `defaultWhereClause` | ✅ Yes | Passed to constructor |
| `txnToUse` | `txnToUse` | ✅ Yes | Passed to constructor |

#### ChainableOperationBuilder → ChainableQueryBuilder (via `.query()`)

| Property | Source | Propagated? | Notes |
|----------|--------|-------------|-------|
| `session` | `session` | ✅ Yes | Passed to constructor |
| `operationSpecs` | `operationSpecs` | ✅ Yes | Shared list |
| `defaultWhereClause` | `defaultWhereClause` | ✅ Yes | Passed to constructor |
| `txnToUse` | `txnToUse` | ✅ Yes | Passed to constructor |

---

## Previously Identified Issues (All Resolved)

### Issue 1: Properties Not Propagating to BinsValuesBuilder ✅ FIXED

**Scenario:**
```java
session.upsert(key)
    .expireRecordAfterSeconds(5)  // Sets currentSpec.expirationInSeconds ✅ FIXED
    .ensureGenerationIs(3)        // Sets currentSpec.generation ✅ FIXED
    .where("$.age > 21")          // Sets currentSpec.whereClause ✅ FIXED
    .failOnFilteredOut()          // Sets currentSpec.failOnFilteredOut ✅ FIXED
    .includeMissingKeys()             // Sets currentSpec.includeMissingKeys ✅ FIXED
    .bins("a", "b")               // Creates BinsValuesBuilder - properties now propagated
    .values(1, 2)
    .execute();
```

**Resolution:**
All properties are now propagated via `BinsValuesBuilder.initFromParent()`:
- `expirationInSeconds` - ✅ Passed to constructor
- `generation` - ✅ Passed via `initFromParent()` to `generationForAll` field
- `whereClause` - ✅ Passed via `initFromParent()` to `dsl` field
- `failOnFilteredOut` - ✅ Passed via `initFromParent()` to inherited field
- `includeMissingKeys` - ✅ Passed via `initFromParent()` to inherited field

### Issue 2: sendKey Not Available on All Builders ⏸️ DEFERRED

The `sendKey()` method is available on `AbstractSessionOperationBuilder` (inherited by `ChainableOperationBuilder` and `ChainableNoBinsBuilder`) but NOT on:
- `BinsValuesBuilder`
- `ObjectBuilder`
- `ChainableQueryBuilder`

**Status:** Deferred - under review whether `sendKey` should be configured via `Behavior` instead of per-operation.

### Issue 3: OperationWithNoBinsBuilder Has Limited Properties ℹ️ BY DESIGN

`OperationWithNoBinsBuilder` (used internally for some operations) does not support:
- `ensureGenerationIs()` - No generation check support
- Per-record expiration - Only batch expiration available

**Status:** This is a known limitation of the internal builder. Use `ChainableNoBinsBuilder` for full functionality.

---

## Resolution Summary

### Issue 1 (BinsValuesBuilder propagation) - ✅ RESOLVED

Fixed by modifying `ChainableOperationBuilder.bins()` to pass all properties via `initFromParent()`:

```java
public BinsValuesBuilder bins(String binName, String... binNames) {
    verifyState("specifying bins");
    BinsValuesBuilder builder = new BinsValuesBuilder(new ChainableBinsValuesOperations(), currentSpec.getKeys(),
            currentSpec.getExpirationInSeconds(), binName, binNames);
    // Propagate additional properties from the current operation spec
    builder.initFromParent(
            currentSpec.getGeneration(),
            currentSpec.getWhereClause(),
            currentSpec.isFailOnFilteredOut(),
            currentSpec.isIncludeMissingKeys());
    return builder;
}
```

### Issue 2 (sendKey consistency) - ⏸️ DEFERRED

Under review - `sendKey` may be better configured via `Behavior` rather than per-operation methods.

### Issue 3 (OperationWithNoBinsBuilder) - ℹ️ DOCUMENTED

Known limitation of internal builder. Users should use `ChainableNoBinsBuilder` for full functionality.

---

## Testing Checklist

To verify property propagation, test these patterns:

- [x] `session.upsert(key).expireRecordAfterSeconds(5).bins(...).values(...).execute()` - Expiration honored
- [x] `session.upsert(key).ensureGenerationIs(3).bins(...).values(...).execute()` - Generation check
- [x] `session.upsert(key).where(...).bins(...).values(...).execute()` - Filter applied
- [x] `session.upsert(key).failOnFilteredOut().bins(...).values(...).execute()` - Flag honored
- [x] `session.upsert(key).includeMissingKeys().bins(...).values(...).execute()` - Flag honored
- [x] Chained operations: `session.upsert(k1)....delete(k2)....execute()` - Transaction shared
- [x] Chained operations: `session.query(k1)....upsert(k2)....execute()` - Transaction shared

---

## Related Documentation

- [Architecture Evolution](architecture-evolution.md) - Overall architecture and implementation details
