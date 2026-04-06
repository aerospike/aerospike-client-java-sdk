# test migrations
## tests not migrated
### udf/aggregation tests

Missing UDF registration and aggregate query support in API

- TestQuerySum.java - Requires UDF registration (`sum_example.lua`) and aggregate queries
- TestQueryRPS.java - Setup registers 2 UDFs; 4 of 8 test methods require UDF/aggregation
- TestQueryFilter.java - Requires UDF registration (`filter_example.lua`) and aggregate queries with filter functions
- TestQueryExecute.java - Setup registers UDF (`record_example.lua`); 1 of 4 methods requires UDF execution
- TestQueryAverage.java - Requires UDF registration (`average_example.lua`) and aggregate queries to compute averages

Needs:
- `session.registerUdf()` - Register Lua UDF files
- `session.query().aggregate()` - Execute aggregate queries
- UDF execution in background operations

### geospatial test

TestQueryGeo.java - Blocked by API bug? Will create a PR/fix if this is indeed a bug

Issue: `IndexType.GEOJSON` enum sends `"GEOJSON"` to server, but server expects `"geo2dsphere"` (lowercase)

Fix: Add enum-to-string mapping in `Session.buildCreateIndexInfoCommand()` (lines 560, 565)

### ctx method test

TestIndex.java - Missing pathExpressions integration

Issue: `CTX` class lacks advanced selection methods for CDT structures

Missing Methods for Remaining Tests:
- `CTX.allChildren()` - Select all children in a CDT structure
- `CTX.allChildrenWithFilter(Exp filter)` - Select children matching a filter expression
- `Exp.mapLoopVar(LoopVarPart)` - Access loop variables in filter expressions

Fix: Implement the missing CTX methods above

### expression-based secondary index test

TestExpSecondaryIndex.java - Blocked by missing public API for expression-based secondary indexes

Issue: `Session.createIndex()` lacks an overload that accepts an `Expression` parameter

Details:
The test creates secondary indexes where the index is computed from an expression (e.g., IF age >= 18 AND country IN ["Australia", "Canada", "USA"]) rather than a simple bin value. This feature requires:
- Creating an index with an expression: `session.createIndex(set, indexName, IndexType.NUMERIC, expression)`
- Querying by index name: `Filter.rangeByIndex(indexName, 1, 1)` (available)
- Querying by expression: `Filter.range(expression, 1, 1)` (available)

Internal Support Exists:
The API's `Session.buildCreateIndexInfoCommand()` (line 512) already has an `Expression exp` parameter and correctly builds the command with `;exp=<base64>` (lines 529-538). However, this is a private method not exposed in the public API.

Test Breakdown (3 methods):
- `createExpSI()` - Creates expression-based secondary index
- `queryExpSIbyName()` - Queries using index name (Filter available)
- `queryExpSIbyExp()` - Queries using expression directly (Filter available)

All tests require the missing `createIndex` overload.

Fix: Add public API method in `Session.java`:

```java
public final IndexTask createIndex(
    DataSet set,
    String indexName,
    IndexType indexType,
    IndexCollectionType indexCollectionType,
    Expression expression
)
```

---

## partially migrated
### testindex.java → indextest.java

Status: 2 of 5 test methods migrated

Migrated Tests:
- `createDrop()` - Index lifecycle operations (create/drop/verify)
- `ctxRestore()` - CTX serialization/deserialization with `toBase64()`/`fromBase64()`

Not Migrated (blocked by missing CTX methods):
- `allChildrenBase()` - Requires `CTX.allChildren()`
- `allChildrenWithFilterBase()` - Requires `CTX.allChildrenWithFilter(Exp filter)` and `Exp.mapLoopVar(LoopVarPart)`
- `mixedContextWithAllChildrenBase()` - Requires both missing methods

---

## issues & workarounds

1. AEL Bug - Logical AND (`&&`) Parsed as Bitwise AND

The AEL parser treats `&&` as bitwise AND (`&`) instead of logical AND.
- Example: `$.bin >= 14 && $.bin <= 18` incorrectly parses as `bin >= (14 & bin)`
- Workaround: Use `Exp.and(Exp.ge(...), Exp.le(...))` instead of AEL strings with `&&`
- Affected Tests: QueryIntegerTest, QueryKeyTest

2. Missing Public `where(Filter)` API

`QueryBuilder` lacks a public method to accept `Filter` objects directly, even though:
- `Filter` class exists with methods like `Filter.range()`, `Filter.contains()`, `Filter.equal()`
- These filters support secondary indexes and CDT contexts (e.g., `CTX.listRank()`, `IndexCollectionType.MAPKEYS`)
- Internal `setWhereClause(WhereClauseProcessor)` method exists but is protected

Issue: Cannot use secondary index queries with collections or CDT contexts through public API
- `where(String dsl)` - Doesn't support all Filter features
- `where(Exp exp)` - Explicitly disables secondary index usage (see QueryBuilder line 403)
- `where(Filter filter)` - Method doesn't exist publicly

Workaround: Use Java reflection to invoke protected `setWhereClause()` method
```java
Filter filter = Filter.contains(binName, IndexCollectionType.MAPKEYS, key);
WhereClauseProcessor processor = new WhereClauseProcessor(true) {
    @Override
    public ParseResult process(String namespace, Session session) {
        return new ParseResult(filter, null);
    }
};
var setWhereMethod = queryBuilder.getClass().getSuperclass()
    .getDeclaredMethod("setWhereClause", WhereClauseProcessor.class);
setWhereMethod.setAccessible(true);
setWhereMethod.invoke(queryBuilder, processor);
```

Affected Tests: QueryContextTest, QueryCollectionTest, QueryBlobTest

Why These Tests Need It:
- QueryContextTest: Uses `Filter.range()` with `CTX.listRank(-1)` for CDT context queries
- QueryCollectionTest: Uses `Filter.contains()` with `IndexCollectionType.MAPKEYS` for map key queries
- QueryBlobTest: Uses `Filter.equal()` and `Filter.contains()` with `IndexType.BLOB` for byte array queries

Recommendation: Add public `where(Filter filter)` method to QueryBuilder to support secondary index queries with CDT contexts and collection types without requiring reflection.

3. Multi-Operation Commands Not Supported on Single Key

The API does **not** support executing multiple operations on a single key in one server call like the original client's `operate()` method.

Original Client:
```java
// Single server call with multiple operations
Record record = client.operate(writePolicy, key,
    Operation.touch(),
    Operation.getHeader()
);
```
Need two calls for SDK.
API:
```java
session.touch(key).expireRecordAfter(Duration.ofSeconds(2)).execute(); 
RecordStream rs = session.query(key).withNoBins().execute();
```
Architectural Limitation:
- `OperationSpec.canHaveBinOperations()` returns `false` for `TOUCH`, `DELETE`, and `EXISTS`
- Touch operations are implemented as standalone `OpType` rather than composable operations

Tests Affected:
- `TouchTest.touchOperate()` - Uses 2 calls instead of 1; functionally equivalent but not architecturally identical
- Any tests requiring `client.operate()` with mixed operation types on a single key
