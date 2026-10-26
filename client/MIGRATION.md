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

2. Public `filter(Filter)` API for Explicit Secondary-Index Queries and Background Tasks

`QueryBuilder` and background update/delete/touch builders now expose `filter(Filter)`
for operations that need an explicit secondary-index access path. This is the
replacement for the old reflection workaround around
`setWhereClause(WhereClauseProcessor)`.

Use it when the desired index cannot be selected reliably from an AEL `where(...)`
clause, such as collection indexes, CDT-context indexes, blob indexes, or expression
indexes.

```java
RecordStream rs = session.query(dataSet)
    .filter(Filter.contains("map_bin", IndexCollectionType.MAPKEYS, "mkey2"))
    .execute();
```

An explicit filter is authoritative: it is sent as the index-range filter and bypasses
server query selection. Any `where(...)` clause chained beside it is sent as a residual
filter expression. Declaration order does not matter.

```java
RecordStream rs = session.query(dataSet)
    .filter(Filter.containsByIndex("idx_vehicle_license", IndexCollectionType.LIST, "7XYZ789"))
    .where("$.status == 'active'")
    .execute();
```

Background update/delete/touch use the same split: the index filter selects candidates,
and `where(...)` decides which candidates are written, deleted, or touched.

```java
ExecuteTask task = session.backgroundTask()
    .update(dataSet)
    .filter(Filter.rangeByIndex("idx_customer_age", 30, 65))
    .where("$.status == 'active'")
    .bin("campaign").setTo("renewal")
    .execute();
```

Notes:
- Only one explicit `Filter` can be attached; subsequent `filter(...)` calls throw.
- Index-selection and scan-policy hints (`forIndex`, `forBin`, `hardHint`, scan flags)
  do not rewrite an explicit filter. Even `forIndex("other").hardHint()` is accepted
  and ignored when the explicit filter names a different index. `queryDuration` still applies.
- String and `PreparedAel` residual `where(...)` clauses require server 8.2+ AEL support.
  Programmatic `Exp` and `Expression` residuals keep their existing server requirements.
- Background tasks send the residual predicate as `FILTER_EXP`; they do not send server-planned
  `WHERE` for this release.
- Background UDF builders intentionally do not expose `filter(Filter)` yet, even though update,
  delete, and touch do.
- Python uses plural `index_filters(...)`; this Java SDK follows its singular foreground
  query API and accepts one explicit filter.

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
