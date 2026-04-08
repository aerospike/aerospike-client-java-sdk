# Aerospike Java SDK — Key Features

## Table of Contents

- [Introduction](#introduction)
- [Connection Model](#connection-model)
- [DataSets and Keys](#datasets-and-keys)
- [Write Operations](#write-operations)
- [Read Operations](#read-operations)
- [RecordStream](#recordstream)
- [Heterogeneous Batch Chains](#heterogeneous-batch-chains)
- [Aerospike Expression Language (AEL)](#aerospike-expression-language-ael)
- [Expression-Based Bin Operations](#expression-based-bin-operations)
- [CDT Operations (Lists and Maps)](#cdt-operations-lists-and-maps)
- [Queries and Scans](#queries-and-scans)
- [Async Operations](#async-operations)
- [UDFs](#udfs)
- [Background Operations](#background-operations)
- [Transactions](#transactions)
- [Behavior (Policy Configuration)](#behavior-policy-configuration)
- [Object Mapping](#object-mapping)
- [Secondary Indexes](#secondary-indexes)
- [Info Commands](#info-commands)
- [Exception Hierarchy](#exception-hierarchy)

---

## Introduction

The Aerospike Java SDK is a ground-up redesign of the Aerospike Java client
with the following goals:

- **Idiomatic Java** — fluent, chainable APIs that read naturally and follow
  modern Java conventions (builders, lambdas, `try`-with-resources,
  `Optional`, `Stream`).
- **Compile-time safety over runtime errors** — write verbs (`insert`,
  `update`, `upsert`, `replace`) enforce intent at the method level rather
  than through policy flags discovered at runtime. Typed selectors in the
  `Behavior` system expose only valid configuration knobs for each operation
  type.
- **Separation of development from operations** — application code uses
  `Session` with a named `Behavior`; operational concerns (timeouts, retries,
  consistency levels) are configured separately and can be changed without
  modifying application logic.
- **Automatic batching** — multiple heterogeneous operations are
  transparently batched into a single server round-trip.
- **Integrated expression language** — the Aerospike Expression Language
  (AEL) provides a concise, text-based syntax for server-side filters,
  with automatic secondary index selection.

Requires Java 21+.

---

## Connection Model

The SDK uses a two-step connection model: **Cluster** (connection) and
**Session** (operation scope).

```java
try (Cluster cluster = new ClusterDefinition("localhost", 3000).connect()) {
    Session session = cluster.createSession(Behavior.DEFAULT);
    // All operations go through session
}
```

`ClusterDefinition` supports fluent configuration:

```java
Cluster cluster = new ClusterDefinition("seed1", 3000)
    .withNativeCredentials("admin", "password")
    .usingServicesAlternate()
    .preferredRacks(1, 2)
    .validateClusterNameIs("prod-cluster")
    .connect();
```

`Cluster` is `Closeable` and manages node discovery, connection pooling, and
partition maps. `Session` is lightweight — create as many as needed with
different `Behavior` profiles.

---

## DataSets and Keys

A `DataSet` represents a namespace/set pair. Keys are created from it.

```java
DataSet users = DataSet.of("test", "users");

Key key     = users.id("user-123");        // String key
Key numKey  = users.id(42L);               // Long key
Key blobKey = users.id(new byte[]{1,2,3}); // Blob key

List<Key> batch = users.ids("alice", "bob", "charlie");
List<Key> nums  = users.ids(1, 2, 3, 4, 5);
```

---

## Write Operations

Five write verbs with distinct semantics:

| Method | Behaviour |
|---|---|
| `upsert` | Create or overwrite (default write) |
| `insert` | Create only — fails if record exists |
| `update` | Update only — fails if record doesn't exist |
| `replace` | Overwrite entire record (non-referenced bins deleted) |
| `replaceIfExists` | Like `replace`, but fails if record doesn't exist |

```java
session.upsert(users.id("alice"))
    .bin("name").setTo("Alice")
    .bin("age").setTo(30)
    .bin("tags").setTo(List.of("admin", "active"))
    .bin("profile").setTo(Map.of("city", "London"))
    .execute();
```

Supported value types: `String`, `int`, `long`, `float`, `double`,
`boolean`, `byte[]`, `List`, `Map`, `SortedMap`.

### Numeric increment and string append/prepend

```java
session.upsert(users.id("alice"))
    .bin("loginCount").add(1)
    .bin("greeting").prepend("Hello, ")
    .bin("bio").append(" [verified]")
    .execute();
```

### TTL and generation control

```java
session.upsert(users.id("alice"))
    .bin("session").setTo("abc123")
    .expireRecordAfter(Duration.ofHours(24))
    .execute();
```

---

## Read Operations

```java
// Read specific bins
RecordStream rs = session.query(users.id("alice"))
    .bin("name").get()
    .bin("age").get()
    .execute();

Record record = rs.getFirstRecord();
String name = record.getString("name");
Long age = record.getLong("age");
```

```java
// Read all bins
RecordStream rs = session.query(users.id("alice")).execute();
```

### Batch reads

```java
RecordStream rs = session.query(users.ids("alice", "bob", "charlie"))
    .bins("name", "age")
    .execute();

for (RecordResult result : rs) {
    Record rec = result.recordOrThrow();
    System.out.println(rec.getString("name"));
}
```

### Delete, touch, exists

These operations return boolean results via `RecordStream`:

```java
// Delete — returns true if deleted, false if not found
boolean deleted = session.delete(users.id("alice"))
    .execute()
    .getFirstBoolean().orElse(false);

// Touch — reset TTL
session.touch(users.id("bob"))
    .expireRecordAfter(Duration.ofDays(30))
    .execute();

// Exists — check without reading
boolean exists = session.exists(users.id("charlie"))
    .execute()
    .getFirstBoolean().orElse(false);

// Batch exists
RecordStream rs = session.exists(users.ids("alice", "bob", "charlie"))
    .execute();

while (rs.hasNext()) {
    RecordResult result = rs.next();
    System.out.println(result.getKey() + ": " + result.asBoolean());
}
```

---

## RecordStream

`RecordStream` is the universal return type for all operations. It
implements `Iterator<RecordResult>` and `Closeable`. In practice, explicit
closing is only necessary for index queries (dataset scans) where the stream
may not be fully consumed. Point lookups and batch operations are fully
buffered and close automatically. Explicitly closing a `RecordStream` on an
in-progress index query aborts the query on the server.

### Single-record shortcuts

```java
RecordStream rs = session.query(users.id("alice")).execute();

// getFirstRecord() — returns Record, throws on error, closes stream
Record record = rs.getFirstRecord();

// getFirst() — returns Optional<RecordResult>, closes stream
Optional<RecordResult> result = rs.getFirst();

// getFirstBoolean() — for exists/delete/touch, closes stream
Optional<Boolean> exists = rs.getFirstBoolean();
```

### Iterating results

```java
RecordStream rs = session.query(users.ids("alice", "bob", "charlie")).execute();

// Iterator pattern
while (rs.hasNext()) {
    RecordResult result = rs.next();
    if (result.isOk()) {
        Record rec = result.recordOrThrow();
        System.out.println(rec.getString("name"));
    }
}

// Enhanced for loop (RecordStream is Iterable via Iterator)
for (RecordResult result : rs) {
    Record rec = result.recordOrThrow();
    System.out.println(rec.getString("name"));
}

// Java Stream API (closes RecordStream when stream closes)
try (Stream<RecordResult> stream = rs.stream()) {
    List<String> names = stream
        .filter(RecordResult::isOk)
        .map(r -> r.recordOrThrow().getString("name"))
        .toList();
}
```

### Pop (consume without closing)

`pop()` retrieves the next result without closing the stream, useful when
processing results one at a time:

```java
RecordStream rs = session.query(users.ids("alice", "bob")).execute();
Optional<RecordResult> first  = rs.pop();
Optional<RecordResult> second = rs.pop();
rs.close();
```

### Async / reactive

See [Async Operations](#async-operations) for full details on
`executeAsync()`, `CompletableFuture`, and `Flow.Publisher`.

---

## Heterogeneous Batch Chains

Multiple operations on different keys and of different types can be chained
into a single server round-trip. Each verb starts a new operation; bin
operations belong to their preceding verb.

```java
session
    .upsert(users.id("alice"))
        .bin("name").setTo("Alice")
        .bin("age").setTo(30)
    .update(users.id("bob"))
        .bin("loginCount").add(1)
    .delete(users.id("charlie"))
    .execute();
```

Reads and writes can be mixed:

```java
session
    .query(users.id("alice"))
        .bin("name").get()
    .upsert(users.id("bob"))
        .bin("status").setTo("active")
    .execute();
```

UDFs can be chained with other operations:

```java
session
    .executeUdf(users.id("alice"))
        .function("myPackage", "transform")
        .passing("arg1")
    .upsert(users.id("bob"))
        .bin("name").setTo("Bob")
    .execute();
```

---

## Aerospike Expression Language (AEL)

The Aerospike Expression Language is a text-based filter syntax that
compiles to server-side filter expressions. It can be used anywhere a
`where` clause is accepted.

```java
// String-based AEL
session.query(users)
    .where("$.age > 30 and $.status == 'active'")
    .execute();

// On point operations (conditional write)
session.upsert(users.id("alice"))
    .bin("tier").setTo("gold")
    .where("$.totalSpend > 10000")
    .execute();
```

AEL supports bin references (`$.binName`), arithmetic, comparisons, logical
operators, `let...then` variable binding, `when...default` conditionals,
list/map path navigation, and type inference.

```java
// Complex AEL with let...then
session.query(users)
    .where("let (cutoff = $.signupDate + 86400000) then ($.lastLogin > ${cutoff})")
    .execute();

// when...default
session.query(users)
    .where("when ($.tier == 'gold' => $.age > 18, $.tier == 'silver' => $.age > 21, default => false)")
    .execute();
```

### Programmatic expressions

The `Exp` builder provides a programmatic alternative:

```java
session.query(users)
    .where(Exp.build(
        Exp.and(
            Exp.gt(Exp.intBin("age"), Exp.val(30)),
            Exp.eq(Exp.stringBin("status"), Exp.val("active"))
        )
    ))
    .execute();
```

### Prepared AEL (parameterised)

```java
PreparedDsl prepared = PreparedDsl.of("$.age > ?1 and $.city == ?2");
session.query(users)
    .where(prepared, 30, "London")
    .execute();
```

---

## Expression-Based Bin Operations

Compute derived values server-side and return them as virtual bins.

```java
// selectFrom: read a computed value
session.query(users.id("alice"))
    .bin("total").selectFrom("$.price * $.quantity")
    .bin("isAdult").selectFrom("$.age >= 18")
    .execute();

// insertFrom / updateFrom / upsertFrom: write a computed value
session.upsert(users.id("alice"))
    .bin("discountedPrice").upsertFrom("$.price * 0.9")
    .execute();
```

---

## CDT Operations (Lists and Maps)

Bin-level fluent access to Aerospike's Collection Data Types. Operations
are reached through `bin("name")` then navigating into the CDT structure.

### Map operations

```java
// Write to a map
session.upsert(users.id("alice"))
    .bin("prefs").setTo(Map.of("theme", "dark", "lang", "en"))
    .execute();

// Read a specific map key
session.query(users.id("alice"))
    .bin("prefs").onMapKey("theme").getValues()
    .execute();

// Read a range of keys
session.query(users.id("alice"))
    .bin("prefs").onMapKeyRange("a", "m").getValues()
    .execute();

// Get by rank (highest value)
session.query(users.id("alice"))
    .bin("scores").onMapRank(-1).getValues()
    .execute();

// Remove by key
session.upsert(users.id("alice"))
    .bin("prefs").onMapKey("oldSetting").removeValues()
    .execute();

// Map put
session.upsert(users.id("alice"))
    .bin("prefs").mapPut("notifications", true)
    .execute();

// Increment a map value
session.upsert(users.id("alice"))
    .bin("counters").onMapKey("views").add(1)
    .execute();
```

### List operations

```java
// Write a list
session.upsert(users.id("alice"))
    .bin("scores").setTo(List.of(95, 87, 92, 78))
    .execute();

// Read by index
session.query(users.id("alice"))
    .bin("scores").onListIndex(0).getValues()
    .execute();

// Read by value range
session.query(users.id("alice"))
    .bin("scores").onListValueRange(80, 95).getValues()
    .execute();

// Append
session.upsert(users.id("alice"))
    .bin("scores").listAppend(99)
    .execute();

// Sort
session.upsert(users.id("alice"))
    .bin("scores").listSort()
    .execute();

// Remove by index range
session.upsert(users.id("alice"))
    .bin("scores").onListIndexRange(0, 2).removeValues()
    .execute();
```

### Inverted operations

Every multi-element selection supports an inverted counterpart that operates
on all elements *not* matching the selection:

```java
// Count scores in range 90–100
session.query(users.id("alice"))
    .bin("scores").onListValueRange(90, 100).getCount()
    .execute();

// Count all scores OUTSIDE that range
session.query(users.id("alice"))
    .bin("scores").onListValueRange(90, 100).countAllOthers()
    .execute();

// Get all map keys EXCEPT "theme" and "lang"
session.query(users.id("alice"))
    .bin("prefs").onMapKeyList(List.of("theme", "lang")).getAllOtherKeys()
    .execute();

// Remove all map entries EXCEPT those in key range "a" to "m"
session.upsert(users.id("alice"))
    .bin("prefs").onMapKeyRange("a", "m").removeAllOthers()
    .execute();

// Get all values NOT matching a specific value
session.query(users.id("alice"))
    .bin("scores").onListValue(87).getAllOtherValues()
    .execute();
```

### Nested CDT navigation

Operations can navigate into nested structures by chaining context steps:

```java
// Read a value from a nested map inside a list
// Data: {"orders": [{"id": 1, "total": 50}, {"id": 2, "total": 120}]}
session.query(users.id("alice"))
    .bin("orders").onListIndex(0).onMapKey("total").getValues()
    .execute();
```

### Return type control

CDT operations support different return types:

```java
// Return keys instead of values
session.query(users.id("alice"))
    .bin("prefs").onMapValueRange(1, 100).getKeys()
    .execute();

// Return count
session.query(users.id("alice"))
    .bin("scores").onListValueRange(90, 100).getCount()
    .execute();

// Return index
session.query(users.id("alice"))
    .bin("scores").onListValue(95).getIndex()
    .execute();
```

---

## Queries and Scans

Dataset-level queries scan entire sets. When a matching secondary index
exists, the SDK automatically selects it — this is transparent to application
code and requires no query changes.

```java
RecordStream rs = session.query(users)
    .where("$.age > 30")
    .readingOnlyBins("name", "age")
    .limit(100)
    .execute();

for (RecordResult result : rs) {
    Record rec = result.recordOrThrow();
    System.out.println(rec.getString("name") + ": " + rec.getLong("age"));
}
```

### Pagination and sorting

Use `NavigatableRecordStream` for page-based iteration with client-side
sorting:

```java
RecordStream rs = session.query(users)
    .where("$.status == 'active'")
    .sortReturnedSubsetBy("age", SortDir.SORT_DESC)
    .limit(100)
    .execute();

NavigatableRecordStream pages = rs.asNavigatableStream();
pages.pageSize(20);

int pageNumber = 1;
while (pages.hasMorePages()) {
    System.out.println("--- Page " + pageNumber + " of " + pages.maxPages() + " ---");
    while (pages.hasNext()) {
        RecordResult result = pages.next();
        Record rec = result.recordOrThrow();
        System.out.println(rec.getString("name") + ": " + rec.getLong("age"));
    }
    pageNumber++;
}
```

You can also jump to a specific page:

```java
pages.setPageTo(3);  // 1-based
while (pages.hasNext()) {
    // process page 3
}
```

### Partition targeting

```java
RecordStream rs = session.query(users)
    .onPartitionRange(0, 2048)
    .limit(1000)
    .execute();
```

### Throttling

```java
RecordStream rs = session.query(users)
    .recordsPerSecond(5000)
    .execute();
```

---

## Async Operations

All operations that return `RecordStream` support an async variant via
`executeAsync()`. The returned `RecordStream` is populated on a background
thread as results arrive from the server.

### Error handling strategies

`executeAsync()` takes either an `ErrorStrategy` or an `ErrorHandler`:

```java
// Errors embedded in the stream — caller checks each result
RecordStream rs = session.query(users.ids("alice", "bob"))
    .executeAsync(ErrorStrategy.IN_STREAM);

while (rs.hasNext()) {
    RecordResult result = rs.next();
    if (result.isOk()) {
        System.out.println(result.recordOrThrow().getString("name"));
    } else {
        System.err.println("Error for " + result.getKey() + ": " + result.resultCode());
    }
}
```

```java
// Errors dispatched to a callback — stream contains only successes
RecordStream rs = session.query(users.ids("alice", "bob"))
    .executeAsync(err -> System.err.println("Failed: " + err.getKey()));

while (rs.hasNext()) {
    Record rec = rs.next().recordOrThrow();
    System.out.println(rec.getString("name"));
}
```

### CompletableFuture

For bounded result sets (point lookups, batch operations), drain the stream
into a `CompletableFuture`. Draining happens on a virtual thread and returns
immediately.

```java
CompletableFuture<List<RecordResult>> future = session
    .query(users.ids("alice", "bob", "charlie"))
    .executeAsync(ErrorStrategy.IN_STREAM)
    .asCompletableFuture();

future.thenAccept(results ->
    results.forEach(r -> System.out.println(r.recordOrThrow().getString("name")))
);
```

With object mapping:

```java
CompletableFuture<List<Customer>> future = session
    .query(customers.ids("C001", "C002"))
    .executeAsync(ErrorStrategy.IN_STREAM)
    .asCompletableFuture(customerMapper);

List<Customer> result = future.join();
```

### Flow.Publisher (reactive streams)

For large or unbounded result sets (dataset queries), use `asPublisher()`
which provides backpressure support via the JDK `Flow.Publisher` interface.
No external dependencies are required.

```java
Flow.Publisher<RecordResult> publisher = session.query(users)
    .where("$.age > 21")
    .executeAsync(ErrorStrategy.IN_STREAM)
    .asPublisher();

publisher.subscribe(new Flow.Subscriber<>() {
    Flow.Subscription sub;

    public void onSubscribe(Flow.Subscription s) {
        sub = s;
        s.request(100);
    }
    public void onNext(RecordResult item) {
        System.out.println(item.recordOrThrow().getString("name"));
        sub.request(1);
    }
    public void onError(Throwable t) { t.printStackTrace(); }
    public void onComplete() { System.out.println("done"); }
});
```

The `Flow.Publisher` integrates with Project Reactor and RxJava via standard
JDK adapters:

```java
// Project Reactor
Flow.Publisher<RecordResult> publisher = session.query(users)
    .where("$.age > 21")
    .executeAsync(ErrorStrategy.IN_STREAM)
    .asPublisher();

Flux.from(JdkFlowAdapter.flowPublisherToFlux(publisher))
    .filter(RecordResult::isOk)
    .map(r -> r.recordOrThrow().getString("name"))
    .buffer(100)
    .subscribe(batch -> saveBatch(batch));
```

```java
// RxJava 3
Flowable.fromPublisher(FlowAdapters.toPublisher(publisher))
    .filter(RecordResult::isOk)
    .map(r -> r.recordOrThrow().getString("name"))
    .buffer(100)
    .subscribe(batch -> saveBatch(batch));
```

### Async on writes

Async is not limited to reads — writes, deletes, and mixed batch chains all
support `executeAsync()`.

**Important:** If an async operation runs inside a `doInTransaction` block,
you must fully consume the `RecordStream` before the transaction lambda
returns. Otherwise the transaction may commit or abort while async results
are still in flight, leading to inconsistent state.

```java
RecordStream rs = session
    .upsert(users.id("alice"))
        .bin("name").setTo("Alice")
    .update(users.id("bob"))
        .bin("loginCount").add(1)
    .executeAsync(ErrorStrategy.IN_STREAM);

rs.asCompletableFuture().thenAccept(results ->
    System.out.println("Batch complete: " + results.size() + " operations")
);
```

---

## UDFs

Register, remove, and execute User Defined Functions (Lua).

```java
// Register
RegisterTask task = session.registerUdf("path/to/udf.lua", "udf.lua");
task.waitTillComplete();

// Execute on a key
session.executeUdf(users.id("alice"))
    .function("myPackage", "myFunction")
    .passing("arg1", 42, true)
    .execute();

// Chain with other operations
session.executeUdf(users.id("alice"))
    .function("myPackage", "transform")
    .passing("arg1")
    .upsert(users.id("bob"))
    .bin("name").setTo("Bob")
    .execute();

// Remove
session.removeUdf("udf.lua");
```

---

## Background Operations

Server-side set-level operations that run asynchronously. Useful for bulk
updates, deletes, or UDF execution across an entire set.

```java
// Bulk update
ExecuteTask task = session.backgroundTask()
    .update(users)
    .where("$.age > 30")
    .bin("category").setTo("senior")
    .execute();

task.waitTillComplete();

// Bulk delete
session.backgroundTask()
    .delete(users)
    .where("$.lastLogin < 1609459200000")
    .execute();

// Extend TTL for active users
session.backgroundTask()
    .touch(users)
    .where("$.status == 'active'")
    .expireRecordAfter(Duration.ofDays(30))
    .execute();

// Execute UDF on matching records
session.backgroundTask()
    .executeUdf(users)
    .function("inventory", "applyDiscount")
    .passing(0.20)
    .where("$.stock > 250")
    .execute();
```

---

## Transactions

Multi-record ACID transactions with automatic retry on transient failures
(`MRT_BLOCKED`, `MRT_VERSION_MISMATCH`, `TXN_FAILED`).

```java
// Void transaction
session.doInTransaction(tx -> {
    tx.upsert(accounts.id("acc1"))
        .bin("balance").add(-100)
        .execute();
    tx.upsert(accounts.id("acc2"))
        .bin("balance").add(100)
        .execute();
});

// Value-returning transaction
String name = session.doInTransactionReturning(tx -> {
    RecordStream rs = tx.query(users.id("alice")).execute();
    return rs.getFirstRecord().getString("name");
});
```

Transactions auto-abort on exception and support nesting.

### Implicit batch transactions

On CP-mode namespaces (server 8.0+), batch operations that include writes
are automatically wrapped in a transaction for atomicity, even without an
explicit `doInTransaction` call. This is controlled by two mechanisms:

**Per-operation opt-out** — use `notInAnyTransaction()` to suppress
transaction creation for a specific batch:

```java
session.upsert(users.id("alice"))
    .bin("name").setTo("Alice")
    .update(users.id("bob"))
    .bin("loginCount").add(1)
    .notInAnyTransaction()
    .execute();
```

**Global opt-out** — disable implicit batch transactions cluster-wide via
`SystemSettings`:

```java
Cluster cluster = new ClusterDefinition("localhost", 3000)
    .withSystemSettings(s -> s.transactions(t ->
        t.implicitBatchWriteTransactions(false)
    ))
    .connect();
```

---

## Behavior (Policy Configuration)

`Behavior` replaces the traditional per-operation policy classes
(`ReadPolicy`, `WritePolicy`, etc.) with a single, cascading configuration
model. All behaviors derive from `Behavior.DEFAULT`.

**Key concept:** Settings are applied via *selectors* that target specific
operation types, with general settings cascading down to specific ones.

```java
Behavior production = Behavior.DEFAULT.deriveWithChanges("production", b -> b
    .on(Selectors.all(), ops -> ops
        .abandonCallAfter(Duration.ofSeconds(5))
    )
    .on(Selectors.writes().retryable().point().cp(), ops -> ops
        .useDurableDelete(true)
    )
    .on(Selectors.reads().batch(), ops -> ops
        .maxConcurrentNodes(16)
    )
);

Session session = cluster.createSession(production);
```

### Selector hierarchy (general to specific)

```
Selectors.all()                                    // everything
Selectors.reads()                                  // all reads
Selectors.reads().batch()                          // batch reads
Selectors.reads().batch().ap()                     // batch reads, AP mode
Selectors.writes().retryable().point().cp()        // retryable point writes, CP mode
```

### Configuration inheritance

Behaviors can form hierarchies:

```java
Behavior highLoad = production.deriveWithChanges("highLoad", b -> b
    .on(Selectors.reads().query(), ops -> ops
        .recordQueueSize(10000)
    )
);
```

---

## Object Mapping

Map Java objects to/from Aerospike records using `RecordMapper`,
`RecordMappingFactory`, and `TypeSafeDataSet`.

### Setup

A `RecordMapper<T>` converts between Java objects and `Map<String, Object>`
(the bin representation). Register mappers via a `RecordMappingFactory` on
the cluster:

```java
RecordMapper<Customer> customerMapper = new RecordMapper<>() {
    public Map<String, Object> toMap(Customer c) {
        return Map.of("name", c.name(), "email", c.email(), "age", c.age());
    }
    public Customer fromMap(Map<String, Object> bins) {
        return new Customer(
            (String) bins.get("name"),
            (String) bins.get("email"),
            ((Number) bins.get("age")).intValue()
        );
    }
    public Key id(Customer c, DataSet ds) {
        return ds.id(c.email());
    }
};

RecordMappingFactory factory = DefaultRecordMappingFactory.of(Customer.class, customerMapper);
cluster.setRecordMappingFactory(factory);
```

### TypeSafeDataSet

A `TypeSafeDataSet<T>` binds a class to a namespace/set. It extends
`DataSet`, so it can be used anywhere a `DataSet` is accepted (queries,
writes, index creation, background tasks, etc.):

```java
TypeSafeDataSet<Customer> customers = TypeSafeDataSet.of("test", "customers", Customer.class);
```

### Writing objects

```java
Customer alice = new Customer("Alice", "alice@example.com", 30);

// Insert a single object
session.insert(customers)
    .object(alice)
    .execute();

// Insert multiple objects
List<Customer> newCustomers = List.of(
    new Customer("Bob", "bob@example.com", 25),
    new Customer("Charlie", "charlie@example.com", 35)
);

session.insert(customers)
    .objects(newCustomers)
    .execute();
```

### Overriding the mapper

Use `using()` to supply a mapper inline instead of the cluster-level factory:

```java
session.insert(customers)
    .object(alice)
    .using(customerMapper)
    .execute();
```

### Reading objects

```java
// Single record — Optional<Customer>
Optional<Customer> alice = session.query(customers.id("alice@example.com"))
    .execute()
    .getFirst(customerMapper);

// Query — List<Customer>
List<Customer> activeCustomers = session.query(customers)
    .where("$.age > 25")
    .execute()
    .toObjectList(customerMapper);
```

---

## Secondary Indexes

```java
// Create a secondary index
IndexTask task = session.createIndex(
    users, "idx_age", "age",
    IndexType.INTEGER, IndexCollectionType.DEFAULT
);
task.waitTillComplete();

// Drop a secondary index
session.dropIndex(users, "idx_age");
```

When AEL queries reference an indexed bin, the SDK automatically selects the
secondary index. This is transparent to the application — no query changes
are needed when indexes are added or removed.

---

## Info Commands

Typed, structured access to Aerospike info commands.

```java
InfoCommands info = session.info();

Set<String> namespaces = info.namespaces();
Optional<NamespaceDetail> ns = info.namespaceDetails("test");
List<Sindex> indexes = info.secondaryIndexes();
List<SetDetail> sets = info.sets();

// Per-node results
Map<Node, List<Sindex>> indexesPerNode = info.secondaryIndexesPerNode();
```

---

## Exception Hierarchy

All Aerospike errors are unchecked exceptions extending `AerospikeException`
(which extends `RuntimeException`). Each carries a `resultCode` for
programmatic handling.

### Common exceptions

| Exception | Result Code | When |
|---|---|---|
| `RecordNotFoundException` | `KEY_NOT_FOUND_ERROR` | `update` / `replaceIfExists` on missing key |
| `RecordExistsException` | `KEY_EXISTS_ERROR` | `insert` on existing key |
| `GenerationException` | `GENERATION_ERROR` | Optimistic locking — generation mismatch |
| `FilteredException` | `FILTERED_OUT` | Record didn't match `where` expression |
| `RecordTooBigException` | `RECORD_TOO_BIG` | Write exceeds record size limit |
| `ParameterError` | `PARAMETER_ERROR` | Invalid operation arguments |
| `Timeout` | — | Operation exceeded `abandonCallAfter` |

### Catching and branching

```java
try {
    session.insert(users.id("alice"))
        .bin("name").setTo("Alice")
        .execute();
} catch (AerospikeException.RecordExistsException e) {
    System.out.println("Record already exists");
} catch (AerospikeException.GenerationException e) {
    System.out.println("Concurrent modification detected");
} catch (AerospikeException.Timeout e) {
    System.out.println("Operation timed out on node: " + e.getNode());
} catch (AerospikeException e) {
    System.out.println("Error code: " + e.getResultCode());
}
```

### Full hierarchy

```
AerospikeException (RuntimeException)
├── RecordNotFoundException
├── RecordExistsException
├── GenerationException
├── FilteredException
├── RecordTooBigException
├── Timeout
├── Connection
├── SecurityException
│   ├── AuthenticationException
│   └── AuthorizationException
├── QuotaException
├── BinException
│   ├── BinExistsException
│   ├── BinNotFoundException
│   ├── BinTypeException
│   └── BinOpInvalidException
├── ElementException
│   ├── ElementNotFoundException
│   └── ElementExistsException
├── TransactionException
│   └── Commit
├── CapacityException
│   └── KeyBusyException
├── IndexException
├── QueryException
├── BatchException
├── UdfException
├── Serialize
├── Parse
├── InvalidNode
├── InvalidNamespace
├── QueryTerminated
└── Backoff
```
