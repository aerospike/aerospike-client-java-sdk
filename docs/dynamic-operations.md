# Dynamic Operations in the Fluent Client

## Who Is This For?

If you've used the standard Aerospike Java client, you're accustomed to building lists of `Bin` objects or `Operation` objects and passing them to methods like `client.put(policy, key, bins)`. The fluent client uses method chaining instead, which can make it less obvious how to handle **dynamic** scenarios — cases where you don't know at compile time which bins to write, how many operations to perform, or which keys to operate on.

This guide shows you how.

---

## Table of Contents

1. [Setting Bins Dynamically](#1-setting-bins-dynamically)
2. [The bins/values Shorthand](#2-the-binsvalues-shorthand)
3. [Building Operations Dynamically](#3-building-operations-dynamically)
4. [Heterogeneous Batch Operations](#4-heterogeneous-batch-operations)
5. [Dynamic CDT (List & Map) Operations](#5-dynamic-cdt-list--map-operations)
6. [Object Mapping](#6-object-mapping)
7. [Dynamic UDF Arguments](#7-dynamic-udf-arguments)
8. [Transactions with Dynamic Operations](#8-transactions-with-dynamic-operations)
9. [Quick Reference](#9-quick-reference)

---

## 1. Setting Bins Dynamically

### The Problem

In the standard client you might write:

```java
// Standard client — bins from a Map
List<Bin> bins = new ArrayList<>();
for (Map.Entry<String, Object> e : data.entrySet()) {
    bins.add(new Bin(e.getKey(), e.getValue()));
}
client.put(policy, key, bins.toArray(new Bin[0]));
```

In the fluent client, `.bin("name").setTo(value)` returns the builder so you can chain more calls. The key insight: **the builder itself is mutable and accumulates operations**, so you can call `.bin().setTo()` in a loop just like you'd add to a list.

### From a Map

```java
Map<String, Object> data = Map.of(
    "name", "Alice",
    "age", 30,
    "city", "London"
);

// Start the builder, then loop
var builder = session.upsert(key);
for (var entry : data.entrySet()) {
    builder.bin(entry.getKey()).setTo(entry.getValue());
}
builder.execute();
```

> **Why does this work?** Each call to `.bin("x").setTo(v)` adds an operation to the builder's internal list and returns the **same builder**. The loop simply adds more operations before you call `.execute()`.

### From Parallel Arrays

```java
String[] names = {"name", "age", "score"};
Object[] values = {"Bob", 25, 98.5};

var builder = session.upsert(key);
for (int i = 0; i < names.length; i++) {
    builder.bin(names[i]).setTo(values[i]);
}
builder.execute();
```

### Conditional Bins

```java
var builder = session.upsert(key)
    .bin("name").setTo(name);

if (email != null) {
    builder.bin("email").setTo(email);
}
if (phone != null) {
    builder.bin("phone").setTo(phone);
}

builder.execute();
```

---

## 2. The bins/values Shorthand

When writing the **same set of bins** across **multiple keys** — potentially with **different values per key** — the `bins()` / `values()` pattern is the cleanest approach.

### Same Values for All Keys

```java
List<Key> keys = List.of(
    users.id("u1"),
    users.id("u2"),
    users.id("u3")
);

session.upsert(keys)
    .bins("status", "updatedAt")
    .values("active", System.currentTimeMillis())
    .execute();
// All three keys get status="active" and the same timestamp.
```

### Different Values Per Key

Call `.values()` once per key, in order:

```java
session.upsert(users.id("u1"), users.id("u2"), users.id("u3"))
    .bins("name", "age")
    .values("Alice", 30)   // u1
    .values("Bob", 25)     // u2
    .values("Carol", 35)   // u3
    .execute();
```

### Different Values Per Key — From a Collection

```java
record Person(String id, String name, int age) {}

List<Person> people = List.of(
    new Person("u1", "Alice", 30),
    new Person("u2", "Bob", 25),
    new Person("u3", "Carol", 35)
);

List<Key> keys = people.stream()
    .map(p -> users.id(p.id()))
    .toList();

var builder = session.upsert(keys)
    .bins("name", "age");

for (Person p : people) {
    builder.values(p.name(), p.age());
}

builder.execute();
```

### Per-Key Options

With `bins/values`, you can set options like TTL or generation **per record**:

```java
session.upsert(users.id("u1"), users.id("u2"))
    .bins("name", "tier")
    .values("Alice", "premium")
        .expireRecordAfter(Duration.ofDays(365))
    .values("Bob", "trial")
        .expireRecordAfter(Duration.ofDays(30))
    .execute();
```

---

## 3. Building Operations Dynamically

### The Problem

In the standard client, you might build a list of `Operation` objects:

```java
List<Operation> ops = new ArrayList<>();
ops.add(Operation.put(new Bin("name", "Tim")));
ops.add(MapOperation.getByKey("rooms", Value.get("room1"), ...));
client.operate(policy, key, ops.toArray(new Operation[0]));
```

In the fluent client, **the builder chain *is* your list**. You build it up step by step, and each step adds to the operation list internally.

### Mixing Bin Writes and CDT Operations

```java
session.update(key)
    .bin("name").setTo("Tim")
    .bin("rooms").onMapKey("room1").count()
    .execute();
```

### Dynamic Operations in a Loop

```java
// Apply a set of updates that are determined at runtime
Map<String, String> updates = getUpdatesFromSomewhere();

var builder = session.update(key);

for (var entry : updates.entrySet()) {
    builder.bin(entry.getKey()).setTo(entry.getValue());
}

// Optionally add CDT operations too
List<String> keysToRemove = getKeysToRemoveFromSomewhere();
for (String mapKey : keysToRemove) {
    builder.bin("metadata").onMapKey(mapKey).remove();
}

builder.execute();
```

### Conditional Operations

```java
var builder = session.update(key)
    .bin("lastUpdated").setTo(System.currentTimeMillis());

if (shouldUpdateName) {
    builder.bin("name").setTo(newName);
}

if (shouldIncrementCounter) {
    builder.bin("visits").add(1);
}

if (shouldAddToList) {
    builder.bin("tags").listAppend("verified");
}

builder.execute();
```

---

## 4. Heterogeneous Batch Operations

### The Problem

Sometimes you need a single network round-trip to perform **different operations on different keys** — update one record, delete another, run a UDF on a third. In the standard client, you'd use `BatchWrite`, `BatchDelete`, `BatchUDF`, etc. in a list.

In the fluent client, you **chain different operation types**. Each call to `.upsert(key2)` or `.delete(key3)` starts a **new operation spec** in the same batch.

### Different Write Operations Per Key

```java
RecordStream results = session
    .upsert(users.id("u1"))
        .bin("name").setTo("Alice")
        .bin("status").setTo("active")
    .update(users.id("u2"))
        .bin("loginCount").add(1)
    .delete(users.id("u3"))
    .touch(users.id("u4"))
        .expireRecordAfter(Duration.ofDays(30))
    .execute();
```

This sends **one batch request** to the server containing four distinct operations.

### Mixing Writes, Reads, and UDFs

```java
RecordStream results = session
    .upsert(users.id("u1"))
        .bin("name").setTo("Alice")
    .query(users.id("u2"))
        .readingOnlyBins("name", "email")
    .executeUdf(users.id("u3"))
        .function("myPackage", "myFunction")
        .passing("arg1", 42)
    .execute();

// Results come back in order — one RecordResult per operation
```

### Building a Heterogeneous Batch Dynamically

This is where the fluent pattern requires a little more thought. Because each operation type (upsert, update, delete) transitions the builder to a different type, you need to use a common parent type or structure your loop carefully.

**Pattern: Build from a list of "tasks"**

```java
record WriteTask(Key key, Map<String, Object> bins) {}
record DeleteTask(Key key) {}

List<WriteTask> writes = ...;
List<DeleteTask> deletes = ...;

// Start with the first write
var builder = session.upsert(writes.get(0).key());
for (var entry : writes.get(0).bins().entrySet()) {
    builder.bin(entry.getKey()).setTo(entry.getValue());
}

// Chain remaining writes
for (int i = 1; i < writes.size(); i++) {
    builder = builder.upsert(writes.get(i).key());
    for (var entry : writes.get(i).bins().entrySet()) {
        builder.bin(entry.getKey()).setTo(entry.getValue());
    }
}

// Chain deletes (transitions to ChainableNoBinsBuilder)
if (!deletes.isEmpty()) {
    var deleteBuilder = builder.delete(deletes.get(0).key());
    for (int i = 1; i < deletes.size(); i++) {
        deleteBuilder = deleteBuilder.delete(deletes.get(i).key());
    }
    deleteBuilder.execute();
} else {
    builder.execute();
}
```

**Pattern: Same operation on many keys (simpler)**

When the operation is the same for all keys, just pass the list of keys:

```java
List<Key> keys = customers.stream()
    .map(c -> users.id(c.getId()))
    .toList();

session.update(keys)
    .bin("status").setTo("migrated")
    .bin("migratedAt").setTo(System.currentTimeMillis())
    .execute();
```

---

## 5. Dynamic CDT (List & Map) Operations

CDT (Collection Data Type) operations support dynamic data natively through `List` and `Map` parameters.

### Lists — Bulk Append/Add

```java
List<Object> newItems = computeNewItems();  // Runtime data

session.update(key)
    .bin("myList").listAppendItems(newItems)
    .execute();
```

With options:

```java
session.update(key)
    .bin("myList").listAppendItems(newItems, opt -> opt.allowFailures())
    .execute();
```

### Maps — Bulk Upsert/Insert/Update

```java
Map<String, Object> newEntries = Map.of(
    "key1", "value1",
    "key2", 42
);

session.update(key)
    .bin("myMap").mapUpsertItems(newEntries)
    .execute();
```

With options:

```java
session.update(key)
    .bin("myMap").mapInsertItems(entries, opt -> opt.allowPartial().allowFailures())
    .execute();
```

### Dynamic Map Key Navigation

```java
String roomId = getRoomIdFromRequest();  // Runtime value

session.update(key)
    .bin("rooms").onMapKey(roomId).upsert("occupied")
    .execute();
```

### Nested CDT — Dynamic Path

```java
String category = "electronics";
String product = "laptop";

long count = session.query(key)
    .bin("inventory")
        .onMapKey(category)
        .onMapKey(product)
        .get()
    .execute()
    .getFirstRecord()
    .getLong("inventory");
```

---

## 6. Object Mapping

When your data lives in Java objects, you don't need to decompose them into bins manually. Use a `RecordMapper` to handle the conversion.

### Writing Objects

```java
RecordMapper<Customer> mapper = new RecordMapper<>() {
    public Map<String, Object> toMap(Customer c) {
        return Map.of(
            "name", c.getName(),
            "email", c.getEmail(),
            "tier", c.getTier()
        );
    }

    public Key toKey(Customer c) {
        return customers.id(c.getId());
    }

    public Customer fromMap(Map<String, Object> map, Key key, int gen) {
        return new Customer(/* ... */);
    }
};

session.upsert(customers)
    .object(customer)
    .using(mapper)
    .execute();
```

### Writing Multiple Objects

```java
List<Customer> customerList = getCustomersToUpdate();

for (Customer c : customerList) {
    session.upsert(customers)
        .object(c)
        .using(mapper)
        .execute();
}
```

> **Note:** Each `.execute()` is a separate server call. If you need these in a single batch, use the `bins/values` pattern or the heterogeneous batch chain described in Section 4.

---

## 7. Dynamic UDF Arguments

UDF arguments can be passed from runtime data:

### From Varargs

```java
session.executeUdf(key)
    .function("myPackage", "myFunction")
    .passing("arg1", 42, true)
    .execute();
```

### From a List

```java
List<Object> args = buildArgsFromConfig();

session.executeUdf(key)
    .function("myPackage", "myFunction")
    .passing(args)
    .execute();
```

### Background UDF with Dynamic Args

```java
double discountRate = getDiscountRate();

ExecuteTask task = session.backgroundTask()
    .executeUdf(products)
    .function("inventory", "applyDiscount")
    .passing(discountRate)
    .where("$.stock > %d", minStock)
    .execute();

task.waitTillComplete();
```

---

## 8. Transactions with Dynamic Operations

When multiple operations need to succeed or fail atomically, wrap them in a transaction. Inside the lambda, you can use all the dynamic patterns described above.

```java
session.doInTransaction(txn -> {
    // Debit one account
    txn.update(accounts.id("acc1"))
        .bin("balance").add(-transferAmount)
        .execute();

    // Credit another
    txn.update(accounts.id("acc2"))
        .bin("balance").add(transferAmount)
        .execute();

    // Log the transfer
    txn.upsert(transfers.id(transferId))
        .bin("from").setTo("acc1")
        .bin("to").setTo("acc2")
        .bin("amount").setTo(transferAmount)
        .bin("timestamp").setTo(System.currentTimeMillis())
        .execute();
});
```

### Dynamic Transactions

```java
session.doInTransaction(txn -> {
    for (LineItem item : order.getItems()) {
        txn.update(inventory.id(item.getProductId()))
            .bin("stock").add(-item.getQuantity())
            .execute();
    }

    txn.upsert(orders.id(order.getId()))
        .bin("status").setTo("confirmed")
        .bin("total").setTo(order.getTotal())
        .execute();
});
```

---

## 9. Quick Reference

| I want to...                                        | Use this pattern                                    |
| --------------------------------------------------- | --------------------------------------------------- |
| Set bins from a `Map<String, Object>`               | Loop: `.bin(name).setTo(value)`                     |
| Set the same bins on multiple keys                  | `session.upsert(keys).bin("x").setTo(v).execute()`  |
| Set different values per key                        | `.bins("a", "b").values(1, 2).values(3, 4)`         |
| Build operations conditionally                      | `var b = session.upsert(key); if (...) b.bin(...)...` |
| Mix writes + reads + deletes in one batch           | Chain: `.upsert(k1).bin(...).delete(k2).query(k3)`  |
| Add many items to a list                            | `.bin("list").listAppendItems(myList)`               |
| Add many entries to a map                           | `.bin("map").mapUpsertItems(myMap)`                  |
| Write a Java object                                 | `.object(myObj).using(mapper).execute()`             |
| Pass dynamic UDF arguments                          | `.passing(myList)` or `.passing(a, b, c)`            |
| Atomic multi-key operations                         | `session.doInTransaction(txn -> { ... })`            |
| Background bulk operation                           | `session.backgroundTask().update(set).where(...)`    |
| Background UDF on a set                             | `session.backgroundTask().executeUdf(set).function(...)` |
