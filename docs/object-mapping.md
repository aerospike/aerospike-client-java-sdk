# Object mapping with the Aerospike Java SDK

Aerospike stores **bins** (name → value). Your application thinks in **Java types** (`Customer`, `Order`, …). **Object mapping** is the bridge: you teach the SDK how to turn bins into instances and back, then you can **read and write domain objects** instead of hand-rolling maps and casts on every call.

This guide explains **why** that matters, **how** to set it up in a few steps, then **how far the compiler** can help you (typed streams vs heterogeneous batches), and **which** `RecordMapper.fromMap` overload runs in which APIs.

For batch ordering, omitted rows, and heterogeneous chains, see [Heterogeneous Batch Chains](key-features.md#heterogeneous-batch-chains) in *Key Features*.

---

## Table of contents

- [Why object mapping?](#why-object-mapping)
- [What you get from the SDK](#what-you-get-from-the-sdk)
- [Learn by doing: one record end-to-end](#learn-by-doing-one-record-end-to-end)
- [Reading with a plain `Key` (explicit mapper)](#reading-with-a-plain-key-explicit-mapper)
- [How the pieces fit together](#how-the-pieces-fit-together)
- [Compile-time vs runtime type safety](#compile-time-vs-runtime-type-safety)
- [`RecordMapper`: three-argument vs four-argument `fromMap`](#recordmapper-three-argument-vs-four-argument-frommap)
- [Which APIs call which `fromMap` overload](#which-apis-call-which-frommap-overload)
- [More examples](#more-examples)
- [See also](#see-also)

---

## Why object mapping?

### Without mapping: every read is manual

After a point read you get a **`Record`** (bins as `Map<String, Object>`). You cast, null-check, and repeat field names everywhere:

```java
// Untyped key read — works, but tedious and easy to break
try (RecordStream rs = session.query(users.id("alice@example.com")).execute()) {
    Record rec = rs.next().recordOrThrow();
    String name = rec.getString("name");
    int age = rec.getInt("age");
    // ... build a Customer yourself, or pass loose maps around
}
```

That is fine for one-off scripts. It does not scale when dozens of features touch **`Customer`** rows: refactors miss a string literal, types drift, and tests duplicate parsing logic.

### With mapping: bins ↔ `Customer` in one place

You implement **`RecordMapper<Customer>`** once (bins → `Customer`, `Customer` → bins, id for keys). You register it on the **`Cluster`**. Then **typed** reads can return **`Customer`** (or **`Optional<Customer>`**, **`List<Customer>`**) **without** passing a mapper at every call site.

```java
TypedDataSet<Customer> users = TypedDataSet.of("test", "users", Customer.class);

// After factory is registered (see walkthrough below):
try (TypedRecordStream<Customer> rs = session.query(users.id("alice@example.com")).execute()) {
    Optional<Customer> alice = rs.getFirstObject(); // Optional<Customer> — not Optional<Map>
}
```

**Why you care:** less boilerplate, one place to evolve your schema mapping, and (on typed read paths) **stronger typing** so `getFirstObject()` really is a `Customer`, not “whatever was in the map.”

---

## What you get from the SDK

| Goal | Mechanism |
|------|-----------|
| **Centralize** bin ↔ Java conversion | Implement **`RecordMapper<T>`** and register **`RecordMappingFactory`** on **`Cluster`**. |
| **Tell the SDK which `T` a read is for** | Use **`TypedDataSet<T>`** (set-level queries) or **`TypedKey<T>`** (point reads / typed batch legs) so results carry a type hint. |
| **Get `List<T>` / `Optional<T>` from a stream** | **`TypedRecordStream<T>`** (from **`query(TypedDataSet)`**, **`query(TypedKey)`** / **`query(TypedKey, TypedKey, …)`** / **`queryTypedKeys(List<TypedKey<T>>)`** with keys from **`TypedDataSet#ids`** (`TypedKeyList<T>`) while the chain stays a single typed point read). |
| **Mix multiple types in one batch** | **`ChainableQueryBuilder`** → **`RecordStream`**; each **`RecordResult.toObject()`** uses **per-row** hints (runtime typing). |

---

## Learn by doing: one record end-to-end

Assume namespace **`"test"`**, set **`"users"`**, and bins **`name`**, **`email`**, **`age`**.

### 1. A small domain type

```java
public final class Customer {
    private final String email;
    private final String name;
    private final int age;

    public Customer(String email, String name, int age) {
        this.email = email;
        this.name = name;
        this.age = age;
    }
    public String email() { return email; }
    public String name() { return name; }
    public int age() { return age; }
}
```

### 2. A `RecordMapper<Customer>`

You **must** implement **`fromMap(Map, Key, int)`**, **`toMap`**, and **`id`**. The four-argument **`fromMap(..., RecordReadContext)`** is optional (see [later section](#recordmapper-three-argument-vs-four-argument-frommap)).

```java
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordMapper;

public final class CustomerMapper implements RecordMapper<Customer> {

    @Override
    public Customer fromMap(Map<String, Object> map, Key recordKey, int generation) {
        String email = (String) map.get("email");
        String name = (String) map.get("name");
        int age = ((Number) map.get("age")).intValue();
        return new Customer(email, name, age);
    }

    @Override
    public Map<String, Object> toMap(Customer c) {
        Map<String, Object> m = new HashMap<>();
        m.put("email", c.email());
        m.put("name", c.name());
        m.put("age", c.age());
        return m;
    }

    @Override
    public Object id(Customer c) {
        return c.email(); // must match how you build keys (e.g. string user key)
    }
}
```

### 3. Register the factory on the cluster

```java
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DefaultRecordMappingFactory;
import com.aerospike.client.sdk.Session;

RecordMapper<Customer> customerMapper = new CustomerMapper();
cluster.setRecordMappingFactory(DefaultRecordMappingFactory.of(Customer.class, customerMapper));
Session session = cluster.createSession(/* Behavior */);
```

### 4. Write a `Customer` as bins

**`TypedDataSet<Customer>`** binds namespace, set name, and **`Customer.class`** so inserts know which mapper to use:

```java
import com.aerospike.client.sdk.TypedDataSet;

TypedDataSet<Customer> users = TypedDataSet.of("test", "users", Customer.class);
Customer alice = new Customer("alice@example.com", "Alice", 30);

session.insert(users).object(alice).execute();
```

### 5. Read it back with a typed **dataset** query

```java
import java.util.List;

List<Customer> gold = session.query(users)
    .where("$.email == 'alice@example.com'")
    .execute()
    .toObjectList(); // List<Customer> — compiler knows the element type
```

### 6. Read by **typed key** (single-key point read)

```java
import java.util.Optional;

import com.aerospike.client.sdk.TypedRecordStream;

try (TypedRecordStream<Customer> rs = session.query(users.id("alice@example.com")).execute()) {
    Optional<Customer> one = rs.getFirstObject(); // Optional<Customer>
}
```

**Note:** **`session.query(TypedKey<T>)`** returns **`TypedKeyQueryBuilder<T>`** and **`TypedRecordStream<T>`** only while the chain is still a **single** typed point read. If you chain a second **`query`**, a write, **`executeUdf`**, etc., the builder **widens** to **`ChainableQueryBuilder`** and **`execute()`** returns **`RecordStream`** again (see *Key Features*).

You now have the full picture: **mapper + factory + typed entry point** → **objects in and out** without repeating bin names at every read.

---

## Reading with a plain `Key` (explicit mapper)

If you only have **`Key`** (no **`TypedKey`** / **`TypedDataSet`** read), the stream does not know **`Customer.class`**. You still **can** map — pass **`RecordMapper`** to **`RecordStream`** helpers:

```java
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.RecordStream;

DataSet usersUntyped = users.asDataSet(); // same ns/set, no Class<T>

try (RecordStream rs = session.query(usersUntyped.id("alice@example.com")).execute()) {
    Optional<Customer> alice = rs.getFirst(customerMapper); // mapper required at call site
}
```

This is the same **`CustomerMapper`**; the difference is **who supplies** it (factory + typed path vs you at the call site).

---

## How the pieces fit together

| Type | Role |
|------|------|
| **`RecordMapper<T>`** | **Your** code: bins ↔ **`T`**, plus **`id(T)`** for object-based writes. |
| **`RecordMappingFactory` / `DefaultRecordMappingFactory`** | Looks up **`RecordMapper<T>`** by **`Class<T>`** on the cluster. |
| **`TypedDataSet<T>`** | Namespace + set + **`Class<T>`** for **typed writes** and **index / set queries** → **`TypedQueryBuilder<T>`** → **`TypedRecordStream<T>`**. |
| **`TypedKey<T>`** | **`Key` + `Class<T>`** so point reads (and homogeneous typed-key batch legs) attach a **read hint** on each **`RecordResult`**. |
| **`RecordReadContext<T>`** | **`Session` + `Class<T>`** passed into the **four-argument** `fromMap` when the SDK has a session and entity type (dependent reads, nested objects). |

---

## Compile-time vs runtime type safety

### When the compiler knows `T` (`TypedRecordStream<T>`)

When the API returns **`TypedRecordStream<T>`**, methods are typed on **`T`** — e.g. **`Optional<T> getFirstObject()`**, **`List<T> toObjectList()`**:

- **`session.query(TypedDataSet<T>)`** → **`TypedQueryBuilder<T>`** → **`TypedRecordStream<T>`**.
- **`session.query(TypedKey<T>)`** (single key) → **`TypedKeyQueryBuilder<T>`** → **`TypedRecordStream<T>`**.
- **`session.query(TypedKey<T>, TypedKey<T>, TypedKey<T>...)`** or **`session.queryTypedKeys(List<TypedKey<T>>)`** (same **`T`**) → **`TypedKeyQueryBuilder<T>`** → **`TypedRecordStream<T>`** for that single typed read spec (any number of keys in the spec). If you then chain another **`query`**, a write, **`executeUdf`**, etc., the builder **widens** to **`ChainableQueryBuilder`** and **`execute()`** returns **`RecordStream`** (see *Key Features*).
- **Typed writes** such as **`session.insert(TypedDataSet<T>).object(T)`** bind the payload to **`T`** at compile time.

Here, “this stream is **`Customer`**” is expressed in **generics** on **`TypedRecordStream<T>`**.

```java
import java.util.List;
import java.util.Optional;

import com.aerospike.client.sdk.TypedKey;
import com.aerospike.client.sdk.TypedRecordStream;

// Example: Customer, TypedDataSet<Customer> users, Session session

TypedKey<Customer> alice = users.id("alice@example.com");

try (TypedRecordStream<Customer> stream = session.query(alice).bin("name").get().execute()) {
    Optional<Customer> row = stream.getFirstObject(); // Optional<Customer> — no cast
}
```

Chaining **another** read/write/UDF step on the same builder **widens** it (see *Key Features*): **`execute()`** then returns **`RecordStream`**, not **`TypedRecordStream<Customer>`**.

```java
import com.aerospike.client.sdk.RecordStream;

try (RecordStream rs = session.query(alice).bin("name").get()
        .query(users.id("bob@example.com")).bin("name").get()
        .execute()) {
    Customer first = rs.next().toObject();
    Customer second = rs.next().toObject(); // still Customer rows, but the stream type is not generic on Customer
}
```

### Homogeneous multi-key reads (`queryTypedKeys`, `query(TypedKey, TypedKey, …)`)

**Common case (preferred):** you have **several keys for the same entity** and the compiler sees **`List<TypedKey<T>>`** (or **`TypedDataSet#ids`** inferred that way). Use these so you get **`TypedKeyQueryBuilder<T>`** → **`TypedRecordStream<T>`** (mapper-free **`toObjectList()`**, **`getFirstObject()`**, …) for that **single** read spec:

- **`session.queryTypedKeys(List<TypedKey<T>>)`** — e.g. **`session.queryTypedKeys(users.ids(1, 2, 3))`** — fixes **`T`** at compile time (`ids` returns **`TypedKeyList<T>`**, which is a **`List<TypedKey<T>>`**).
- **`session.query(TypedKey<T> k1, TypedKey<T> k2, TypedKey<T>... more)`** — same **`T`** enforced by the varargs signature.

```java
import java.util.List;

import com.aerospike.client.sdk.TypedKeyList;
import com.aerospike.client.sdk.TypedRecordStream;

TypedKeyList<Customer> keys = users.ids(1, 2, 3); // or users.ids("a", "b")

try (TypedRecordStream<Customer> stream = session.queryTypedKeys(keys).bin("email").get().execute()) {
    List<Customer> rows = stream.toObjectList(); // List<Customer>
}

// Varargs overload keeps a single T on the whole call:
try (TypedRecordStream<Customer> stream2 =
        session.query(users.id(1), users.id(2), users.id(3)).bin("email").get().execute()) {
    stream2.forEachObject(c -> { /* Consumer<Customer> */ });
}
```

#### `queryTypedKeysAny` (unusual — wildcard list only)

Use **`session.queryTypedKeysAny(List<? extends TypedKey<?>>)`** only when the key list is **stuck at a wildcard type** at compile time — for example a library method returns **`List<? extends TypedKey<?>>`**, or a generic helper is declared with that wildcard so **`queryTypedKeys(List<TypedKey<T>>)`** does not compile. **`Session`** cannot add a second overload that differs only by generic type arguments on **`List`** (they erase to the same raw **`List`**), so this separate method name is the escape hatch.

You still get a **homogeneous** batch at **runtime** (one entity class per leg); mixing **`TypedKey<Customer>`** and **`TypedKey<Order>`** in one list throws **`IllegalArgumentException`** from **`TypedKey.requireSharedEntityClass`** — **`queryTypedKeysAny`** is **not** for that scenario.

**Trade-off:** **`queryTypedKeysAny`** returns **`ChainableQueryBuilder`** → **`RecordStream`** (not **`TypedKeyQueryBuilder<T>`** / **`TypedRecordStream<T>`**) because **`T`** is not in the method signature. Per-row **`RecordResult.toObject()`** still works on successful typed legs when the batch is homogeneous.

**Per-row mapping** on **`RecordStream`** typed legs is unchanged: **`RecordResult.toObject()`** uses the row hint.

```java
import java.util.List;

import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.TypedKey;

List<? extends TypedKey<?>> keys = fetchKeysSomehow(); // library returns wildcard-typed list

try (RecordStream rs = session.queryTypedKeysAny(keys).bin("name").get().execute()) {
    while (rs.hasNext()) {
        RecordResult rr = rs.next();
        Customer c = rr.toObject(); // row carries readMappingClass from the typed keys
    }
}
```

### Heterogeneous batch chains (runtime only)

When you chain **different** read legs (e.g. customers then orders) or mix reads and writes on **`ChainableQueryBuilder`**, **`execute()`** returns **`RecordStream`**. Each **`RecordResult.toObject()`** uses that row’s hint. The compiler cannot prove **`Customer c = rs.next().toObject()`** matches the next row — you rely on **order** and **row metadata** (see *Key Features*).

```java
import com.aerospike.client.sdk.TypedDataSet;
// Example: TypedDataSet<Customer> users; TypedDataSet<Order> orders;

// First leg is typed; chaining a second query with a different entity widens to RecordStream:
try (RecordStream rs = session.query(users.id("a@example.com"))
        .bin("name").get()
        .query(orders.id(100L))
        .bin("total").get()
        .execute()) {

    RecordResult rCustomer = rs.next();
    RecordResult rOrder = rs.next();

    Customer c = rCustomer.toObject(); // per-row read hint; stream itself is untyped RecordStream
    Order o = rOrder.toObject();
}
```

---

## `RecordMapper`: three-argument vs four-argument `fromMap`

```java
T fromMap(Map<String, Object> map, Key recordKey, int generation);

default T fromMap(Map<String, Object> map, Key recordKey, int generation, RecordReadContext<T> ctx) {
    return fromMap(map, recordKey, generation);
}
```

| Method | Required? | When to use / override |
|--------|-----------|-------------------------|
| **`fromMap(map, key, generation)`** | **Yes** (abstract) | Core deserialization: bins + key + generation only. **Always** implement this. |
| **`fromMap(..., RecordReadContext)`** | No (has default) | Override when you need **`ctx.getSession()`**, **`getRecordMappingFactory()`**, or **`getEntityClass()`** (nested bins, lazy references, extra reads). |

You **cannot** implement only the four-argument overload.

**Typical pattern:** put real field extraction in **`fromMap(map, key, gen)`** (or a private helper). Override **`fromMap(..., ctx)`** only where you need the session/factory, and delegate to the same helper or to **`fromMap(map, key, gen)`** when context is irrelevant.

**Tiny example — dependent read only in four-arg:**

```java
@Override
public Customer fromMap(Map<String, Object> map, Key recordKey, int generation) {
    return new Customer(
        (String) map.get("email"),
        (String) map.get("name"),
        ((Number) map.get("age")).intValue());
}

@Override
public Customer fromMap(Map<String, Object> map, Key recordKey, int generation, RecordReadContext<Customer> ctx) {
    Customer base = fromMap(map, recordKey, generation);
    // if (map.containsKey("profileId")) { ... ctx.getSession().query(...). ... }
    return base;
}
```

---

## Which APIs call which `fromMap` overload

### Four-argument `fromMap(..., RecordReadContext<T>)`

Used when the SDK can build **`RecordReadContext`** (session + entity class):

| Location | When |
|----------|------|
| **`RecordResult.toObject()`** | Row has **`readMappingSession`** + **`readMappingClass`** (typed read / typed leg). |
| **`RecordResult.udfResultAsObject()`** (mapper-less, typed UDF hint) | Same. |
| **`RecordResult.udfResultAsObject(RecordMapper, RecordReadContext)`** | You pass **`ctx`**. |
| **`TypedRecordStream` / `TypedNavigatableRecordStream`** | Mapper-less **`toObjectList()`**, **`getFirstObject()`**, …; **`RecordMapper`** overloads that take **`RecordReadContext`**. |
| **`MapUtil.asObjectFromMap(..., RecordReadContext)`** | Nested mapping with context. |

### Three-argument `fromMap(map, key, generation)`

Used when you pass a **`RecordMapper`** but the SDK does **not** thread **`RecordReadContext`**:

| Location | When |
|----------|------|
| **`RecordStream`** | **`getFirst(RecordMapper)`**, **`toObjectList(RecordMapper)`**, **`forEach(RecordMapper, …)`**, … |
| **`NavigatableRecordStream`** | Explicit **`RecordMapper`** APIs. |
| **`RecordResult.udfResultAsObject(RecordMapper)`** | Single mapper argument. |

**Rule of thumb:** typed factory-backed streams and **`RecordResult.toObject()`** on typed legs → **four-arg** path (override when you need **`Session`**). **`RecordStream`** + mapper you pass in → **three-arg**, unless you use an overload that documents **`RecordReadContext`**.

---

## More examples

### Typed multi-key read (same entity class)

```java
TypedKey<Customer> k1 = users.id("a@example.com");
TypedKey<Customer> k2 = users.id("b@example.com");

try (TypedRecordStream<Customer> rs = session.query(k1, k2).execute()) {
    List<Customer> both = rs.toObjectList();
}
```

### Wrong `TypedRecordStream` type (does not compile)

```java
// TypedDataSet<Customer> users = ...;
// TypedRecordStream<Order> wrong = session.query(users.id("x")).execute(); // compile error
```

### Two entity classes in one typed-key varargs leg (compiles, fails at runtime)

```java
TypedDataSet<Order> orders = TypedDataSet.of("test", "users", Order.class);
TypedKey<Customer> ck = users.id("a@example.com");
TypedKey<Order> ok = orders.id("order-1");

session.queryTypedKeysAny(List.of(ck, ok)).execute(); // IllegalArgumentException — one class per typed-key leg
```

**Fix:** use **two** **`query(...)`** legs (heterogeneous batch), or untyped **`Key`** reads with explicit mappers.

### Heterogeneous batch (compiles; order and omitted rows matter)

```java
TypedDataSet<Customer> customers = TypedDataSet.of("test", "users", Customer.class);
TypedDataSet<Order> orders = TypedDataSet.of("test", "users", Order.class);
// Factory must register both Customer.class and Order.class

try (RecordStream rs = session
        .query(customers.id(1001))
        .query(orders.id("order-9"))
        .execute()) {

    Customer c = rs.next().toObject();
    Order o = rs.next().toObject();
}
```

See [Heterogeneous Batch Chains](key-features.md#heterogeneous-batch-chains) for **ordering** and **omitted rows** (do not blindly `next()` twice without checking **`isOk()`** / **`key()`** when failures or filters drop rows).

---

## See also

- [`RecordMapper`](../client/src/main/java/com/aerospike/client/sdk/RecordMapper.java) — interface Javadoc.
- [`RecordReadContext`](../client/src/main/java/com/aerospike/client/sdk/RecordReadContext.java).
- [`RecordMappingFactory`](../client/src/main/java/com/aerospike/client/sdk/RecordMappingFactory.java) / [`DefaultRecordMappingFactory`](../client/src/main/java/com/aerospike/client/sdk/DefaultRecordMappingFactory.java).
- [Key Features — Object Mapping](key-features.md#object-mapping) — read entry point table.
- [Key Features — Heterogeneous Batch Chains](key-features.md#heterogeneous-batch-chains).
