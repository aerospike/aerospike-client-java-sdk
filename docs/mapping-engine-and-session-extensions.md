# Mapping engine and pluggable session extensions

Implementation specification for integrating the Aerospike SDK Mapper (JOM) with the Java SDK via a **cluster-scoped mapping engine** and **optional pluggable session extensions**. This document captures agreed design decisions only.

## Goals

1. **Behavior belongs on `Session`**, not on a mapper object. One mapping configuration per cluster; many sessions with different `Behavior` profiles.
2. **SDK object mapping continues to work on plain `Session`** — no extended session type required.
3. **Mapper-module extras** (imperative CRUD, virtual lists, reactive helpers, etc.) are available only when the application opts in via a **`MappingSession`** created through a pluggable session extension.
4. **Compile-time safety** when creating extended sessions.
5. **Minimal SDK surface** — core SDK changes are generic (`SessionExtension` + `createSession` overload only). Mapping wiring stays in the mapper module.

---

## Two layers (do not conflate)

| Layer | What it is | Session type | How you use it |
|-------|------------|--------------|----------------|
| **SDK object mapping** | `RecordMapper`, `RecordMappingFactory`, `TypedDataSet`, `.object()` / `.toObjectList()` | Plain `Session` (or `TransactionalSession`) | Register factory on cluster; use normal session APIs |
| **Mapper module extensions** | Annotation/YAML model, `save` / `read(Class, key)`, virtual lists, reactive mapper, etc. | `MappingSession` (opt-in) | `cluster.createSession(behavior, engine.sessionExtension())` |

**FAQ (document this clearly for users):**

- **Can I use mapping without a `MappingSession`?** Yes. Call `engine.installOn(cluster)` and use `Session` with `TypedDataSet` and typed streams.
- **What is `MappingSession` for?** Mapper-module-only methods that the base `Session` deliberately does not expose. It is **not** required to enable mapping.

---

## Architecture overview

```text
Cluster
  ├── RecordMappingFactory  ← set by MappingEngine.installOn(cluster)
  ├── createSession(behavior)                    → Session
  └── createSession(behavior, extension)         → S extends Session (e.g. MappingSession)

MappingEngine (mapper module, cluster-scoped)
  ├── installOn(cluster)
  ├── sessionExtension()  → SessionExtension<MappingSession>
  ├── typedDataSet(Class)
  └── imperative APIs (save, read, …) taking Session

MappingSession extends Session
  └── delegates mapper-module APIs to MappingEngine, passing the active Session for execution
```

**Separation of concerns:**

| Concern | Scope |
|---------|--------|
| Introspection, factory, annotation-derived datasets | **MappingEngine** (mapper module) |
| Timeouts, retries, consistency | **Session** (`Behavior`) |
| Imperative mapper API, virtual lists | **`MappingSession`** (behavior + engine) |

---

## Core SDK changes (generic only)

The core SDK gains **no mapping-specific API**. Changes are limited to pluggable session extensions so other derivatives (metrics, audit, etc.) can use the same mechanism later.

### `SessionExtension<S>`

```java
public interface SessionExtension<S extends Session> {
    S create(Cluster cluster, Behavior behavior);
}
```

### `Cluster.createSession` overload

Keep the existing method unchanged. Add a generic overload:

```java
public Session createSession(Behavior behavior) {
    return new Session(this, behavior);
}

public <S extends Session> S createSession(Behavior behavior, SessionExtension<S> extension) {
    return extension.create(this, behavior);
}
```

**Compile-time safety:** the extension must implement `SessionExtension<MappingSession>` (or another concrete session type). The compiler infers `S` from the extension argument:

```java
MappingSession session = cluster.createSession(durableBehavior, engine.sessionExtension());
//     ^ MappingSession — no cast
```

### `Session` constructor

`Session(Cluster, Behavior)` is already `protected`. Extended session types in extension modules use the same pattern as `TransactionalSession`.

---

## Mapper module: `MappingEngine`

Replace the session-bound role of `AeroMapper` for **shared state**. The engine owns everything that is not behavior-specific:

- Class introspection (`ClassCache`)
- YAML / class configuration
- Custom converters
- `RecordMappingFactory` implementation (refactored from `AeroRecordMappingFactory`)
- `typedDataSet(Class<T>)`, `getNamespace(Class)`, `getSet(Class)`, key helpers, etc.
- `SessionExtension<MappingSession>` factory via `sessionExtension()`

**Critical refactor:** `RecordMappingFactory` must **not** hold a session-bound `IBaseAeroMapper`. It holds the `MappingEngine`. Reads resolve the executing session from `RecordReadContext.getSession()` (already true in `AeroRecordMapper`). Imperative writes and virtual lists pass the **active `Session`** into the engine.

### Bootstrap — `installOn(cluster)`

The mapper module wires itself onto the cluster. The SDK is not taught about mapping install:

```java
MappingEngine engine = MappingEngine.builder()
    .withYaml("mapping.yml")
    .addConverter(new DateConverter())
    .build();

Cluster cluster = new ClusterDefinition("127.0.0.1", 3000).connect();
engine.installOn(cluster);   // calls cluster.setRecordMappingFactory(...) internally

TypedDataSet<Customer> customers = engine.typedDataSet(Customer.class);
SessionExtension<MappingSession> mappingExt = engine.sessionExtension();
```

```java
public final class MappingEngine {

    public void installOn(Cluster cluster) {
        cluster.setRecordMappingFactory(asRecordMappingFactory());
        // engine reference held by application, or registered in app-level context
    }

    public SessionExtension<MappingSession> sessionExtension() {
        return new MappingSessionExtension(this);
    }

    public <T> TypedDataSet<T> typedDataSet(Class<T> clazz) {
        // annotation/YAML-derived namespace and set via ClassCache
    }

    // save(session, object), read(session, clazz, key), etc.
}
```

This replaces the current session-bound setup:

```java
// Before
AeroMapper mapper = new AeroMapper.Builder(session).build();
cluster.setRecordMappingFactory(mapper.asMappingFactory());
TypedDataSet<Customer> customers = mapper.getTypedDataSet(Customer.class);

// After
engine.installOn(cluster);
TypedDataSet<Customer> customers = engine.typedDataSet(Customer.class);
```

### `typedDataSet`

- **Location:** `MappingEngine` only (not on `Cluster`).
- **Not** on `MappingSession` (datasets are metadata, not behavior).
- Requires `installOn` to have been called so the factory is registered before typed reads/writes.
- Derives namespace/set from annotations/YAML via `ClassCache`, same as today's `AeroMapper.getTypedDataSet`.

```java
public class CustomerRepository {
    private final MappingEngine engine;
    private final TypedDataSet<Customer> customers;

    CustomerRepository(MappingEngine engine) {
        this.engine = engine;
        this.customers = engine.typedDataSet(Customer.class);
    }
}
```

---

## Mapper module: `MappingSession`

`MappingSession` **extends `Session`** intentionally so it can be used wherever a `Session` is expected (same behavior, factory-backed SDK paths, `RecordReadContext`, etc.).

### Two API families on `MappingSession`

| API family | Examples | Mapped? |
|------------|----------|---------|
| **SDK (inherited)** | `query(...)`, `insert(ds).object(...)`, `upsert(key).bin(...)` | Factory-backed paths yes; raw bin builders no |
| **Mapper module** | `save(...)`, `read(Class, key)`, `asBackedList(...)` | Yes |

Users who want annotation-driven CRUD use mapper methods. SDK builder paths remain valid and use the cluster `RecordMappingFactory` when typed. Document this distinction; do not hide inherited methods.

```java
public final class MappingSession extends Session {

    private final MappingEngine engine;

    MappingSession(Cluster cluster, Behavior behavior, MappingEngine engine) {
        super(cluster, behavior);
        this.engine = engine;
    }

    public <T> void save(T object, String... binNames) {
        engine.save(this, object, binNames);
    }

    public <T> T read(Class<T> clazz, Object userKey) {
        return engine.read(this, clazz, userKey);
    }

    // virtual lists, scan/query helpers, etc.
}
```

### `MappingSessionExtension`

Bound to a **`MappingEngine` instance** (not a global singleton):

```java
public final class MappingSessionExtension implements SessionExtension<MappingSession> {

    private final MappingEngine engine;

    MappingSessionExtension(MappingEngine engine) {
        this.engine = engine;
    }

    @Override
    public MappingSession create(Cluster cluster, Behavior behavior) {
        return new MappingSession(cluster, behavior, engine);
    }
}
```

### Usage

```java
MappingEngine engine = MappingEngine.builder().build();
engine.installOn(cluster);

SessionExtension<MappingSession> mappingExt = engine.sessionExtension();

MappingSession fast    = cluster.createSession(fastBehavior, mappingExt);
MappingSession durable = cluster.createSession(durableBehavior, mappingExt);

durable.save(criticalAccount);
Customer c = fast.read(Customer.class, "alice@example.com");
```

Same shared `MappingEngine`; different behaviors per session.

---

## Creating a session with a different behavior

Sessions are **immutable** with respect to behavior: a method that needs another profile returns a **new** session; it does not alter the receiver.

**Naming:** prefer **`sessionFor(Behavior)`** over `withBehavior` — the latter suggests in-place mutation (as in `Behavior.deriveWithChanges`).

### On `MappingSession` (mapper module)

```java
public MappingSession sessionFor(Behavior behavior) {
    return getCluster().createSession(behavior, engine.sessionExtension());
}

// Usage — one-off read with a different profile
MappingSession fast = durableSession.sessionFor(fastBehavior);
Customer c = fast.read(Customer.class, "cust1");
```

### On `Session` (optional SDK convenience)

The same pattern is a one-line delegate and may be added to base `Session` for symmetry:

```java
public Session sessionFor(Behavior behavior) {
    return cluster.createSession(behavior);
}
```

This is **optional** in the core SDK. Applications can always use `cluster.createSession(behavior)` directly. **`sessionFor` on `MappingSession` is required** (or equivalent) so callers do not need to retain `engine.sessionExtension()` for every behavior switch.

---

## Using SDK mapping without `MappingSession`

After `engine.installOn(cluster)`, transparent SDK mapping works on **any** `Session`:

```java
engine.installOn(cluster);
TypedDataSet<Customer> customers = engine.typedDataSet(Customer.class);

Session session = cluster.createSession(Behavior.DEFAULT);

session.insert(customers).object(alice).execute();

List<Customer> active = session.query(customers)
    .where("$.age > 25")
    .execute()
    .toObjectList();
```

No `MappingSession`, no extension argument, no extra syntax.

---

## Custom `RecordMapper` (no mapper module)

Applications that implement their own `RecordMapper` and do **not** use the mapper module are unchanged:

```java
cluster.setRecordMappingFactory(DefaultRecordMappingFactory.of(Customer.class, customerMapper));
TypedDataSet<Customer> customers = TypedDataSet.of("test", "customers", Customer.class);

Session session = cluster.createSession(Behavior.DEFAULT);
session.insert(customers).object(alice).execute();
```

They never use `MappingEngine`, `MappingSession`, or `SessionExtension`.

---

## Transactional operations with mapping (no `MappingTransactionalSession`)

Transactions stay **lightweight**: reuse the SDK's existing `TransactionalSession` for txn semantics. Do **not** introduce a public `MappingTransactionalSession` parallel hierarchy.

### SDK-only transactions (unchanged)

On any `Session`, including `MappingSession`:

```java
session.doInTransaction(txn -> {
    txn.upsert(accounts.id("acc1")).bin("balance").add(-100).execute();
    txn.upsert(accounts.id("acc2")).bin("balance").add(100).execute();
});
```

The callback receives `TransactionalSession` with txn state. Use SDK builders inside the lambda.

### Mapper methods inside a transaction

`MappingSession` provides **`doInMappingTransaction`** / **`doInMappingTransactionReturning`**. Implementation:

1. Allocate a standard `TransactionalSession` (same as `Session.doInTransaction` today).
2. Run the SDK retry/commit loop on it.
3. Inside the callback, pass a **package-private txn-scoped `MappingSession` view** that forwards all mapper-module calls to `engine.save(txnSession, …)` (etc.) using the **transactional** session, not the outer non-txn session.

No public subclass of `TransactionalSession`. The txn-scoped view is an implementation detail (inner class or package-private delegate).

```java
@FunctionalInterface
public interface MappingTransactionalVoid {
    void execute(MappingSession txnMappingView);
}

public final class MappingSession extends Session {

    public void doInMappingTransaction(MappingTransactionalVoid operation) {
        TransactionalSession txnSession = new TransactionalSession(getCluster(), getBehavior());
        txnSession.doInTransaction(txn -> {
            MappingSession txnView = new TxnMappingView(engine, txn);
            operation.execute(txnView);
        });
    }
}
```

**Package-private txn view** — extends `MappingSession` or delegates to engine + txn session:

```java
/** Package-private. Not part of public API. */
final class TxnMappingView extends MappingSession {

    private final TransactionalSession txnSession;

    TxnMappingView(MappingEngine engine, TransactionalSession txnSession) {
        super(txnSession.getCluster(), txnSession.getBehavior(), engine);
        this.txnSession = txnSession;
    }

    @Override
    public <T> void save(T object, String... binNames) {
        engine.save(txnSession, object, binNames);
    }

    @Override
    public <T> T read(Class<T> clazz, Object userKey) {
        return engine.read(txnSession, clazz, userKey);
    }

    // other mapper methods use txnSession similarly
}
```

**Example:**

```java
mappingSession.doInMappingTransaction(ms -> {
    ms.save(debitAccount);
    ms.save(creditAccount);
});
```

The lambda parameter type is **`MappingSession`** (public). Under the hood it is `TxnMappingView` bound to the active transaction. Mapper-module reads/writes inside the txn use transactional semantics; no second public session type.

---

## Virtual lists and session binding

Virtual lists are **long-lived** (created once, used many times). By default they use the **`Session` passed when the list was created** (typically the `MappingSession` from `asBackedList`).

### Default behavior

```java
MappingSession durable = cluster.createSession(durableBehavior, mappingExt);
VirtualList<Address> addresses = durable.asBackedList(customer, "addresses", Address.class);

addresses.append(home);   // uses durable's behavior
addresses.removeByKey(1); // uses durable's behavior
```

### Per-call session override — `withSession(Session)`

For long-lived lists, or when a one-off operation needs a different behavior, **`withSession`** applies the given session **only for that call**:

```java
MappingSession fast = durable.sessionFor(fastBehavior);

addresses.append(home);                        // durable (creation session)
addresses.withSession(fast).append(work);      // fast, this call only
addresses.removeByKey(1);                      // durable again
```

Rules to document for users:

- **Default:** each virtual-list operation uses the session that created the list (`asBackedList` caller's session at creation time).
- **Override:** `withSession(session)` returns the same list handle (or a thin wrapper) and runs **only the immediately following operation** with the supplied session's behavior.
- **Chaining:** if the API supports fluent multi-ops, document whether `withSession` scopes one op or the whole chain (recommend: one op unless explicitly chained as `withSession(s).beginMultiOperation()…`).

This supports short-lived lists (default session only) and long-lived lists (occasional `withSession` overrides) without pinning all operations to one behavior forever.

Dependency resolution during hydration continues to use `RecordReadContext.getSession()` for factory-backed reads.

---

## Migration from `AeroMapper`

| Today | After |
|-------|--------|
| `new AeroMapper.Builder(session).build()` | `cluster.createSession(behavior, engine.sessionExtension())` |
| `mapper.asMappingFactory()` + `cluster.setRecordMappingFactory(...)` | `engine.installOn(cluster)` |
| `mapper.getTypedDataSet(T.class)` | `engine.typedDataSet(T.class)` |
| `mapper.save(x)` | `mappingSession.save(x)` |
| SDK typed reads/writes on `Session` | Unchanged after `installOn` |

`AeroMapper` may remain temporarily as a deprecated façade delegating to `MappingEngine` + `MappingSession` for backward compatibility.

---

## End-to-end example: `Customer`

This walkthrough shows the full flow: domain model, cluster + engine setup, **SDK built-in mapping** on plain `Session` (batch write, single read, list read), then **mapper-module virtual lists** on `MappingSession`.

### Domain model

```java
import com.aerospike.mapper.annotations.*;
import com.aerospike.mapper.annotations.AerospikeEmbed.EmbedType;
import java.util.ArrayList;
import java.util.List;

@AerospikeRecord(namespace = "test", set = "customer")
public class Customer {

    @AerospikeKey
    @AerospikeBin(name = "id")
    private String customerId;

    private String email;
    private int age;

    /** Backing bin for virtual-list operations (see below). */
    @AerospikeEmbed(type = EmbedType.MAP, elementType = EmbedType.LIST)
    private List<Address> addresses = new ArrayList<>();

    public Customer(String customerId, String email, int age) {
        this.customerId = customerId;
        this.email = email;
        this.age = age;
    }

    public String getCustomerId() { return customerId; }
    public String getEmail() { return email; }
    public int getAge() { return age; }
    public List<Address> getAddresses() { return addresses; }
}

@AerospikeRecord
public class Address {

    @AerospikeKey
    private String line1;
    private String city;
    private String state;
    @AerospikeBin(name = "zip")
    private String zipCode;

    public Address(String line1, String city, String state, String zipCode) {
        this.line1 = line1;
        this.city = city;
        this.state = state;
        this.zipCode = zipCode;
    }
}
```

### 1. Connect cluster and install mapping

```java
try (Cluster cluster = new ClusterDefinition("127.0.0.1", 3000).connect()) {

    MappingEngine engine = MappingEngine.builder().build();
    engine.installOn(cluster);

    TypedDataSet<Customer> customers = engine.typedDataSet(Customer.class);
    Session session = cluster.createSession(Behavior.DEFAULT);

    SessionExtension<MappingSession> mappingExt = engine.sessionExtension();
    MappingSession mappingSession =
            cluster.createSession(Behavior.DEFAULT, mappingExt);

    // ...
}
```

No `MappingSession` is required for the insert/read steps below.

### 2. Store three customers (SDK built-in mapping, plain `Session`)

```java
Customer alice   = new Customer("cust1", "alice@example.com", 30);
Customer bob     = new Customer("cust2", "bob@example.com", 25);
Customer charlie = new Customer("cust3", "charlie@example.com", 35);

session.insert(customers)
    .objects(alice, bob, charlie)
    .execute();
```

### 3. Read one customer, then a list (SDK built-in mapping, plain `Session`)

```java
Optional<Customer> one = session.query(customers.id("cust1"))
    .execute()
    .getFirstObject();

List<Customer> batch = session.query(customers.ids("cust1", "cust2", "cust3"))
    .execute()
    .toObjectList();

List<Customer> adults = session.query(customers)
    .where("$.age > 25")
    .execute()
    .toObjectList();
```

### 4. Virtual list of addresses (`MappingSession`)

```java
mappingSession.save(alice);

VirtualList<Address> addressList =
        mappingSession.asBackedList(alice, "addresses", Address.class);

addressList.append(new Address("123 Main St", "Denver", "CO", "80202"));

// Optional: one call with a different behavior
MappingSession fastSession = mappingSession.sessionFor(fastBehavior);
addressList.withSession(fastSession)
    .append(new Address("456 Oak Ave", "Boulder", "CO", "80301"));

Customer updated = session.query(customers.id("cust1"))
    .execute()
    .getFirstObject()
    .orElseThrow();
```

### What each layer did

| Step | API | Session type |
|------|-----|--------------|
| Install factory + datasets | `engine.installOn`, `engine.typedDataSet` | — |
| Insert 3 customers | `session.insert(customers).objects(...)` | Plain `Session` |
| Read one / read many | `query(...).getFirstObject()` / `toObjectList()` | Plain `Session` |
| Virtual list on addresses | `mappingSession.asBackedList(...)` | `MappingSession` |
| Override list behavior for one call | `addressList.withSession(fastSession).append(...)` | Per-call session |

---

## Implementation checklist

### Core SDK (`aerospike-client-java-sdk`)

- [ ] Add `SessionExtension<S extends Session>` interface.
- [ ] Add `Cluster.createSession(Behavior, SessionExtension<S>)` overload.
- [ ] (Optional) Add `Session.sessionFor(Behavior)` convenience delegate.
- [ ] Document two-layer model in user-facing docs; link to this spec for implementers.

### Mapper module (`aerospike-sdk-mapper-java`)

- [ ] Introduce `MappingEngine` (extract state from `AeroMapper`).
- [ ] Implement `installOn(Cluster)`, `sessionExtension()`, `typedDataSet(Class)`.
- [ ] Refactor `AeroRecordMappingFactory` to use `MappingEngine` instead of session-bound `IBaseAeroMapper`.
- [ ] Implement `MappingSession` + `MappingSessionExtension`.
- [ ] Implement `MappingSession.sessionFor(Behavior)`.
- [ ] Implement `doInMappingTransaction*` with package-private `TxnMappingView` (no public `MappingTransactionalSession`).
- [ ] Virtual lists: default to creation session; add `withSession(Session)` per-call override.
- [ ] Deprecate session-constructing `AeroMapper.Builder(Session)` in favor of `installOn` + `createSession(..., sessionExtension())`.
- [ ] Update tests and examples.

---

## User-facing summary (for docs)

> **Object mapping** is a cluster feature: call `MappingEngine.installOn(cluster)` and use `Session` with `TypedDataSet` and typed streams.
>
> **`MappingSession`** is an optional mapper-module extension for extra APIs (annotation-driven CRUD, virtual lists, etc.). Create it with `cluster.createSession(behavior, engine.sessionExtension())` only when you need those APIs. It is not required for mapping to work.
>
> Use **`sessionFor(behavior)`** (not `withBehavior`) when you need a new session with a different profile — it returns a new session and does not change the receiver.
