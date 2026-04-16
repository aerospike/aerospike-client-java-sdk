# Aerospike Java SDK

> [!NOTE]
> This is a developer preview, not yet ready for production use, but with future releases planned. We encourage feedback from the Aerospike
> developer community through GitHub issues.

A ground-up redesign of the Aerospike Java client — fluent, type-safe, and built for modern Java (21+).

## Quick Start

```java
import com.aerospike.client.sdk.*;
import java.util.List;
import java.util.Map;

public class QuickStart {
    public static void main(String[] args) {
        // Connect to the cluster
        try (Cluster cluster = new ClusterDefinition("localhost", 3000)
                .withNativeCredentials("admin", "password")
                .connect()) {

            Session session = cluster.createSession(Behavior.DEFAULT);
            DataSet users = DataSet.of("test", "users");

            // --- Insert some data ---
            session.upsert(users.id("alice"))
                .bin("name").setTo("Alice")
                .bin("age").setTo(30)
                .bin("tags").setTo(List.of("admin", "active"))
                .bin("scores").setTo(List.of(95, 87, 92, 78))
                .bin("prefs").setTo(Map.of("theme", "dark", "lang", "en"))
                .execute();

            // --- Read it back ---
            Record record = session.query(users.id("alice"))
                .execute()
                .getFirstRecord();

            System.out.println(record.getString("name"));  // Alice
            System.out.println(record.getLong("age"));      // 30

            // --- Update a bin ---
            session.update(users.id("alice"))
                .bin("age").setTo(31)
                .execute();

            // --- Append to a list ---
            session.upsert(users.id("alice"))
                .bin("scores").listAppend(99)
                .execute();

            // --- Query a list range ---
            session.query(users.id("alice"))
                .bin("scores").onListValueRange(90, 100).getValues()
                .execute();

            // --- Set a map entry ---
            session.upsert(users.id("alice"))
                .bin("prefs").onMapKey("notifications").upsert(true)
                .execute();

            // --- Read a map key ---
            session.query(users.id("alice"))
                .bin("prefs").onMapKey("theme").getValues()
                .execute();

            // --- Filter with AEL ---
            session.query(users)
                .where("$.age > 25 and 'active' in $.tags")
                .execute();
        }
    }
}
```

### What's happening

1. **Connect** — `ClusterDefinition` configures the connection (hosts, credentials, TLS);
   calling `.connect()` returns a `Cluster` that manages node discovery and connection
   pooling. `Cluster` is `Closeable` — wrap it in try-with-resources.

2. **Session** — A lightweight handle through which all operations flow. Create as many as
   you need with different `Behavior` profiles to vary timeouts, retries, and consistency
   settings without changing application code.

3. **Write verbs** — `upsert`, `insert`, `update`, `replace`, and `replaceIfExists` each
   enforce intent at the method level. No more runtime policy flags.

4. **Reads** — `session.query(key)` returns a `RecordStream` that works as an `Iterator`,
   a Java `Stream`, or a `CompletableFuture`.

5. **CDT operations** — Bins containing Lists and Maps are manipulated through a fluent
   navigation chain: select elements (`.onListValueRange(...)`, `.onMapKey(...)`), then
   choose an action (`.getValues()`, `.remove()`, `.count()`). See the
   [List Operations](docs/list-operations.md) and [Map Operations](docs/map-operations.md)
   references for every available operation.

6. **AEL filtering** — The Aerospike Expression Language lets you write server-side filters
   as concise strings (`"$.age > 25"`). When a matching secondary index exists, the SDK
   selects it automatically. See the [AEL Documentation](docs/ael-documentation.md) for
   the full language reference.

## Features

For comprehensive documentation with code examples for every feature, see the
[Key Features Guide](docs/key-features.md).

| Feature | Description | Details |
|---|---|---|
| **Fluent write verbs** | `insert`, `update`, `upsert`, `replace`, `replaceIfExists` — intent is explicit at the method level | [Write Operations](docs/key-features.md#write-operations) |
| **Reads & RecordStream** | Universal `RecordStream` return type with Iterator, Stream, and CompletableFuture support | [Read Operations](docs/key-features.md#read-operations), [RecordStream](docs/key-features.md#recordstream) |
| **Heterogeneous batching** | Mix reads, writes, deletes, and UDFs across different keys in a single server round-trip | [Batch Chains](docs/key-features.md#heterogeneous-batch-chains) |
| **AEL (Expression Language)** | Text-based server-side filters with automatic secondary index selection | [AEL Reference](docs/ael-documentation.md) |
| **Expression bin ops** | Compute derived values server-side (`selectFrom`, `upsertFrom`) | [Expression Ops](docs/key-features.md#expression-based-bin-operations) |
| **List & Map CDT operations** | Fluent navigation and manipulation of nested collection data types | [List Ops](docs/list-operations.md), [Map Ops](docs/map-operations.md) |
| **Async & reactive** | `executeAsync()` with `CompletableFuture` and `Flow.Publisher` (backpressure) | [Async Operations](docs/key-features.md#async-operations) |
| **Transactions** | Multi-record ACID transactions with automatic retry on transient failures | [Transactions](docs/key-features.md#transactions) |
| **Behavior (policy config)** | Cascading, selector-based configuration — separate dev concerns from ops concerns | [Behavior](docs/key-features.md#behavior-policy-configuration) |
| **Object mapping** | `RecordMapper` and `TypeSafeDataSet` for mapping Java objects to/from records | [Object Mapping](docs/key-features.md#object-mapping) |
| **Queries & scans** | Dataset-level queries with pagination, sorting, partition targeting, and throttling | [Queries](docs/key-features.md#queries-and-scans) |
| **Background operations** | Server-side bulk updates, deletes, and UDF execution across entire sets | [Background Ops](docs/key-features.md#background-operations) |
| **UDFs** | Register, execute, and chain Lua User Defined Functions | [UDFs](docs/key-features.md#udfs) |
| **Secondary indexes** | Create and drop indexes; AEL queries use them automatically | [Indexes](docs/key-features.md#secondary-indexes) |
| **Info commands** | Typed, structured access to cluster metadata (namespaces, sets, indexes) | [Info Commands](docs/key-features.md#info-commands) |
| **Exception hierarchy** | Fine-grained unchecked exceptions with result codes for programmatic handling | [Exceptions](docs/key-features.md#exception-hierarchy) |

## Building

```bash
mvn compile        # Compile
mvn test           # Run tests
mvn javadoc:javadoc  # Generate Javadoc
```

## Requirements

- Java 21+ (Java 24+ recommended for improved virtual thread performance)
- Maven 3.9.5+
