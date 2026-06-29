# SDK logging design (SLF4J)

**Related document:** [Aerospike client observability strategy](observability.md) covers **metrics**, **traces**, and export (OpenTelemetry, file snapshots). This document covers **diagnostic logs** only.

This document describes how logging for the Aerospike Java SDK should be structured so the library embeds cleanly in host applications. Hosts choose the concrete implementation (Logback, Log4j2, Spring Boot defaults, JUL bridges, etc.); the SDK should depend only on the SLF4J API.

## Current state (baseline)

- `client/src/main/java/com/aerospike/client/sdk/Log.java` is a **global** gate: `setLevel`, `setCallback`, `*Enabled()` methods, and `Log.Context` (today used for values such as cluster name in the callback).
- Call sites across the client mix global level checks with optional `Log.Context` for scoped messages (for example in node and tend code paths).
- `ClusterDefinition.withLogLevel` / `useLogSink` still tie into **global** `Log` configuration; true per-cluster log levels are not implemented end-to-end today.
- Code outside the client JAR (for example benchmarks) may implement `Log.Callback` for custom sinks.

The sections below are the **target** design once migration is complete.

## Design principles

| Requirement | How SLF4J + the host app satisfy it |
|-------------|-------------------------------------|
| Embeddable library; host picks implementation | Declare **`org.slf4j:slf4j-api`** (recommend **2.0.x**) as the **only** logging dependency on the published client artifact. Do **not** ship Logback, Log4j2, or reload4j as transitive runtime dependencies of the client. (It is fine and recommended for the `examples` to bring in a logger implementation like `slf4j-simple`) |
| Package / class granularity | Use the standard idiom: `private static final Logger log = LoggerFactory.getLogger(ThatClass.class);`. Logger names follow the class hierarchy under `com.aerospike.client.sdk`, so package-level configuration works (for example all of `com.aerospike.client.sdk.tend`). |
| Cross-cutting “tags” (for example all serialization) | Prefer **named loggers** with stable string constants (see [Cross-cutting loggers](#cross-cutting-loggers-example) and the [operational strategy](#operational-logging-strategy) table). Hosts can set `com.aerospike.client.sdk.serde` independently of `com.aerospike.client.sdk.tend`. Optional later: SLF4J **markers** for finer taxonomy—often unnecessary if named loggers are enough. |
| Negligible cost when a logger is off | Follow [SDK logging style](#sdk-logging-style-prescriptive): explicit `isDebugEnabled()` / `isTraceEnabled()` (etc.) guards and `{}` placeholders. Never log payloads or user data—only operational metadata when a logger is enabled. |
| Change levels without restart | This is **not** an SLF4J feature. The **binding** must support it: Logback (`LoggerContext`, JMX), Log4j2 (`Configurator`, JMX), Spring Boot **`/actuator/loggers`**, and similar. Document that expectation so it matches common Java practice. |
| Per-cluster log levels | **Not expressible** in SLF4J: levels are keyed by **logger name**, not by cluster id. Treat as a **non-goal** for the SDK. Multi-cluster JVMs tune component loggers (`tend`, `command`, …) globally; cluster identity appears as a **structured field** on emitted lines (see [Structured context](#structured-context-slf4j-2-key-values)). Hosts may filter on that field in log aggregation pipelines if needed. |
| **No user data in logs** | **Never** log application or customer data at **any** level (ERROR through TRACE). See [User data — never log](#user-data--never-log). This is required for regulated industries (for example financial services) and is not relaxed for DEBUG or TRACE. |

## SDK logging style (prescriptive)

Use **explicit level guards** and plain `log.debug` / `log.trace` / `log.warn` calls. **Do not** use SLF4J 2’s **`log.atDebug().log(() -> "..." )`** (or other levels with a `Supplier` message) in this codebase—one pattern only, predictable in review, and avoids supplier allocation on hot paths.

**DEBUG (standard pattern):**

```java
if (log.isDebugEnabled()) {
    log.debug("Packed {} fields for namespace {}", fieldCount, namespace);
}
```

**TRACE / WARN / INFO:** use `if (log.isTraceEnabled())`, `if (log.isWarnEnabled())`, `if (log.isInfoEnabled())` the same way whenever the log line is non-trivial or you want consistency with DEBUG.

**When a guard is optional:** a single `log.warn("...", e)` or `log.info("static message")` with **no** non-trivial argument computation does not require a guard; use guards whenever computing **arguments** or the **message** would be wasted work at the current level (same rule as DEBUG). This applies only in the hot path; low fideilty messages such as nodes entering or leaving the cluster do not require specific guards.

**Parameterized messages:** use `{}` placeholders; do not build strings with `+` in the message template when those parts should be skipped while the level is disabled (see [Eager argument evaluation](#eager-argument-evaluation)).

**`addKeyValue`:** allowed only **inside** an `is*Enabled()` guard, using `atDebug()` / `atTrace()` as a fluent builder for key-values—**not** for lazy string suppliers:

```java
if (log.isDebugEnabled()) {
    log.atDebug()
        .addKeyValue("aerospike.cluster", cluster.getName())
        .log("Unexpected partition redirect for namespace {}", namespace);
}
```

## Operational logging strategy

This section aligns the SDK with patterns used by other database clients (MongoDB’s [logging specification](https://github.com/mongodb/specifications/blob/master/source/logging/logging.md), PyMongo’s [component loggers](https://pymongo.readthedocs.io/en/stable/examples/logging.html), DataStax’s [logging taxonomy](https://docs.datastax.com/en/developer/java-driver/4.3/manual/core/logging/index.html) and [request tracker](https://docs.datastax.com/en/developer/java-driver/4.0/manual/core/request_tracker/index.html)): split **topology**, **connection**, **routing**, **commands**, and **serde** so operators can raise verbosity in one area without enabling everything. Treat **per-request logging** as a separate concern from diagnostics—not “DEBUG on the whole driver” in production.

### User data — never log

**At no log level** (ERROR, WARN, INFO, DEBUG, or TRACE) may the SDK write **user data** or other **customer-controlled content** to diagnostic logs. This rule does **not** bend for support investigations, temporary DEBUG, or developer forensics. Financial-services and other regulated deployments routinely prohibit customer data in application logs; accidental logging can violate policy, trigger audit findings, and force log-pipeline purges.

**User data** includes anything the application stored in Aerospike or passed through the client API, for example:

- **Record keys** that identify customer records. Digests are acceptable to log
- **Bin names** and **bin values** (including maps, lists, blobs, and nested structures)
- **Query / scan filter literals**, index predicates, or expression text that embed customer values
- **Batch key lists**, UDF arguments, and operation payloads
- **Credentials**, tokens, session material, TLS private keys, and auth challenge/response bodies

**Allowed in logs** (operational metadata only): cluster name, node name, `host:port`, namespace and set **names** (when they are deployment topology, not secret identifiers), partition id, operation **type**, result **code**, latency, counts, generations, and similar non-content fields.

**Also avoid:**

- Logging **`Throwable.getMessage()`** or exception text when it may echo user-supplied or record-derived strings—prefer exception **class** and SDK **result code**.
- Hex dumps, “truncated” payload snippets, or serde previews of wire bytes that decode to user content.
- `toString()` on `Key`, `Record`, `Bin`, batch entries, or query builders unless guaranteed free of user data.

For content-level debugging, use **non-logging** tooling (tests, local captures outside production, server-side tools under customer control)—not SDK log lines. Per-request correlation at scale belongs in **metrics and tracing**, with the same no-user-data rule applied there.

**Code review:** any log argument that touches keys, bins, records, filters, or wire buffers requires explicit justification that only operational metadata is emitted.

### Log levels (semantics)

Use SLF4J’s levels consistently so hosts can tune one policy across the SDK.

| Level | Purpose | Examples (driver context) |
|-------|---------|---------------------------|
| **ERROR** | Process or background path cannot continue, or a defect surfaced **outside** the normal API outcome | Tend thread died; invariant violated after internal retries. **Avoid** using ERROR for every failed user operation if the public API already throws or returns an error—prefer **DEBUG** (or metrics) so logs are not duplicates of application error handling ([MongoDB logging spec — errors vs levels](https://github.com/mongodb/specifications/blob/master/source/logging/logging.md)). |
| **WARN** | Cluster or client continues but is **degraded**, **misconfigured**, or in a **surprising** state | Node removed from cluster; repeated connection failures; peer identity mismatch; unrecognized client option. |
| **INFO** | **Sparse**, operator-relevant **lifecycle** | Cluster/session ready (summary: node count, generation); TLS enabled; major topology change (one line per meaningful event, not every tend tick). |
| **DEBUG** | **Support playbook**: bounded summaries | Tend iteration summary (duration, nodes touched); connect to `host:port`; batch split counts; “retry on node Y”. Highest volume for “what happened this request” when deliberately enabled. |
| **TRACE** | **Developer / wire forensics** | Serde steps; per-frame or per-command outline; scheduler internals; overridden policy settings (`sendKey`, `durableDelete`, etc.). **Metadata only**—byte **lengths**, type codes, message boundaries; **never** decoded record content. |

Production logging should focus on **ERROR / WARN / INFO**, and use **DEBUG / TRACE** only on **narrow logger categories** while investigating issues. Logging should always keep performance as a foremost concern.

### Cross-cutting areas: loggers and what to log

Use **stable logger names** (constants such as `SdkLoggers.TEND`) so configuration matches operator mental models; implementation classes may still use `LoggerFactory.getLogger(MyClass.class)` for low-volume code, but **high-volume or cross-package concerns** should route to the names below.

| Area | Logger name | Typical levels | What to log (include / emphasize) |
|------|-------------|----------------|-------------------------------------|
| **Cluster tend & topology** — seeds, peers, add/remove nodes, partition map refresh, rebalance / rack signals | `com.aerospike.client.sdk.tend` | **INFO**: rare milestones (initial stabilization, large membership change). **DEBUG**: each tend cycle summary (duration, invalid peer count, whether partition gen changed). **WARN**: tend loop errors that will retry; seed failures when not failing fast. | Structured fields: **cluster** id/name (`aerospike.cluster`); **node** name; **host:port** for seeds; **partition generation** / rebalance markers when available; error summary (not full stack at INFO). |
| **Connections** — TCP/TLS, auth handshake, pool grow/shrink, idle close, reconnect | `com.aerospike.client.sdk.connection` | **DEBUG** / **TRACE**: connect attempts, handshake success/failure, channel close reason. **WARN**: auth failure, TLS verification failure, repeated connect failures to same host. | **host:port**; **TLS** cipher/protocol if useful; **connection id** if assigned; latency to connect; exception **class** + generic message—**never** passwords, tokens, or challenge bodies. |
| **Routing** — partition → node, batch fan-out across nodes | `com.aerospike.client.sdk.routing` | **DEBUG**: which node serves a partition; batch split (key **count** per node). **TRACE**: bounded routing summaries (counts, sampling)—**never** individual record keys. | **namespace** / **set** name; **partition id**; **node** name; batch **size** and **shard** counts—no key values at any level. |
| **Commands & batches** — read/write paths, retries, wire-level operation summaries | `com.aerospike.client.sdk.command` | **DEBUG**: operation **type**, **result code** or outcome, **latency**, retry count, target **node**. **TRACE**: step timing and counts only with guards. | Structured operational fields only; **never** keys, bins, UDF args, or wire payloads ([user data rule](#user-data--never-log)). |
| **Serde** — pack/unpack, type codes, framing | `com.aerospike.client.sdk.serde` | **TRACE** / **DEBUG**: byte **lengths**, type tags, message boundaries—**never** default on in prod. | **Lengths** and **type** metadata only; **no** hex dumps or decoded content at any level. |
| **Queries** — secondary index, scans, query executor | `com.aerospike.client.sdk.query` | Same spirit as **command**: **DEBUG** for bounded summaries; **never** filter literals or result records. | **index** name/type if present; **namespace** / **set**; result **cardinality** or **continuation** state; latency—no predicate values or row content. |
| **Client behavior & config** — YAML / `BehaviorRegistry`, option loading, overrides | `com.aerospike.client.sdk.behavior` | **WARN**: unknown keys, conflicting options, fallbacks applied. **INFO**: optional “loaded behavior profile X” once at startup. | **Source** (file path or resource name, non-secret); **key** names in conflict; not full file contents. |
| **Info protocol** — `asinfo`-style admin requests | `com.aerospike.client.sdk.info` | **DEBUG**: request/response **summaries**; **INFO** only if rare admin milestones matter. | **host:port**; **command** name; response **length** or status—**not** response body content. |
| **Background tasks** — index build, UDF register, long-running jobs | `com.aerospike.client.sdk.task` | **INFO**: task started/completed/failed (one line per phase). **DEBUG**: polling interval progress. | **task** type; **namespace** / **set** / **index** name as applicable; terminal **result** or error class. |
| **Lifecycle** — cluster/session construction and shutdown | `com.aerospike.client.sdk` (root) or a dedicated `com.aerospike.client.sdk.lifecycle` if you split it | **INFO**: client/cluster **open** and **close** with identity (cluster name). | **Cluster** name/id; SDK **version** optionally once. |

**Optional “log every request” product behavior** (slow-operation thresholds, success lines) is better handled with a **dedicated mechanism** (metrics, listener)—similar to DataStax **RequestLogger** ([request tracker](https://docs.datastax.com/en/developer/java-driver/4.0/manual/core/request_tracker/index.html))—that still obeys [User data — never log](#user-data--never-log), rather than turning **DEBUG** on for all of `com.aerospike.client.sdk.command` in production.

### What every log line should carry

Where possible, attach **structured** key-value context on each **emitted** line via SLF4J 2 **`addKeyValue`** (inside level guards) or message parameters — see [Structured context](#structured-context-slf4j-2-key-values). The SDK does **not** use SLF4J **`MDC`**.

- **Cluster**: name or id (`aerospike.cluster` via `addKeyValue` or `{}` in the message).
- **Data plane**: **namespace**, **set**, **operation type**, **result code** or exception type, **latency** when meaningful and already captured.
- **Topology**: **partition generation**, rebalance flags, or other server-reported generation fields when debugging routing.

**User data:** see [User data — never log](#user-data--never-log). In particular, **never** log record keys, bin names/values, credentials, tokens, query literals, or wire payloads at **any** level—including DEBUG and TRACE. “Truncation” or “short previews” of customer content are **not** acceptable substitutes.

### Principles (summary)

1. **Split channels** — topology (`tend`), **connection**, **routing**, **command**, **query**, **serde**, **behavior**, **task** — so tuning matches how other drivers expose components (Eg, see [MongoDB components](https://github.com/mongodb/specifications/blob/master/source/logging/logging.md)).
2. **Reserve INFO** for lifecycle and rare operator-visible events; push **hot-path** detail to **DEBUG** / **TRACE** with explicit guards per [SDK logging style](#sdk-logging-style-prescriptive).
3. **API-visible failures** — log at **DEBUG** at most when the caller already receives an exception or error result, unless the failure is **not** surfaced through the API.
4. **No user data** — at any level; logs carry operational metadata only ([User data — never log](#user-data--never-log)).
5. **Volume** — treat **command**/**query**/**serde** loggers as **high cost**; keep off in production unless investigating ([MongoDB Node.js logging](https://www.mongodb.com/docs/drivers/node/current/monitoring-and-logging/logging/)).

## Cross-cutting loggers (example)

Some concerns span many classes (serialization, wire tracing). Use one **logger name** (a stable string) from every call site in that concern so operators can enable or disable it with a single configuration key.

**1. Centralize the name** (package-private or public API depending on your preference):

```java
public final class SdkLoggers {
    private SdkLoggers() {}

    /** All high-volume encode/decode diagnostics. */
    public static final String SERDE = "com.aerospike.client.sdk.serde";

    /** YAML / BehaviorRegistry and client option resolution. */
    public static final String BEHAVIOR = "com.aerospike.client.sdk.behavior";
}
```

**2. Obtain a logger for that name** wherever the concern applies (not necessarily the declaring class’s name):

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class Packer {
    private static final Logger log = LoggerFactory.getLogger(SdkLoggers.SERDE);

    void example(int bytesWritten) {
        if (log.isTraceEnabled()) {
            log.trace("Packed {} bytes", bytesWritten);
        }
    }
}
```

```java
final class Unpacker {
    private static final Logger log = LoggerFactory.getLogger(SdkLoggers.SERDE);

    void example() {
        if (log.isDebugEnabled()) {
            log.debug("Starting decode");
        }
    }
}
```

Hosts then set **`com.aerospike.client.sdk.serde`** to `TRACE` or `DEBUG` in Logback, Log4j2, Spring Boot, or `slf4j-simple` (see below) without touching loggers named after individual classes. Add matching constants for **`TEND`**, **`CONNECTION`**, **`BEHAVIOR`**, and other names from the [operational strategy](#operational-logging-strategy) table as you migrate call sites.

## Logger taxonomy

Default pattern: each class uses its own class logger (finest control, minimal extra design).

For cross-cutting areas, use shared constants in a small internal type (for example `SdkLoggers.SERDE`) only where a **logical** channel is needed; see the **Cross-cutting areas** table under [Operational logging strategy](#operational-logging-strategy) for the full set of recommended names.

### Class logger vs cross-cutting logger

**Do not** restrict the SDK to cross-cutting loggers only. Use **both**, with a simple rule:

| Use | Logger | Example |
|-----|--------|---------|
| **Cross-cutting channel** (operator-tunable area, may span many classes, may be high volume) | `LoggerFactory.getLogger(SdkLoggers.TEND)` (etc.) | Tend cycle summary, peer refresh, partition map change — from `ClusterTend`, `NodeValidator`, or any helper in the tend package |
| **Class logger** (implementation detail, low volume, only interesting when debugging *this* class) | `LoggerFactory.getLogger(ClusterTend.class)` | One-off invariant checks, constructor/shutdown breadcrumbs, refactor scaffolding you expect to delete |

**Decision checklist** (answer in order):

1. **Is this message part of an [operational strategy](#operational-logging-strategy) row** (tend, connection, command, …)? → **Cross-cutting** logger for that row (`SdkLoggers.TEND`, …).
2. **Would an operator want to turn on “tend DEBUG” without enabling every class under `com.aerospike.client.sdk.tend`?** → **Cross-cutting** (that is the point of `com.aerospike.client.sdk.tend`).
3. **Is the line rare, class-specific, and not something we document for hosts to tune?** → **Class** logger is fine.
4. **Unsure?** → Prefer **cross-cutting** for anything that might fire every tend tick or every request; prefer **class** for truly local diagnostics.

**Anti-pattern:** two channels for the same concern (some lines on `SdkLoggers.TEND`, some on `ClusterTend.class`) for **operational** tend output—hosts must then enable two logger names to see one story. Pick **one** channel per concern; for tend, that channel is **`com.aerospike.client.sdk.tend`**. Do not log the same messge to two different logges (one cross-cutting, one class logger)

#### Tend cadence (~1 Hz)

The background tend thread typically wakes about **once per second** (`def.getTendInterval()`). That affects **volume**, not **which logger**:

| Event | Typical frequency | Logger | Level | Notes |
|-------|-------------------|--------|-------|-------|
| Node added / removed | Rare (membership change) | **`SdkLoggers.TEND`** | **INFO** | One line per node; operator-visible milestone |
| Cluster has **no** active nodes (reseeding) | Rare or during outage | **`SdkLoggers.TEND`** | **WARN** (failure) or **DEBUG** (attempt) | “Detached” / empty cluster — not every tick unless stuck reseeding |
| Seed host failed (non-fatal) | Per failed seed, not every second | **`SdkLoggers.TEND`** | **WARN** | Short message + host; connection handshake detail → **`SdkLoggers.CONNECTION`** at DEBUG if split |
| **Per-cycle** summary (duration, node count, invalid peers, partition gen changed) | **~1×/second** when DEBUG on | **`SdkLoggers.TEND`** | **DEBUG** | **One** bounded line per `tend()` — not per-node TRACE each tick |
| Partition / rebalance refresh on a node | Only when flags set | **`SdkLoggers.TEND`** | **DEBUG** | Log **that** refresh happened, not full map dumps |
| Internal array resize / copy-on-write sanity | Should never happen | **`ClusterTend.class`** | **WARN** | Class-local invariant (e.g. remove-node count mismatch) |
| Recover-queue drain internals | Every tick if queue non-empty | **`ClusterTend.class`** | **TRACE** | Implementation forensics; keep off in prod |

With tend at ~1 Hz, **DEBUG on `com.aerospike.client.sdk.tend`** is roughly **one summary line per second** — acceptable for short investigations. Do **not** emit DEBUG/TRACE **per node per tick**; fold counts into the single cycle summary.

#### Worked example: `ClusterTend.tend()` (simplified from source)

Attach **`aerospike.cluster`** on each emitted tend line via **`addKeyValue`** (tend runs ~1 Hz — repeating the field is cheap). Below, **`tendLog`** = `SdkLoggers.TEND`, **`log`** = `ClusterTend.class`.

```java
final class ClusterTend implements Runnable {
    private static final Logger tendLog = LoggerFactory.getLogger(SdkLoggers.TEND);
    private static final Logger log = LoggerFactory.getLogger(ClusterTend.class);

    private void tend(boolean failIfNotConnected, boolean isInit) {
        long startNanos = System.nanoTime();
        Node[] nodes = cluster.getNodes();
        Peers peers = new Peers(nodes.length + 16);

        for (Node node : nodes) {
            node.tendReset();
        }

        if (nodes.length == 0) {
            // Cross-cutting WARN/DEBUG: cluster temporarily has no attached nodes — reseed path.
            if (tendLog.isDebugEnabled()) {
                tendLog.atDebug()
                    .addKeyValue("aerospike.cluster", cluster.getName())
                    .log("No active nodes; attempting seed from {} seed host(s)", seeds.length);
            }
            seedNode(peers, failIfNotConnected);
            // addNode() below logs INFO "Add node …" on SdkLoggers.TEND when seed succeeds.
        } else {
            for (Node node : nodes) {
                node.refresh(peers);
            }

            if (peers.genChanged) {
                peers.refreshCount = 0;
                for (Node node : nodes) {
                    node.refreshPeers(peers);
                }
                findNodesToRemove(peers);

                if (!peers.removeNodes.isEmpty()) {
                    // removeNodesCopy → INFO per removed node on tendLog (cross-cutting).
                    removeNodes(peers.removeNodes);
                }
            }

            if (!peers.nodes.isEmpty()) {
                // addNodes → INFO per added node on tendLog (cross-cutting).
                addNodes(peers.nodes);
                refreshPeers(peers);
            }
        }

        invalidNodeCount += peers.getInvalidCount();

        boolean partitionChanged = false;
        for (Node node : nodes) {
            if (node.isPartitionChanged()) {
                partitionChanged = true;
                node.refreshPartitions(peers);
            }
            if (node.isRebalanceChanged()) {
                node.refreshRacks();
            }
        }

        tendCount++;

        // ~once per 30s: connection pool balance — class-local TRACE unless promoted to tend DEBUG.
        if (tendCount % 30 == 0) {
            if (log.isTraceEnabled()) {
                log.trace("Connection balance pass (tendCount={})", tendCount);
            }
            for (Node node : nodes) {
                node.balanceConnections();
            }
        }

        processRecoverQueue();

        // Cross-cutting: one DEBUG line per tend iteration (~1 Hz), not one line per node.
        if (tendLog.isDebugEnabled()) {
            long millis = (System.nanoTime() - startNanos) / 1_000_000L;
            tendLog.atDebug()
                .addKeyValue("aerospike.cluster", cluster.getName())
                .addKeyValue("durationMs", millis)
                .addKeyValue("nodeCount", cluster.getNodes().length)
                .addKeyValue("invalidPeers", peers.getInvalidCount())
                .addKeyValue("partitionChanged", partitionChanged)
                .addKeyValue("peersGenChanged", peers.genChanged)
                .log("Tend cycle complete");
        }
    }

    private void addNode(Node node) {
        // ...
        if (tendLog.isInfoEnabled()) {
            tendLog.atInfo()
                .addKeyValue("aerospike.cluster", cluster.getName())
                .log("Add node {}", node);   // cross-cutting INFO — rare membership event
        }
        nodesMap.put(node.getName(), node);
    }

    private void removeNodesCopy(HashSet<Node> nodesToRemove) {
        // ...
        for (Node node : nodes) {
            if (nodesToRemove.contains(node)) {
                if (tendLog.isInfoEnabled()) {
                    tendLog.atInfo()
                        .addKeyValue("aerospike.cluster", cluster.getName())
                        .log("Remove node {}", node);   // cross-cutting INFO
                }
            } else {
                nodeArray[count++] = node;
            }
        }
        if (count < nodeArray.length) {
            // Class logger: internal invariant / bug signal — not operator "tend DEBUG".
            if (log.isWarnEnabled()) {
                log.warn("Node remove mismatch: expected array length {}, got {}", nodeArray.length, count);
            }
            // resize ...
        }
        cluster.setNodes(nodeArray);
    }
}
```

**`run()` loop**: catch block uses **`tendLog.warn`** for retryable tend failures — cross-cutting, not `ClusterTend.class`:

```java
@Override
public void run() {
    while (valid) {
        try {
            tend(false, false);
        } catch (Throwable e) {
            if (tendLog.isWarnEnabled()) {
                tendLog.atWarn()
                    .addKeyValue("aerospike.cluster", cluster.getName())
                    .log("Cluster tend failed: {}", e.toString(), e);
            }
        }
        Util.sleep(def.getTendInterval());
    }
}
```

Exact subdivisions can evolve with the codebase; the important part is a **stable, documented** set of named loggers for areas users tune often.

## Structured context (SLF4J 2 key-values)

Replace callback-oriented context (`Log.Context`) with **structured fields on each emitted log event**. The SDK uses SLF4J 2 **`LoggingEventBuilder.addKeyValue`** (inside level guards) or message parameters — **not** SLF4J **`MDC`**.

### Why not MDC?

**MDC** is a thread-local map (`org.slf4j.MDC`). The SDK **does not use it**:

- **Virtual threads** and **shared pool threads** make thread-local logging context easy to leak or mis-attribute when work is multiplexed on carriers.
- The legacy **JDK 21 Aerospike Java client** removed thread-local variables for similar reasons; the new SDK should not reintroduce them via logging.
- At **500k–1M+** application operations per second, any per-scope `put`/`remove` on hot paths adds cost and review burden.

Attach context **only on lines that are actually emitted**, inside `is*Enabled()` guards — see [SDK logging style](#sdk-logging-style-prescriptive).

### Standard field names

| Field | Meaning |
|-------|---------|
| `aerospike.cluster` | Cluster name or id |
| `aerospike.node` | Optional: `host:port` for connection-scoped lines, `nodeId` for other lines where applicable |

Add area-specific fields as needed (`durationMs`, `nodeCount`, `namespace`, `resultCode`, …). Use consistent names so JSON encoders and aggregation pipelines can index them. **`addKeyValue` and message arguments must obey [User data — never log](#user-data--never-log)**—only operational metadata, never keys, bins, or values.

### Examples

**Tend cycle summary** (~1 Hz when DEBUG on — cluster field on each line is fine):

```java
if (tendLog.isDebugEnabled()) {
    tendLog.atDebug()
        .addKeyValue("aerospike.cluster", cluster.getName())
        .addKeyValue("durationMs", millis)
        .addKeyValue("nodeCount", nodeCount)
        .log("Tend cycle complete");
}
```

**Rare connection / TLS diagnostic** (low rate — still use per-event key-values, not MDC):

```java
if (connLog.isDebugEnabled()) {
    connLog.atDebug()
        .addKeyValue("aerospike.cluster", cluster.getName())
        .addKeyValue("aerospike.remote", host.toString())
        .log("Starting TLS handshake");
}
```

**Sparse hot-path DEBUG** (command, routing) — only when the level is on:

```java
if (cmdLog.isDebugEnabled()) {
    cmdLog.atDebug()
        .addKeyValue("aerospike.cluster", cluster.getName())
        .addKeyValue("namespace", namespace)
        .addKeyValue("latencyMs", latencyMs)
        .log("Read completed with result {}", resultCode);
}
```

**Anti-pattern — thread-local or per-operation scope** (do **not** use MDC or wrappers at max QPS):

```java
public Record get(Key key) {
    MDC.put("aerospike.cluster", cluster.getName());  // do not do this
    try {
        return doGet(key);
    } finally {
        MDC.remove("aerospike.cluster");
    }
}
```

At full throughput, use **metrics or tracing** for per-operation correlation; keep command/routing logs sparse and guarded.

## Dependencies and build layout

- The parent POM manages **`slf4j-api`** and **`slf4j-simple`** versions via **`slf4j.version`** (for example 2.0.16).
- The **`client`** module depends on **`slf4j-api`** only (compile scope)—no binding is shipped to applications that depend on the SDK.
- **`examples`** adds **`slf4j-simple`** so runnable sample programs have a **default console binding** without pulling Logback or Log4j2 into that module.
- **`benchmarks`** and **client tests** may use **`slf4j-simple`** or **`logback-classic`** in **test** scope (or another binding) so output or silence is explicit; avoid adding a binding to the published client artifact.

### `examples` module: `slf4j-simple` in this repository

The **`aerospike-examples-sdk`** module declares **`org.slf4j:slf4j-simple`** (version from the parent). At runtime SLF4J selects **`SimpleLogger`**, which writes to **stderr** and reads configuration from:

1. **Classpath resource** `simpleLogger.properties` — in this repo, see `examples/src/main/resources/simpleLogger.properties` (defaults for `com.aerospike.client.sdk` and `com.aerospike.examples`).
2. **System properties** — same keys as in the file; system properties override file values. Prefix is `org.slf4j.simpleLogger.` (see the [SimpleLogger](https://www.slf4j.org/api/org/slf4j/simple/SimpleLogger.html) documentation for the full list).

Useful keys:

| Property | Meaning |
|----------|---------|
| `org.slf4j.simpleLogger.defaultLogLevel` | Default level for loggers without a more specific rule (`trace`, `debug`, `info`, `warn`, `error`, `off`). |
| `org.slf4j.simpleLogger.log.<logger name>` | Level for the logger whose name is exactly `<logger name>` (dots included), e.g. `org.slf4j.simpleLogger.log.com.aerospike.client.sdk.serde=trace`. |

**Example: run the examples JAR with extra SDK verbosity** (system properties override `simpleLogger.properties`):

```bash
mvn -q -pl examples package -DskipTests
java -Dorg.slf4j.simpleLogger.log.com.aerospike.client.sdk=debug \
  -jar examples/target/aerospike-examples-sdk-*-jar-with-dependencies.jar
```

**Example: tune only the cross-cutting serde logger** (once the SDK uses that name), by passing the same `org.slf4j.simpleLogger.*` keys on the **`java`** command line:

```bash
java -Dorg.slf4j.simpleLogger.log.com.aerospike.client.sdk.serde=trace \
  -jar examples/target/aerospike-examples-sdk-*-jar-with-dependencies.jar
```

You can also set defaults for the whole JVM (including Maven-invoked tools) with **`MAVEN_OPTS`** or a **`JAVA_TOOL_OPTIONS`** environment variable using the same `-Dorg.slf4j.simpleLogger...` keys.

For a packaged **`jar-with-dependencies`**, ensure `simpleLogger.properties` remains on the classpath (it is copied from `examples/src/main/resources` by default), or pass the same properties with `-D` on the `java` command line.

### Eager argument evaluation

When replacing `Log.debugEnabled()` / `Log.debug(...)` pairs, watch for **eager argument evaluation**: in Java, arguments to `log.debug("{}", foo())` are evaluated **before** `debug` runs, so `foo()` runs even when the level is disabled—unlike a guarded block:

```java
if (log.isDebugEnabled()) {
    log.debug("{}", expensive());
}
```

**`log.isDebugEnabled()`** is a cheap level check, comparable to **`Log.debugEnabled()`**; SLF4J is not inherently more expensive for guards. Follow [SDK logging style](#sdk-logging-style-prescriptive): use explicit `is*Enabled()` guards whenever building the log payload is non-trivial (large strings, serialization, collection walks)—**do not** replace those guards with `log.atDebug().log(() -> ...)`.

Most work is mechanical aside from auditing call sites for this pattern.

## Host application integration

The client POM should not depend on these; document patterns for integrators.

### Logback (`logback.xml`)

```xml
<configuration>
  <logger name="com.aerospike.client.sdk" level="INFO"/>
  <logger name="com.aerospike.client.sdk.behavior" level="WARN"/>
  <logger name="com.aerospike.client.sdk.serde" level="TRACE"/>
  <root level="WARN">
    <appender-ref ref="STDOUT"/>
  </root>
</configuration>
```

Runtime level changes: `LoggerContext` APIs, Logback’s JMX configurator, or tooling that wraps them.

### Log4j2

Use `log4j2.xml` with **`log4j-slf4j2-impl`** (for SLF4J 2.x). Configure package loggers; use JMX or programmatic `Configurator.setLevel` for live updates where appropriate.

### Spring Boot (`application.yml`)

```yaml
logging:
  level:
    com.aerospike.client.sdk: DEBUG
    com.aerospike.client.sdk.behavior: WARN
    com.aerospike.client.sdk.serde: TRACE
```

With Spring Boot Actuator, **`/actuator/loggers`** can adjust levels at runtime (when exposed and secured per your policies).

## Tests and benchmarks

- Benchmarks that implemented `Log.Callback` should move to a test-scoped Logback configuration or `slf4j-simple`.
- Tests that called `Log.setCallback(null)` for quiet runs can rely on a **test** binding with level OFF, or a dedicated Logback test config.

## Risks and product decisions

- **Per-cluster levels**: **non-goal** for the SDK — SLF4J levels are per logger name. Cluster identity is a structured field on log lines; hosts filter in aggregation if needed.
- **User data in logs**: a single DEBUG/TRACE line that logs keys, bins, or payloads can violate customer policy (especially in financial services). Treat as a **severity-1 defect**; code review and tests should enforce [User data — never log](#user-data--never-log).

## Implementation checklist (internal)

Use this as a working tracker for the migration itself:

1. **Parent / client / examples SLF4J setup** — `slf4j-api` is in parent `dependencyManagement` and on the `client` module; `examples` uses `slf4j-simple` plus `simpleLogger.properties`. Add a **test-scoped** binding for `client` tests if you want SLF4J output during `mvn test` without warnings.
2. Define named area loggers (`serde`, `tend`, `connection`, `routing`, `command`, `query`, `behavior`, `info`, `task`, lifecycle as needed) per the [operational logging strategy](#operational-logging-strategy); document standard **structured field names** (for example `aerospike.cluster`).
3. Replace `Log.*` usage across the client with per-class `Logger` and [SDK logging style](#sdk-logging-style-prescriptive) guards; route high-volume serde paths through the serde logger; audit for [User data — never log](#user-data--never-log).
4. Deprecate or remove global `Log` mutation from `ClusterDefinition`; update examples, benchmarks, and tests.
5. Remove `Log.java`.
