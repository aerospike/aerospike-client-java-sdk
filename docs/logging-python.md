# Python SDK logging design (stdlib `logging`)

**Related documents:**

- [Java SDK logging design](logging-java.md) — shared operational semantics (component split, user-data policy, structured fields). Field names and level meanings align across language SDKs where possible.
- Aerospike Python SDK user guide: [`docs/guide/logging.md`](https://github.com/aerospike/aerospike-client-python-sdk/blob/main/docs/guide/logging.md) (quick start, env vars).
- Metrics and tracing (when documented) complement diagnostic logs; this document covers **logs only**.

This document describes how logging for the **Aerospike Python SDK** should be structured so the library embeds cleanly in host applications. Hosts use Python’s standard [`logging`](https://docs.python.org/3/library/logging.html) package — handlers, formatters, and optional JSON formatters (`python-json-logger`, structlog processors, etc.) are **application choices**. The published SDK must not require a third-party logging framework.

## Architecture: three layers

Unlike the Java SDK (monolithic JVM client), the Python SDK sits on the **Rust 3.0** stack:

```
Application
  └─ aerospike_sdk          Python orchestration (session, query, AEL, index monitor, pools)
       └─ aerospike_async    Python Async Client (PyO3 PAC)
            └─ aerospike_core   Rust client (tend, pools, wire protocol, commands)
```

Each layer has its own **logger name prefix**. Operators tune them independently.

| Logger prefix | Layer | Typical ownership |
|---------------|-------|-----------------|
| `aerospike_core` | Rust core (via PyO3) | Cluster tend, connection pools, routing, command execution, serde/framing |
| `aerospike_async` | PAC | Client init, connect/disconnect to seeds, bridge/runtime errors |
| `aerospike_sdk` | Python SDK | Connect lifecycle wrappers, query/batch summaries, info admin, background tasks, index monitor, record streams, async pools |

**Implication:** many concerns that the Java doc assigns to `com.aerospike.client.sdk.tend`, `.connection`, `.command`, and `.serde` are implemented in **Rust** and appear under `aerospike_core` (and sometimes `aerospike_async`). The Python SDK documents **what operators can tune** and **what Python code must log**; sub-logger names under `aerospike_core.*` are owned by the Rust/PAC release and should be documented in PAC release notes as they stabilize.

## Current state (baseline)

- Logging uses **stdlib `logging`** only — no global `Log` gate like the legacy Java `Log.java`.
- Three coarse roots: `aerospike_core`, `aerospike_async`, `aerospike_sdk` (see user guide).
- SDK call sites mix `logging.getLogger(__name__)` and **hard-coded** names (`aerospike_sdk.query`, `aerospike_sdk.info`, …) — naming is not fully consistent yet.
- **No** documented component taxonomy, structured field convention, or user-data policy in the Python repo (only the short user guide).
- **Cluster name** is carried on `ClientPolicy` for server validation, not routinely attached to log lines.
- Tests/examples honor `AEROSPIKE_LOG_LEVEL` and `AEROSPIKE_LOG_FILE` (`conftest.py`, `examples/_env.py`).

The sections below are the **target** design as the Python SDK matures.

## Design principles

| Requirement | How stdlib `logging` + the host app satisfy it |
|-------------|-----------------------------------------------|
| Embeddable library; host picks handlers/formatters | Use **`logging.getLogger(...)`** only. Do **not** call `basicConfig()` inside the SDK on import. Do **not** require structlog/loguru as a dependency. |
| Package / module granularity | Default: `logger = logging.getLogger(__name__)` under `aerospike_sdk.*`. For **cross-cutting** channels, use stable names from [`SdkLoggers`](#cross-cutting-loggers-example) (not ad-hoc short strings). |
| Cross-cutting “tags” (query, background, index monitor) | Named loggers under `aerospike_sdk.<component>` so hosts can set `logging.getLogger("aerospike_sdk.query").setLevel(logging.DEBUG)` without enabling all of `aerospike_sdk`. |
| Negligible cost when a logger is off | Use **`logger.isEnabledFor(logging.DEBUG)`** before expensive message construction. Prefer **`logger.debug("msg %s", arg)`** (percent formatting with separate args) on hot paths — the stdlib avoids formatting when the level is disabled. **Do not** use f-strings or pre-built messages in hot-path `debug`/`info` calls. |
| Change levels without restart | Host configures `logging.config.dictConfig`, environment-driven setup, or runtime `logger.setLevel` (and shared handler config). Not an SDK feature. |
| Per-cluster log levels | **Not expressible** in stdlib logging: levels are per **logger name**, not per cluster id. **Non-goal** for the SDK. Multi-cluster processes tune component loggers globally; cluster identity appears as a **structured field** on emitted lines ([Structured context](#structured-context)). |
| **No user data in logs** | **Never** log application or customer data at **any** level. Same rules as [Java — User data](logging-java.md#user-data--never-log). Required for regulated industries (for example financial services). |
| **Rust vs Python visibility** | Document which components log from **Python** vs **Rust** so operators know whether raising `aerospike_sdk` DEBUG is enough or `aerospike_core` must be enabled. |

## SDK logging style (prescriptive)

### Levels

Python’s stdlib defines `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` — there is **no `TRACE`**. Use:

- **`DEBUG`** for support-playbook detail and developer forensics in **Python** code (metadata only).
- Finer “trace” detail in **Rust** may map to `DEBUG` under `aerospike_core` sub-loggers; keep those loggers off in production.

Align semantics with [Java log levels](logging-java.md#log-levels-semantics): ERROR for process/background failure, WARN for degraded cluster, INFO for sparse lifecycle, DEBUG for bounded diagnostics.

### Hot-path pattern

```python
import logging

log = logging.getLogger(__name__)

if log.isEnabledFor(logging.DEBUG):
    log.debug(
        "execute: %s.%s specs=%d keys=%d",
        namespace,
        set_name,
        spec_count,
        key_count,
        extra={"aerospike.cluster": cluster_name},
    )
```

**Percent-args (preferred on warm paths):**

```python
log.debug("Packed %d fields for namespace %s", field_count, namespace)
```

The message is not formatted unless DEBUG is enabled for this logger.

**Avoid on hot paths:**

```python
log.debug(f"Packed {field_count} fields")  # f-string always runs
log.debug("Packed %s", expensive())        # expensive() always runs
```

### When a guard is optional

Sparse, low-rate lines (for example node added/removed if logged from Python, index monitor refresh once per interval) may call `log.info(...)` without a guard when arguments are cheap literals or simple integers. Use `isEnabledFor` whenever building **large** strings, walking collections, or calling into Rust for debug-only data.

### Asyncio, threads, and I/O

Python does **not** use virtual threads. Relevant concurrency patterns:

| Pattern | Where | Logging note |
|---------|-------|----------------|
| **asyncio** | `aerospike_sdk.aio.*` | Coroutines share the event loop; do not rely on thread-local log context. |
| **Daemon threads** | `IndexesMonitor`, `AsyncPool` loop threads | Attach **cluster** and **pool index** in the message or `extra` on each line — no implicit task context. |
| **Sync API** | `aerospike_sdk.sync.*` | Per-thread clients (`threading.local`); still **do not** use `contextvars` or thread-local maps for logging context on the request path. |
| **uvloop** | PAC default event loop | Performance only; does not change logging API. |

Per-request correlation at high QPS belongs in **metrics/tracing**, not verbose logs.

## Operational logging strategy

Split diagnostics like other database drivers ([MongoDB logging spec](https://github.com/mongodb/specifications/blob/master/source/logging/logging.md), [PyMongo component loggers](https://pymongo.readthedocs.io/en/stable/examples/logging.html), [DataStax Java driver logging](https://docs.datastax.com/en/developer/java-driver/4.3/manual/core/logging/index.html)): topology, connection, routing, commands, serde, query, behavior, tasks — so operators can raise verbosity in one area without enabling everything.

### User data — never log

**At no log level** may the Python SDK (or Python wrappers around Rust) write **user data** or **customer-controlled content** to diagnostic logs. This does not bend for support DEBUG or local reproduction.

**User data** includes anything stored in Aerospike or passed through the client API:

- **Record keys** that identify customer records (**digests** are acceptable to log)
- **Bin names** and **bin values** (maps, lists, blobs, nested structures)
- **Query / scan filter literals**, index predicates, or expression text with customer values
- **Batch key lists**, UDF arguments, operation payloads
- **Credentials**, tokens, TLS private material, auth bodies

**Allowed:** cluster name, node/`host:port`, namespace and set **names** (deployment topology), partition id, operation type, result code, latency, counts, generations.

**Also avoid:** logging `str(exception)` or `exception.args` when text may echo user data — prefer exception **type** and SDK **result code**. No hex dumps or “truncated” payload previews.

For content-level debugging use tests, local captures, or server-side tools — not production SDK logs.

### Cross-cutting areas: loggers, layer, and what to log

Use **stable logger names** (`SdkLoggers` constants). **Layer** indicates where messages are emitted today; Python-only rows are implementation targets for the SDK repo.

| Area | Logger name (target) | Layer today | Typical levels | What to log |
|------|----------------------|-------------|----------------|-------------|
| **Cluster tend & topology** | `aerospike_core.cluster` (Rust); optional `aerospike_sdk.tend` if Python hooks added | **Rust** | INFO: membership milestones. DEBUG: tend cycle summary (~1 Hz). WARN: retryable tend/seed failures. | `aerospike.cluster`, node name, `host:port`, partition generation / rebalance flags — not full partition maps. |
| **Connections** | `aerospike_async`, `aerospike_core` | **PAC / Rust** | DEBUG: connect/handshake. WARN: auth/TLS/repeated failures. | `host:port`, TLS cipher if useful, latency; never secrets. |
| **Lifecycle (Python connect)** | `aerospike_sdk.aio.client`, `aerospike_sdk.sync.client` | **Python** | DEBUG: seeds, connected summary. INFO: rare milestones. | Seeds, `aerospike.cluster`, build info **by node** (status strings only — not record data). |
| **Routing** | `aerospike_core` | **Rust** | DEBUG: partition → node, batch split **counts**. | Namespace/set name, partition id, node, batch sizes — **no key values**. |
| **Commands & batches** | `aerospike_core` (wire); `aerospike_sdk.command` (future Python summaries) | **Rust** (+ optional Python) | DEBUG: op type, result code, latency, retry, node. | Operational fields only; [user data rule](#user-data--never-log). |
| **Serde** | `aerospike_core` | **Rust** | DEBUG: lengths, type tags, boundaries — off in prod. | Metadata only; no decoded content. |
| **Queries** | `aerospike_sdk.query` | **Python** | DEBUG: bounded execute summaries. | Namespace, set, spec/key **counts**, latency — no filter literals or rows. |
| **Info protocol** | `aerospike_sdk.info`, `aerospike_sdk.sync.info` | **Python** | DEBUG: command name, host, failure class. | Response **length** or status — not body content with customer data. |
| **Background tasks** | `aerospike_sdk.background` | **Python** | INFO: phase start/complete/fail. | Task type, namespace/set/index names, result class. |
| **Index monitor** | `aerospike_sdk.index_monitor` | **Python** | DEBUG: cache refresh summary. | Index **counts** per namespace — not key data. |
| **Record streams** | `aerospike_sdk.record_stream` | **Python** | DEBUG: chunk/batch **counts** only. | Never log record bodies. |
| **Async pool** | `aerospike_sdk.aio.pool` | **Python** | INFO/DEBUG: pool start/stop, loop thread identity. | Pool index, client count — not user payloads. |
| **Behavior & config** | `aerospike_sdk.behavior` (future) | **Python** | WARN: unknown keys, conflicts. INFO: profile loaded once. | Config **key** names, non-secret source path — not full YAML. |
| **AEL** | `aerospike_sdk.ael` (future) | **Python** | DEBUG: parse milestones only if needed. | Never log expression literals containing customer values. |

**Production default:** `aerospike_core` and `aerospike_async` at **WARNING** or **INFO**; `aerospike_sdk` at **WARNING**. Enable narrow DEBUG (for example `aerospike_sdk.query` only) during incidents — not `DEBUG` on all three roots.

**“Log every request”** (slow-op thresholds, success lines) should use metrics/listeners, not unbounded `aerospike_sdk.query` DEBUG in production.

### Principles (summary)

1. **Split channels** — tune `aerospike_core` vs `aerospike_async` vs `aerospike_sdk.<component>`.
2. **Reserve INFO** for lifecycle; push hot-path detail to **DEBUG** with guards.
3. **API-visible failures** — DEBUG at most when the caller already gets an exception.
4. **No user data** at any level ([User data — never log](#user-data--never-log)).
5. **Volume** — command/query/serde paths are high-cost; Rust DEBUG can flood at scale.

## Cross-cutting loggers (example)

Centralize stable names (for example `aerospike_sdk/loggers.py`):

```python
"""Stable logger names for operator tuning."""

class SdkLoggers:
  QUERY = "aerospike_sdk.query"
  INFO = "aerospike_sdk.info"
  BACKGROUND = "aerospike_sdk.background"
  INDEX_MONITOR = "aerospike_sdk.index_monitor"
  LIFECYCLE = "aerospike_sdk.lifecycle"  # optional umbrella for client/pool
  BEHAVIOR = "aerospike_sdk.behavior"    # future
  AEL = "aerospike_sdk.ael"              # future
  COMMAND = "aerospike_sdk.command"      # future Python-side summaries only
```

```python
import logging
from aerospike_sdk.loggers import SdkLoggers

log = logging.getLogger(SdkLoggers.QUERY)

if log.isEnabledFor(logging.DEBUG):
    log.debug(
        "Query execute complete latency_ms=%d",
        latency_ms,
        extra={"aerospike.cluster": cluster_name, "namespace": namespace},
    )
```

Hosts:

```python
logging.getLogger("aerospike_sdk.query").setLevel(logging.DEBUG)
logging.getLogger("aerospike_core").setLevel(logging.WARNING)
```

### Module logger vs cross-cutting logger

| Use | Logger | Example |
|-----|--------|---------|
| Operator-tunable area (query, background, index monitor) | `SdkLoggers.QUERY`, etc. | Execute summaries, index cache refresh |
| Rare, module-local invariant | `logging.getLogger(__name__)` | Internal pool loop setup detail |

**Anti-pattern:** logging the same operational event on both `SdkLoggers.QUERY` and `__name__`. **Anti-pattern:** inconsistent names (`aerospike_sdk.info` vs `aerospike_sdk.sync.info`) for the same operational story — converge on one documented name per concern (sync/async may share `SdkLoggers.INFO` with sync-specific sub-module only for truly sync-only code paths).

### Worked example: async client connect (simplified)

```python
import logging

log = logging.getLogger("aerospike_sdk.aio.client")

async def connect(self) -> None:
    cluster = self._policy.cluster_name
    if log.isEnabledFor(logging.DEBUG):
        log.debug(
            "Connecting seeds=%r",
            self._seeds,
            extra={"aerospike.cluster": cluster},
        )
    self._client = await new_client(self._policy, self._seeds)
    self._connected = True
    if log.isEnabledFor(logging.DEBUG):
        log.debug(
            "Connected seeds=%r",
            self._seeds,
            extra={"aerospike.cluster": cluster},
        )
```

Tend cycle summaries and add/remove node lines are expected from **`aerospike_core`** (Rust), not duplicated in Python unless the SDK adds explicit forwarding — avoid duplicate stories at different levels.

## Structured context

Attach context on **each emitted** log record — via the `extra` dict (for formatters that support it) or explicit fields in the message. The SDK does **not** use:

- **`logging.LoggerAdapter`** with mutable context on hot paths
- **`contextvars`** for per-request logging context
- Thread-local dicts for cluster identity

### Standard field names

Align with [Java structured fields](logging-java.md#standard-field-names) where applicable:

| Field | Meaning |
|-------|---------|
| `aerospike.cluster` | Cluster name or id (`ClientPolicy.cluster_name` or server-reported) |
| `aerospike.node` | `host:port` or node id when relevant |
| `namespace`, `set` | Data plane topology names (not secret identifiers) |
| `operation`, `result_code`, `latency_ms`, `key_count` | Command/query summaries |

`extra` keys must obey [User data — never log](#user-data--never-log). Formatters must include `%(message)s` and optionally merge `extra` into JSON output — **host configuration**, not SDK dependency.

**JSON example (host application):**

```python
import logging
import json

class JsonExtraFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in ("aerospike.cluster", "aerospike.node", "namespace", "latency_ms"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        return json.dumps(payload)
```

## Host application integration

The SDK does not configure logging on import. Applications choose one of:

### Environment variables (tests / examples)

```bash
export AEROSPIKE_LOG_LEVEL=DEBUG
export AEROSPIKE_LOG_FILE=/tmp/aerospike.log   # optional
```

### Programmatic (three layers)

```python
import logging

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
))

for name in ("aerospike_core", "aerospike_async", "aerospike_sdk"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
```

### `dictConfig` (production-shaped)

```python
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        },
    },
    "loggers": {
        "aerospike_core": {"level": "WARNING", "propagate": True},
        "aerospike_async": {"level": "INFO", "propagate": True},
        "aerospike_sdk": {"level": "WARNING", "propagate": True},
        "aerospike_sdk.query": {"level": "WARNING", "propagate": True},
        "aerospike_sdk.index_monitor": {"level": "INFO", "propagate": True},
    },
    "root": {"level": "WARNING", "handlers": ["console"]},
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
        },
    },
}
```

**Note:** child loggers under `aerospike_sdk` do not inherit level changes on `aerospike_sdk` unless `propagate` and handler attachment are understood — document explicit per-component levels for incident playbooks.

### Django / Flask / FastAPI

Configure the same logger names in the framework’s logging dict or override `LOGGING` / `dictConfig` — no Aerospike-specific API.

## What the Python SDK cannot expose (Rust boundary)

Because wire protocol, tend loop, connection pooling, and serde run in **aerospike-core**:

- Python cannot add DEBUG lines inside Rust command execution without PAC/Rust changes.
- Sub-logger names under `aerospike_core` are defined in Rust; Python docs should **link to PAC/Rust logging docs** as they appear.
- Tuning **only** `aerospike_sdk` DEBUG will **not** show tend cycles or wire-level retries — operators must raise `aerospike_core` (temporarily, in controlled incidents).
- Sync and async APIs share the same Rust core; logging volume scales with **thread count** (sync per-thread runtimes) and **async pool size** — warn in runbooks.

Coordinate field names (`aerospike.cluster`, etc.) with Rust log bridges when PAC adds structured fields to Rust-emitted records.

## Tests and benchmarks

- Tests use `AEROSPIKE_LOG_LEVEL` / `AEROSPIKE_LOG_FILE` via `conftest.py`; default quiet unless env set.
- Unit tests should not require log output assertions for behavior — use caplog sparingly.
- CI may grep for forbidden patterns in log call sites (raw key/bin literals passed to `log.*`).

## Risks and product decisions

- **Per-cluster levels:** non-goal — levels are per logger name; use `aerospike.cluster` in `extra` and filter in aggregation.
- **User data in logs:** severity-1 defect; especially critical for financial-services customers.
- **Rust DEBUG volume:** enabling `aerospike_core` DEBUG in production can overwhelm pipelines — prefer narrow Python loggers first.
- **Logger name drift:** mixed `__name__` vs short strings hurts operability — migrate to `SdkLoggers` constants.

## Implementation checklist (Python SDK)

1. Add `aerospike_sdk/loggers.py` with stable `SdkLoggers` constants; migrate call sites.
2. Normalize info logger naming (`aerospike_sdk.info` vs `aerospike_sdk.sync.info`).
3. Attach `aerospike.cluster` via `extra` on connect, query, index monitor, and pool lifecycle logs.
4. Audit hot paths (`aerospike_sdk/aio/operations/query.py`, etc.) for f-strings and unguarded expensive DEBUG.
5. Expand `docs/guide/logging.md` with component table and link to this design doc.
6. Document `aerospike_core` sub-loggers with PAC/Rust releases.
7. Add `aerospike_sdk.behavior` / `aerospike_sdk.ael` loggers when those areas gain DEBUG.
8. User-data audit before GA — same bar as [logging-java.md](logging-java.md).

## Cross-language alignment

| Concept | Java SDK | Python SDK |
|---------|----------|--------------|
| Logging API | SLF4J 2.x | stdlib `logging` |
| Component tend/connection/command/serde | `com.aerospike.client.sdk.*` | Primarily `aerospike_core` (Rust) |
| Python/Java orchestration loggers | `com.aerospike.client.sdk.query`, etc. | `aerospike_sdk.*` |
| Structured cluster field | `addKeyValue("aerospike.cluster", …)` | `extra={"aerospike.cluster": …}` |
| User data policy | [User data — never log](logging-java.md#user-data--never-log) | Same |
| Per-cluster log levels | Non-goal | Non-goal |
