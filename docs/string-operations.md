# String operations in the Java SDK

Examples in this repo: run **`./run_examples StringOperationsExample`** from the [`examples`](../examples) module (see [`StringOperationsExample.java`](../examples/src/main/java/com/aerospike/examples/StringOperationsExample.java)); it skips automatically if the cluster is below 8.1.3.

Server string read/modify operations require **Aerospike Database 8.1.3** or later. Applications use them through:

| Surface | Role |
|--------|------|
| [`BinBuilder`](../client/src/main/java/com/aerospike/client/sdk/BinBuilder.java) | Fluent `session.upsert(key).bin("s").strlen().…` — each method queues one op on the named top-level bin. |
| [`StringOperation`](../client/src/main/java/com/aerospike/client/sdk/operation/StringOperation.java) | Low-level operation factories for `appendOperations(...)`, including optional `CTX` navigation into string values nested inside list/map bins. |
| [`StringExp`](../client/src/main/java/com/aerospike/client/sdk/exp/StringExp.java) | Expression API for filters, `selectFrom` / AEL, and composable reads. |

Cross-type string conversion of whole bins uses **`readAsString()`** on `BinBuilder` (server top-level string representation of int, float, string, bool, or UTF-8 blob). It is **not** named `toString()` to avoid clashing with `Object#toString`; unlike string sub-ops composed inside CDT paths, that conversion does not take a nested path.

## Semantics aligned with the server

- **Indices** — Unicode **codepoints**, left-to-right; negatives count from the end (`-1` = last). Out-of-range indices are clamped (no error for reads in the usual sense).
- **`substr` / `snip` (two integer bounds)** — half-open **`[start, end)`**, matching AEL **`from` / `to`** (`to` exclusive). One-argument forms run through the **end of the string**.
- **`find`** — Substring search; the optional second argument is **occurrence** (1 = first, -1 = last), **not** a start offset. If the second argument is omitted, the server default is **1** (first match). Some older implementation tables described `FIND` with a “start offset”; treat that as **errata** — live server and AEL use **occurrence**.
- **Modify write flags** — [`StringWriteOptions`](../client/src/main/java/com/aerospike/client/sdk/StringWriteOptions.java) / [`StringWriteFlags`](../client/src/main/java/com/aerospike/client/sdk/operation/StringWriteFlags.java) expose `DEFAULT`, `CREATE_ONLY`, `UPDATE_ONLY`, and `NO_FAIL`. `CREATE_ONLY` applies only when the top-level string bin does not exist and is valid only on `insert`, `overwrite`, `concat`, `append`, `prepend`, `padStart`, `padEnd`, and `repeat`; it is invalid with `CTX` and mutually exclusive with `UPDATE_ONLY`. `UPDATE_ONLY` applies only when the string bin exists; a missing bin is a no-op and is not created. `NO_FAIL` suppresses parsed modify failures such as `CREATE_ONLY` on a live bin or an unreachable `CTX` path that resolves to `OP_NOT_APPLICABLE`, leaving the value unchanged. It does not suppress bad flag combinations, `CREATE_ONLY` with `CTX`, malformed CDT paths, wrong bin type, or invalid UTF-8.

Nested string writes are available through [`StringOperation`](../client/src/main/java/com/aerospike/client/sdk/operation/StringOperation.java) factories that accept `CTX`, appended with `appendOperations(...)`, or by composing [`StringExp`](../client/src/main/java/com/aerospike/client/sdk/exp/StringExp.java) inside CDT `modifyBy(...)` paths. Fluent `BinBuilder` string methods operate on the selected top-level bin.

## AEL and `StringExp`

[`StringExp`](../client/src/main/java/com/aerospike/client/sdk/exp/StringExp.java) exposes the same string read/modify operations as [`BinBuilder`](../client/src/main/java/com/aerospike/client/sdk/BinBuilder.java) (`strlen`, `find`, `substr`, …), as composable expressions. [AEL string functions](ael/string-functions.md) describe the expression language; AEL naming is being aligned with these server operations. `BinBuilder` uses those short names without a `str()` sub-builder; chain `bin("x")` again for multiple steps on the same bin.

### Examples with optional arguments omitted

Many string ops have overloads or optional parameters. When the trailing bound or occurrence is omitted, the server uses its default (e.g. `substr` from `start` through the **end** of the string; `find` with default **first** occurrence).

**[`BinBuilder`](../client/src/main/java/com/aerospike/client/sdk/BinBuilder.java)** — `substr` / `find` / `split`:

```java
session.upsert(key)
    .bin("s").substr(3)                    // from codepoint 3 through end (optional end omitted)
    .bin("s").substr(1, 4)                 // half-open [1, 4)
    .bin("s").find(":")                   // first ":" (optional occurrence omitted; default 1)
    .bin("s").find(":", -1)               // last ":"
    .bin("s").split()                     // split per codepoint (optional separator omitted)
    .bin("s").split(",")                  // split on comma
    .execute();
```

**[`StringExp`](../client/src/main/java/com/aerospike/client/sdk/exp/StringExp.java)** — bound expressions come **before** `src`; omit the `end` expression for the one-bounded form:

```java
Exp bin = Exp.stringBin("s");
StringExp.substr(Exp.val(3), bin);                      // through end (no end expression)
StringExp.substr(Exp.val(1), Exp.val(4), bin);          // [1, 4)
StringExp.find(Exp.val(":"), bin);                     // first match
StringExp.find(Exp.val(":"), Exp.val(-1), bin);       // last match
StringExp.split(bin);                                  // per-codepoint split
```

## `Operation` list (`appendOperations`) and `StringExp` reads

[`BinBuilder`](../client/src/main/java/com/aerospike/client/sdk/BinBuilder.java) string methods queue the same wire ops as the internal
[`StringOperation`](../client/src/main/java/com/aerospike/client/sdk/operation/StringOperation.java) factories. When you already have
`Operation` instances, append them on [`ChainableOperationBuilder`](../client/src/main/java/com/aerospike/client/sdk/ChainableOperationBuilder.java)
or [`ChainableQueryBuilder`](../client/src/main/java/com/aerospike/client/sdk/ChainableQueryBuilder.java) with **`appendOperations`**
instead of going through `bin()`:

```java
import com.aerospike.client.sdk.operation.StringOperation;

session.upsert(key)
    .appendOperations(
        StringOperation.strlen("s"),
        StringOperation.substr("s", 1, 4),
        StringOperation.substr("s", 3))
    .execute();
```

For **expression** reads, pass [`StringExp`](../client/src/main/java/com/aerospike/client/sdk/exp/StringExp.java) nodes to
`selectFrom(Exp)` on [`QueryBinBuilder`](../client/src/main/java/com/aerospike/client/sdk/QueryBinBuilder.java) / [`BinBuilder`](../client/src/main/java/com/aerospike/client/sdk/BinBuilder.java)
(virtual bin names hold the results):

```java
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.StringExp;

session.query(key)
    .bin("slen").selectFrom(StringExp.strlen(Exp.stringBin("s")))
    .bin("stail").selectFrom(StringExp.substr(Exp.val(3), Exp.stringBin("s")))
    .execute();
```

Tests and runnable sample: [`OperateStringTest`](../client/src/test/java/com/aerospike/client/sdk/OperateStringTest.java) (integration, server 8.1.3+),
[`StringApiPackagingTest`](../client/src/test/java/com/aerospike/client/sdk/StringApiPackagingTest.java) (packaging / no cluster),
[`StringOperationsExample`](../examples/src/main/java/com/aerospike/examples/StringOperationsExample.java) (`./run_examples StringOperationsExample`).

## List `join`

Joining list elements into one string is a **list** path operation in AEL (`$.listBin.join('-')`), not a string-bin sub-op. When the server exposes it for expressions, the SDK may add a `ListExp` helper; until then see AEL/list docs.

## Regex flags

[`StringRegexFlags`](../client/src/main/java/com/aerospike/client/sdk/operation/StringRegexFlags.java) includes `UNIX_LINES` and the alias **`UNIX_LINES_ONLY`** (ICU Unix-line mode).
