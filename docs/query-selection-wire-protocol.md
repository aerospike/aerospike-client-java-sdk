# Query selection wire protocol (two-phase)

Normative wire contract for **new clients** using server-led index selection on dataset queries with **string AEL**. Applies to any client (Java, C, Go, Python, …) talking to Aerospike server **8.1.3+** with field **44** WHERE explain support.

**Server reference:** `aerospike-server` branch `suresh/dsl-queryOptimization-integration` — `query_where.h` / `query_where.c` (field `44` parse), `query_plan.c` (explain), `query.c` (`get_query_filter_exp`, `get_range_field` on execute).

**Java reference:** `QueryWhereWire`, `CommandBuffer.setQueryExplain`, `QueryCommand.forPlan`, `IndexRangeWire.forExecuteWithIndexName`.

**Supersedes:** `INFO4` bit 7 + field **43** on the explain path (CLIENT-4800 draft). Do **not** use `INFO4` bit 7 or field **43** for string-AEL query selection.

---

## Overview

| Phase | Trigger | Partitions | Predicate field | Purpose |
|-------|---------|------------|-----------------|---------|
| **Explain** | Field **44** `EXPLAIN` flag set | **No** | **44** WHERE | Server returns SI vs PI plan (or `FILTERED_OUT`) |
| **Execute** | Field **44** `EXPLAIN` cleared | **Yes** | **44** WHERE | Normal partitioned query using plan pins |

Field types (AS_MSG): `21` = `INDEX_NAME`, `22` = `INDEX_RANGE`, `26` = `INDEX_TYPE`, `43` = `PREDEXP` / `FILTER_EXP`, `44` = `WHERE`.

**Capability:** cluster minimum server version **8.1.3** (Java: `Cluster.supportsQuerySelection()`).

---

## Field `44` (WHERE) — string AEL on queries

### Payload layout

```
[flags: u8][AEL source UTF-8...]
```

- **Not** MessagePack `[128, "<ael>"]`.
- **Not** client-compiled packed predexp bytes.
- **Not** a varint flags field — exactly **one** flags byte, then raw UTF-8 AEL text.

Server parses the AEL body via `as_exp_filter_build_ael()` → `ael_parse()`.

### Flags (`query_where.h`)

Unknown flag bits → server `PARAMETER`.

| Flag | Value | Explain request | Execute request |
|------|-------|-----------------|-----------------|
| `EXPLAIN` | `1 << 0` (`0x01`) | **Set** | **Clear** (required — leaving `EXPLAIN` set re-runs explain, never returns records) |
| `REQUIRE_INDEX` | `1 << 1` (`0x02`) | Optional — reject PI fallback | Ignored on execute today |
| `HARD_HINT` | `1 << 2` (`0x04`) | Reserved — **not used by server** | Do not send |

**Examples:**

| Phase | Flags byte | AEL (UTF-8) | Full field `44` value |
|-------|------------|-------------|------------------------|
| Explain | `0x01` | `$.age > 30` | `[0x01]['$','.','a','g','e',' ',...]` |
| Execute | `0x00` | same text | `[0x00]['$','.','a','g','e',' ',...]` |

### Mutual exclusion

Server rejects a message that sends **both** field **44** (WHERE) and field **43** (PREDEXP):

```
cannot specify both WHERE and PREDEXP
```

v1: one AEL expression per WHERE field.

### Explain vs execute AEL bytes

The server does **not** echo field **44** on the explain response. The client must retain the AEL string from the explain **request** and rebuild execute WHERE as `flags=0x00` + same UTF-8 text.

**Do not** replay explain bytes verbatim on execute (that leaves `EXPLAIN` set).

---

## Field `43` — legacy and non-AEL paths

Field **43** is **not** used on the new two-phase string-AEL path.

| Path | Field `43` payload |
|------|-------------------|
| **Legacy SI query** (client index selection, `forBin`, gate off) | Client-compiled **packed predexp** (residual row filter on SI execute) |
| **Non-AEL query** (`Exp` / `BooleanExpression`) | Packed predexp — no explain |
| **Read/write filter ops** (non-query) | Packed predexp, or (future) op `128` `[128, "<ael>"]` on field **43** |

**Do not** send field **43** `[128, ael]` for dataset query selection — use field **44** instead. Op `128` on field **43** is query-only unsupported on the optimizer branch; field **44** is the query AEL wire.

Field **44** is rejected on background query / aggregation jobs (`UNSUPPORTED_FEATURE`).

---

## Field `22` inner body (`INDEX_RANGE` value)

Both explain response and execute send field `22` as a message field whose **value** is a binary body:

```
[n_ranges: u8][bin_name_len: u8][bin_name: bin_name_len bytes][ktype: u8][range_payload...]
```

- `n_ranges` must be `1` today.
- `ktype` is particle type (`INTEGER`, `STRING`, …).
- `range_payload` is type-specific (e.g. for integer range: two `(u32 len)(u64 value)` pairs for start/end).

### Explain response vs execute request (SI path)

| | Field `21` | Field `22` — `bin_name_len` | Field `22` — rest |
|--|------------|------------------------------|-------------------|
| **Explain response** | Index registry name | **`> 0`** (driving bin, e.g. `age`) | `ktype` + `range_payload` |
| **Execute request** | Same index name from explain | **`0`** | Same `ktype` + `range_payload` as explain |

**Do not** copy explain field `22` verbatim onto execute when field `21` is present. The server execute parser (`get_range_field`) rejects **both** an index name and a bin name in the range body:

```
specified expression or index name with bin name
```

### Execute transform (all clients)

Given explain `22` body bytes `P`:

1. Require `P[0] == 1` (`n_ranges`).
2. Let `binLen = P[1]`.
3. If `binLen == 0`, use `P` unchanged on execute.
4. Else build execute body: `[1][0]` + `P[2 + binLen .. end]` (drop bin name; keep ktype + range).

Store the **explain** bytes on the plan object; apply this transform only when encoding execute.

**Example (integer equality `age == 25`):**

| | Size | Shape |
|--|------|-------|
| Explain `22` | 30 bytes | `[1][3]['a','g','e'][INTEGER][range…]` |
| Execute `22` | 27 bytes | `[1][0][INTEGER][range…]` |

### Legacy execute (unchanged)

Legacy clients **do not** use two-phase explain. On SI execute:

| Field `21` | Field `22` |
|------------|------------|
| Absent (typical) | `bin_name_len > 0` — client-built range |

Legacy path: bin in `22`, no `21`. New path: `21` + `22` with `bin_name_len == 0`.

---

## Field `26` (`INDEX_TYPE`)

On the **SI explain response**, the server sends field **26** after field **21** and before field **22`:

| Byte | Meaning |
|------|---------|
| `0` | `DEFAULT` (numeric / string index) |
| other | Collection index type ordinal (`LIST`, `MAPKEYS`, `MAPVALUES`, …) |

Clients should store `INDEX_TYPE` from explain and forward it on SI execute when non-`DEFAULT` (required for LIST/MAPCDT correctness; optional on execute today for simple types — server defaults if absent).

---

## Phase 1 — explain

### Request

| Field / bit | Required | Notes |
|-------------|----------|-------|
| Field **44** WHERE | Yes | `[0x01][<AEL UTF-8>]` — `EXPLAIN` flag set |
| `0` namespace | Yes | |
| `1` set | If query has set | |
| `7` socket timeout | Yes | |
| `9` query id | Yes | |
| `21` index name | No | Soft hint only (`forIndex`-style); **not** `forBin` |
| `22` INDEX_RANGE | **No** | Server selects from AEL |
| `43` PREDEXP | **No** | Mutually exclusive with field **44** |
| Partition fields | **No** | |
| `INFO4` bit 7 | **No** | Deprecated — do not set |

### Response

| Outcome | `result_code` | Fields |
|---------|---------------|--------|
| PI scan | `AS_OK` (0) | none |
| SI query | `AS_OK` (0) | `21` + `26` + `22` (explain shape: **with** bin name in `22`) |
| Filtered out | `AS_ERR_FILTERED_OUT` | none |
| `REQUIRE_INDEX` + PI plan | `AS_ERR_SINDEX_NOT_FOUND` | none |

Infer **PI vs SI** from presence of `21`/`22` (no separate selection-type field in v1).

**PI explain:** `OK` with no fields is normal. Execute needs field **44** + partitions only (explain is optional for PI if the client already knows it will scan).

---

## Phase 2 — execute

### Request

| Field **44** WHERE | Partition fields | SI: field `21` | SI: field `26` | SI: field `22` |
|--------------------|------------------|----------------|----------------|----------------|
| `[0x00][same AEL UTF-8]` | Yes (normal query) | From explain | From explain when non-`DEFAULT` | **Execute shape** (strip bin; see above) |

### SI plan

Send field **44** (execute flags) + field **21** + transformed field **22** (+ field **26** when needed).

### PI plan

Send field **44** only (+ normal partition fields). No `21` / `22` / `26`.

### `FILTERED_OUT` plan

Do not execute; surface `FILTERED_OUT` to the application.

---

## Explain response → execute request (alignment)

| Execute field | SI path | PI path | Source |
|---------------|---------|---------|--------|
| `0` namespace | required | required | Explain request / plan context |
| `1` set | optional | optional | Explain request / plan context |
| `44` WHERE | `[0x00][same AEL UTF-8]` | same | **Explain request** (client-stored AEL; rebuild — do not replay `0x01` bytes) |
| `21` INDEX_NAME | from explain | absent | Explain response |
| `26` INDEX_TYPE | from explain when non-`DEFAULT` | absent | Explain response |
| `22` INDEX_RANGE | execute shape (`bin_name_len=0`) | absent | Explain response after bin-name strip |
| Partitions, socket timeout, query id, … | normal query encoding | same | Client query builder |

---

## Quick reference — three SI wire shapes

| Client | Explain | Execute `21` | Execute `22` inner | Execute predicate |
|--------|---------|--------------|---------------------|-------------------|
| **Legacy** | — | Absent | Bin name + range (client-built) | Field **43** residual / omitted |
| **New (two-phase)** | Field **44** `EXPLAIN` | From explain | `bin_name_len=0` + range tail from explain | Field **44** same AEL, `EXPLAIN` cleared |
| **Invalid (new)** | — | Present | Bin name + range (verbatim explain) | — → server `PARAMETER` |

---

## Client routing summary

| Condition | Path |
|-----------|------|
| String AEL + server 8.1.3+ | Explain → execute on field **44** |
| `forBin` hint | Legacy: client SI + field **43** packed predexp |
| Gate off / server &lt; 8.1.3 | Legacy: client SI + field **43** |
| `Exp` / `BooleanExpression` WHERE | Legacy: field **43** packed predexp; no explain |
| Background query / UDF | Field **44** not supported |

---

## Server validation notes

- Execute does **not** verify that execute field **44** matches an earlier explain; each message is parsed independently. Clients should still send the same AEL text on both phases.
- Field **44** with msgpack `[128,"…"]` or packed predexp → `bad AEL filter` / `PARAMETER`.
- `REQUIRE_INDEX` without `EXPLAIN` on the same WHERE payload: server logs a warning; parse succeeds; flag is ignored on execute.
- Agg / background query with field **44**: `UNSUPPORTED_FEATURE`.

### Common mistakes (old client → new server)

| Mistake | Server result |
|---------|---------------|
| Explain uses `INFO4` bit 7 + field **43**, no partitions | Broken basic query — missing partitions / `UNSUPPORTED_FEATURE` |
| Field **44** with msgpack `[128,"…"]` or packed predexp | `bad AEL filter` → `PARAMETER` |
| Execute replays explain bytes (`flags=0x01`) | Explain runs again — no records returned |
| Both field **44** and field **43** on same message | `cannot specify both WHERE and PREDEXP` → `PARAMETER` |

---

## Related docs

- [todo-client-two-phase-server-index-selection.md](./todo-client-two-phase-server-index-selection.md) — Java implementation checklist
- [plan-two-phase-server-index-selection.md](./plan-two-phase-server-index-selection.md) — fluent client phase plan
- [query-selection-and-ael-roadmap-overview.md](./query-selection-and-ael-roadmap-overview.md) — product context
