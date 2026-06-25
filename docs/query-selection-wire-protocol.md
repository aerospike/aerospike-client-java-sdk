# Query selection wire protocol (two-phase)

Normative wire contract for **new clients** using server-led index selection. Applies to any client (Java, C, Go, Python, …) talking to a server with `AS_MSG_INFO4_QUERY_SELECTION` (`INFO4` bit 7).

**Server reference:** `aerospike-server` — `query_plan.c` (probe), `query.c` `get_range_field` (execute).

**Java reference:** `IndexRangeWire.forExecuteWithIndexName`, `CommandBuffer.setIndexProbe`, `QueryCommand.forPlan`.

---

## Overview

| Phase | `INFO4` bit 7 | Partitions | Purpose |
|-------|---------------|------------|---------|
| **Probe** | **Set** | **No** | Server returns SI vs PI plan (or `FILTERED_OUT`) |
| **Execute** | **Clear** | **Yes** | Normal partitioned query using plan pins |

Field types (AS_MSG): `21` = `INDEX_NAME`, `22` = `INDEX_RANGE`, `43` = `PREDEXP` / `FILTER_EXP`.

---

## Field `43` (predicate)

Same field type on probe and execute. Payload encoding is capability-driven (see [todo doc](./todo-client-two-phase-server-index-selection.md#field-43-encoding--interim-vs-merged-server)):

| When | Payload |
|------|---------|
| **Interim (now)** | Client-compiled packed expression bytes |
| **Post-merge (TBD)** | Server AEL MessagePack `[128, "<ael text>"]` |

| Phase | Field `43` |
|-------|------------|
| **Probe request** | Full WHERE — required |
| **Probe response** | — (not returned) |
| **Execute** | **Same bytes as probe request** (store on plan, replay verbatim) |

**New vs legacy on SI execute:** new clients send the **full** predicate in `43`. Legacy clients send a **residual** row filter only (often omitted when the whole predicate is covered by the SI slice).

---

## Field `22` inner body (`INDEX_RANGE` value)

Both probe response and execute send field `22` as a message field whose **value** is a binary body. Layout:

```
[n_ranges: u8][bin_name_len: u8][bin_name: bin_name_len bytes][ktype: u8][range_payload...]
```

- `n_ranges` must be `1` today.
- `ktype` is particle type (`INTEGER`, `STRING`, …).
- `range_payload` is type-specific (e.g. for integer range: two `(u32 len)(u64 value)` pairs for start/end).

### Probe response vs execute request (SI path)

| | Field `21` | Field `22` — `bin_name_len` | Field `22` — rest |
|--|------------|------------------------------|-------------------|
| **Probe response** | Index registry name | **`> 0`** (driving bin, e.g. `age`) | `ktype` + `range_payload` |
| **Execute request** | Same index name from probe | **`0`** | Same `ktype` + `range_payload` as probe |

**Do not** copy probe field `22` verbatim onto execute when field `21` is present. The server execute parser (`get_range_field`) rejects **both** an index name and a bin name in the range body:

```
specified expression or index name with bin name
```

### Execute transform (all clients)

Given probe `22` body bytes `P`:

1. Require `P[0] == 1` (`n_ranges`).
2. Let `binLen = P[1]`.
3. If `binLen == 0`, use `P` unchanged on execute.
4. Else build execute body: `[1][0]` + `P[2 + binLen .. end]` (drop bin name; keep ktype + range).

Store the **probe** bytes on the plan object; apply this transform only when encoding execute.

**Example (integer equality `age == 25`):**

| | Size | Shape |
|--|------|-------|
| Probe `22` | 30 bytes | `[1][3]['a','g','e'][INTEGER][range…]` |
| Execute `22` | 27 bytes | `[1][0][INTEGER][range…]` |

### Legacy execute (unchanged)

Legacy clients **do not** use two-phase probe. On SI execute:

| Field `21` | Field `22` |
|------------|------------|
| Absent (typical) | `bin_name_len > 0` — client-built range |

Legacy path: bin in `22`, no `21`. New path: `21` + `22` with `bin_name_len == 0`.

---

## Phase 1 — probe

### Request

| Field / bit | Required | Notes |
|-------------|----------|-------|
| `INFO4` bit 7 (`QUERY_SELECTION`) | Yes | |
| `0` namespace | Yes | |
| `1` set | If query has set | |
| `43` predicate | Yes | Full WHERE |
| `21` index name | No | Hint only (`forIndex`-style); not `forBin` |
| `22` | **No** | Server selects from `43` |
| Partition fields | **No** | |

### Response

| Outcome | `result_code` | Fields |
|---------|---------------|--------|
| PI scan | `AS_OK` (0) | none |
| SI query | `AS_OK` (0) | `21` + `22` (probe shape: **with** bin name) |
| Filtered out | `AS_ERR_FILTERED_OUT` | none |

Infer **PI vs SI** from presence of `21`/`22` (no separate selection-type field in v1).

---

## Phase 2 — execute

### Request

| `INFO4` bit 7 | Partition fields | Field `43` | SI: field `21` | SI: field `22` |
|---------------|------------------|------------|----------------|----------------|
| **Clear** | Yes (normal query) | Same as probe | From probe | **Execute shape** (strip bin; see above) |

### PI plan

Send field `43` only. No `21` / `22`.

### `FILTERED_OUT` plan

Do not execute; surface `FILTERED_OUT` to the application.

---

## Quick reference — three SI wire shapes

| Client | Probe | Execute `21` | Execute `22` inner | Execute `43` |
|--------|-------|--------------|-------------------|--------------|
| **Legacy** | — | Absent | Bin name + range (client-built) | Residual / omitted |
| **New (two-phase)** | `43` only | From probe | `bin_name_len=0` + range tail from probe | Full (same as probe) |
| **Invalid (new)** | — | Present | Bin name + range (verbatim probe) | — → server `PARAMETER_ERROR` |

---

## Server validation notes

- Execute does **not** verify that execute `43` matches an earlier probe; each message is independent.
- Product may later change probe to emit execute-shaped `22` directly; clients should still apply the transform when `bin_name_len > 0` and `21` is sent (no-op when already zero).

---

## Related docs

- [todo-client-two-phase-server-index-selection.md](./todo-client-two-phase-server-index-selection.md) — Java implementation checklist
- [plan-two-phase-server-index-selection.md](./plan-two-phase-server-index-selection.md) — fluent client phase plan
