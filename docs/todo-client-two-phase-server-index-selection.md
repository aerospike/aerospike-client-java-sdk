# Client TODO: two-phase server index selection

Implementation checklist for the fluent Java client.

**Progress:** M1–M11 complete (including M10 wire-protocol doc). **Next:** user docs / examples (E1–E7).

**North star:** No client-side AEL parsing and no client-side index selection on queries. String AEL → field **44** only; non-AEL / explicit index material → legacy field **43** + Filter fields.

**Sources of truth**

- **Wire contract (normative, all clients):** [query-selection-wire-protocol.md](./query-selection-wire-protocol.md).
- **Server implementation (authoritative):** `aerospike-server` branch `suresh/dsl-queryOptimization-integration` — `query_where.h` / `query_where.c` (field `44` parse), `query_plan.c` (explain), `query.c` `get_query_filter_exp` + `get_range_field` (execute).
- **Product notes:** Slack (May–Jun 2026) — field `44` WHERE blob; **no** `INFO4` bit 7; **no** separate field `47` query flags. Early Slack drafts mentioned varint flags + msgpack `[128,"…"]`; **implemented server uses 1-byte flags + raw AEL UTF-8** (see below).
- **Prior client branch:** `CLIENT-4800-index-selection-server-side` — reference for routing, `IndexRangeWire`, integration test scenarios (re-encode wire only).

---

## Protocol migration (INFO4 + field `43` → field `44` WHERE)

### What changed (Jun 2026)

| Topic | Old (CLIENT-4800) | New (server-implemented) |
|-------|-------------------|--------------------------|
| Phase 1 trigger | `INFO4` bit 7 (`QUERY_SELECTION`) on message header | **EXPLAIN** flag in field **44** WHERE payload — **no INFO4 bit** |
| Predicate on new path | Field **43** — client-compiled packed predexp | Field **44** — `[flags: u8][AEL source UTF-8]` |
| Phase 1 name | Probe | **Explain** (plan only; no records) |
| Phase 1 request | `0/1/7/9/43` + optional `21`; no partitions | `0/1/7/9/44` + optional `21`; no partitions |
| Phase 1 response (SI) | `21` + `22` | `21` + `22` + **`INDEX_TYPE` (26)** |
| Phase 2 execute | Field **43** replay + `21` + transformed `22` | Field **44** (same AEL text, **EXPLAIN cleared**; optional **REQUIRE_INDEX**) + `21` + `22` |
| Query policy flags | N/A | **Inside WHERE flags byte** — not a separate field `47` |

### Field `44` WHERE payload

**Server definition** (`query_where.h`):

```
[flags: u8][AEL source string...]
```

**Flags** (single byte; unknown bits → server `PARAMETER`):

| Flag | Value | Phase 1 (explain) | Phase 2 (execute) |
|------|-------|-------------------|-------------------|
| `EXPLAIN` | `1 << 0` | **Set** (`0x01`) | **Clear** (required — leaving EXPLAIN set re-runs explain, never executes) |
| `REQUIRE_INDEX` | `1 << 1` | Optional (product TBD) | Ignored on execute today |
| `HARD_HINT` | `1 << 2` | Reserved — **not used by server** | Do not send |

**AEL body (v1):** raw UTF-8 AEL source text (e.g. `"$.age > 30 and $.country == 'US'"`). Server parses via `as_exp_filter_build_ael()` → `ael_parse()`.

**Not** MessagePack `[128, "<ael>"]` and **not** `Expression.fromServerCompiledFilter()` on the **query** path. Field **44** carries plain AEL text after the flags byte.

**Drop for queries:** field **43** + `[128, ael]` (`EXP_AEL_COMPILE` / op 128) is **not** supported on the merged server query optimizer branch (`build_ael_compile` is stubbed). Client must not use it for query WHERE — use field **44** instead. Op 128 on field **43** may remain temporarily for **read/write** string-AEL filter ops until server wires it there (field **44** is query-only on the server).

**Mutual exclusion:** server rejects requests that send both field **44** (WHERE) and field **43** (PREDEXP).

Client should restrict to one AEL expression per WHERE until server allows more.

### Two-phase flow (new)

**Phase 1 — Explain**

| Field | Content |
|-------|---------|
| `0` namespace | required |
| `1` set | optional |
| `44` WHERE | `[0x01][<AEL UTF-8>]` |
| `21` INDEX_NAME | optional soft hint (`forIndex`) |

Server: parse WHERE, run planner → `result_code` + on SI: `21`, `22`, `26` (INDEX_TYPE). No field `43` in response.

**Phase 2 — Execute**

| Field | SI path | PI path |
|-------|---------|---------|
| `44` WHERE | Same AEL text; flags `0x00` (EXPLAIN cleared) | Same |
| `21` | From explain | Absent |
| `22` | **Execute shape** from explain (`bin_name_len=0`; see wire-protocol doc) | Absent |
| Partitions, … | Normal `setQuery` | Normal `setQuery` |

**Unchanged from prior work:** field `22` execute transform (`IndexRangeWire.forExecuteWithIndexName`); `21` XOR bin in `22` on execute; legacy `forBin` path still uses field **43** + client index selection.

### Phase 1 response → Phase 2 request (alignment)

Explain response carries **server pins only**. Field **44** (AEL) is **not** echoed — client retains the AEL string from the explain **request** and rebuilds WHERE for execute (`flags=0x00` + same UTF-8 text).

| Phase 2 execute field | SI path | PI path | Source |
|-----------------------|---------|---------|--------|
| `0` namespace | required | required | Explain request / `QueryPlan` context |
| `1` set | optional | optional | Explain request / `QueryPlan` context |
| `44` WHERE | `[0x00][same AEL UTF-8]` | same | **Explain request** (client-stored `aelString`; rebuild — do not replay `0x01` bytes) |
| `21` INDEX_NAME | from explain | absent | **Explain response** field `21` |
| `26` INDEX_TYPE | from explain when non-`DEFAULT` | absent | **Explain response** field `26` *(optional on execute today — server defaults if absent; required for LIST/MAPCDT later)* |
| `22` INDEX_RANGE | execute shape (`bin_name_len=0`) | absent | **Explain response** field `22` after `IndexRangeWire.forExecuteWithIndexName` |
| Partitions, socket timeout, query id, … | normal `setQuery` | same | Client query builder (not from explain) |

**SI explain response fields (server sends):** `21` + `26` + `22` (probe shape — `22` includes bin name).  
**PI explain response:** no fields; `result_code=OK`. Execute needs only field `44` + partitions.  
**`FILTERED_OUT`:** no execute — throw before phase 2.

**Client `QueryPlan` must hold:** `aelString` (or rebuildable WHERE bytes), `indexName`, `indexRangeBytes` (probe shape), `indexType` (from field `26`), plus namespace/set/selection. Migration **M4–M6** cover storing and encoding these on execute.

### Migration checklist (client)

- [x] **M1** Add `FieldType.WHERE = 44`; WHERE flag constants; `QueryWhereWire` encoder (`[flags: u8][AEL UTF-8]`) + unit tests.
- [x] **M2** Replace `CommandBuffer.setIndexProbe` → explain encoder: field **44** + EXPLAIN flag; **remove** `INFO4_QUERY_SELECTION` from explain path.
- [x] **M3** `WhereClauseProcessor` / planner: materialize **AEL string** for field `44` via `toExplainAel()` *(remove deprecated `toProbeExpression` when safe)*.
- [x] **M4** `QueryPlan`: store `explainWhereBytes` + `indexType`; `fromExplainResponse`; `getExecuteWhereBytes()` rebuilds with EXPLAIN cleared.
- [x] **M5** `MsgFieldParser` / plan decode: parse **INDEX_TYPE** on SI explain response (`getIndexCollectionType()`).
- [x] **M6** `QueryCommand.forPlan` + `setQuery`: plan-driven execute uses field **44**, not **43**; forward **21** + transformed **22**; send **26** when `indexType` non-`DEFAULT`.
- [x] **M7** Capability gate: `supportsQuerySelection()` uses `versionGE813` (cluster minimum {@link Version#SERVER_VERSION_8_1_3}).
- [x] **M8** Unit wire tests (`IndexProbeCommandTest`, `QueryPlanExecuteWireTest`, `QueryWhereWireTest`).
- [x] **M9** Re-run / update `QuerySelectionIntegrationTest` — 26 scenarios green on field `44` server.
- [x] **M10** Rewrite [query-selection-wire-protocol.md](./query-selection-wire-protocol.md).
- [x] **M11** Remove query use of field **43** `[128, ael]` — see [End-state routing](#end-state-routing-no-client-parse) and [Drop path A (queries)](#drop-path-a-queries).

---

## End-state routing (no client parse)

When client-side AEL parsing and index selection are fully removed:

| Case | Wire | Notes |
|------|------|-------|
| **SI + string AEL** | Field **44** explain → execute | Server picks index; client replays `21`/`22`/`26` from explain |
| **SI + non-AEL** (`Exp`, `BooleanExpression`, explicit `Filter`) | Field **43** packed predexp + **21**/**22**/**26** | Legacy: app supplies index material; **no** explain / server index selection |
| **PI + string AEL** | Field **44** (execute; explain optional) | Explain → `OK`, no index fields; execute → field **44** + partitions. Server does **not** require prior explain. Fails on bad AEL (`PARAMETER`), `FILTERED_OUT`, or `REQUIRE_INDEX` + PI plan (`SINDEX_NOT_FOUND`) |
| **PI + no predicate** | Partitions only | Full partition scan; no field **43**/**44** |

Same field **44** shape for SI and PI — server chooses plan on explain (or inline later). Client does not parse AEL to decide SI vs PI.

### `allowsIndex` (client-only flag)

On `WhereClauseProcessor` — **not sent on the wire**. Answers: “should this WHERE participate in secondary-index planning?”

| `allowsIndex` | Meaning today |
|---------------|---------------|
| `true` | String/prepared AEL may produce a `Filter` + `Exp` (client parse) **or** use server field **44** two-phase path when `useServerQuerySelection()` |
| `false` | Row-level predicate only — no client SI catalog lookup, no `useServerQuerySelection()` |

**Dataset queries** (`IndexQueryBuilderImpl`): string/prepared `where()` always sets `allowsIndex=true`. `where(Exp)` / `where(BooleanExpression)` always `false` (packed predexp, no `Filter` from client parse).

**End state:** `allowsIndex=false` + string AEL on a query uses field **44** (PI row filter) via `hasStringAel()` routing (M11).

---

## Field `43` vs field `44` (legacy vs new)

| Path | Predicate field | Payload |
|------|-----------------|--------|
| **Legacy** (`forBin`, gate off, `where(Exp)`, non-AEL + explicit Filter) | **43** `FILTER_EXP` | Client-compiled **packed predexp** (not `[128, ael]`); SI execute uses **residual** row filter |
| **New** (string AEL, explain → execute) | **44** `WHERE` | `[flags: u8][AEL UTF-8]` on both phases (EXPLAIN set on explain, cleared on execute); **not** field `43` |
| **Path A (drop for queries)** | **43** `[128, ael]` | `Expression.fromServerCompiledFilter()` — **broken** on merged server for queries; remove from `AelMaterializer.parseWhereFromString` |

Do **not** send field `43` on the new two-phase path. Do **not** send field `44` on legacy non-AEL paths or background query / agg (server `UNSUPPORTED_FEATURE`).

### Drop path A (queries)

- [x] Remove `serverCompiledFilterResult()` and `!allowsIndex && supportsAel()` branch in `AelMaterializer.parseWhereFromString`.
- [x] Route string-AEL **query** WHERE through field **44** via `hasStringAel()` in `useServerQuerySelection()` (including `allowsIndex=false` / PI).
- [x] Keep `Expression.fromServerCompiledFilter()` / `expressionFromString()` for **read/write** `ExpOperation` until server implements op 128 on field **43** for those ops (or product defines another wire).
- [ ] Remove or repurpose `supportsAel()` gate for query routing once read/write op 128 is wired (`supportsQuerySelection()` gates queries today).

---

## Agreed behavior (Slack + Jun 2026 wire update)

### Legacy clients (unchanged)

- Client chooses the secondary index locally.
- SI execute sends client-built field `22` (+ field `21` when needed) and row filter in field **43**.
- No explain phase.

### New clients (two-phase)

| Phase | Purpose | Wire |
|-------|---------|------|
| **Explain** | Server selects SI vs PI | Field **44** with **EXPLAIN** flag; **no partitions**; **no field `22`** |
| **Execute** | Normal partitioned query | Field **44** without EXPLAIN; server pins `21`/`22` on SI |

**Soft hints:** `QueryHint.forIndex` → field `21` on explain only.  
**Legacy override:** `QueryHint.forBin` → client SI path (field **43**), no explain.

---

## Server status

Verified on `aerospike-server` branch `suresh/dsl-queryOptimization-integration` (commit `51a69cbf9` SERVER-485 and follow-ons).

| Item | Prior (INFO4) | New protocol (implemented) |
|------|---------------|----------------------------|
| Phase 1 entry | `INFO4` bit 7 → `as_query_plan` | `basic_query_job_start`: field **44** + `EXPLAIN` → `as_query_plan` |
| Phase 1 parses | `43` predexp via `as_exp_filter_build` | `44` via `as_query_where_parse` → `as_exp_filter_build_ael` (raw AEL text) |
| SI explain response | `21` + `22` | `21` + `26` (INDEX_TYPE) + `22` |
| PI explain response | empty fields, `OK` | Same |
| `FILTERED_OUT` | `AS_ERR_FILTERED_OUT` | Same |
| `REQUIRE_INDEX` + PI plan | — | `AS_ERR_SINDEX_NOT_FOUND` on explain |
| Execute filter | field `43` predexp | field **44** AEL via `get_query_filter_exp` |
| Execute range | `get_range_field`: `21` XOR bin in `22` | Same |
| Agg / bg queries | — | Field **44** rejected (`UNSUPPORTED_FEATURE`) |

Server does **not** verify that execute WHERE matches explain WHERE. Client must send the same AEL text in field `44` on execute (with EXPLAIN cleared).

### Migration breakage (old client → new server)

| Mistake | Server result |
|---------|---------------|
| Explain still uses INFO4 + field `43`, no partitions | Broken basic query — missing partitions / `UNSUPPORTED_FEATURE` |
| Field `44` with msgpack `[128,"…"]` or packed predexp | `bad AEL filter` → `PARAMETER` |
| Execute replays explain bytes (`flags=0x01`) | Explain runs again — no records returned |
| Both field `44` and field `43` on same message | `cannot specify both WHERE and PREDEXP` → `PARAMETER` |

---

## Client gaps

### Done on CLIENT-4800 (reuse after migration)

- [x] `IndexRangeWire` + `Filter.fromWireRange` — execute field `22` transform.
- [x] `IndexProbePlanner` routing (`forBin` → legacy; string AEL + gate → two-phase).
- [x] `QueryCommand.forPlan` orchestration skeleton.
- [x] Integration test **scenarios** Tiers 1–4 + `compoundPredicateServerLedMatchesLegacyForBin` *(re-validate on field `44`)*.

### Open

- [ ] **Documentation & examples** — E1–E7 (wire-protocol doc: M10 done).

---

## Out of scope (v1)

- `binName` hint on explain for new clients.
- `INDEX_CONTEXT` / `INDEX_EXPRESSION` in explain response (CDT / map / geo follow-on).
- Background query / UDF with server selection.
- Pagination / re-explain per chunk.
- Separate field `47` QUERY_FLAGS (rejected — flags live in field `44`).
- Multi-expression WHERE in field `44` (v1: single AEL expression only).

---

## Phase 0 — types and constants

### Done (CLIENT-4800) — update for migration

- [x] `QueryPlan` + `QuerySelection` + `MsgFieldParser` *(extended for `indexType`, field `44`)*.
- [ ] ~~`Command.INFO4_QUERY_SELECTION`~~ — **remove from explain path** (may delete constant).
- [x] `FieldType.WHERE = 44` + WHERE flag constants (`QueryWhereWire.FLAG_*`).
- [x] `QueryWhereWire` — `[flags: u8][AEL UTF-8]` encoder (not varint; not msgpack).

---

## Phase 1 — explain (was: probe)

### Superseded — replace encoder

- [x] ~~`IndexProbeCommand` / `IndexProbeExecutor`~~ — **rework** for field `44`; rename to explain-oriented names optional.
- [x] ~~`setIndexProbe` + INFO4 bit 7 + field `43`~~ — **replaced** with `setQueryExplain` + field `44` + EXPLAIN flag.
- [x] ~~`toProbeExpression` / packed predexp for probe~~ — **replaced** with `toExplainAel()` raw AEL string in field `44`.
- [x] Explain response decode includes **INDEX_TYPE** (`MsgFieldParser.getIndexCollectionType()`).
- [x] Unit tests: explain wire layout (field `44`, `flags=0x01` + AEL bytes, no INFO4).

### API (mostly unchanged)

- [x] Package-private planner (`IndexProbePlanner`); transparent `execute()` only.
- [x] `Cluster.supportsQuerySelection()` — `versionGE813`.
- [ ] Optional public `explain()` *(not implemented; internal `plan()` for tests)*.

---

## Phase 2 — execute with plan

### Done (CLIENT-4800) — update execute predicate field

- [x] `QueryCommand.forPlan` + `IndexQueryBuilderImpl` guess-path `execute()`.
- [x] SI: `21` + transformed `22` via `IndexRangeWire`.
- [x] Execute: field **44** (same AEL, non-EXPLAIN flags) — **not** field `43` replay.
- [x] PI: scan-style execute with WHERE field only.
- [x] `FILTERED_OUT` → throw before execute.

---

## When to use which path

### Today (interim)

| Condition | Path |
|-----------|------|
| `!supportsQuerySelection()` | Legacy: client index selection + field **43** packed predexp |
| `QueryHint.forBin` | Legacy only |
| String-AEL `where`, gate on, `allowsIndex()` | Explain → execute (field **44**) |
| `QueryHint.forIndex` | Field `21` on explain only |
| `where(Exp)` / `where(BooleanExpression)` | No explain; field **43** packed predexp; `allowsIndex=false` |
| String-AEL, `allowsIndex=false`, `supportsAel()` | ~~**Path A** field **43** `[128, ael]`~~ — **removed (M11)**; uses field **44** when gate on |

### Target (no client parse on queries)

| Condition | Path |
|-----------|------|
| String AEL (any `allowsIndex`) | Field **44** — explain when SI selection wanted; PI may skip explain |
| Non-AEL + explicit SI | Field **43** + **21**/**22**/**26** — no explain |
| Non-AEL / no WHERE, PI scan | Partitions only, or field **43** row filter |
| `forBin`, gate off | Legacy client SI + field **43** until removed |

---

## Orchestration and UX

- [x] `execute()` runs explain + execute transparently when `useServerQuerySelection()`.
- [x] No public plan API for applications.
- [x] Explain sync; execute async via `QueryExecutor`.

---

## Tests

### Unit

- [x] `IndexRangeWireTest`, `QueryPlanTest`, `MsgFieldParserTest` *(still valid)*.
- [x] Explain wire layout — field **44**, `flags=0x01` + raw AEL UTF-8 (`IndexProbeCommandTest`).
- [x] Execute wire — field **44** on plan path, EXPLAIN cleared (`QueryPlanExecuteWireTest`).
- [ ] WHERE flags — unknown flag bits rejected by server; client must only send known bits.

### Integration (`QuerySelectionIntegrationTest` on CLIENT-4800)

**Status:** 26 scenarios green on field `44` server (M9).

**Prerequisites:** server with field `44` explain; client gate on; fixture with `age` (+ `score` for multi-index tests).

#### Tier 1 — Explain only (`plan()`)

| # | Scenario | Assert |
|---|----------|--------|
| 1.1–1.6 | SI / PI / hint / stable / smoke / FILTERED_OUT | Same behavioral asserts; plan stores **WHERE** bytes + optional **indexType** |

- [x] 1.1–1.6 — re-validated on field `44`

#### Tier 2 — Explain → execute E2E

| # | Notes |
|---|--------|
| 2.1–2.7 | Core SI/PI/compound/reading bins |
| 2.5 | **Update:** execute field **44** has same AEL text with `flags=0x00` (EXPLAIN cleared) — not field `43`; cannot replay explain bytes verbatim |

- [x] 2.1–2.7 — re-validated on field `44`

#### Tier 3 — Hints & routing

- [x] 3.1–3.5 — re-validated
- [x] 3.6 gate-off (version &lt; 8.1.3 → legacy path)

#### Tier 4 — Errors & edge cases

- [x] 4.1–4.6 — re-validated

#### Legacy vs new equivalence

- [x] `compoundPredicateServerLedMatchesLegacyForBin` — re-validated on field `44`

#### Regression smoke (optional)

- [ ] `QueryIntegerTest.queryInteger`
- [ ] `QueryHintBuilderTest.queryWithBinHintExecutes`

---

## Documentation & examples

*(Unchanged scope — update wording: explain not probe; field `44` not `43` on new path.)*

- [ ] E1–E7 examples (`ServerQuerySelectionExample`, etc.)
- [ ] `README.md`, `key-features.md`, `api-builder-reference.md`
- [ ] `query-selection-user-guide.md` *(new)*
- [x] Rewrite `query-selection-wire-protocol.md` (M10)
- [ ] Cross-links in `ael-documentation.md`, `query-selection-and-ael-roadmap-overview.md`

---

## Implementation order

1. ~~CLIENT-4800 baseline~~ (INFO4 + field `43`) — on branch; **do not ship as-is**.
2. ~~**M1–M8**~~ — field `44`, explain/execute encoders, plan model, unit wire tests.
3. **M9** — integration re-validation on field `44` server.
4. ~~**M11**~~ — drop query path field **43** `[128, ael]`; unify string-AEL queries on field **44**.
5. ~~**M9**~~ — integration re-validation on field `44` server.
6. ~~**M10**~~ — wire-protocol doc.
7. User docs / examples (E1–E7).

---

## Files likely touched (migration)

| Area | Files |
|------|--------|
| Constants | `FieldType.java`, `Command.java` (drop INFO4 from explain path) |
| WHERE wire | New `QueryWhereWire` — `[flags: u8][AEL UTF-8]` (not `Expression.fromServerCompiledFilter`) |
| Encoder | `CommandBuffer.java` — `setQueryExplain`, `setQuery` plan branch |
| Commands | `IndexProbeCommand.java`, `IndexProbeExecutor.java` (explain semantics) |
| Plan | `QueryPlan.java`, `MsgFieldParser.java` |
| Materialize | `WhereClauseProcessor.java`, `AelMaterializer.java` (M11: remove `serverCompiledFilterResult` for queries) |
| Drop path A | `Expression.java` — keep for read/write; remove query use in `AelMaterializer.java` |
| Execute | `QueryCommand.java` |
| Routing | `IndexProbePlanner.java`, `IndexQueryBuilderImpl.java` |
| Cluster | `Cluster.java` — capability gate |
| Tests | `IndexProbeCommandTest`, `QueryPlanExecuteWireTest`, `QuerySelectionIntegrationTest` |
| Docs | `query-selection-wire-protocol.md`, this file |

**Unchanged:** `IndexRangeWire.java`, legacy `VisitorUtils.getFilterExp` / `forBin` path.

---

## Appendix — Product / behavior questions

### 1. Retry behavior on explain

**Current client behavior (concise):** Explain uses `SyncExecutor` with READ+QUERY `Behavior` (default 6 attempts, round-robin nodes on retry). Transport failures retry; `OK` and `FILTERED_OUT` succeed. Not re-run on execute failure.

**Questions:** Fewer retries for explain? Node stickiness vs round-robin?

### 2. `forIndex` hint when index is wrong or missing

**Observed:** Server ignores bad hints; auto-selects fitting index (Tiers 4.4, 4.6).

**Questions:** Fail vs ignore? Soft suggestion vs hard pin? Relation to **HARD_HINT** flag?

### 3. `FILTERED_OUT` vs empty result

**Observed:** Contradiction → `FILTERED_OUT`; valid SI with no rows → 0 records.

**Questions:** Same split for PI and compound WHERE?

### 4. Plan freshness between explain and execute

**Questions:** Re-explain on cluster/index change? Pagination: re-explain per chunk?

### 5. New path field `44` vs legacy field `43`

**Test:** `compoundPredicateServerLedMatchesLegacyForBin` — same rows; different wire (field **44** full AEL text vs field **43** residual packed predexp).

**Resolved (queries):** Drop field **43** `[128, ael]` for queries — field **44** raw AEL only. Field **43** remains for packed predexp (non-AEL) and read/write op 128 until server wires the latter.

**Questions:** Require equivalent row sets for all queries? Migration docs for `forBin` users? Re-validate after AEL-on-server vs client-packed selection change?

### 6. Multi-index / ambiguous WHERE

**Questions:** Tie-break rules; per-node cardinality for auto-select?

### 7. Which node answers explain?

**Questions:** Plan consistency across nodes; prefer master vs any node?

### 8. REQUIRE_INDEX flag (new)

**Server behavior:** On explain, `REQUIRE_INDEX` + PI plan → `SINDEX_NOT_FOUND`. Server logs a warning if `REQUIRE_INDEX` is set without `EXPLAIN` but does not fail parse. Ignored on execute path today.

**Questions:** Public API for `REQUIRE_INDEX` (`0x02` in WHERE flags byte)? Fail explain or execute when server would pick PI/scan?

### 9. INDEX_TYPE on explain response

**Questions:** Required on client for execute encoding, or opaque passthrough for future CDT/geo?
