# Client TODO: two-phase server index selection

Implementation checklist for the fluent Java client.

**Progress:** Prior work on `CLIENT-4800-index-selection-server-side` (INFO4 bit 7 + field `43` probe) is **superseded**. **Next:** migrate to field **44** WHERE + EXPLAIN flag protocol (see **Protocol migration** below), re-validate integration tests, then docs/examples and real capability gate.

**Sources of truth**

- **Wire contract (normative, all clients):** [query-selection-wire-protocol.md](./query-selection-wire-protocol.md) — **needs rewrite** for field `44` protocol.
- **Product / wire contract:** Slack (May–Jun 2026) — Suresh, Kevin Porter: field `44` WHERE blob with flags varint; **no** `INFO4` bit 7; **no** separate field `47` query flags.
- **Server implementation:** `aerospike-server` — explain/planner path, `query.c` `get_range_field` (execute).
- **Prior client branch:** `CLIENT-4800-index-selection-server-side` — reference for routing, `IndexRangeWire`, integration test scenarios (re-encode wire only).

---

## Protocol migration (INFO4 + field `43` → field `44` WHERE)

### What changed (Jun 2026)

| Topic | Old (CLIENT-4800) | New (current spec) |
|-------|-------------------|---------------------|
| Phase 1 trigger | `INFO4` bit 7 (`QUERY_SELECTION`) on message header | **EXPLAIN** flag in field **44** WHERE payload — **no INFO4 bit** |
| Predicate on new path | Field **43** — client-compiled packed predexp | Field **44** — `[flags varint][msgpack [128, "<AEL>"]]` (server AEL) |
| Phase 1 name | Probe | **Explain** (plan only; no records) |
| Phase 1 request | `0/1/7/9/43` + optional `21`; no partitions | `0/1/44` + optional `21`; no partitions |
| Phase 1 response (SI) | `21` + `22` | `21` + `22` + **`INDEX_TYPE` (26)** |
| Phase 2 execute | Field **43** replay + `21` + transformed `22` | Field **44** (same AEL, flags **without** EXPLAIN; optional **REQUIRE_INDEX**) + `21` + `22` |
| Query policy flags | N/A | **Inside WHERE varint** — not a separate field `47` |

### Field `44` WHERE payload

```
[flags: varint][filter: msgpack]
```

**Flags** (low bits of varint; bit 7 = continuation for multi-byte varint, unused for v1):

| Flag | Value | Phase 1 (explain) | Phase 2 (execute) |
|------|-------|-------------------|-------------------|
| `EXPLAIN` | `1 << 0` | **Set** (`0x01`) | **Clear** |
| `REQUIRE_INDEX` | `1 << 1` | Optional (product TBD) | Optional per policy (`0x02`) |
| `HARD_HINT` | `1 << 2` | Internal retry only | Internal retry only |

**Filter body (v1):** single AEL expression as MessagePack `[128, "<ael text>"]` (`Expression.fromServerCompiledFilter`). Client should restrict to one expression per WHERE until server allows more.

**Varint rules:** pack tightly (reject illegal padding e.g. `0x0081` → should be `0x01`); extendable via continuation bit if more than 7 flags needed later.

### Two-phase flow (new)

**Phase 1 — Explain**

| Field | Content |
|-------|---------|
| `0` namespace | required |
| `1` set | optional |
| `44` WHERE | `[EXPLAIN flag][msgpack [128, "<AEL>"]]` |
| `21` INDEX_NAME | optional soft hint (`forIndex`) |

Server: parse WHERE, run planner → `result_code` + on SI: `21`, `22`, `26` (INDEX_TYPE). No field `43` in response.

**Phase 2 — Execute**

| Field | SI path | PI path |
|-------|---------|---------|
| `44` WHERE | Same AEL; flags `0x00` or `REQUIRE_INDEX` | Same |
| `21` | From explain | Absent |
| `22` | **Execute shape** from explain (`bin_name_len=0`; see wire-protocol doc) | Absent |
| Partitions, … | Normal `setQuery` | Normal `setQuery` |

**Unchanged from prior work:** field `22` execute transform (`IndexRangeWire.forExecuteWithIndexName`); `21` XOR bin in `22` on execute; legacy `forBin` path still uses field **43** + client index selection.

### Migration checklist (client)

- [ ] **M1** Add `FieldType.WHERE = 44`; WHERE flag constants; varint pack/unpack util + unit tests.
- [ ] **M2** Replace `CommandBuffer.setIndexProbe` → explain encoder: field **44** + EXPLAIN flag; **remove** `INFO4_QUERY_SELECTION` from explain path.
- [ ] **M3** `WhereClauseProcessor` / planner: materialize **AEL string** for field `44` (not packed predexp in `43`).
- [ ] **M4** `QueryPlan`: store `wherePayloadBytes` (or AEL + flags); add `indexType` from explain response; rename `fromProbeResponse` → `fromExplainResponse` (or keep name, update semantics).
- [ ] **M5** `MsgFieldParser` / plan decode: parse **INDEX_TYPE** on SI explain response.
- [ ] **M6** `QueryCommand.forPlan` + `setQuery`: plan-driven execute uses field **44**, not **43** replay.
- [ ] **M7** Capability gate: redefine `supportsQuerySelection()` for **WHERE/explain** feature (not INFO4 / `versionGE813` alone).
- [ ] **M8** Rewrite unit wire tests (`IndexProbeCommandTest`, `QueryPlanExecuteWireTest`).
- [ ] **M9** Re-run / update `QuerySelectionIntegrationTest` (behavioral scenarios kept; wire assertions → field `44`).
- [ ] **M10** Rewrite [query-selection-wire-protocol.md](./query-selection-wire-protocol.md).

---

## Field `43` vs field `44` (legacy vs new)

| Path | Predicate field | Payload |
|------|-----------------|--------|
| **Legacy** (`forBin`, gate off, `where(Exp)`) | **43** `FILTER_EXP` | Client-compiled packed predexp; SI execute uses **residual** row filter |
| **New** (explain → execute) | **44** `WHERE` | `[flags varint][msgpack [128, "<AEL>"]]` on both phases; **not** field `43` |

Do **not** send field `43` on the new two-phase path. Do **not** send field `44` on legacy paths.

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

| Item | Prior (INFO4) | New protocol |
|------|---------------|--------------|
| Phase 1 entry | `INFO4` bit 7 → `as_query_plan` | Explain via field **44** EXPLAIN flag *(confirm server entry point)* |
| Phase 1 parses | `43` predexp | `44` WHERE blob (flags + AEL) |
| SI explain response | `21` + `22` | `21` + `22` + **`26` INDEX_TYPE** |
| Execute | `get_range_field`: `21` XOR bin in `22` | Same |

Server does **not** verify that execute WHERE matches explain WHERE. Client replays same AEL in field `44` on execute.

---

## Client gaps

### Done on CLIENT-4800 (reuse after migration)

- [x] `IndexRangeWire` + `Filter.fromWireRange` — execute field `22` transform.
- [x] `IndexProbePlanner` routing (`forBin` → legacy; string AEL + gate → two-phase).
- [x] `QueryCommand.forPlan` orchestration skeleton.
- [x] Integration test **scenarios** Tiers 1–4 + `compoundPredicateServerLedMatchesLegacyForBin` *(re-validate on field `44`)*.

### Open

- [ ] **Protocol migration M1–M10** (above).
- [ ] **Capability gate** — `supportsQuerySelection()` stubbed `true` on branch; wire tend/feature flag for WHERE/explain.
- [ ] **REQUIRE_INDEX** mapping — WHERE flag `0x02` vs public API *(product TBD)*.
- [ ] **Documentation & examples** — see Documentation section.
- [ ] Broader explicit legacy override beyond `forBin` / `!allowsIndex` *(if product needs it)*.

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

- [x] `QueryPlan` + `QuerySelection` + `MsgFieldParser` *(extend for `indexType`, field `44`)*.
- [ ] ~~`Command.INFO4_QUERY_SELECTION`~~ — **remove from explain path** (may delete constant).
- [ ] `FieldType.WHERE = 44` + WHERE flag constants.
- [ ] Varint flags encoder/decoder.

---

## Phase 1 — explain (was: probe)

### Superseded — replace encoder

- [x] ~~`IndexProbeCommand` / `IndexProbeExecutor`~~ — **rework** for field `44`; rename to explain-oriented names optional.
- [x] ~~`setIndexProbe` + INFO4 bit 7 + field `43`~~ — **replace** with `setQueryExplain` (or equivalent) + field `44` + EXPLAIN flag.
- [x] ~~`toProbeExpression` / packed predexp for probe~~ — **replace** with AEL string → `[128,"…"]` inside field `44`.
- [ ] Explain response decode includes **INDEX_TYPE**.
- [ ] Unit tests: explain wire layout (field `44`, flags varint, no INFO4).

### API (mostly unchanged)

- [x] Package-private planner (`IndexProbePlanner`); transparent `execute()` only.
- [ ] `Cluster.supportsQuerySelection()` — gate on **WHERE/explain** capability.
- [ ] Optional public `explain()` *(not implemented; internal `plan()` for tests)*.

---

## Phase 2 — execute with plan

### Done (CLIENT-4800) — update execute predicate field

- [x] `QueryCommand.forPlan` + `IndexQueryBuilderImpl` guess-path `execute()`.
- [x] SI: `21` + transformed `22` via `IndexRangeWire`.
- [ ] Execute: field **44** (same AEL, non-EXPLAIN flags) — **not** field `43` replay.
- [x] PI: scan-style execute with WHERE field only.
- [x] `FILTERED_OUT` → throw before execute.

---

## When to use which path

| Condition | Path |
|-----------|------|
| `!supportsQuerySelection()` | Legacy: client index selection + field **43** |
| `QueryHint.forBin` | Legacy only |
| String-AEL `where`, gate on | Explain → execute (field **44**) |
| `QueryHint.forIndex` | Field `21` on explain only |
| `where(Exp)` / no WHERE | No explain; existing behavior |

---

## Orchestration and UX

- [x] `execute()` runs explain + execute transparently when `useServerQuerySelection()`.
- [x] No public plan API for applications.
- [x] Explain sync; execute async via `QueryExecutor`.

---

## Tests

### Unit

- [x] `IndexRangeWireTest`, `QueryPlanTest`, `MsgFieldParserTest` *(still valid)*.
- [ ] Explain wire layout — field **44**, EXPLAIN flag, AEL msgpack *(replaces `IndexProbeCommandTest`)*.
- [ ] Execute wire — field **44** on plan path *(replaces `QueryPlanExecuteWireTest` field `43` assertions)*.
- [ ] Varint flags — legal/illegal encodings.

### Integration (`QuerySelectionIntegrationTest` on CLIENT-4800)

**Status:** Scenarios implemented and green on **old** protocol; **re-validate** after M9 migration.

**Prerequisites:** server with field `44` explain; client gate on; fixture with `age` (+ `score` for multi-index tests).

#### Tier 1 — Explain only (`plan()`)

| # | Scenario | Assert |
|---|----------|--------|
| 1.1–1.6 | SI / PI / hint / stable / smoke / FILTERED_OUT | Same behavioral asserts; plan stores **WHERE** bytes + optional **indexType** |

- [x] 1.1–1.6 *(old protocol)* — [ ] re-validate on field `44`

#### Tier 2 — Explain → execute E2E

| # | Notes |
|---|--------|
| 2.1–2.7 | Core SI/PI/compound/reading bins |
| 2.5 | **Update:** execute field **44** matches explain (EXPLAIN flag cleared), not field `43` |

- [x] 2.1–2.7 *(old protocol)* — [ ] re-validate on field `44`

#### Tier 3 — Hints & routing

- [x] 3.1–3.5 *(old protocol)* — [ ] re-validate
- [x] 3.6 gate-off *(skipped until gate wired)*

#### Tier 4 — Errors & edge cases

- [x] 4.1–4.6 *(old protocol)* — [ ] re-validate

#### Legacy vs new equivalence

- [x] `compoundPredicateServerLedMatchesLegacyForBin` — same row set; wire: field **44** (full AEL) vs field **43** (residual) — [ ] update wire assertion after migration

#### Regression smoke (optional)

- [ ] `QueryIntegerTest.queryInteger`
- [ ] `QueryHintBuilderTest.queryWithBinHintExecutes`

---

## Documentation & examples

*(Unchanged scope — update wording: explain not probe; field `44` not `43` on new path.)*

- [ ] E1–E7 examples (`ServerQuerySelectionExample`, etc.)
- [ ] `README.md`, `key-features.md`, `api-builder-reference.md`
- [ ] `query-selection-user-guide.md` *(new)*
- [ ] Rewrite `query-selection-wire-protocol.md`
- [ ] Cross-links in `ael-documentation.md`, `query-selection-and-ael-roadmap-overview.md`

---

## Implementation order

1. ~~CLIENT-4800 baseline~~ (INFO4 + field `43`) — on branch; **do not ship as-is**.
2. **Protocol migration M1–M7** — field `44`, explain encoder, execute encoder, plan model.
3. **M8–M9** — unit + integration re-validation.
4. **M10** + user docs / examples.
5. **Real capability gate** (tend / features key for WHERE explain).

---

## Files likely touched (migration)

| Area | Files |
|------|--------|
| Constants | `FieldType.java`, `Command.java` (drop INFO4 from explain path) |
| WHERE wire | New `WhereFieldWire` or similar (flags varint + AEL msgpack) |
| Encoder | `CommandBuffer.java` — `setQueryExplain`, `setQuery` plan branch |
| Commands | `IndexProbeCommand.java`, `IndexProbeExecutor.java` (explain semantics) |
| Plan | `QueryPlan.java`, `MsgFieldParser.java` |
| Materialize | `WhereClauseProcessor.java`, `AelMaterializer.java` |
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

**Test:** `compoundPredicateServerLedMatchesLegacyForBin` — same rows; different wire (44 full AEL vs 43 residual).

**Questions:** Require equivalent row sets for all queries? Migration docs for `forBin` users?

### 6. Multi-index / ambiguous WHERE

**Questions:** Tie-break rules; per-node cardinality for auto-select?

### 7. Which node answers explain?

**Questions:** Plan consistency across nodes; prefer master vs any node?

### 8. REQUIRE_INDEX flag (new)

**Questions:** Public API for `REQUIRE_INDEX` (`0x02` in WHERE flags)? Fail explain or execute when server would pick PI/scan?

### 9. INDEX_TYPE on explain response

**Questions:** Required on client for execute encoding, or opaque passthrough for future CDT/geo?
