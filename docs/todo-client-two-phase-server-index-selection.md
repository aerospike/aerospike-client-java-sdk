# Client TODO: two-phase server index selection

Implementation checklist for the fluent Java client.

**Progress:** Phase 0 + Phase 1 + Phase 2 **code** done; **Tier 1 integration tests** done. **Next:** Tier 2 probe→execute E2E, then docs/examples, then real version gate (`supportsQuerySelection()` stubbed `true` for dev).

**Sources of truth**

- **Wire contract (normative, all clients):** [query-selection-wire-protocol.md](./query-selection-wire-protocol.md)
- **Product / wire contract:** Slack thread (May 2026) — Suresh, Tim, Ronen, Gagan aligned on new vs legacy behavior.
- **Server implementation:** `aerospike-server` — `as_query_plan`, `AS_MSG_INFO4_QUERY_SELECTION`, `query_plan.c`, `query.c`.
- **Server roadmap:** index-selection and server-side AEL live in separate trees today; they will merge. **Any server that supports index selection is expected to also support server AEL** (ignore AEL gaps in the index-selection-only tree).

---

## Field `43` encoding — interim vs merged server

Both encodings use the **same field type** (`FILTER_EXP` / `43`). Only the **payload bytes** differ. `QueryPlan.predicateBytes` is always opaque field-`43` material to replay on execute.

| Encoding | Field `43` payload | When |
|----------|-------------------|------|
| **Client-compiled expression** | Packed predexp bytes from local AEL parse (`Expression.getBytes()` after `AelMaterializer` / visitor compile) | **Now** — index-selection server tree (`as_exp_filter_build` on packed exp; no AEL `[128, "…"]` yet) |
| **Server AEL** | `Expression.fromServerCompiledFilter(ael)` → MessagePack `[128, "<ael text>"]` | **After merge** — when product enables it on servers with selection + AEL |

**Client plan**

1. **Ship now:** new path sends **client-compiled expression bytes** on probe and execute (no `chooseExprForFilter`, no field `22` on probe). Integrate against the index-selection-only server.
2. **After server merge + product input:** switch (or gate) field `43` to **server-AEL bytes**; **may** drop local compile on the new path if product agrees. Keep capability-driven choice so both formats can coexist during rollout.
3. **Dual support is feasible:** probe/execute wiring (`IndexProbeCommand`, `QueryPlan`, `setIndexProbe`, `setQuery`) stays the same — only the **factory that fills `where` / `predicateBytes`** changes by capability. No second field type.

**Server note:** merged server must accept the chosen encoding in `43` (detect `[128, …]` vs packed predexp, or rely on capability / tend flag so client sends only what the node expects).

---

## Agreed behavior (Slack)

### Legacy clients (unchanged)

- Client chooses the secondary index locally.
- SI execute sends a **client-built index filter** (field `22`, plus field `21` when needed) and a **row filter** in field `43` (`FILTER_EXP`).
- Server uses the existing execute path; no probe.

### New clients (two-phase)

See **[query-selection-wire-protocol.md](./query-selection-wire-protocol.md)** for the full normative spec (field `22` probe vs execute layout, transform algorithm, cross-client reference).

| Phase | Purpose | Wire |
|-------|---------|------|
| **Probe** | Server selects SI vs PI scan | `INFO4` query-selection bit set; **no partitions**; **no field `22`** |
| **Execute** | Normal partitioned query | `INFO4` bit clear; server pins + field `43` |

**Probe request**

- Field `43` — full WHERE (see **Field `43` encoding** above). **Interim:** client-compiled expression bytes.
- Field `21` — optional **index-name hint only** (probe only; no `binName` hint on new path).
- No field `22`.

**Probe response (SI)**

| Field | Content |
|-------|---------|
| `21` | Secondary-index registry name |
| `22` | Range body **with bin name** (`bin_name_len > 0`) — built by server `plan_build_range_payload` |

**Execute request (after probe)**

| Field | SI path | PI path |
|-------|---------|---------|
| `43` | **Same bytes as probe** (store on plan; replay verbatim) | Same |
| `21` | Index name from probe | Absent |
| `22` | **Execute shape:** `bin_name_len = 0` + ktype/range tail from probe (strip bin name; see wire-protocol doc) | Absent |
| Partitions, timeouts, … | Normal `setQuery` | Normal `setQuery` |

**Server execute rule:** field `21` and a bin name inside field `22` are **mutually exclusive** (`query.c` `get_range_field`). Verbatim probe `22` + `21` → `PARAMETER_ERROR`.

**Backward compatibility (Ronen):** legacy clients choose the index locally and send client-built `22` (bin in range, typically no `21`). New clients probe first; on execute they send server-authored **`21` + transformed `22`**, not locally derived filter material.

---

## Server status (verified in code)

| Item | Status |
|------|--------|
| Probe entry | `basic_query_job_start` → `as_query_plan` when `INFO4` bit 7 set |
| Probe parses | `0` ns, `1` set, optional `21` hint, required `43` |
| Probe does not execute | Returns plan reply; no partition fan-out |
| SI response | Fields `21` + `22` via `plan_build_range_payload` (**`22` includes bin name**) |
| PI response | `AS_OK`, zero fields |
| Execute | `get_range_field`: rejects `21` + bin in `22`; accepts `21` + `22` with `bin_name_len=0` |

Server does **not** validate that execute `43` matches probe `43`. It applies whatever is in the current message.

---

## Legacy vs new — field `43` on SI execute

| Path | Field `22` | Field `43` on SI execute |
|------|------------|--------------------------|
| **Legacy** | Client-built from `chooseExprForFilter` | **Residual** row filter (`VisitorUtils.getFilterExp` skips SI subtree) |
| **New (two-phase)** | Probe bytes stored; execute sends `bin_name_len=0` body with field `21` | **Full** field-`43` payload (same bytes as probe; not legacy residual split) |

Both shapes are accepted by the server execute handler. New-client work follows the **full `43`** contract from Slack, not the legacy residual split.

---

## Client gaps (what we still need to build)

1. ~~**Execute field `22` from probe**~~ — `IndexRangeWire.forExecuteWithIndexName` strips bin name when field `21` is sent; `Filter.fromWireRange` + `CommandBuffer` write execute-shaped body. ✅
2. ~~**Probe encoder**~~ — `CommandBuffer.setIndexProbe(IndexProbeCommand)` (no `chooseExprForFilter`, no field `22`).
3. ~~**Predicate bytes lifecycle**~~ — **`QueryPlan` stores `predicateBytes`**; probe encoder + execute replay wiring done.
4. **Capability gate (partial)** — routing via `IndexProbePlanner.useServerQuerySelection()` + `execute()` branch. `Cluster.supportsQuerySelection()` **stubbed `true` for dev** (`versionGE813` commented out). Wire real version/tend check **after** integration tests pass; until then gate-off = legacy fallback.
5. **Hint rules (partial)** — new path: `QueryHint.forIndex(...)` → field `21` on probe only; `forBin` forces legacy execute. Unit-tested on probe; not integration-tested on full probe→execute.
6. **Field `43` factory** — interim: compile AEL → packed exp for probe/execute. Post-merge: add capability branch for `fromServerCompiledFilter(ael)`; keep both paths until product drops client compile.
7. **Integration tests** — Tier 1 probe tests done; Tier 2 execute E2E in progress; see Tests section.
8. ~~**Wire protocol doc**~~ — [query-selection-wire-protocol.md](./query-selection-wire-protocol.md) for cross-client implementers. ✅
9. **Documentation & examples** — user-facing guide for server-led selection and hint overrides; see Documentation section.

---

## Out of scope (v1)

- **Defaulting to server-AEL `[128, "…"]` in field `43`** before merged server + product sign-off (interim uses client-compiled exp bytes).
- `binName` hint on probe for new clients.
- `INDEX_TYPE` / `INDEX_CONTEXT` / `INDEX_EXPRESSION` in probe response (CDT / list / map / geo may need follow-on).
- Background query / UDF query with server selection.
- Pagination / continuation policy (re-probe vs pin plan across chunks).
- Brainstorm `INFO5` / fields `44`/`45` — server uses `INFO4` bit 7 + `21`/`22`.

---

## Phase 0 — types and constants ✅

- [x] `Command.INFO4_QUERY_SELECTION = 1 << 7` in `Command.java` (mirror `proto.h`).
- [x] `QueryPlan` immutable type (`com.aerospike.client.sdk.query.plan`):
  - `QuerySelection` enum: `PRIMARY_INDEX`, `SECONDARY_INDEX`, `FILTERED_OUT`
  - `selection`, `namespace`, `set`, `predicateBytes` (field `43` payload), `indexName` (nullable), `indexRangeBytes` (nullable)
  - `QueryPlan.fromProbeResponse(...)` factory
- [x] `MsgFieldParser` — parse reply `AS_MSG` fields by `FieldType` id (`MsgFieldParser.from(RecordParser)`).

---

## Phase 1 — probe

### Encoder + command: `IndexProbeCommand` / `IndexProbeExecutor` ✅

- [x] `IndexProbeCommand extends Command` — namespace, set, predicate (`where`), optional index-name hint, task id, policies via `ResolvedSettings`.
- [x] `IndexProbeCommand.execute()` → `IndexProbeExecutor` → `QueryPlan`.
- [x] `CommandBuffer.setIndexProbe(IndexProbeCommand)` — `INFO4_QUERY_SELECTION`, fields `0`/`1`/`9`/`7`/optional `21`/`43`, no partitions.
- [x] `IndexProbeExecutor` — sync single-node probe, rotate nodes on retry, decode via `MsgFieldParser` + `QueryPlan.fromProbeResponse`.
- [x] Unit tests: wire layout (`IndexProbeCommandTest`).

### API ✅

- [x] `Cluster.supportsQuerySelection()` — method exists; **version check stubbed** (`return true`) until tend advertises capability.
- [x] Probe orchestration — package-private `IndexProbePlanner`; **no public `plan()` / `planQuery()`** (transparent `execute()` only).
- [x] Field `43` material for probe: **interim** — compile AEL → packed expression bytes (`AelMaterializer.expressionForQueryProbe` / `WhereClauseProcessor.toProbeExpression`); no client index selection. **Later** — capability branch for `fromServerCompiledFilter(ael)`.

---

## Phase 2 — execute with plan (code ✅, E2E not validated)

**Scope:** `IndexQueryBuilderImpl` guess-path `execute()` only (`allowsIndex` WHERE, no `QueryHint.forBin`). Pagination re-probe, background query, and other entry points remain out of scope (see Out of scope).

### Wiring ✅

- [x] `QueryCommand.forPlan(...)` — build execute from `QueryPlan` (PI / SI / `FILTERED_OUT` → exception).
- [x] Guess-path `execute()`: when `IndexProbePlanner.useServerQuerySelection()` → sync probe, then async execute with plan.
- [x] **PI plan:** `filter = null`; `filterExp` = `predicateBytes` from plan; normal `setQuery`.
- [x] **SI plan:**
  - `filter` = `IndexRangeWire.forExecuteWithIndexName(indexRangeBytes)` + `indexName` (`Filter.fromWireRange`).
  - `filterExp` = same `predicateBytes` as probe (full field-`43` replay).
  - `setQuery` with `INFO4` bit 7 clear (default).
- [x] No `chooseExprForFilter` / client index selection on the new path.
- [x] `QueryCommand.applyHintToFilter` skipped when plan-driven; plan pins win.

### Still open

- [ ] **Integration tests** — Tier 2+ (execute E2E); see Tests section.
- [ ] **Documentation & examples** — see Documentation section.
- [ ] Real `supportsQuerySelection()` version gate (after E2E green; stub OK for dev).
- [ ] Broader “explicit legacy override” beyond `forBin` / `!allowsIndex` (if product needs it).

### E2E triage notes (existing suite, gate stubbed `true`)

Running the full query integration suite against a query-selection server commit may show **0 records** on many tests. That pattern is **expected until probe→execute E2E is validated** — not evidence of unrelated client regressions.

| Symptom | Likely cause |
|---------|----------------|
| String-AEL `where(...).execute()` → 0 rows | Was: field `22` included bin name with field `21` (server `PARAMETER_ERROR`). Fixed: `IndexRangeWire` strips bin on execute. If still failing, check field `43`. |
| Same tests pass with gate `false` | Legacy path still works; routing is the delta |
| `where(Exp)` tests pass (`QueryIntegerTest`, `QueryFilterExpTest`) | Non-probe path (`allowsIndex=false`); PI scan + field `43` only |
| `QueryHintBuilderTest.queryWithBinHintExecutes` passes | `forBin` forces legacy SI (client `Filter` + residual `43`) |
| String-AEL tests fail | **Expected for now** on selection-server branch until dedicated integ tests pass; AEL-on-wire gaps on server are a separate issue |
| `QueryPlanApiTest` version-gate tests fail | Stub gate — ignore until `versionGE813` is wired |

**Quick triage** (no new tests): `QueryIntegerTest.queryInteger` (no probe) → `queryWithBinHintExecutes` (legacy SI) → `QueryBuilder.plan()` (probe only) → string-AEL `execute()` (probe + execute).

---

## When to use which path

| Condition | Path |
|-----------|------|
| `!supportsQuerySelection()` | Legacy: client index selection + residual `43` on SI |
| Explicit legacy-style query with client-built filter | Legacy (unchanged) |
| Guess-path WHERE, gate on | New: probe → execute with plan |
| `QueryHint.forIndex` on new path | Field `21` on probe only |
| `QueryHint.forBin` | Legacy only; not on new probe path |
| No WHERE / index not allowed | No probe; existing behavior |

*Today `supportsQuerySelection()` is stubbed `true` for dev — all string-AEL dataset queries probe. Set to `false` locally to run legacy suite against selection server without probe routing.*

**Note:** There is no public `where(Filter)` API. True legacy SI (field `22` + residual `43`) in integration tests today = string AEL + `forBin` hint, or reflection (see `MIGRATION.md`). `where(Exp)` never uses SI on the wire (PI scan only).

---

## Orchestration and UX

- [x] `execute()` runs probe + execute transparently on guess path when `useServerQuerySelection()` (`IndexQueryBuilderImpl`).
- [x] No public probe/plan API — selection is internal; users call `.where(...).execute()` only.
- [ ] `explain()` (optional; not implemented).
- [x] Probe is sync (`IndexProbeExecutor`); execute stays async via `QueryExecutor` (unchanged).

---

## Tests

### Unit

- [x] Probe buffer layout (`IndexProbeCommandTest`).
- [x] `MsgFieldParser`: field TLV parsing + `RecordParser` integration (`MsgFieldParserTest`).
- [x] `QueryPlan.fromProbeResponse`: PI / SI / FILTERED_OUT / inconsistent response (`QueryPlanTest`).
- [x] Execute field `22` transform: probe bytes stored; execute sends `bin_name_len=0` body (`IndexRangeWireTest`, `QueryPlanExecuteWireTest`).

### Integration (index-selection server: `suresh/dsl-query-optimizer`)

**Goal:** Prove probe → execute on **packed predexp in field `43`** (interim encoding). Dedicated tests first; then decide if existing string-AEL suite should pass or stay legacy-gated.

**Prerequisites:** query-selection server running; client gate on (`supportsQuerySelection() == true`); dedicated set with indexed integer bin (e.g. `age` 1–50) and predictable record distribution.

**Suggested implementation order:** Tier 1 → Tier 2 (SI first) → Tier 3 → Tier 4 → Tier 2b stretch.

#### Tier 1 — Probe only (`plan()`)

Validates server selection + client decode without execute.

| # | Scenario | WHERE / hint | Assert |
|---|----------|--------------|--------|
| 1.1 | SI plan — simple range | `$.age >= 14 and $.age <= 18` | `SECONDARY_INDEX`; `indexName` + `indexRangeBytes` + `predicateBytes` non-null |
| 1.2 | PI plan — no matching SI | Predicate on non-indexed bin, or shape server returns as PI | `PRIMARY_INDEX`; `indexName` / `indexRangeBytes` null |
| 1.3 | `forIndex` hint on probe | Same as 1.1 + `forIndex("age_idx")` | Plan uses hinted index (or document server reject/fallback) |
| 1.4 | Probe bytes stable | Call `plan()` twice on same WHERE | Same `predicateBytes`; SI plan stable (or document if server may vary) |
| 1.5 | `IndexProbePlanner` smoke | Direct `IndexProbePlanner.plan(...)` (same path as `execute()`) | Same as 1.1; matches `QueryBuilder.plan()` |
| 1.6 | FILTERED_OUT | Contradiction: `$.age > 100 and $.age < 10` | `FILTERED_OUT`; null `21`/`22`; `predicateBytes` present |

- [x] 1.1 SI plan — simple range
- [x] 1.2 PI plan — no matching SI
- [x] 1.3 `forIndex` hint on probe
- [x] 1.4 Probe bytes stable
- [x] 1.5 `IndexProbePlanner` smoke
- [x] 1.6 FILTERED_OUT — contradiction probe (`planContradictionPredicate`)

#### Tier 2 — Probe → execute (core E2E)

Validates full two-phase flow; primary gap today (0 records on string-AEL `execute()`).

| # | Scenario | Flow | Assert |
|---|----------|------|--------|
| 2.1 | SI execute — range | `where("$.age >= 14 and $.age <= 18").execute()` | 5 records; each `age` in [14, 18] |
| 2.2 | SI execute — equality | `where("$.age == 25").execute()` | 1 record; `age == 25` |
| 2.3 | PI execute | Predicate that probe returns as PI (from 1.2) | Correct filtered records; not silent empty stream |
| 2.4 | Plan then execute consistency | `plan()` → assert SI → same `execute()` | Record count matches plan expectation |
| 2.5 | Predicate replay | Probe then execute on same query | Execute sends same field `43` bytes as probe |

**Tier 2b — after 2.1 passes:**

| # | Scenario | Notes |
|---|----------|-------|
| 2.6 | Compound predicate | `$.age > 30 and $.country == "US"` — full `43` replay vs legacy residual |
| 2.7 | Reading bins | `.readingOnlyBins("age").where(...).execute()` | Records + bin projection on new path |

- [x] 2.1 SI execute — range
- [x] 2.2 SI execute — equality
- [x] 2.3 PI execute
- [x] 2.4 Plan then execute consistency
- [x] 2.5 Predicate replay
- [x] 2.6 Compound predicate *(stretch)*
- [x] 2.7 Reading bins *(stretch)*

#### Tier 3 — Hints & routing

Proves gate logic; legacy paths must not be accidentally probed.

| # | Scenario | Expected path | Assert |
|---|----------|---------------|--------|
| 3.1 | `forBin` → legacy | `where(...).withHint(forBin("age")).execute()` | Same count as gate-off / pre-selection; must not depend on probe |
| 3.2 | `forIndex` → probe | `withHint(forIndex("age_idx")).execute()` | Records match; hint on probe (field `21`) |
| 3.3 | Duration-only hint | `withHint(queryDuration(SHORT))` only | Still probes; execute returns records |
| 3.4 | `where(Exp)` — no probe | `Exp.ge/le` on indexed bin | Correct records; non-probe path unaffected |
| 3.5 | No WHERE | `session.query(dataSet).execute()` | Scan returns data |

**Gate-off regression** *(when gate is configurable or real):*

| # | Scenario | Assert |
|---|----------|--------|
| 3.6 | Gate off + string AEL | String-AEL query uses legacy client SI; records returned |

- [x] 3.1 `forBin` → legacy
- [x] 3.2 `forIndex` → probe
- [x] 3.3 Duration-only hint
- [x] 3.4 `where(Exp)` — no probe
- [x] 3.5 No WHERE
- [x] 3.6 Gate off + string AEL *(skipped until gate wired; test in place)*

#### Tier 4 — Errors & edge cases

| # | Scenario | Assert |
|---|----------|--------|
| 4.1 | FILTERED_OUT on execute | `execute()` throws `FILTERED_OUT` (not empty stream) |
| 4.2 | `plan()` without WHERE | `AerospikeException` |
| 4.3 | Empty result vs error | Valid SI query with no matching data → 0 records, not exception |
| 4.4 | `forIndex` non-existent index | Probe succeeds; bogus hint ignored; auto-selects `qsel_age_idx` *(observed; product may want error)* |
| 4.5 | Multiple indexes — server auto-select | Fixture with 2+ real indexes; WHERE unambiguous; probe + execute pick correct `indexName` and row set |
| 4.6 | `forIndex` existing but wrong index | Second real index on another bin; hint names it while WHERE fits first bin; document observed server behavior *(product rule TBD)* |

- [x] 4.1 FILTERED_OUT on execute
- [x] 4.2 `plan()` without WHERE
- [x] 4.3 Empty result vs error
- [x] 4.4 `forIndex` non-existent index *(observed: hint ignored, auto-select; confirm with product)*
- [x] 4.5 Multiple indexes — server auto-select
- [x] 4.6 `forIndex` existing but wrong index *(observed: hint ignored, auto-select; product rule TBD)*

#### Pass / fail interpretation

| Result | Meaning |
|--------|---------|
| Tier 1 fails | Probe wire / server selection / response decode |
| Tier 1 passes, Tier 2 fails | Execute field `22` shape (bin + `21`) or field `43` / server execute handler |
| Tier 2 passes, existing AEL suite fails | Expected; not a v1 blocker |
| Tier 3.4 fails | Broke non-probe path — real client bug |
| Tier 3.1 fails | Broke legacy SI path — real client bug |

#### Explicit non-goals (do not block v1)

| Area | Why skip |
|------|----------|
| Full existing AEL suite (`QueryStringTest`, `QueryOperationsTest`, …) | Many fail on probe path today; AEL-on-wire gaps on selection-only server |
| CDT / map / geo / blob hex literals | `INDEX_TYPE` / `INDEX_CONTEXT` not in probe response v1 |
| Background query / UDF with selection | Out of scope v1 |
| Pagination / chunk re-probe | Out of scope v1 |
| Server-AEL `[128,"…"]` in field `43` | Post-merge |
| New vs legacy equivalent results on same query | Product may differ (full `43` vs residual); separate comparison later |

#### Regression smoke (reuse existing tests — optional, not blockers)

- [ ] `QueryIntegerTest.queryInteger` — non-probe baseline
- [ ] `QueryHintBuilderTest.queryWithBinHintExecutes` — legacy SI baseline

#### Later / post-merge

- [ ] New path vs legacy path: same string-AEL query, equivalent results (where product says they should match).
  - [x] Compound SI WHERE: `compoundPredicateServerLedMatchesLegacyForBin` (ages match; field `43` differs on wire)
- [ ] Probe + execute with server-AEL bytes in `43` when capability enabled.

---

## Documentation & examples

User-facing material for **server-led index selection** (two-phase probe → execute) and **overriding** selection via `QueryHint`. No public `plan()` API — examples show `execute()` only.

**Prerequisites to state in all docs:** server build with query selection (e.g. 8.1.3+ / feature branch); secondary index created before querying; string-AEL `where(...)` on dataset queries (not `where(Exp)` for SI path).

### Examples (`examples/`)

| Item | File | Show |
|------|------|------|
| E1 | **`ServerQuerySelectionExample.java`** *(new)* | End-to-end: create index, seed data, **default** `where("$.age …").execute()` — server auto-selects SI vs PI; print record count. No `plan()` call. |
| E2 | same or section in E1 | **`forIndex("age_idx")`** — pin index name on probe (field `21`); when to use vs auto-select; note bogus name may be ignored (observed server behavior). |
| E3 | same or section in E1 | **`forBin("age")`** — **legacy** client index selection (forces pre-selection path); contrast with E2 on same WHERE. |
| E4 | same | Optional: `queryDuration(SHORT)` combined with `forIndex`; unsatisfiable WHERE → `FilteredException` on execute. |
| E5 | `QueryExamples.java` | Refresh **Query hints** section: label **new path** (`forIndex`) vs **legacy override** (`forBin`); link to `ServerQuerySelectionExample`. |
| E6 | `Main.java` | Register `ServerQuerySelectionExample` in `EXAMPLE_NAMES` and usage list. |
| E7 | `examples/README` or run script | One-liner how to run: `./run_examples ServerQuerySelectionExample -h host -p port` |

- [ ] E1 Default server-led query (`execute()` only)
- [ ] E2 Override with `forIndex`
- [ ] E3 Legacy override with `forBin`
- [ ] E4 Duration + filtered-out *(optional)*
- [ ] E5 Update `QueryExamples` hints commentary
- [ ] E6 Register in `Main.java`
- [ ] E7 Run instructions in examples docs

### README & docs

| Doc | Update |
|-----|--------|
| **`README.md`** | Quick Start / Features: note that on supported servers, string-AEL dataset queries use **server index selection** (probe + execute under the hood). Link to user guide. Mention `withHint(forIndex)` / `withHint(forBin)` briefly. |
| **`docs/key-features.md`** | **Queries & scans** / AEL: replace “automatic secondary index selection” (client-only) with **server-led** when `supportsQuerySelection()`; table row for **Query hints** (`forIndex` vs `forBin` vs `queryDuration`). |
| **`docs/api-builder-reference.md`** | Expand `.withHint(...)` — type-state API, routing table (`forIndex` → probe field `21`; `forBin` → legacy path), code snippets. |
| **`docs/query-selection-user-guide.md`** *(new, recommended)* | Short user guide: how it works (transparent `execute()`), when PI vs SI, hints, legacy `forBin`, server version gate. |
| **`docs/query-selection-wire-protocol.md`** | Normative probe/execute wire spec for all clients (field `21`/`22`/`43`). |
| **`docs/todo-client-two-phase-server-index-selection.md`** | Java implementer checklist (this file). |
| **`docs/ael-documentation.md`** or cross-link | One paragraph: index selection interaction with AEL `where` on dataset queries. |
| **`docs/query-selection-and-ael-roadmap-overview.md`** | Add pointer at top: “For **using** the feature in the fluent client, see `query-selection-user-guide.md`.” |

- [ ] `README.md` — server selection + hints summary
- [ ] `docs/key-features.md` — server-led selection + hints
- [ ] `docs/api-builder-reference.md` — `withHint` detail
- [ ] `docs/query-selection-user-guide.md` — new user guide
- [ ] `docs/ael-documentation.md` — cross-link *(if applicable)*
- [ ] `docs/query-selection-and-ael-roadmap-overview.md` — link to user guide

**Suggested order:** Tier 2.1 green → E1–E3 example → user guide → README / key-features → api-builder-reference.

---

## Implementation order

1. ~~Constants + `QueryPlan` + `MsgFieldParser`~~ ✅
2. ~~`IndexProbeCommand` / `IndexProbeExecutor` + `setIndexProbe` + unit tests~~ ✅
3. ~~Opaque `22` replay in `Filter` / `CommandBuffer`~~ ✅
4. ~~`IndexQueryBuilderImpl` + `QueryCommand.forPlan` wiring~~ ✅ (guess-path `execute()` only)
5. ~~Integration tests Tier 1~~ ✅ — `QuerySelectionIntegrationTest`; Tier 2 execute E2E in progress.
6. **Documentation & examples** — `ServerQuerySelectionExample` + README / key-features / user guide (see Documentation section).
7. **Real capability gate** — `versionGE813` (or tend flag) once Tier 2 passes.

---

## Files likely touched

| Area | Files | Status |
|------|--------|--------|
| Protocol | `Command.java`, `CommandBuffer.java`, `IndexProbeCommand.java`, `IndexProbeExecutor.java`, `MsgFieldParser.java` | done |
| API | `Session.java`, `QueryBuilder.java`, `IndexQueryBuilderImpl.java`, `IndexProbePlanner.java`, `WhereClauseProcessor.java`, `QueryPlan.java` | done |
| Query wire | `Filter.java`, `QueryCommand.java` | done (unit-tested wire layout) |
| AEL compile | `AelMaterializer.java` — `expressionForQueryProbe` done; `fromServerCompiledFilter` after merge | partial |
| Cluster | `Cluster.java`, `Version.java` | partial — `supportsQuerySelection()` stubbed `true` |
| Tests | unit tests done; Tier 1 integration done; Tier 2 in progress | partial |
| Docs / examples | `ServerQuerySelectionExample`, README, user guide — see Documentation section | not started |

**Not required on new path:** changes to `VisitorUtils.getFilterExp` / residual split / `IndexContext` for server-led selection.

---

## Appendix — Product / behavior questions

Open product decisions not blocking v1 client wiring; pin answers here when settled.

### 1. Retry behavior on probe

**Current client behavior (concise):** Probe uses the standard `SyncExecutor` loop with **READ + QUERY** `Behavior` settings (default **6 attempts**, **0 ms** between retries, **1 s** total `abandonCallAfter`). Each attempt is a single `INFO4_QUERY_SELECTION` RPC to **one cluster node** (random start; **round-robin** to the next node on retry in multi-node clusters). Transport/transient failures retry; `OK` and `FILTERED_OUT` succeed; other server result codes fail immediately. Probe is **not** re-run on execute failure, and there is no partition/replica routing.

**Questions for product:**

- **Fewer retries?** Probe is a cheap planner RPC prepended to every selection-enabled query — should we use fewer than query execute attempts (e.g. 1–2) to avoid adding latency on a failing cluster?
- **Node stickiness?** Retries currently rotate nodes (`IndexProbeExecutor.prepareRetry`). Should retries **stick to the same node** (plan may be node-local / cache-sensitive) or keep rotating (resilience to a bad node)?

### 2. `forIndex` hint when index is wrong or missing

**Current observed behavior (concise):** Client sends field `21` on probe only; no client-side validation. Server **ignores** hints that name a **non-existent** index (Tier 4.4) or an **existing index on the wrong bin** for the WHERE (Tier 4.6) and **auto-selects** the index that fits the predicate. Execute then uses the plan the server returned, not the hint.

**Questions for product:**

- **Fail vs ignore?** Should a bad `forIndex` hint return `INDEX_NOTFOUND` / parameter error instead of silent auto-select?
- **Honor wrong hint?** Should the server ever use a hinted index even when the WHERE “fits” a different bin (force index even if suboptimal)?
- **User-facing contract:** Document `forIndex` as a **soft suggestion** (today) or a **hard pin** (future)?

### 3. `FILTERED_OUT` vs empty result

**Current observed behavior (concise):** **Contradictory / unsatisfiable** predicate (e.g. `$.age > 100 and $.age < 10`) → probe returns `FILTERED_OUT`; client throws on `execute()` (Tier 4.1). **Valid SI plan with no matching rows** (e.g. `$.age == 999`) → probe succeeds, execute returns **0 records** (Tier 4.3).

**Questions for product:**

- Is that split correct for all predicate shapes (including PI and compound WHERE)?
- Should any “empty but valid” cases be `FILTERED_OUT` instead (or vice versa)?

### 4. Plan freshness between probe and execute

**Current client behavior (concise):** One probe per `execute()`; plan bytes (`21`, `22`, `43`) are replayed on execute immediately. Server does **not** verify that execute `43` matches what was probed. Client does **not** re-probe on execute failure, partition-map change, or pagination chunks (v1 out of scope).

**Questions for product:**

- If cluster state changes between probe and execute (migrations, index drop/create, partition map), should the client **re-probe**, **fail**, or **best-effort execute** with a stale plan?
- Pagination / chunked queries: **re-probe per chunk** or **pin plan** for the whole scan?

### 5. New path field `43` vs legacy residual filter

**Current behavior (concise):** **Legacy** SI execute sends a **residual** row filter in `43` (SI subtree stripped client-side). **New** two-phase path replays the **full** WHERE bytes from probe on execute. Compound predicates (e.g. `age` + `country`) can therefore **differ in field `43` on the wire** between paths even when the same index is used.

**Client test:** `compoundPredicateServerLedMatchesLegacyForBin` — `$.age > 30 and $.country == 'US'`; asserts **same ages** on server-led vs `forBin` legacy, and **different field `43` bytes** on execute wire.

**Questions for product:**

- Must new and legacy paths return **equivalent row sets** for the same string-AEL query, or is “full `43` on new path only” intentional?
- When should docs/examples warn users migrating from `forBin` (legacy) to default server selection?

### 6. Multi-index selection and ambiguous WHERE

**Current observed behavior (concise):** With indexes on `age` and `score`, unambiguous WHERE on one bin → server picks the matching index (Tier 4.5). No v1 test for **ambiguous** cases (multiple viable indexes, optimizer tie-break).

**Questions for product:**

- Tie-break rules when several indexes could serve the same WHERE?
- **Cardinality / stats differ per node** — deferred for v1; needed for correct auto-select in production?

### 7. Which node answers the probe?

**Current client behavior (concise):** Probe goes to a **random** cluster node (then round-robin on retry); not partition- or master-aware.

**Questions for product:**

- Must all nodes return the **same** plan for the same WHERE + catalog snapshot?
- Should the client prefer **master**, a **query coordinator**, or any node?

