# Client TODO: two-phase server index selection

Implementation checklist for the fluent Java client.

**Progress:** Phase 0 + Phase 1 + Phase 2 **code** done (unit tests). **Next: dedicated integration tests** on query-selection server (`suresh/dsl-query-optimizer`). Real version gate in `supportsQuerySelection()` after E2E is green (gate is stubbed `true` for dev only).

**Sources of truth**

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

| Phase | Purpose | Wire |
|-------|---------|------|
| **Probe** | Server selects SI vs PI scan | `INFO4` query-selection bit set; **no partitions**; **no client-built index filter** |
| **Execute** | Normal partitioned query | `INFO4` bit clear; replay server pins + same field-`43` bytes as probe |

**Probe request**

- Field `43` — full WHERE material for field `43` (see **Field `43` encoding** above). **Interim:** client-compiled expression bytes. **Post-merge (TBD):** server-AEL bytes `[128, "<text>"]`.
- Field `21` — optional **index-name hint only** (probe only; no `binName` hint on new path). Omitted on auto-select.
- No field `22` — server performs auto-selection from `43`.

**Probe response**

| Outcome | `result_code` | Response fields |
|---------|---------------|-----------------|
| PI scan | `AS_OK` | none |
| SI query | `AS_OK` | `21` INDEX_NAME + `22` INDEX_RANGE (opaque bytes) |
| Filtered out | `AS_ERR_FILTERED_OUT` | — |

**Execute request (after probe)**

- Field `43` — **same bytes as probe** (store on `QueryPlan`, replay verbatim). Encoding tracks capability (packed exp now; server AEL later if product enables).
- **SI path:** replay probe `21` + `22` (server-authored; client does not build index filter locally).
- **PI path:** field `43` only; no `21`/`22`.
- Partitions, timeouts, etc. — same as today's `CommandBuffer.setQuery`.

**Backward compatibility (Ronen):** legacy clients always send an index filter on SI queries. New clients do not send a client-chosen index filter on probe; on execute they send **server-identified** `21`/`22` from the probe response, not locally derived filter material.

---

## Server status (verified in code)

| Item | Status |
|------|--------|
| Probe entry | `basic_query_job_start` → `as_query_plan` when `INFO4` bit 7 set |
| Probe parses | `0` ns, `1` set, optional `21` hint, required `43` |
| Probe does not execute | Returns plan reply; no partition fan-out |
| SI response | Fields `21` + `22` via `plan_build_range_payload` |
| PI response | `AS_OK`, zero fields |
| Execute | Existing basic query path (`INFO4` bit 7 clear); reads `21`/`22`/`43` independently |

Server does **not** validate that execute `43` matches probe `43`. It applies whatever is in the current message.

---

## Legacy vs new — field `43` on SI execute

| Path | Field `22` | Field `43` on SI execute |
|------|------------|--------------------------|
| **Legacy** | Client-built from `chooseExprForFilter` | **Residual** row filter (`VisitorUtils.getFilterExp` skips SI subtree) |
| **New (two-phase)** | Opaque replay from probe | **Full** field-`43` payload (same bytes as probe; not legacy residual split) |

Both shapes are accepted by the server execute handler. New-client work follows the **full `43`** contract from Slack, not the legacy residual split.

---

## Client gaps (what we still need to build)

1. ~~**Opaque replay of field `22`**~~ — `Filter.fromWireRange(indexName, rangeBytes)` + `CommandBuffer` opaque body write. ✅
2. ~~**Probe encoder**~~ — `CommandBuffer.setIndexProbe(IndexProbeCommand)` (no `chooseExprForFilter`, no field `22`).
3. ~~**Predicate bytes lifecycle**~~ — **`QueryPlan` stores `predicateBytes`**; probe encoder + execute replay wiring done.
4. **Capability gate (partial)** — routing via `IndexProbePlanner.useServerQuerySelection()` + `execute()` branch. `Cluster.supportsQuerySelection()` **stubbed `true` for dev** (`versionGE813` commented out). Wire real version/tend check **after** integration tests pass; until then gate-off = legacy fallback.
5. **Hint rules (partial)** — new path: `QueryHint.forIndex(...)` → field `21` on probe only; `forBin` forces legacy execute. Unit-tested on probe; not integration-tested on full probe→execute.
6. **Field `43` factory** — interim: compile AEL → packed exp for probe/execute. Post-merge: add capability branch for `fromServerCompiledFilter(ael)`; keep both paths until product drops client compile.
7. **Integration tests** — probe → execute against `suresh/dsl-query-optimizer` (or equivalent) server; see Tests section.

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
- [x] `Session.planQuery(...)` / `QueryBuilder.plan()` — build `IndexProbeCommand`, return `QueryPlan`.
- [x] Field `43` material for probe: **interim** — compile AEL → packed expression bytes (`AelMaterializer.expressionForQueryProbe` / `WhereClauseProcessor.toProbeExpression`); no client index selection. **Later** — capability branch for `fromServerCompiledFilter(ael)`.

---

## Phase 2 — execute with plan (code ✅, E2E not validated)

**Scope:** `IndexQueryBuilderImpl` guess-path `execute()` only (`allowsIndex` WHERE, no `QueryHint.forBin`). Pagination re-probe, background query, and other entry points remain out of scope (see Out of scope).

### Wiring ✅

- [x] `QueryCommand.forPlan(...)` — build execute from `QueryPlan` (PI / SI / `FILTERED_OUT` → exception).
- [x] Guess-path `execute()`: when `IndexProbePlanner.useServerQuerySelection()` → sync probe, then async execute with plan.
- [x] **PI plan:** `filter = null`; `filterExp` = `predicateBytes` from plan; normal `setQuery`.
- [x] **SI plan:**
  - `filter` = opaque replay of `indexRangeBytes` + `indexName` from plan (`Filter.fromWireRange`, not client-built).
  - `filterExp` = same `predicateBytes` as probe (full field-`43` replay).
  - `setQuery` with `INFO4` bit 7 clear (default).
- [x] No `chooseExprForFilter` / client index selection on the new path.
- [x] `QueryCommand.applyHintToFilter` skipped when plan-driven; plan pins win.

### Still open

- [ ] **Integration tests** (primary next step — see Tests section).
- [ ] Real `supportsQuerySelection()` version gate (after E2E green; stub OK for dev).
- [ ] Broader “explicit legacy override” beyond `forBin` / `!allowsIndex` (if product needs it).

### E2E triage notes (existing suite, gate stubbed `true`)

Running the full query integration suite against a query-selection server commit may show **0 records** on many tests. That pattern is **expected until probe→execute E2E is validated** — not evidence of unrelated client regressions.

| Symptom | Likely cause |
|---------|----------------|
| String-AEL `where(...).execute()` → 0 rows | New path: probe + `forPlan` execute (field `43` full replay + opaque `22`) |
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
- [x] `plan()` for visibility (`QueryBuilder.plan()` / `Session.planQuery()` — Phase 1).
- [ ] `explain()` (optional; not implemented).
- [x] Probe is sync (`IndexProbeExecutor`); execute stays async via `QueryExecutor` (unchanged).

---

## Tests

### Unit

- [x] Probe buffer layout (`IndexProbeCommandTest`).
- [x] `MsgFieldParser`: field TLV parsing + `RecordParser` integration (`MsgFieldParserTest`).
- [x] `QueryPlan.fromProbeResponse`: PI / SI / FILTERED_OUT / inconsistent response (`QueryPlanTest`).
- [x] Opaque `22` replay: probe bytes → `setQuery` field `22` matches input (`FilterWireRangeTest`, `QueryPlanExecuteWireTest`).

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
| 1.5 | `Session.planQuery()` smoke | `session.planQuery(dataSet, "$.age >= 14 and $.age <= 18")` | Same as 1.1 |
| 1.6 | FILTERED_OUT *(optional)* | Predicate server rejects | `isFilteredOut()` / exception — skip if not reproducible |

- [ ] 1.1 SI plan — simple range
- [ ] 1.2 PI plan — no matching SI
- [ ] 1.3 `forIndex` hint on probe
- [ ] 1.4 Probe bytes stable
- [ ] 1.5 `Session.planQuery()` smoke
- [ ] 1.6 FILTERED_OUT *(optional)*

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

- [ ] 2.1 SI execute — range
- [ ] 2.2 SI execute — equality
- [ ] 2.3 PI execute
- [ ] 2.4 Plan then execute consistency
- [ ] 2.5 Predicate replay
- [ ] 2.6 Compound predicate *(stretch)*
- [ ] 2.7 Reading bins *(stretch)*

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

- [ ] 3.1 `forBin` → legacy
- [ ] 3.2 `forIndex` → probe
- [ ] 3.3 Duration-only hint
- [ ] 3.4 `where(Exp)` — no probe
- [ ] 3.5 No WHERE
- [ ] 3.6 Gate off + string AEL *(when gate wired)*

#### Tier 4 — Errors & edge cases

| # | Scenario | Assert |
|---|----------|--------|
| 4.1 | FILTERED_OUT on execute | `execute()` throws `FILTERED_OUT` (not empty stream) |
| 4.2 | `plan()` without WHERE | `AerospikeException` |
| 4.3 | Empty result vs error | Valid SI query with no matching data → 0 records, not exception |

- [ ] 4.1 FILTERED_OUT on execute
- [ ] 4.2 `plan()` without WHERE
- [ ] 4.3 Empty result vs error

#### Pass / fail interpretation

| Result | Meaning |
|--------|---------|
| Tier 1 fails | Probe wire / server selection / response decode |
| Tier 1 passes, Tier 2 fails | Execute replay (opaque `22`, full `43`) or server execute handler |
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
- [ ] Probe + execute with server-AEL bytes in `43` when capability enabled.

---

## Implementation order

1. ~~Constants + `QueryPlan` + `MsgFieldParser`~~ ✅
2. ~~`IndexProbeCommand` / `IndexProbeExecutor` + `setIndexProbe` + unit tests~~ ✅
3. ~~Opaque `22` replay in `Filter` / `CommandBuffer`~~ ✅
4. ~~`IndexQueryBuilderImpl` + `QueryCommand.forPlan` wiring~~ ✅ (guess-path `execute()` only)
5. **Integration tests** — dedicated probe→execute tests on `suresh/dsl-query-optimizer`; triage table above.
6. **Real capability gate** — `versionGE813` (or tend flag) once step 5 passes.

---

## Files likely touched

| Area | Files | Status |
|------|--------|--------|
| Protocol | `Command.java`, `CommandBuffer.java`, `IndexProbeCommand.java`, `IndexProbeExecutor.java`, `MsgFieldParser.java` | done |
| API | `Session.java`, `QueryBuilder.java`, `IndexQueryBuilderImpl.java`, `IndexProbePlanner.java`, `WhereClauseProcessor.java`, `QueryPlan.java` | done |
| Query wire | `Filter.java`, `QueryCommand.java` | done (unit-tested wire layout) |
| AEL compile | `AelMaterializer.java` — `expressionForQueryProbe` done; `fromServerCompiledFilter` after merge | partial |
| Cluster | `Cluster.java`, `Version.java` | partial — `supportsQuerySelection()` stubbed `true` |
| Tests | unit tests done; integration TBD | partial |

**Not required on new path:** changes to `VisitorUtils.getFilterExp` / residual split / `IndexContext` for server-led selection.
