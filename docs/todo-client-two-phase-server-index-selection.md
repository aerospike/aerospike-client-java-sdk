# Client TODO: two-phase server index selection

Implementation checklist for the fluent Java client.

**Progress:** Phase 0 + Phase 1 (encoder + API) done. Next: opaque `22` replay + execute wiring + integration tests.

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

1. **Opaque replay of field `22`** — `Filter.write()` only builds structured ranges. Add passthrough for probe response bytes (e.g. `Filter.fromWireRange(indexName, rangeBytes)` or `CommandBuffer` hook).
2. ~~**Probe encoder**~~ — `CommandBuffer.setIndexProbe(IndexProbeCommand)` (no `chooseExprForFilter`, no field `22`).
3. ~~**Predicate bytes lifecycle**~~ — **`QueryPlan` stores `predicateBytes`**; still need probe encoder + execute replay wiring.
4. **Capability gate** — `Cluster.supportsQuerySelection()` (version/feature check until tend advertises it). On merged servers, selection + server AEL ship together; gate may imply AEL-on-wire. Fallback to legacy path when unsupported.
5. **Hint rules** — new path: `QueryHint.forIndex(...)` → field `21` on **probe only**. Do **not** send `QueryHint.forBin(...)` on probe (Slack: index name only). Legacy `forBin` / `forIndex` behavior unchanged when gate is off.
6. **Field `43` factory** — interim: compile AEL → packed exp for probe/execute. Post-merge: add capability branch for `fromServerCompiledFilter(ael)`; keep both paths until product drops client compile.

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

- [x] `Cluster.supportsQuerySelection()`.
- [x] `Session.planQuery(...)` / `QueryBuilder.plan()` — build `IndexProbeCommand`, return `QueryPlan`.
- [x] Field `43` material for probe: **interim** — compile AEL → packed expression bytes (`AelMaterializer.expressionForQueryProbe` / `WhereClauseProcessor.toProbeExpression`); no client index selection. **Later** — capability branch for `fromServerCompiledFilter(ael)`.

---

## Phase 2 — execute with plan

- [ ] Wire `IndexQueryBuilderImpl` / `QueryCommand` to accept optional `QueryPlan`.
- [ ] Guess-path `execute()`: if gate on and no explicit legacy override → probe then execute.
- [ ] **PI plan:** `filter = null`; `filterExp` = `predicateBytes` from plan; normal `setQuery`.
- [ ] **SI plan:**
  - `filter` = opaque replay of `indexRangeBytes` + `indexName` from plan (not client-built).
  - `filterExp` = same `predicateBytes` as probe (full field-`43` replay).
  - `setQuery` with `INFO4` bit 7 clear (default).
- [ ] Do **not** run `chooseExprForFilter` on the new path.
- [ ] `QueryCommand.applyHintToFilter` — not used when plan pins index; plan wins.

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

---

## Orchestration and UX

- [ ] `execute()` runs probe + execute transparently on guess path when supported.
- [ ] Optional `plan()` / `explain()` for visibility (selection, index name).
- [ ] Probe is sync; execute stays async via `QueryExecutor`.

---

## Tests

### Unit

- [x] Probe buffer layout (`IndexProbeCommandTest`).
- [x] `MsgFieldParser`: field TLV parsing + `RecordParser` integration (`MsgFieldParserTest`).
- [x] `QueryPlan.fromProbeResponse`: PI / SI / FILTERED_OUT / inconsistent response (`QueryPlanTest`).
- [x] `QueryPlan.predicateBytes` defensive copy (`QueryPlanTest`).
- [ ] Opaque `22` replay: probe bytes → `setQuery` field `22` matches input.

### Integration (index-selection server today; merged server + AEL later)

- [ ] Probe → SI execute: correct records for compound predicate (e.g. `age > 30 && country = "US"`) — **packed exp in `43`** against current tree.
- [ ] Probe → PI execute: scan + filter.
- [ ] `FILTERED_OUT` from probe surfaced to caller.
- [ ] Optional field `21` hint on probe when index name provided.
- [ ] Gate off / old server: legacy path unchanged, same results as today.
- [ ] New path vs legacy path: same query, both return equivalent results (where both apply).
- [ ] **Post-merge:** probe + execute with server-AEL bytes in `43` when capability enabled.

---

## Implementation order

1. ~~Constants + `QueryPlan` + `MsgFieldParser`~~ ✅
2. ~~`IndexProbeCommand` / `IndexProbeExecutor` + `setIndexProbe` + unit tests~~ ✅
4. [ ] Capability gate wired into execute — `supportsQuerySelection` + auto probe in `execute()`
5. Opaque `22` replay in `Filter` / `CommandBuffer`
6. `IndexQueryBuilderImpl` wiring (plan → execute, full `43` replay)
7. Integration tests

---

## Files likely touched

| Area | Files | Status |
|------|--------|--------|
| Protocol | `Command.java`, `CommandBuffer.java`, `IndexProbeCommand.java`, `IndexProbeExecutor.java`, `MsgFieldParser.java` | done |
| API | `Session.java`, `QueryBuilder.java`, `IndexQueryBuilderImpl.java`, `IndexProbePlanner.java`, `WhereClauseProcessor.java`, `QueryPlan.java` | done |
| Query wire | `Filter.java`, `QueryCommand.java` | |
| AEL compile | `AelMaterializer.java` — `expressionForQueryProbe` done; optional `fromServerCompiledFilter` after merge | partial |
| Cluster | `Cluster.java`, `Version.java` | done |
| Tests | `MsgFieldParserTest.java`, `QueryPlanTest.java`, `IndexProbeCommandTest.java`, integration TBD | unit tests done |

**Not required on new path:** changes to `VisitorUtils.getFilterExp` / residual split / `IndexContext` for server-led selection.
