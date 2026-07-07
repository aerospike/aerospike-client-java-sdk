# TODO: server query selection — integration tests (cross-client)

**Purpose:** Shared integration-test plan for **two-phase server query selection**
(explain → execute, field `44` WHERE). This doc lives in the **Java fluent client**
repo because **`QuerySelectionIntegrationTest`** is the reference implementation.

**Normative wire:** [query-selection-wire-protocol.md](./query-selection-wire-protocol.md)

**Implementation checklist:** [todo-client-two-phase-server-index-selection.md](./todo-client-two-phase-server-index-selection.md)

**Other clients**

| Client | Dedicated integ suite | Status |
|--------|----------------------|--------|
| Java fluent | `client/src/test/java/.../query/QuerySelectionIntegrationTest.java` | Reference (~30 scenarios) |
| Python SDK | `tests/integration/async/query_server_selection_test.py` (+ sync subset) | ~80% parity with Java |
| Rust core | Unit/wire only (`query_plan`, `query_where_wire`, `as_msg_fields`) | No cluster integ |

**Prerequisites (all integ tests)**

- Cluster minimum **8.1.3** (`Cluster.supportsQuerySelection()` / PAC equivalent).
- Dedicated set **`qselint`**, 50 records, bins `age`/`score`/`country` (1..50, US/CA pattern).
- Secondary indexes: **`qsel_age_idx`** on `age`, **`qsel_score_idx`** on `score` (INTEGER, DEFAULT).
- Skip entire module when gate is false (Java: `@BeforeAll assumeTrue`; Python: `supports_query_selection` fixture).

**Shared fixture constants** (keep identical across clients)

```
namespace:  test (or cluster default)
set:        qselint
keys:       qselkey1 .. qselkey50
age/score:  i for key i
country:    US if i even, CA if odd
indexes:    qsel_age_idx (age), qsel_score_idx (score)
```

Python helpers: `aerospike-client-python-sdk/tests/integration/query_selection_helpers.py`

---

## Test strategy

1. **User-facing API only** — `session.query(dataset).where("...").execute()` (and `.plan()` where exposed).
2. **Golden results** — assert sorted bin values or key lists, not just counts (counts alone miss wrong rows).
3. **Explain vs execute** — split asserts: plan `selection` / `index_name` / `index_type` vs execute row set.
4. **Parity rule** — when Java and Python both implement a scenario, they must use the **same AEL string** and **same expected results**.
5. **Legacy path** — `forBin` / `QueryHint(bin_name=...)` / `where(Exp)` / gate-off must remain testable separately.

---

## Already covered (reference impl)

Source: `QuerySelectionIntegrationTest.java` — **done in Java**. Python column notes port status.

### Tier 1 — Explain only (`.plan()`)

| ID | Java test | Scenario | Python async | Python sync |
|----|-----------|----------|--------------|-------------|
| 1.1 | `planSimpleRangeSelectsSecondaryIndex` | SI range on `age` | ✅ | — |
| 1.2 | `planNonIndexedPredicateSelectsPrimaryIndex` | PI on `country` | ✅ | — |
| 1.3 | `planForIndexHintUsesHintedIndex` | `forIndex(age_idx)` hint | ✅ (explain only) | — |
| 1.4 | `planBytesStableAcrossRepeatedProbes` | Repeated explain → same bytes | ❌ | ❌ |
| 1.5 | `indexProbePlannerSmoke` | `IndexProbePlanner.plan` ≡ builder `.plan()` | ❌ | ❌ |
| 1.6 | `planContradictionPredicate` | FILTERED_OUT explain | ✅ | ✅ |
| 1.7 | `planForIndexHintOnNonExistentIndex` | Bogus hint → server picks `age_idx` | ❌ | ❌ |
| 1.8 | `planForIndexHintOnWrongExistingIndex` | Hint `score_idx`, AEL on age → `age_idx` | ❌ | ❌ |

### Tier 2 — Explain → execute E2E

| ID | Java test | Scenario | Python async | Python sync |
|----|-----------|----------|--------------|-------------|
| 2.1 | `executeSimpleRangeReturnsMatchingRecords` | `age` 14–18 → `[14..18]` | ✅ | ✅ |
| 2.2 | `executeEqualityReturnsSingleRecord` | `age == 25` | ✅ | — |
| 2.3 | `executePrimaryIndexPredicateReturnsMatchingRecords` | `country == 'US'` → 25 rows | ✅ | — |
| 2.4 | `planThenExecuteConsistencyForSecondaryIndex` | Plan index_name matches execute rows | ❌ | ❌ |
| 2.5 | `executeReplaysPlanWhereBytesOnWire` | Execute field 44 = plan bytes, EXPLAIN cleared | ❌ (Rust/PAC wire) | ❌ |
| 2.6 | `executeCompoundPredicateReturnsMatchingRecords` | `age > 30 and country == 'US'` | ✅ | — |
| 2.7 | `executeWithReadingOnlyBinsProjectsRequestedBins` | Bin projection on selection path | ❌ | ❌ |

### Tier 3 — Hints & routing

| ID | Java test | Scenario | Python async | Python sync |
|----|-----------|----------|--------------|-------------|
| 3.1 | `compoundPredicateServerLedMatchesLegacyForBin` | Server-led ≡ legacy `forBin` rows | ✅ | ✅ |
| 3.2 | `forBinHintUsesLegacyExecutePath` | `forBin(age)` same rows as default | partial (compound only) | partial |
| 3.3 | `forIndexHintProbesAndExecutes` | `forIndex` hint E2E | ❌ | ❌ |
| 3.4 | `queryDurationOnlyHintStillProbesAndExecutes` | Duration-only hint still probes | ❌ | ❌ |
| 3.5 | `whereExpUsesNonProbeExecutePath` | `where(Exp)` → legacy, no explain | implicit | implicit |
| 3.6 | `noWhereScanReturnsAllRecords` | Scan without WHERE → 50 rows | ✅ | ✅ |
| 3.7 | `gateOffStringAelUsesLegacySelection` | Version &lt; 8.1.3 → legacy execute | skip only | skip only |

**Unit routing (not cluster):**

| ID | Java | Python |
|----|------|--------|
| R.1 | `IndexProbePlannerRoutingTest` (gate, Exp, forBin, allowsIndex) | `tests/unit/query_server_selection_test.py` |
| R.2 | `QueryPlanApiTest.supportsQuerySelectionVersionGate` | PAC `query_plan_test.py` (method exists) |

### Tier 4 — Errors & edge cases

| ID | Java test | Scenario | Python async | Python sync |
|----|-----------|----------|--------------|-------------|
| 4.1 | `executeContradictionPredicateThrowsFilteredOut` | Execute unsatisfiable → FILTERED_OUT | ✅ | ✅ |
| 4.2 | `planWithoutWhereThrows` | `.plan()` without WHERE | ❌ | ❌ |
| 4.3 | `executeValidSecondaryIndexQueryWithNoMatchesReturnsEmptyStream` | `age == 999` → SI plan, 0 rows | ✅ | — |
| 4.4 | `multipleIndexesServerAutoSelectsMatchingIndex` | age vs score auto-select | ✅ | — |

### Wire / unit (Java reference; Rust for Python stack)

| Area | Java | Rust (PAC dependency) |
|------|------|------------------------|
| Plan from explain fields | `QueryPlanTest` | `query_plan.rs` tests |
| Execute wire layout | `QueryPlanExecuteWireTest` | `index_range_wire`, `buffer` |
| Explain wire layout | `IndexProbeCommandTest` | `query_explain_command`, `query_where_wire` |
| Field TLV parse | `MsgFieldParserTest` | `as_msg_fields.rs` |

---

## TODO — Python SDK catch-up (port from Java)

Implement in `tests/integration/async/query_server_selection_test.py` (+ sync mirror where useful).

- [ ] **1.4** `planBytesStableAcrossRepeatedProbes`
- [ ] **1.7** `planForIndexHintOnNonExistentIndex` (bogus index name hint)
- [ ] **1.8** `planForIndexHintOnWrongExistingIndex`
- [ ] **2.4** `planThenExecuteConsistencyForSecondaryIndex`
- [ ] **2.7** `executeWithReadingOnlyBinsProjectsRequestedBins`
- [ ] **3.2** `forBinHintUsesLegacyExecutePath` (simple range, not only compound)
- [ ] **3.3** `forIndexHintProbesAndExecutes`
- [ ] **3.4** `queryDurationOnlyHintStillProbesAndExecutes`
- [ ] **3.7** `gateOffStringAelUsesLegacySelection` (mock/stub version gate; Java uses `cluster.setVersion`)
- [ ] **4.2** `planWithoutWhereThrows` (if Python exposes `.plan()` via PAC — else N/A)

Optional: expand sync suite to full Tier 2–4 parity with async.

---

## TODO — Tier A: index types & collection types (both clients)

**New fixture per type** — separate set or isolated index names; do not overload `qselint` numeric fixture.

Each test: create index → load data → `.where("<AEL>")` → assert explain (`SECONDARY_INDEX`, `index_name`, **`index_type`** when not DEFAULT) → assert execute rows.

| ID | IndexType | CollectionIndexType | Sample data | Sample AEL | Java | Python |
|----|-----------|---------------------|-------------|------------|------|--------|
| A.1 | INTEGER | DEFAULT | 1..N scalar | `$.n >= 10 and $.n <= 20` | covered by reference fixture | covered |
| A.2 | STRING | DEFAULT | `val1..valN` | `$.s == 'val3'` | ❌ (legacy `QueryStringTest` only) | ❌ |
| A.3 | BLOB | DEFAULT | fixed bytes | `$.b == x'...'` | ❌ (legacy `QueryBlobTest`) | ❌ |
| A.4 | GEO2DSPHERE | DEFAULT | point grid | geo AEL / `geoCompare` | ❌ (legacy `QueryGeoTest`; AEL geo TODO there) | ❌ |
| A.5 | STRING | MAPKEYS | map bins | key predicate | ❌ (legacy `QueryCollectionTest`) | ❌ |
| A.6 | STRING | MAPVALUES | map bins | value predicate | ❌ | ❌ |
| A.7 | NUMERIC | LIST | list bins | element range | ❌ | ❌ |

**Suggested class:** `QuerySelectionIndexTypesIntegrationTest` (Java); `query_selection_index_types_test.py` (Python).

---

## TODO — Tier B: data & range diversity (both clients)

Extend numeric fixture or add variant datasets.

| ID | Data pattern | Query | Expected | Java | Python |
|----|--------------|-------|----------|------|--------|
| B.1 | Sparse ages (gaps in data) | range crossing gaps | exact subset | ❌ | ❌ |
| B.2 | Full index span | `age >= 1 and age <= 50` | all 50 keys | ❌ | ❌ |
| B.3 | Point degenerate range | `age >= 25 and age <= 25` | `[25]` | ❌ | ❌ |
| B.4 | Range above dataset max | `age >= 51 and age <= 60` | SI plan, 0 rows | partial (`age == 999`) | partial |
| B.5 | Partial bin population | some records missing `age` | define: empty vs FILTERED_OUT | ❌ | ❌ |
| B.6 | Skewed `country` distribution | PI filter on minority | correct count | ❌ | ❌ |

**Suggested class:** `QuerySelectionDataShapesIntegrationTest`.

---

## TODO — Tier C: operational / edge (both clients, lower priority)

| ID | Scenario | Notes | Java | Python |
|----|----------|-------|------|--------|
| C.1 | Partition filter + server selection | `PartitionFilter.by_range` + `.where()` | ❌ | ❌ |
| C.2 | Chunked query / `max_records` | Re-execute must reuse same plan | ❌ | ❌ |
| C.3 | Set-scoped vs namespace-level index | query set with set index | ❌ | ❌ |
| C.4 | Explain/execute without set name | namespace-wide | ❌ | ❌ |
| C.5 | Background query + selection | out of scope until product supports | ❌ | ❌ |

**Suggested class:** `QuerySelectionOperationalIntegrationTest`.

---

## TODO — Java-only maintenance

Reference impl stays authoritative; keep these green on field `44` server.

- [ ] Keep `QuerySelectionIntegrationTest` in CI gate (8.1.3+ cluster job).
- [ ] When adding Tier A/B/C scenarios here, **implement in Java first**, then port row to Python table above.
- [ ] Optional regression smoke (from implementation TODO): `QueryIntegerTest`, `QueryHintBuilderTest` on legacy path after selection merge.
- [ ] Document public `.plan()` API in user guide when E1–E7 land.

---

## Priority order

1. **Python catch-up** — close gaps in table § "Python SDK catch-up" (1.7, 1.8, 3.3, 3.7 highest user value).
2. **Tier A** — MAPKEYS / MAPVALUES / LIST (validates field `26` INDEX_TYPE on wire); then STRING / BLOB / GEO.
3. **Tier B** — sparse, boundary, full-span ranges.
4. **Tier C** — partition + chunked plan reuse.

---

## Adding a new scenario (checklist)

1. Add test method to `QuerySelectionIntegrationTest.java` with javadoc: indexes on fixture, user AEL, expected plan + rows.
2. Update the **Already covered** tables in this doc.
3. Port to Python SDK integ (async + sync if applicable).
4. If wire-specific, add Java unit test (`QueryPlanExecuteWireTest` / `IndexProbeCommandTest`) or Rust unit test for PAC.

---

## Related legacy tests (not server selection)

These exercise **client-side index pick** or **field 43** paths. Keep for regression; **do not** treat as selection coverage:

- `QueryIntegerTest`, `QueryStringTest`, `QueryBlobTest`, `QueryGeoTest`, `QueryCollectionTest`
- `QueryOperationsTest`, `QueryHintBuilderTest`
- Python: general `tests/integration/async/query*.py` without `query_server_selection` in name
