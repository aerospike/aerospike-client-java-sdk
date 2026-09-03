# CDT path-style operations — legacy `CTX` mapping (engineering)

**Not the application API.** Application code should use [CDT path operations](../cdt-path-operations.md) (`onEachChild`, `collect*`, `modifyBy`, `removeMatches`). This file is the engineering mapping onto `CdtOperation.selectByPath` / `modifyByPath` for legacy-client ports.

**Audience:** SDK / client engineering  
**Status:** Shipped for **`Exp`** filters and modify bodies. **`String` (AEL)** and **`PreparedAel`** overloads compile but throw **`UnsupportedOperationException`** until AEL compilation supports path-scoped fragments (`com.aerospike.client.sdk.cdt.path.CdtPathExpressionAel`).  
**Depends on:** Aerospike server **≥ 8.1.1** path expression support (`CTX.allChildren`, `CTX.allChildrenWithFilter`, `CdtOperation.selectByPath` / `modifyByPath`, loop-variable `Exp` APIs, etc.)

**Entry points:** `BinBuilder` / `QueryBinBuilder` / `QueryBuilderBinBuilder` expose `onEachChild()` at the bin root; nested navigation continues on `CdtGetOrRemoveBuilder` (writes) or `CdtReadOnlyBuilder` (reads). Options types: `com.aerospike.client.sdk.cdt.path.CdtCollectOptions`, `CdtModifyOptions`.

---

## 1. Goals

1. **Hide `CTX` from application code** — navigation and iteration are expressed with the same style as today’s nested CDT API (`onMapKey`, `onListIndex`, …).
2. **Align iteration with the server** — one concept: **`CTX.allChildren()`** / **`CTX.allChildrenWithFilter(Exp)`**; the SDK exposes this as **`onEachChild()`** with an optional filter (not separate `onEachListElement` / `onEachMapEntry` entry points unless later added as convenience overloads).
3. **Avoid redundant “path” in method names** — the path is already established by chained navigation; terminals must not repeat “path”.
4. **Avoid naming collisions** — terminals for multi-match reads must **not** reuse **`getValues()` / `getKeys()`** (already used for classic CDT reads at a single selection).
5. **Support filter predicates as `Exp` or as AEL string** — wire format is always `Exp`; AEL is compiled by the SDK before building `CTX.allChildrenWithFilter`.

---

## 2. API specification

### 2.1 Navigation (extends existing CDT fluent builders)

Navigation methods accumulate an internal **context list** (implementation maps to `List<CTX>` / equivalent used when assembling `CdtOperation.*`).

| Method | Semantics | Legacy mapping |
|--------|-------------|----------------|
| *(existing)* `onMapKey(...)`, `onListIndex(...)`, … | Fixed single step into map/list | `CTX.mapKey` / `CTX.listIndex` / … |
| **`onEachChild()`** | Iterate **all** children of the current map or list | **`CTX.allChildren()`** |
| **`onEachChild(Exp filter)`** | Iterate children matching predicate | **`CTX.allChildrenWithFilter(filter)`** |
| **`onEachChild(String aelFilter)`** | Same as `onEachChild(Exp)`; **throws `UnsupportedOperationException` today** | **`CTX.allChildrenWithFilter(compiledExp)`** |

**Design decision:** Use **`onEachChild`** only (not `onEachListElement` / `onEachMapEntry`) because **`allChildren()`** is identical at the wire level for maps and lists; type-specific helpers belong **inside** the predicate (`MapExp` / `ListExp` / small SDK facades), not as separate iteration entry points.

**Ordering:** Call order must mirror the legacy `ctx1, ctx2, …` order passed to `selectByPath` / `modifyByPath`.

---

### 2.2 Terminals (multi-match operations)

These apply only when the accumulated context includes at least one **`onEachChild`** segment (implementation may enforce this; invalid combinations should fail fast with a clear error).

| Method | Semantics | Legacy mapping |
|--------|-------------|----------------|
| **`collectValues()`** | Return flat list of matched **values** | `CdtOperation.selectByPath(bin, SELECT_VALUE \| flags, ctx…)` |
| **`collectKeys()`** | Return matched **map keys** (where applicable) | `selectByPath` with **MAP_KEY** flag |
| **`collectKeyValues()`** | Return **(key, value)** pairs | `selectByPath` with **MAP_KEY_VALUE** flag |
| **`collectTree()`** | Return **matching tree** structure | `selectByPath` with **MATCHING_TREE** flag |
| **`collect*(Consumer<CdtCollectOptions>)`** | Same as above with **noFail** etc. | OR in `SELECT_NO_FAIL` (and related) into flags |
| **`modifyBy(Exp newValueExpression)`** | Replace **each** matched **leaf** with expression result | `CdtOperation.modifyByPath(bin, modifyFlags, newValueExpression, ctx…)` |
| **`modifyBy(Exp e, Consumer<CdtModifyOptions>)`** | Same with options | modify flags |
| **`removeMatches()`** | Remove **each** matched element (container element, not only leaf semantics — follow server rules for current path shape) | `modifyByPath` with **`Exp.removeResult()`** |
| **`removeMatches(Consumer<CdtModifyOptions>)`** | Same with options | modify flags |

**Naming decisions:**

- Use **`collect*`** instead of `get*` or `pathSelect*` to avoid collision with **`getValues()`** / **`getKeys()`** and to drop redundant “path”.
- Use **`modifyBy(Exp)`** instead of bare **`modify`** to avoid ambiguity with other “modify record/bin” wording.
- Use **`removeMatches()`** instead of bare **`remove`** to avoid collision with existing single-selection remove helpers and to stress **multi-match** deletion.

**Read-side parity (if exposed):** same navigation + **`collect*`** can compile to **`CdtExp.selectByPath`** for expression-read / synthetic bin patterns; mapping is the same `ctx` chain and select flags.

---

### 2.3 Options objects

Follow existing SDK patterns (`Consumer<ExpressionReadOptions>`, etc.):

- **`CdtCollectOptions`** — e.g. `noFail(boolean)`, return shape if not fixed by method name.
- **`CdtModifyOptions`** — e.g. `noFail`; used by both `modifyBy` and `removeMatches`.

Exact flag names should match **`Exp.SELECT_*`** / **`Exp.MODIFY_*`** (or the client’s public constants) internally.

---

### 2.4 AEL in filters (optional product decision)

| Surface | Behavior |
|---------|----------|
| `onEachChild(String ael)` | Compile **only** as a **boolean filter** in the **current iteration scope** (define whether full AEL is allowed or a restricted subset). |
| `onEachChild(Exp e)` | No compilation; direct use. |

**Engineering note:** Wire always uses **`Exp`**. AEL support implies a compiler path from string → `Exp` with correct **loop-variable** binding for the innermost `onEachChild`. If the full AEL compiler cannot express that yet, either ship **Exp-only** first or implement a **narrow “filter fragment”** grammar and document it.

---

## 3. Legacy mapping summary

| SDK | Legacy client |
|----------------|----------------|
| `onEachChild()` | `CTX.allChildren()` |
| `onEachChild(Exp)` | `CTX.allChildrenWithFilter(Exp)` |
| `onEachChild(String)` | `CTX.allChildrenWithFilter(parseAelToExp(...))` — throws today |
| `collectValues()` / `collectKeys()` / … | `CdtOperation.selectByPath(bin, flags, ctx…)` |
| `modifyBy(Exp)` | `CdtOperation.modifyByPath(bin, modifyFlags, Exp, ctx…)` |
| `removeMatches()` | `CdtOperation.modifyByPath(bin, modifyFlags, Exp.build(Exp.removeResult()), ctx…)` |
| Predicate / modify body | `Exp.*LoopVar`, `MapExp` / `ListExp`, literals, etc. |

---

## 4. Examples

### 4.1 Select all `price` fields under `store → book → *`

**Legacy:**

```java
CTX c1 = CTX.mapKey(Value.get("book"));
CTX c2 = CTX.allChildren();
CTX c3 = CTX.mapKey(Value.get("price"));
Operation op = CdtOperation.selectByPath("store", Exp.SELECT_VALUE, c1, c2, c3);
```

**SDK:**

```java
session.update(key)
    .bin("store")
        .onMapKey("book")
        .onEachChild()
        .onMapKey("price")
        .collectValues()
    .execute();
```

---

### 4.2 Filter with **`Exp`**: books with `price < 10`, then collect `title` values

**Legacy:**

```java
CTX c1 = CTX.mapKey(Value.get("book"));
CTX c2 = CTX.allChildrenWithFilter(
    Exp.lt(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
            Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
        Exp.val(10.0)));
CTX c3 = CTX.allChildrenWithFilter(
    Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title")));
Operation op = CdtOperation.selectByPath("store", Exp.SELECT_VALUE, c1, c2, c3);
```

**SDK:**

```java
.bin("store")
    .onMapKey("book")
    .onEachChild(Exp.lt(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
            Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
        Exp.val(10.0)))
    .onEachChild(Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title")))
    .collectValues()
```

---

### 4.3 Filter with **AEL** (same semantics as 4.2; throws `UnsupportedOperationException` until the compiler supports path-scoped fragments)

```java
.bin("store")
    .onMapKey("book")
    .onEachChild("@.price < 10.0")
    .onEachChild("@key == 'title'")
    .collectValues()
```

---

### 4.4 Modify: 10% discount on each `price` under `store → book → *`

**Legacy:**

```java
CTX book = CTX.mapKey(Value.get("book"));
CTX all = CTX.allChildren();
CTX price = CTX.mapKey(Value.get("price"));
Exp modify = Exp.mul(Exp.floatLoopVar(LoopVarPart.VALUE), Exp.val(0.9));
Operation op = CdtOperation.modifyByPath("store", Exp.MODIFY_DEFAULT, modify, book, all, price);
```

**SDK:**

```java
.bin("store")
    .onMapKey("book")
    .onEachChild()
    .onMapKey("price")
    .modifyBy(Exp.mul(Exp.floatLoopVar(LoopVarPart.VALUE), Exp.val(0.9)))
```

---

### 4.5 Remove: out-of-stock books (`inStock == false`)

**Legacy:**

```java
CTX c1 = CTX.mapKey(Value.get("book"));
CTX c2 = CTX.allChildrenWithFilter(
    Exp.eq(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.BOOL,
            Exp.val("inStock"), Exp.mapLoopVar(LoopVarPart.VALUE)),
        Exp.val(false)));
Operation op = CdtOperation.modifyByPath(
    "store", Exp.MODIFY_DEFAULT, Exp.build(Exp.removeResult()), c1, c2);
```

**SDK — `Exp` filter:**

```java
.bin("store")
    .onMapKey("book")
    .onEachChild(Exp.eq(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.BOOL,
            Exp.val("inStock"), Exp.mapLoopVar(LoopVarPart.VALUE)),
        Exp.val(false)))
    .removeMatches()
```

**SDK — AEL filter (throws `UnsupportedOperationException` today):**

```java
.bin("store")
    .onMapKey("book")
    .onEachChild("@.inStock == false")
    .removeMatches()
```

---

### 4.6 Multi-bin update (same session style as existing examples)

```java
session.update(customerDataSet.id(500))
    .bin("scores").listAppendItems(List.of(50, 60, 70))
    .bin("store")
        .onMapKey("book")
        .onEachChild()
        .onMapKey("price")
        .collectValues()
    .bin("inventory").mapUpsertItems(Map.of("figs", 12))
    .bin("nested").onMapKey("team2").onMapKey("members").listAppend("Quinn")
    .bin("nested").onMapKey("team1").onMapKey("members").listSize()
    .execute();
```

---

## 5. Remaining work

The fluent surface in §2 is shipped. Left:

- **AEL path fragments** — `onEachChild(String)` / `modifyBy(String)` / `PreparedAel` still throw `UnsupportedOperationException` (`CdtPathExpressionAel`).
- **AEL syntax page** — `docs/ael/path-expressions.md` still describes the low-level `CTX` / `CdtOperation` mapping; application Java should follow [CDT path operations](../cdt-path-operations.md).

---

## 6. References (internal)

- Server / client behavior and flag meanings: product docs on path expressions.
- Existing tests using `CTX.allChildren`, `allChildrenWithFilter`, `CdtOperation.selectByPath` / `modifyByPath`.
- AEL path-expression syntax (separate from this Java API): `docs/ael/path-expressions.md`.
