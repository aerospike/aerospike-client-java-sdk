# Full syntax of all list / map functions

> **AEL v2** — Quick overview: [cheat-sheet.md](cheat-sheet.md). Selector spellings in examples follow [path-and-selectors.md](path-and-selectors.md) and [DECISIONS.md](DECISIONS.md). Read terminals (`getKeys()`, implicit get, …): [map-keys-values-and-filter-chains.md](map-keys-values-and-filter-chains.md), [path-operations.md](path-operations.md). **Last reviewed:** 2026-05-20.

This section compares the full API surface of `MapExp` and `ListExp` against the
current AEL spec and grammar, identifying what is missing or incomplete.

### Read operations — coverage summary

The AEL path element syntax maps to `getBy*` methods for **addressing** elements. **Returning** values vs keys vs tree uses **read terminals** (implicit get, `getKeys()`, …), not `get(return: …)` — see the table below and [map-keys-values-and-filter-chains.md](map-keys-values-and-filter-chains.md).

| MapExp method | AEL path syntax | Status |
|---|---|---|
| `getByKey` | `$.m.key`, `$.m.'key'`, `$.m."1"` (digit-only keys quoted) | ✓ Covered |
| `getByKeyRange` | `$.m.{a-c}`, `$.m.{a-}`, `$.m.{-c}` | ✓ Covered |
| `getByKeyList` | `$.m.{a,b,c}` | ✓ Covered |
| `getByKeyRelativeIndexRange` (open) | `$.m.{0:~key}` | ✓ Covered |
| `getByKeyRelativeIndexRange` (bounded) | `$.m.{0:1~key}` | ✓ Covered |
| `getByValue` | `$.m.{=val}` | ✓ Covered |
| `getByValueRange` | `$.m.{=5:8}`, `$.m.{=5:}` | ✓ Covered |
| `getByValueList` | `$.m.{=1,2,3}` | ✓ Covered |
| `getByValueRelativeRankRange` (open) | `$.m.{#-1:~ref}` | ✓ Covered |
| `getByValueRelativeRankRange` (bounded) | `$.m.{#-1:1~ref}` | ✓ Covered |
| `getByIndex` | `$.m.{1}` | ✓ Covered |
| `getByIndexRange` (open) | `$.m.{1:}` | ✓ Covered |
| `getByIndexRange` (bounded) | `$.m.{1:3}` | ✓ Covered |
| `getByRank` | `$.m.{#1}` | ✓ Covered |
| `getByRankRange` (open) | `$.m.{#1:}` | ✓ Covered |
| `getByRankRange` (bounded) | `$.m.{#1:3}` | ✓ Covered |
| `size` | `$.m.count()` | ✓ Covered |

| ListExp method | AEL path syntax | Status |
|---|---|---|
| `getByIndex` | `$.l.[1]` | ✓ Covered |
| `getByIndexRange` (open) | `$.l.[1:]` | ✓ Covered |
| `getByIndexRange` (bounded) | `$.l.[1:3]` | ✓ Covered |
| `getByValue` | `$.l.[=val]` | ✓ Covered |
| `getByValueRange` | `$.l.[=5:8]`, `$.l.[=5:]` | ✓ Covered |
| `getByValueList` | `$.l.[=1,2,3]` | ✓ Covered |
| `getByValueRelativeRankRange` (open) | `$.l.[#-1:~ref]` | ✓ Covered |
| `getByValueRelativeRankRange` (bounded) | `$.l.[#-1:1~ref]` | ✓ Covered |
| `getByRank` | `$.l.[#1]` | ✓ Covered |
| `getByRankRange` (open) | `$.l.[#1:]` | ✓ Covered |
| `getByRankRange` (bounded) | `$.l.[#1:3]` | ✓ Covered |
| `size` | `$.l.count()` | ✓ Covered |

Inverted variants (`!` prefix) are also covered: `{!a-c}`, `{!=1,2,3}`, `[!0:3]`, etc.

### Whole-bin / whole-path collection typing and `count()`

**Empty selectors** **`$.path.{}`** and **`$.path.[]`** are **not** valid spellings for map/list typing. Name the collection type with path suffixes **`$.path:MAP`** and **`$.path:LIST`** whenever the expression must treat the value as a map or list (for example, equality against a map or list literal, or static inference).

For **element count** on a path that already denotes **one** LIST or MAP value, write **`$.bin.count()`** (or **`$.nested.field.count()`**). **Do not** append **`{}`** / **`[]`** only to distinguish list from map: compilation targets a **CDT count**–class primitive ([DECISIONS.md](DECISIONS.md), [open-issues.md](open-issues.md) §4).

**What the path selects vs what you read back:** selectors and navigation (`$.m.{a:b}`, `$.l.[0:3]`, …) address **which** elements the CDT op targets, analogous to the `getBy*` family in `MapExp` / `ListExp`. The **shape** of the result is **not** controlled by a `get(return: …)` parameter on a generic `get()`. Instead, AEL uses **read terminals**: **implicit get** (default **values**, same intent as `getValues()` without calling it), plus explicit **`getKeys()`**, **`getKeyValues()`**, **`getTree()`**, **`count()`**, **`exists()`**, and write terminals **`remove()`**, **`modify()`**, **`setTo()`**, inserts, etc. ([DECISIONS.md](DECISIONS.md), [map-keys-values-and-filter-chains.md](map-keys-values-and-filter-chains.md)). The compiler still maps each expression to the appropriate `MapReturnType` / `ListReturnType` on the wire — that is an implementation detail, not surface syntax.

Per [DECISIONS.md](DECISIONS.md), **`.get({selector})`** is a **bridge** read form (selector as argument) where the compiler cannot fold **all** selector shapes into “**path only** + **no-arg** read terminal” while staying aligned with the **`MapExp` / `ListExp`** surface and server **CTX** / wire rules for that release. That is **implementation coverage** (codegen + library + server), not an undecided AEL syntax choice. Use the tables in this file to audit which **`getBy*`** paths are expressible as plain path + terminal vs bridge for a given SDK.

It is **not** the same surface as a hypothetical **`get(return: …)`** parameter on a generic **`get()`** call.

#### Bridge: concrete examples (map)

**Path + implicit get** — selection only on **`$.…`** (usual):

```text
$.scores.{@bronze,silver,gold}
```

**Bridge** — the same map **key-list** selector passed as the argument to **`get`** (and **`remove`**); **`.get`** returns a **list**-shaped read per [DECISIONS.md](DECISIONS.md):

```text
$.scores.get({@bronze,silver,gold})
$.scores.remove({@bronze,silver,gold})
```

Contrast **path + terminal** remove (equivalent intent in many lowers):

```text
$.scores.{@bronze,silver,gold}.remove()
```

#### Bridge: list slice (same idea, `[…]` inside the call)

**Path + implicit get:**

```text
$.log.[0:10]
```

**Bridge:**

```text
$.log.get([0:10])
```

[DECISIONS.md](DECISIONS.md) writes **`.get({selector})`** for the map case; **list** reads use the same **positional** pattern with a **`[…]`** selector inside **`get(…)`**, matching the list selector grammar in [path-and-selectors.md](path-and-selectors.md).

#### Where this came from

The **`MapExp` / `ListExp`** comparison tables above are grounded in the **Java client** surface (e.g. **`getByKeyList`**, **`removeByKeyList`**, …), where the **selection** is often a **separate argument** to a `getBy*` / `removeBy*` helper. AEL’s **primary** model puts that selection on the **`$.…`** path and ends with **implicit get** or **`remove()`** with **no** selector argument. The **bridge** spelling exists so authors (and compilers) can still express **“bin + selector in one call”** when lowering matches those APIs or when path-only folding is not available for a given shape/SDK — see the **bridge** row in [DECISIONS.md](DECISIONS.md). The word **bridge** is spec vocabulary for that optional second spelling; it is not a separate third read model.

### Read operations — shape of results

| User intent | AEL (illustrative) | Notes |
|-------------|-------------------|--------|
| Values at path (default) | `$.m.{@a,@b}` or path ending after selectors | **Implicit get** — no trailing call required; **`getValues()`** is optional/redundant. |
| Keys | `….getKeys()` | Use this terminal when the result must be keys. |
| Key–value pairs | `….getKeyValues()` | |
| Preserving structure | `….getTree()` | |
| Size / match count | `….count()` | See [DECISIONS.md](DECISIONS.md) on **single-select** vs **multi-select** `count()` semantics. |
| Presence | `….exists()` | |

Do **not** model **`VALUE`**, **`COUNT`**, **`INDEX`**, **`RANK`**, **`KEY`**, **`KEY_VALUE`**, **`EXISTS`**, **`ORDERED_MAP`**, **`UNORDERED_MAP`**, **`REVERSE_INDEX`**, **`REVERSE_RANK`**, **`NONE`** as a single **`get(return: …)`** knob in user-facing AEL; those enums remain relevant to the Java **`MapReturnType`** / **`ListReturnType`** APIs and to codegen, not as the primary author-facing control.

### Remove operations — coverage summary

The `remove()` function combined with path context covers all `removeBy*` methods.
The path element determines what to select, and `remove()` applies the removal:

| API method | AEL syntax | Status |
|---|---|---|
| `removeByKey` | `$.m.key.remove()` | ✓ Concept covered |
| `removeByKeyRange` | `$.m.{a-c}.remove()` | ✓ Concept covered |
| `removeByKeyList` | `$.m.{a,b,c}.remove()` | ✓ Concept covered |
| `removeByKeyRelativeIndexRange` | `$.m.{0:1~key}.remove()` | ✓ Concept covered |
| `removeByValue` | `$.m.{=val}.remove()` | ✓ Concept covered |
| `removeByValueRange` | `$.m.{=5:8}.remove()` | ✓ Concept covered |
| `removeByValueList` | `$.m.{=1,2,3}.remove()` | ✓ Concept covered |
| `removeByValueRelativeRankRange` | `$.m.{#-1:1~ref}.remove()` | ✓ Concept covered |
| `removeByIndex` | `$.m.{1}.remove()`, `$.l.[1].remove()` | ✓ Concept covered |
| `removeByIndexRange` | `$.m.{1:3}.remove()`, `$.l.[1:3].remove()` | ✓ Concept covered |
| `removeByRank` | `$.m.{#1}.remove()`, `$.l.[#1].remove()` | ✓ Concept covered |
| `removeByRankRange` | `$.m.{#1:3}.remove()`, `$.l.[#1:3].remove()` | ✓ Concept covered |

Inverted removes use the `!` prefix: `$.m.{!a-c}.remove()` → removeByKeyRange with
INVERTED, which removes everything **outside** the range.

**Note on expression context:** In `MapExp`/`ListExp`, all write operations (including
remove) return the **modified collection**, not the removed items. The `returnType`
parameter on remove methods in expression context only accepts `NONE` (remove matched)
or `INVERTED` (remove unmatched). This differs from operation context (`MapOperation`)
where remove can return COUNT, VALUE, etc. The AEL's `remove()` in expression context
therefore does not need a `return:` parameter — the `!` prefix handles inversion.

### Mutation functions — MISSING: parameters and verb-based write semantics

**This is the primary gap.** The grammar defines all mutation functions as parameterless
stubs (`'insert' '()'`, `'set' '()'`, etc.) but the API methods require parameters
(values to insert, amounts to add, etc.).

#### Design principle: verbs carry write intent

Following the SDK's `CdtGetOrRemoveBuilder` pattern, the **function name
encodes the write mode** instead of using explicit `writeFlags:` parameters. The key
or index comes from the path navigation, not the function arguments:

| Verb | Write mode | Maps to (MapExp) | Maps to (ListExp) |
|---|---|---|---|
| `setTo(value)` | Unconditional (DEFAULT) | `MapExp.put(DEFAULT, key, value, bin)` | `ListExp.set(DEFAULT, index, value, bin)` |
| `insert(value)` | CREATE_ONLY (fail if exists) | `MapExp.put(CREATE_ONLY, key, value, bin)` | `ListExp.insert(DEFAULT, index, value, bin)` |
| `update(value)` | UPDATE_ONLY (fail if missing) | `MapExp.put(UPDATE_ONLY, key, value, bin)` | N/A (not applicable for lists) |
| `add(amount)` | Numeric increment | `MapExp.increment(DEFAULT, key, amount, bin)` | `ListExp.increment(DEFAULT, index, amount, bin)` |

For maps, `setTo` is an upsert — it creates the key if it doesn't exist, or overwrites
it if it does. For lists, `setTo` overwrites the value at the given index. The name
`setTo` was chosen over `upsert` because it reads naturally for both data structures
(`$.m.name.setTo('Alice')`, `$.l.[0].setTo(99)`) and `insert`/`update` cover the
cases where create-only or update-only semantics are needed.

#### Map mutation functions

| Function | Current grammar | Proposed syntax |
|---|---|---|
| `setTo` | `set()` ← stub | `$.m.key.setTo(value_expr)` |
| `insert` | `insert()` ← stub | `$.m.key.insert(value_expr)` |
| `update` | not in grammar | `$.m.key.update(value_expr)` |
| `putItems` | not in grammar | `$.m.putItems(map_expr)` |
| `add` | `increment()` ← stub | `$.m.key.add(amount_expr)` |
| `clear` | `clear()` | `$.m.clear()` ✓ |

The key is always provided by the path context. The function takes only the value.

**Map write examples:**
```
$.m.name.setTo('Alice')                  unconditional: set key "name" to "Alice"
$.m.name.insert('Alice')                 create only: fail if "name" already exists
$.m.name.update('Alice')                 update only: fail if "name" doesn't exist
$.m.($.keyBin).setTo($.valueBin)         key and value from other bins
$.m.putItems({a: 1, b: 2, c: 3})        insert/update multiple literal entries
$.m.putItems(($.mapBin))                 insert/update entries from another bin
```

Exp equivalents:
```java
MapExp.put(new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.DEFAULT),
    Exp.val("name"), Exp.val("Alice"), Exp.mapBin("m"))
MapExp.put(new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.CREATE_ONLY),
    Exp.val("name"), Exp.val("Alice"), Exp.mapBin("m"))
MapExp.put(new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.UPDATE_ONLY),
    Exp.val("name"), Exp.val("Alice"), Exp.mapBin("m"))
MapExp.putItems(MapPolicy.Default, mapExp, Exp.mapBin("m"))
```

**Map add (increment) examples:**
```
$.m.counter.add(1)                       increment map key "counter" by 1
$.m.counter.add(-($.decrement))          decrement by a bin value
$.m.balance.add($.amount)                add amount from another bin
```

Exp equivalent:
```java
MapExp.increment(MapPolicy.Default, Exp.val("counter"), Exp.val(1), Exp.mapBin("m"))
```

#### List mutation functions

| Function | Current grammar | Proposed syntax |
|---|---|---|
| `append` | `append()` ← stub | `$.l.append(value_expr)` |
| `appendItems` | not in grammar | `$.l.appendItems(list_expr)` |
| `insert` | `insert()` ← stub | `$.l.[3].insert(value_expr)` |
| `insertItems` | not in grammar | `$.l.[3].insertItems(list_expr)` |
| `setTo` | `set()` ← stub | `$.l.[3].setTo(value_expr)` |
| `add` | `increment()` ← stub | `$.l.[3].add(amount_expr)` |
| `clear` | `clear()` | `$.l.clear()` ✓ |
| `sort` | `sort()` ← stub | `$.l.sort()` or `$.l.sort(dropDuplicates: true)` |

For lists, `insert` means "insert at this position, shifting existing elements right"
while `setTo` means "overwrite the value at this position". Both take the index from
the path context.

**List examples:**
```
$.l.append(42)                           append to end
$.l.append($.valueBin)                   append value from another bin
$.l.appendItems([1, 2, 3])              append multiple literal values
$.l.appendItems(($.otherList))           append items from another list bin
$.l.[0].insert('first')                  insert at index 0, shifting existing
$.l.[3].insertItems([10, 20])           insert multiple at index 3
$.l.[0].setTo(99)                        overwrite first element
$.l.[-1].setTo('last')                   overwrite last element
$.l.[0].add(10)                          increment first element by 10
$.l.[2].add(-($.delta))                  decrement third element by a bin value
```

Exp equivalents:
```java
ListExp.append(ListPolicy.Default, Exp.val(42), Exp.listBin("l"))
ListExp.appendItems(ListPolicy.Default, Exp.val(List.of(1, 2, 3)), Exp.listBin("l"))
ListExp.insert(ListPolicy.Default, Exp.val(0), Exp.val("first"), Exp.listBin("l"))
ListExp.set(ListPolicy.Default, Exp.val(0), Exp.val(99), Exp.listBin("l"))
ListExp.increment(ListPolicy.Default, Exp.val(0), Exp.val(10), Exp.listBin("l"))
```

**List sort examples:**
```
$.l.sort()                               sort preserving duplicates
$.l.sort(dropDuplicates: true)           sort and remove duplicates
```

Exp equivalents:
```java
ListExp.sort(ListSortFlags.DEFAULT, Exp.listBin("l"))
ListExp.sort(ListSortFlags.DROP_DUPLICATES, Exp.listBin("l"))
```

### Optional modifiers: noFail and order

While the verb name carries the primary write intent, two optional modifiers may be
needed in some cases:

**`noFail`** — suppress errors when the write mode condition isn't met:
```
$.m.key.insert(value, noFail: true)      CREATE_ONLY + NO_FAIL: silently skip if exists
$.m.key.update(value, noFail: true)      UPDATE_ONLY + NO_FAIL: silently skip if missing
$.l.append(value, noFail: true)          NO_FAIL on append
```

**`order`** — specify the collection ordering when it matters:
```
$.l.append(value, order: ORDERED)        insert into correct sorted position
$.m.key.setTo(value, order: KEY_ORDERED) set with key-ordered map policy
```

These are optional named parameters, not write flags. The defaults (no `noFail`,
`UNORDERED` order) apply when omitted.

### Mutation functions — grammar changes needed

The parameterless stubs need to become proper parameterised rules:

```
pathFunction
    : pathFunctionCast
    | pathFunctionExists
    | pathFunctionRead
    | pathFunctionCount
    | pathFunctionRemove
    | pathFunctionSetTo
    | pathFunctionInsert
    | pathFunctionUpdate
    | pathFunctionAdd
    | pathFunctionPutItems
    | pathFunctionAppend
    | pathFunctionAppendItems
    | pathFunctionInsertItems
    | pathFunctionClear
    | pathFunctionSort
    ;

pathFunctionRead
    : 'getKeys' '(' ')'
    | 'getValues' '(' ')'
    | 'getKeyValues' '(' ')'
    | 'getTree' '(' ')'
    ;

pathFunctionRemove: 'remove' '()';

pathFunctionSetTo: 'setTo' '(' expression mutationOpts? ')';

pathFunctionInsert: 'insert' '(' expression mutationOpts? ')';

pathFunctionUpdate: 'update' '(' expression mutationOpts? ')';

pathFunctionAdd: 'add' '(' expression mutationOpts? ')';

pathFunctionPutItems: 'putItems' '(' expression mutationOpts? ')';

pathFunctionAppend: 'append' '(' expression mutationOpts? ')';

pathFunctionAppendItems: 'appendItems' '(' expression mutationOpts? ')';

pathFunctionInsertItems: 'insertItems' '(' expression mutationOpts? ')';

pathFunctionClear: 'clear' '()';

pathFunctionSort: 'sort' '(' sortParams? ')';

mutationOpts: (',' mutationOpt)+;
mutationOpt
    : 'noFail' ':' booleanOperand
    | 'order' ':' cdtOrder
    ;

cdtOrder: 'UNORDERED' | 'ORDERED' | 'KEY_ORDERED' | 'KEY_VALUE_ORDERED';

sortParams: 'dropDuplicates' ':' booleanOperand;
```

### Complete function reference

For reference, this is the full list of AEL path functions after these changes:

**Read terminals (after path / selectors):**

| Terminal | Parameters | Description |
|---|---|---|
| *(none)* | — | **Implicit get** — default is **values** at the path; same meaning as `getValues()` but **prefer omitting** `getValues()` unless it aids clarity ([map-keys-values-and-filter-chains.md](map-keys-values-and-filter-chains.md)). |
| `getValues()` | none | Explicit spelling of the default value read (redundant with implicit get). |
| `getKeys()` | none | Keys per CDT semantics for the addressed elements. |
| `getKeyValues()` | none | `(key, value)` pairs where applicable. |
| `getTree()` | none | Structure-preserving tree / ordered map shape where applicable. |
| `count()` | none | Size or match count per [DECISIONS.md](DECISIONS.md). |
| `exists()` | none | `BOOLEAN` — whether the path addresses any elements. |
| `asInt()` | none | Cast to INTEGER. |
| `asFloat()` | none | Cast to FLOAT. |
| `type()` | none | Returns data type as INTEGER. |

Path **typing** uses **`$.path:T`** and loop **`@:T`** ([DECISIONS.md](DECISIONS.md)), not a `type:` parameter on a generic path `get()`.

**Write functions (map) — key from path context:**

| Function | Parameters | Write mode | Description |
|---|---|---|---|
| `setTo(value)` | value expr | DEFAULT | Unconditional create-or-overwrite |
| `insert(value)` | value expr | CREATE_ONLY | Create, fail if exists |
| `update(value)` | value expr | UPDATE_ONLY | Update, fail if missing |
| `add(amount)` | amount expr | DEFAULT | Increment numeric value |
| `putItems(map)` | map expr | DEFAULT | Insert/update multiple entries |
| `remove()` | none | — | Remove matched elements |
| `clear()` | none | — | Remove all elements |

**Write functions (list) — index from path context where applicable:**

| Function | Parameters | Description |
|---|---|---|
| `append(value)` | value expr | Append to end of list |
| `appendItems(list)` | list expr | Append multiple to end |
| `insert(value)` | value expr | Insert at index, shifting elements right |
| `insertItems(list)` | list expr | Insert multiple at index |
| `setTo(value)` | value expr | Overwrite value at index |
| `add(amount)` | amount expr | Increment numeric value at index |
| `remove()` | none | Remove matched elements |
| `clear()` | none | Remove all elements |
| `sort()` | optional `dropDuplicates: true` | Sort the list |

All write functions accept optional `noFail: true` and `order: ...` modifiers.

