# Full syntax of all list / map functions

This section compares the full API surface of `MapExp` and `ListExp` against the
current DSL spec and grammar, identifying what is missing or incomplete.

### Read operations — coverage summary

The DSL path element syntax maps to `getBy*` methods. The current coverage is
**complete** for all read operations:

| MapExp method | DSL path syntax | Status |
|---|---|---|
| `getByKey` | `$.m.key`, `$.m.'key'`, `$.m.1` | ✓ Covered |
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
| `size` | `$.m.count()`, `$.m.{}.count()` | ✓ Covered |

| ListExp method | DSL path syntax | Status |
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
| `size` | `$.l.count()`, `$.l.[].count()` | ✓ Covered |

Inverted variants (`!` prefix) are also covered: `{!a-c}`, `{!=1,2,3}`, `[!0:3]`, etc.

The `return:` parameter on `get()` handles `MapReturnType`/`ListReturnType` selection
(VALUE, COUNT, INDEX, RANK, KEY, KEY_VALUE, EXISTS, ORDERED_MAP, UNORDERED_MAP,
REVERSE_INDEX, REVERSE_RANK, NONE).

### Read operations — what needs attention

The `get()` function `return:` parameter currently only appears on an explicit `get()`
call. But the DSL spec says the default return type depends on context (single element
→ VALUE, multi-element map → ORDERED_MAP, multi-element list → VALUE). This implicit
behaviour needs to be documented as a formal rule so that:

```
$.m.{a-c}                      →  getByKeyRange(ORDERED_MAP, ...)  (implicit)
$.m.{a-c}.get(return: VALUE)   →  getByKeyRange(VALUE, ...)        (explicit override)
$.m.{a-c}.get(return: COUNT)   →  getByKeyRange(COUNT, ...)        (explicit override)
$.l.[0:3]                      →  getByIndexRange(VALUE, ...)       (implicit)
$.l.[0:3].get(return: INDEX)   →  getByIndexRange(INDEX, ...)       (explicit override)
```

### Remove operations — coverage summary

The `remove()` function combined with path context covers all `removeBy*` methods.
The path element determines what to select, and `remove()` applies the removal:

| API method | DSL syntax | Status |
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
where remove can return COUNT, VALUE, etc. The DSL's `remove()` in expression context
therefore does not need a `return:` parameter — the `!` prefix handles inversion.

### Mutation functions — MISSING: parameters and verb-based write semantics

**This is the primary gap.** The grammar defines all mutation functions as parameterless
stubs (`'insert' '()'`, `'set' '()'`, etc.) but the API methods require parameters
(values to insert, amounts to add, etc.).

#### Design principle: verbs carry write intent

Following the fluent client's `CdtGetOrRemoveBuilder` pattern, the **function name
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
    | pathFunctionGet
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

For reference, this is the full list of DSL path functions after these changes:

**Read functions:**

| Function | Parameters | Description |
|---|---|---|
| `get()` | `type:`, `return:` | Explicit type and return type override |
| `count()` | none | Element count (maps to `size()` for full bins) |
| `exists()` | none | Returns BOOLEAN |
| `asInt()` | none | Cast to INTEGER |
| `asFloat()` | none | Cast to FLOAT |
| `type()` | none | Returns the data type as INTEGER |

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

