# AEL Path Expressions — Syntax Proposal (v2)

## Background

Aerospike server 8.1.1+ introduces **path expressions**: a way to iterate over all children
of a nested CDT (list or map), optionally filter them, and then **select**, **modify**, or
**remove** the matching elements — all in a single server-side operation.

The Java client exposes this through:

| Java API | Purpose |
|----------|---------|
| `CdtExp.selectByPath(type, flags, bin, ctx…)` | Read: extract values from nested structures |
| `CdtExp.modifyByPath(type, flags, modifyExp, bin, ctx…)` | Write: transform values in nested structures |
| `CdtOperation.selectByPath(binName, flags, ctx…)` | Read via Operation (same semantics) |
| `CdtOperation.modifyByPath(binName, flags, modifyExp, ctx…)` | Write via Operation (same semantics) |

Special expressions used within modify:

| Java API | Purpose |
|----------|---------|
| `Exp.removeResult()` | Used as modify expression to **delete** matched elements |

The traversal path is a chain of `CTX` objects:

| CTX factory | Meaning |
|-------------|---------|
| `CTX.mapKey(v)` | Navigate into a specific map key |
| `CTX.listIndex(i)` | Navigate into a specific list index |
| `CTX.allChildren()` | Iterate over **all** children |
| `CTX.allChildrenWithFilter(exp)` | Iterate over children matching a predicate |

Within filter and modify expressions, **loop variables** reference the current
iteration element:

| Java API | Part | Description |
|----------|------|-------------|
| `Exp.intLoopVar(VALUE)` | `VALUE` | Element value as integer |
| `Exp.floatLoopVar(VALUE)` | `VALUE` | Element value as float |
| `Exp.stringLoopVar(VALUE)` | `VALUE` | Element value as string |
| `Exp.mapLoopVar(VALUE)` | `VALUE` | Element value as map |
| `Exp.listLoopVar(VALUE)` | `VALUE` | Element value as list |
| `Exp.boolLoopVar(VALUE)` | `VALUE` | Element value as boolean |
| `Exp.stringLoopVar(MAP_KEY)` | `MAP_KEY` | Parent map key (string) |
| `Exp.intLoopVar(LIST_INDEX)` | `LIST_INDEX` | Parent list index (int) |

---

## Reference Data Structure

All examples in this document use the following bin named `store`:

```json
{
  "book": [
    {"title": "Sayings of the Century", "author": "Nigel Rees",     "category": "reference", "price": 8.95,  "inStock": true},
    {"title": "Sword of Honour",        "author": "Evelyn Waugh",   "category": "fiction",   "price": 12.99, "inStock": false},
    {"title": "Moby Dick",              "author": "Herman Melville","category": "fiction",   "price": 8.99,  "inStock": true},
    {"title": "The Lord of the Rings",  "author": "J.R.R. Tolkien", "category": "fiction",   "price": 22.99, "inStock": true},
    {"title": "Clean Code",             "author": "Robert Martin",  "category": "technical", "price": 31.99, "inStock": true}
  ],
  "music": [
    {"title": "Abbey Road",  "artist": "The Beatles", "price": 14.99, "inStock": true},
    {"title": "Dark Side",   "artist": "Pink Floyd",  "price": 11.50, "inStock": false}
  ],
  "stationery": {
    "pen":    {"price": 2.50,  "quantity": 100},
    "pencil": {"price": 1.25,  "quantity": 200},
    "eraser": {"price": 0.75,  "quantity": 50}
  }
}
```

This is stored in a single bin called `store`. The bin is a **map** with three keys:
`book` (a list of maps), `music` (a list of maps), and `stationery` (a map of maps).

---

## 1. Wildcard Operator: `*`

`*` in a path position means "iterate over all children" at that level. It maps to
`CTX.allChildren()`.

```
$.store.*                              iterate top-level keys (book, music, stationery)
$.store.book.*                         iterate all books in the list
$.store.stationery.*                   iterate all stationery items (pen, pencil, eraser)
```

### Disambiguation from `*` as WILDCARD value

| Context | Meaning | Example |
|---------|---------|---------|
| `[1, *]` | WILDCARD value literal inside brackets | `$.l.[=1,*]` |
| `$.store.*` | Iterate all children (path segment after dot) | Always between dots |

Syntactically distinct: WILDCARD-as-value appears inside `[]`/`{}` brackets;
wildcard-as-iteration appears as a bare path segment between dots.

---

## 2. Loop Variable: `@`

### Overview

Within filter and modify expressions, `@` references the current iteration element.

| Syntax | Meaning | Maps to |
|--------|---------|---------|
| `@` | Element value | `Exp.xxxLoopVar(VALUE)` |
| `@.field` | Navigate one level into element value | `MapExp.getByKey(VALUE, type, "field", mapLoopVar(VALUE))` |
| `@.field.subfield` | Navigate multiple levels into element value | Chained `MapExp.getByKey` with CTX |
| `@.[n]` | Navigate into element value at list index | `ListExp.getByIndex(VALUE, type, n, listLoopVar(VALUE))` |
| `@key` | Parent map key of this element | `Exp.stringLoopVar(MAP_KEY)` |
| `@index` | Parent list index of this element | `Exp.intLoopVar(LIST_INDEX)` |

### Why `@key` (no dot) vs `@.key` (with dot)

- `@.key` — get the field named `key` from the element value (data navigation)
- `@key` — get the map key metadata of this iteration (no dot = metadata access)

### Type inference for `@`

The type of `@` is inferred from context, like bin references:

```
@.price > 10.0                @.price is FLOAT (from literal 10.0)
@key == 'revenue'             @key is STRING (from literal 'revenue')
@ + 5                         @ is INT (from literal 5)
@ * 1.5                       @ is FLOAT (from literal 1.5)
not(@)                        @ is BOOL (from boolean context)
```

### When type cannot be inferred

When both sides are untyped (e.g., comparing `@` to another bin), use explicit `get()`:

```
@.get(type: INT) == $.otherBin.get(type: INT)
@.price > $.threshold.get(type: FLOAT)
```

Note that only one side needs to have its type specified, the AEL should derive the type of the other side.

This follows the same pattern as existing bin-to-bin comparisons in the AEL.

### Multi-level navigation within `@`

`@.address.city` navigates two levels deep into the current element:

```
*[?(@.address.city == 'Denver')]
```

**Exp equivalent:**
```java
CTX.allChildrenWithFilter(
    Exp.eq(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
            Exp.val("city"),
            MapExp.getByKey(MapReturnType.VALUE, Exp.Type.MAP,
                Exp.val("address"),
                Exp.mapLoopVar(LoopVarPart.VALUE))),
        Exp.val("Denver")
    )
)
```

### Where `@` is valid

`@` is **only** valid in two contexts:

**1. Inside a filter predicate `*[?(...)]`:**
```
$.store.book.*[?(@.price < 10)]        ✓  filter each book by price
$.store.book.*[?(@key == 'title')]     ✓  filter by parent map key
```

**2. Inside a `.modify()` or `.remove()` expression:**
```
$.store.book.*.price.modify(@ * 0.9)   ✓  discount each price by 10%
```

**`@` is NOT valid in these contexts:**
```
$.store.book.[0].price + @             ✗  no wildcard in scope
@ > 10                                 ✗  top-level expression, no iteration
let (x = @) then (${x} + 1)           ✗  @ outside filter/modify
```

The rule: `@` requires an enclosing `*` wildcard. In a filter, it refers to the
element at that `*` level. In `.modify()`, it refers to the leaf element reached
by the full path.

---

## 3. Filtered Wildcard: `*[?(…)]`

Attach a filter predicate to `*` using `[?(expression)]` syntax.

```
$.store.book.*[?(@.price > 10)]                     books priced above 10
$.store.book.*[?(@.category == 'fiction')]           fiction books
$.store.book.*[?(@.inStock == true)]                in-stock books
$.store.stationery.*[?(@.quantity > 50)]            stationery with qty > 50
$.store.book.*[?(@.price > 5 and @.price < 20)]    books in a price range
```

Maps to `CTX.allChildrenWithFilter(filterExp)`.

### Why `[?(…)]` — syntax rationale

The filter syntax follows the **JSONPath** convention (Stefan Goessner, 2007), which
is the de facto standard for path expressions in JSON data (analogous to XPath for XML).
Each character serves a specific disambiguation purpose:

- **`[` `]`** — Brackets already mean "subscript operation on the current path element"
  in the AEL (`[0]` for index, `[=5]` for value matching). They are the natural container
  for "apply an operation to this path element."
- **`?`** — Signals "this is a filter predicate" rather than an index or value lookup.
  Without it, the parser cannot distinguish `[@.price < 10]` from a complex index
  expression. The `?` is the disambiguator.
- **`(` `)`** — Delimits the boolean expression, which may itself contain parentheses
  for grouping (e.g., `[?(@.price < 10 and (@.category == 'fiction' or @.inStock))]`).
  Without inner parentheses, a closing `]` would be ambiguous if the expression
  contained brackets.

Alternatives considered:

| Syntax | Problem |
|--------|---------|
| `*{@.price < 10}` | `{}` already means map index/value in the AEL |
| `*(@.price < 10)` | Ambiguous with path functions like `.select()` |
| `*[@.price < 10]` | No `?` — ambiguous with list index access |
| `*<@.price < 10>` | `<` and `>` conflict with comparison operators |
| `*(filter: @.price < 10)` | Verbose, invented syntax unfamiliar to developers |

The JSONPath convention was chosen for least-surprise: anyone who has used JSONPath,
jq, MongoDB projections, or SQL/JSON path will recognise `[?(...)]` immediately.

### Chaining wildcards

Multiple `*` can appear in a single path, each with its own filter scope:

```
$.store.*.*[?(@.price < 10)].title
```

This iterates the top-level keys (`book`, `music`, `stationery`), then iterates
each child within, filtering for price < 10, then navigates to `title`.

### Regex filtering within path expressions

The `=~` operator (see [Regex filtering](regex-filtering.md) for full syntax, flags,
and semantics) can be used inside `*[?(...)]` filters to match on string values or
map keys:

```
$.store.book.*[?(@.title =~ /Lord.*/)]                title matches "Lord..."
$.store.book.*[?(@.author =~ /j\.r\.r\./i)]          case-insensitive match
$.store.stationery.*[?(@key =~ /pen.*/)]              keys starting with "pen"
$.store.book.*[?(@.title =~ /^the/im)]                case-insensitive, multiline
```

**Exp equivalent for `@key =~ /pen.*/`:**
```java
CTX.allChildrenWithFilter(
    Exp.regexCompare("pen.*", RegexFlag.NONE,
        Exp.stringLoopVar(LoopVarPart.MAP_KEY))
)
```

**Exp equivalent for `@.author =~ /j\.r\.r\./i`:**
```java
CTX.allChildrenWithFilter(
    Exp.regexCompare("j\\.r\\.r\\.", RegexFlag.ICASE,
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
            Exp.val("author"),
            Exp.mapLoopVar(LoopVarPart.VALUE)))
)
```

---

## 4. Select Operation: `.select()`

`select()` collects values from a path containing `*` wildcards.
Maps to `CdtExp.selectByPath()` / `CdtOperation.selectByPath()`.

```
$.store.book.*.price.select()                          default: return values
$.store.book.*.price.select(return: VALUE)              explicit: return values
$.store.stationery.*.select(return: KEY)                return parent map keys
$.store.stationery.*.select(return: KEY_VALUE)          return (key, value) pairs
$.store.book.*.select(return: MATCHING_TREE)            return structure-preserving tree
$.store.book.*.select(noFail: true)                     tolerate type mismatches
$.store.book.*.select(return: KEY, noFail: true)        combined
```

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `return` | `VALUE`, `KEY`, `KEY_VALUE`, `MATCHING_TREE` | `VALUE` | What to extract |
| `noFail` | `true`, `false` | `false` | Skip type mismatches silently |

### Return type mapping

| AEL `return:` value | `SelectFlags` constant | Numeric | Description |
|---------------------|----------------------|---------|-------------|
| `MATCHING_TREE` | `SelectFlags.MATCHING_TREE` | `0` | Tree from root to matched nodes (server default) |
| `VALUE` | `SelectFlags.VALUE` | `1` | Flat list of matched values (AEL default) |
| `KEY` | `SelectFlags.MAP_KEY` | `2` | List of parent map keys of matched nodes |
| `KEY_VALUE` | `SelectFlags.MAP_KEY_VALUE` | `3` | List of (key, value) pairs from matched map nodes |

Note: `SelectFlags.LIST_VALUE` and `SelectFlags.MAP_VALUE` are source-code aliases
for `SelectFlags.VALUE` (all = `1`). The AEL uses `VALUE` for all three.

Note: `MATCHING_TREE` is the server-level default (`0`), but the AEL defaults to
`VALUE` when `.select()` is called without arguments, since flat value extraction
is the most common use case.

### Implicit select

A path with `*` but no terminal function implicitly behaves as `select(return: VALUE)`:

```
$.store.book.*.price                   ≡  $.store.book.*.price.select()
```

---

## 5. Modify Operation: `.modify()`

`modify()` transforms each matching element in-place. The argument is a AEL expression
using `@` for the current value; the result replaces the element.
Maps to `CdtExp.modifyByPath()` / `CdtOperation.modifyByPath()`.

```
$.store.book.*.price.modify(@ * 0.9)                  10% discount
$.store.stationery.*.quantity.modify(@ + 50)           restock each by 50
$.store.stationery.*.price.modify(@ * 1.1)             10% price increase
```

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| *first argument* | AEL expression using `@` | (required) | Transformation expression |
| `noFail` | `true`, `false` | `false` | Skip type mismatches silently |

---

## 6. Remove Operation: `.remove()`

`remove()` deletes all matched elements. It maps to `CdtExp.modifyByPath()` with
`Exp.removeResult()` as the modify expression.

```
$.store.book.*[?(@.inStock == false)].remove()         remove out-of-stock books
$.store.stationery.*[?(@.quantity == 0)].remove()      remove depleted items
$.store.book.*.remove()                                 remove ALL books (clear list)
```

**Exp equivalent for removing out-of-stock books:**
```java
CTX ctx1 = CTX.mapKey(Value.get("book"));
CTX ctx2 = CTX.allChildrenWithFilter(
    Exp.eq(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.BOOL,
            Exp.val("inStock"), Exp.mapLoopVar(LoopVarPart.VALUE)),
        Exp.val(false)));

Expression removeExp = Exp.build(Exp.removeResult());
CdtOperation.modifyByPath("store", ModifyFlags.DEFAULT, removeExp, ctx1, ctx2);
```

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `noFail` | `true`, `false` | `false` | Skip type mismatches silently |

---

## 7. Complete Examples with Exp Equivalents

All examples use the [Reference Data Structure](#reference-data-structure) above.

---

### 7.1 Select all book prices

**AEL:**
```
$.store.book.*.price
```

**Exp equivalent:**
```java
CTX ctx1 = CTX.mapKey(Value.get("book"));
CTX ctx2 = CTX.allChildren();
CTX ctx3 = CTX.mapKey(Value.get("price"));

CdtOperation.selectByPath("store", SelectFlags.VALUE, ctx1, ctx2, ctx3);
```

**Result:** `[8.95, 12.99, 8.99, 22.99, 31.99]`

---

### 7.2 Select titles of cheap books (price < 10)

**AEL:**
```
$.store.book.*[?(@.price < 10.0)].*[?(@key == 'title')].select()
```

**Exp equivalent:**
```java
CTX ctx1 = CTX.mapKey(Value.get("book"));
CTX ctx2 = CTX.allChildrenWithFilter(
    Exp.lt(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
            Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
        Exp.val(10.0)));
CTX ctx3 = CTX.allChildrenWithFilter(
    Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title")));

CdtOperation.selectByPath("store", SelectFlags.VALUE, ctx1, ctx2, ctx3);
```

**Result:** `["Sayings of the Century", "Moby Dick"]`

The second `*[?(@key == 'title')]` iterates each surviving book's key-value pairs and
picks only the `title` entry. This is how the API works: to select a specific field
from each child, iterate the child's key-value pairs and filter by key.

---

### 7.3 Select titles of fiction books priced under 20

**AEL:**
```
$.store.book.*[?(@.category == 'fiction' and @.price < 20.0)].*[?(@key == 'title')].select()
```

**Exp equivalent:**
```java
CTX ctx1 = CTX.mapKey(Value.get("book"));
CTX ctx2 = CTX.allChildrenWithFilter(
    Exp.and(
        Exp.eq(
            MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                Exp.val("category"), Exp.mapLoopVar(LoopVarPart.VALUE)),
            Exp.val("fiction")),
        Exp.lt(
            MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
            Exp.val(20.0))));
CTX ctx3 = CTX.allChildrenWithFilter(
    Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title")));

CdtOperation.selectByPath("store", SelectFlags.VALUE, ctx1, ctx2, ctx3);
```

**Result:** `["Sword of Honour", "Moby Dick"]`

---

### 7.4 Select stationery item keys with quantity > 50

**AEL:**
```
$.store.stationery.*[?(@.quantity > 50)].select(return: KEY)
```

**Exp equivalent:**
```java
CTX ctx1 = CTX.mapKey(Value.get("stationery"));
CTX ctx2 = CTX.allChildrenWithFilter(
    Exp.gt(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
            Exp.val("quantity"), Exp.mapLoopVar(LoopVarPart.VALUE)),
        Exp.val(50)));

CdtOperation.selectByPath("store", SelectFlags.MAP_KEY, ctx1, ctx2);
```

**Result:** `["pen", "pencil"]`  (eraser has quantity=50, not >50)

---

### 7.4b Select stationery key-value pairs with quantity > 50

**AEL:**
```
$.store.stationery.*[?(@.quantity > 50)].select(return: KEY_VALUE)
```

**Exp equivalent:**
```java
CTX ctx1 = CTX.mapKey(Value.get("stationery"));
CTX ctx2 = CTX.allChildrenWithFilter(
    Exp.gt(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
            Exp.val("quantity"), Exp.mapLoopVar(LoopVarPart.VALUE)),
        Exp.val(50)));

CdtOperation.selectByPath("store", SelectFlags.MAP_KEY_VALUE, ctx1, ctx2);
```

**Result:** `[["pen", {"price": 2.50, "quantity": 100}], ["pencil", {"price": 1.25, "quantity": 200}]]`

---

### 7.5 Select cheap books as a matching tree

**AEL:**
```
$.store.book.*[?(@.price < 10.0)].select(return: MATCHING_TREE)
```

**Exp equivalent:**
```java
CTX ctx1 = CTX.mapKey(Value.get("book"));
CTX ctx2 = CTX.allChildrenWithFilter(
    Exp.lt(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
            Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
        Exp.val(10.0)));

CdtOperation.selectByPath("store", SelectFlags.MATCHING_TREE, ctx1, ctx2);
```

**Result:** The full book map structures for "Sayings of the Century" and "Moby Dick",
preserving the parent list/map hierarchy.

---

### 7.6 Select all book titles (using regex on key)

**AEL:**
```
$.store.book.*.*[?(@key =~ /titl.*/)].select()
```

**Exp equivalent:**
```java
CTX ctx1 = CTX.mapKey(Value.get("book"));
CTX ctx2 = CTX.allChildren();
CTX ctx3 = CTX.allChildrenWithFilter(
    Exp.regexCompare("titl.*", RegexFlag.NONE,
        Exp.stringLoopVar(LoopVarPart.MAP_KEY)));

CdtOperation.selectByPath("store", SelectFlags.VALUE, ctx1, ctx2, ctx3);
```

**Result:** `["Sayings of the Century", "Sword of Honour", "Moby Dick", "The Lord of the Rings", "Clean Code"]`

---

### 7.7 Select first 3 books by list index

**AEL:**
```
$.store.book.*[?(@index < 3)].*[?(@key == 'title')].select()
```

**Exp equivalent:**
```java
CTX ctx1 = CTX.mapKey(Value.get("book"));
CTX ctx2 = CTX.allChildrenWithFilter(
    Exp.lt(Exp.intLoopVar(LoopVarPart.INDEX), Exp.val(3)));
CTX ctx3 = CTX.allChildrenWithFilter(
    Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title")));

CdtOperation.selectByPath("store", SelectFlags.VALUE, ctx1, ctx2, ctx3);
```

**Result:** `["Sayings of the Century", "Sword of Honour", "Moby Dick"]`

---

### 7.8 Apply 10% discount to all book prices

**AEL:**
```
$.store.book.*.price.modify(@ * 0.9)
```

**Exp equivalent:**
```java
CTX ctx1 = CTX.mapKey(Value.get("book"));
CTX ctx2 = CTX.allChildren();
CTX ctx3 = CTX.mapKey(Value.get("price"));

Expression modifyExp = Exp.build(
    Exp.mul(Exp.floatLoopVar(LoopVarPart.VALUE), Exp.val(0.9)));

CdtOperation.modifyByPath("store", ModifyFlags.DEFAULT, modifyExp, ctx1, ctx2, ctx3);
```

**Result:** prices become `[8.055, 11.691, 8.091, 20.691, 28.791]`

---

### 7.9 Restock all stationery by +50

**AEL:**
```
$.store.stationery.*.quantity.modify(@ + 50)
```

**Exp equivalent:**
```java
CTX ctx1 = CTX.mapKey(Value.get("stationery"));
CTX ctx2 = CTX.allChildren();
CTX ctx3 = CTX.mapKey(Value.get("quantity"));

Expression modifyExp = Exp.build(
    Exp.add(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(50)));

CdtOperation.modifyByPath("store", ModifyFlags.DEFAULT, modifyExp, ctx1, ctx2, ctx3);
```

**Result:** pen=150, pencil=250, eraser=100

---

### 7.10 Double the price of only cheap stationery (price < 2.0)

**AEL:**
```
$.store.stationery.*[?(@.price < 2.0)].price.modify(@ * 2)
```

**Exp equivalent:**
```java
CTX ctx1 = CTX.mapKey(Value.get("stationery"));
CTX ctx2 = CTX.allChildrenWithFilter(
    Exp.lt(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
            Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
        Exp.val(2.0)));
CTX ctx3 = CTX.mapKey(Value.get("price"));

Expression modifyExp = Exp.build(
    Exp.mul(Exp.floatLoopVar(LoopVarPart.VALUE), Exp.val(2.0)));

CdtOperation.modifyByPath("store", ModifyFlags.DEFAULT, modifyExp, ctx1, ctx2, ctx3);
```

**Result:** pencil price becomes 2.50, eraser becomes 1.50, pen unchanged (was 2.50)

---

### 7.11 Remove out-of-stock books

**AEL:**
```
$.store.book.*[?(@.inStock == false)].remove()
```

**Exp equivalent:**
```java
CTX ctx1 = CTX.mapKey(Value.get("book"));
CTX ctx2 = CTX.allChildrenWithFilter(
    Exp.eq(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.BOOL,
            Exp.val("inStock"), Exp.mapLoopVar(LoopVarPart.VALUE)),
        Exp.val(false)));

Expression removeExp = Exp.build(Exp.removeResult());
CdtOperation.modifyByPath("store", ModifyFlags.DEFAULT, removeExp, ctx1, ctx2);
```

**Result:** "Sword of Honour" (inStock=false) is removed from the list; 4 books remain.

---

### 7.12 Remove stationery items with low quantity

**AEL:**
```
$.store.stationery.*[?(@.quantity <= 50)].remove()
```

**Exp equivalent:**
```java
CTX ctx1 = CTX.mapKey(Value.get("stationery"));
CTX ctx2 = CTX.allChildrenWithFilter(
    Exp.le(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
            Exp.val("quantity"), Exp.mapLoopVar(LoopVarPart.VALUE)),
        Exp.val(50)));

Expression removeExp = Exp.build(Exp.removeResult());
CdtOperation.modifyByPath("store", ModifyFlags.DEFAULT, removeExp, ctx1, ctx2);
```

**Result:** eraser (qty=50) is removed; pen and pencil remain.

---

### 7.13 Deep navigation: filter by nested field

Given data where each book has an `address` sub-map:
```json
{"title": "...", "publisher": {"city": "London", "country": "UK"}}
```

**AEL:**
```
$.store.book.*[?(@.publisher.city == 'London')].*[?(@key == 'title')].select()
```

**Exp equivalent:**
```java
CTX ctx1 = CTX.mapKey(Value.get("book"));
CTX ctx2 = CTX.allChildrenWithFilter(
    Exp.eq(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
            Exp.val("city"),
            MapExp.getByKey(MapReturnType.VALUE, Exp.Type.MAP,
                Exp.val("publisher"),
                Exp.mapLoopVar(LoopVarPart.VALUE))),
        Exp.val("London")));
CTX ctx3 = CTX.allChildrenWithFilter(
    Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title")));

CdtOperation.selectByPath("store", SelectFlags.VALUE, ctx1, ctx2, ctx3);
```

---

### 7.14 Explicit type annotation when type can't be inferred

When comparing `@` or `@.field` to another bin (no literal to infer from):

**AEL:**
```
$.store.book.*[?(@.price.get(type: FLOAT) > $.minPrice.get(type: FLOAT))].*[?(@key == 'title')].select()
```

**Exp equivalent:**
```java
CTX ctx1 = CTX.mapKey(Value.get("book"));
CTX ctx2 = CTX.allChildrenWithFilter(
    Exp.gt(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
            Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
        Exp.floatBin("minPrice")));
CTX ctx3 = CTX.allChildrenWithFilter(
    Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title")));

CdtOperation.selectByPath("store", SelectFlags.VALUE, ctx1, ctx2, ctx3);
```

When comparing bare `@` to a bin:
```
$.store.stationery.*.quantity.*[?(@.get(type: INT) > $.threshold.get(type: INT))]
```

---

### 7.15 Select with noFail across mixed-type children

The top-level `$.store.*` iterates `book` (a list), `music` (a list), and `stationery`
(a map). These have different structures, so deeper navigation may fail on some.

**AEL:**
```
$.store.*.*[?(@.price < 10.0)].select(noFail: true)
```

**Exp equivalent:**
```java
CTX ctx1 = CTX.allChildren();
CTX ctx2 = CTX.allChildrenWithFilter(
    Exp.lt(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
            Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
        Exp.val(10.0)));

CdtOperation.selectByPath("store", SelectFlags.VALUE | SelectFlags.NO_FAIL, ctx1, ctx2);
```

**Result:** Elements from all three sections where price < 10 (some sections may be
silently skipped if they don't have a `price` field structure).

---

## 8. Complex Modify Operations

The Aerospike documentation shows modify expressions that use `MapExp.put()` to update
a specific field within the current element (treating `@` as a whole map). This is more
complex than simple arithmetic on a leaf scalar.

### 8.1 MapExp.put within modify

**Scenario:** For each in-stock book, set a `discountedPrice` field to `price * 0.9`.

**Exp equivalent:**
```java
CTX ctx1 = CTX.mapKey(Value.get("book"));
CTX ctx2 = CTX.allChildrenWithFilter(
    Exp.eq(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.BOOL,
            Exp.val("inStock"), Exp.mapLoopVar(LoopVarPart.VALUE)),
        Exp.val(true)));

Expression modifyExp = Exp.build(
    MapExp.put(
        MapPolicy.Default,
        Exp.val("discountedPrice"),
        Exp.mul(
            MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                Exp.val("price"),
                Exp.mapLoopVar(LoopVarPart.VALUE)),
            Exp.val(0.9)),
        Exp.mapLoopVar(LoopVarPart.VALUE)));

CdtOperation.modifyByPath("store", ModifyFlags.DEFAULT, modifyExp, ctx1, ctx2);
```

**Potential AEL syntax (for future consideration):**

This pattern modifies the element-as-a-whole (the book map) rather than a specific
leaf field. In the current AEL proposal, simple leaf modifications are straightforward:

```
$.store.book.*[?(@.inStock == true)].price.modify(@ * 0.9)
```

But adding a NEW field to each element requires expressing `MapExp.put` in AEL.
Options to explore:

**Option A: `.put()` as a modifier function:**
```
$.store.book.*[?(@.inStock == true)].modify(@.put('discountedPrice', @.price * 0.9))
```

**Option B: Explicit write path:**
```
$.store.book.*[?(@.inStock == true)].discountedPrice.modify(@.price * 0.9)
```

Option B is cleaner but requires that `modify` can create new keys. Option A is
more explicit about the operation.

### 8.2 Conditional modify

**Scenario:** Set a `tier` field based on price thresholds.

**Exp equivalent (using Exp.cond/when):**
```java
Exp modifyExp = Exp.cond(
    Exp.ge(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
        Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
        Exp.val(20.0)),
    Exp.val("premium"),
    Exp.val("standard"));
```

**Potential AEL:**
```
$.store.book.*.modify(
    when (@.price >= 20.0 => @.put('tier', 'premium'),
          default => @.put('tier', 'standard'))
)
```

These complex modify operations can be explored further once the basic `modify()` and
`remove()` are stable.

---

## 9. Field Selection Shorthand

The pattern `*[?(@key == 'fieldName')]` to pick a specific field from each child is
common but verbose. A shorthand `*::fieldName` is proposed:

| Longhand | Shorthand | Meaning |
|----------|-----------|---------|
| `*[?(@key == 'title')]` | `*::title` | Select the "title" entry from each child |
| `*[?(@key == 'price')]` | `*::price` | Select the "price" entry from each child |
| `*[?(@key == 'name')]` | `*::name` | Select the "name" entry from each child |

The `::` operator after `*` means "project this field from each iterated child".
It maps to `CTX.allChildrenWithFilter(Exp.eq(Exp.stringLoopVar(MAP_KEY), Exp.val(field)))`.

**Examples using shorthand:**

```
$.store.book.*[?(@.price < 10.0)].*::title.select()
// equivalent to:
$.store.book.*[?(@.price < 10.0)].*[?(@key == 'title')].select()
```

```
$.store.book.*.*::price.select()
// equivalent to:
$.store.book.*.*[?(@key == 'price')].select()
// which is also equivalent to (navigate, not iterate):
$.store.book.*.price.select()
```

Note that `*.price` (navigate to key) and `*.*::price` (iterate and filter by key)
produce the same result for maps, but `*.price` is simpler because it uses
`CTX.mapKey("price")` directly rather than `CTX.allChildrenWithFilter(...)`.
The shorthand `*::field` is primarily useful when chaining after a filtered wildcard:

```
$.store.book.*[?(@.inStock == true)].*::title
```

For quoted or special-character keys:
```
*::'special-key'
*::"key with spaces"
```

---

## 10. Grammar Changes Summary

```antlr
// Add wildcardPart to basePath
basePath: binPart ('.' (mapPart | listPart | wildcardPart))*?;

// New wildcard path segment
wildcardPart
    : WILDCARD_ALL                                          // allChildren
    | WILDCARD_ALL '[?(' expression ')]'                    // allChildrenWithFilter
    | WILDCARD_ALL '::' (NAME_IDENTIFIER | QUOTED_STRING)   // field projection shorthand
    ;

WILDCARD_ALL: '*';

// Loop variable references (new operand type)
loopVarReference
    : '@'                                                   // element value
    | '@' '.' basePath                                      // navigate into element value
    | '@key'                                                // parent map key
    | '@index'                                              // parent list index
    ;

// Add to operand rule
operand
    : ... existing operands ...
    | loopVarReference
    ;

// Regex comparison operator
comparisonExpression
    : ... existing ...
    | additiveExpression '=~' regexLiteral                  // regex match
    ;

regexLiteral: REGEX_LITERAL;
REGEX_LITERAL: '/' ~[/]+ '/' [imswx]*;                      // /pattern/flags (ICU)

// New path functions
pathFunction
    : ... existing ...
    | pathFunctionSelect
    | pathFunctionModify
    | pathFunctionRemove
    ;

pathFunctionSelect
    : 'select' '(' pathFunctionParams? ')'
    | 'select' '()'
    ;

pathFunctionModify
    : 'modify' '(' expression (',' pathFunctionParams)? ')'
    ;

pathFunctionRemove
    : 'remove' '(' pathFunctionParams? ')'
    | 'remove' '()'
    ;

// Extend return types
PATH_FUNCTION_CDT_RETURN_TYPE
    : ... existing ...
    | 'MATCHING_TREE'
    ;

// New parameter
pathFunctionParamName
    : ... existing ...
    | 'noFail'
    ;
```

---

## 11. Java API ↔ AEL Mapping Reference

| Java API | AEL |
|----------|-----|
| `CTX.mapKey(Value.get("book"))` | `.book` |
| `CTX.listIndex(0)` | `.[0]` |
| `CTX.allChildren()` | `.*` |
| `CTX.allChildrenWithFilter(exp)` | `.*[?(filter)]` |
| `Exp.mapLoopVar(VALUE)` | `@` (element is a map) |
| `Exp.intLoopVar(VALUE)` | `@` (element is an int) |
| `Exp.floatLoopVar(VALUE)` | `@` (element is a float) |
| `Exp.boolLoopVar(VALUE)` | `@` (element is a boolean) |
| `Exp.stringLoopVar(MAP_KEY)` | `@key` |
| `Exp.intLoopVar(LIST_INDEX)` | `@index` |
| `MapExp.getByKey(VALUE, T, "f", mapLoopVar(VALUE))` | `@.f` |
| Chained `MapExp.getByKey` for deep nav | `@.f.g` |
| `Exp.regexCompare(pat, flags, exp)` | `expr =~ /pat/flags` |
| `Exp.removeResult()` | `.remove()` |
| `CdtExp.selectByPath(LIST, SelectFlags.VALUE, …)` | `$.bin.path.*.leaf.select()` |
| `CdtExp.selectByPath(LIST, SelectFlags.MAP_KEY, …)` | `$.bin.path.*.select(return: KEY)` |
| `CdtExp.selectByPath(LIST, SelectFlags.MAP_KEY_VALUE, …)` | `$.bin.path.*.select(return: KEY_VALUE)` |
| `CdtExp.selectByPath(MAP, SelectFlags.MATCHING_TREE, …)` | `$.bin.path.*.select(return: MATCHING_TREE)` |
| `CdtExp.modifyByPath(MAP, ModifyFlags.DEFAULT, exp, …)` | `$.bin.path.*.leaf.modify(expr)` |
| `CdtOperation.modifyByPath(bin, ModifyFlags.DEFAULT, removeExp, …)` | `$.bin.path.*.remove()` |

---

## 12. Possible Implementation Considerations

### 12.1 Scope of `@`

Each `*[?(...)]` filter has its own `@` scope. In
`*[?(filter1)].*[?(filter2)]`, `@` in `filter1` refers to elements at the first
`*` level, and `@` in `filter2` to elements at the second level. The server manages
scoping — the AEL emits the correct CTX chain.

### 12.2 Return type inference

| Operation | Default `Exp.Type` |
|-----------|-------------------|
| `select()` | `LIST` |
| `select(return: MATCHING_TREE)` | `MAP` |
| `modify()` | Same type as source bin |
| `remove()` | Same type as source bin |

### 12.3 Navigating into `@`

`@.field` maps to `MapExp.getByKey(VALUE, type, "field", mapLoopVar(VALUE))`.
`@.field.subfield` chains: first resolve `@.field` as a MAP, then get `subfield`
from that result (using CTX context within the expression).

### 12.4 Error handling

| Scenario | Behavior |
|----------|----------|
| `*` on a scalar bin | Error (bin is not a CDT) |
| `*` on non-existent bin | Error unless `noFail: true` |
| Type mismatch in filter | Error unless `noFail: true` |
| `@key` on a list iteration | Error (lists don't have keys) |
| `@index` on a map iteration | Valid (maps have ordered index positions) |
| `.remove()` with no matching elements | No-op |
