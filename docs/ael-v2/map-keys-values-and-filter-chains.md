# Map key lists, value projections, and “and then” filter chains

This page explains how to **restrict a map to a set of keys**, optionally **filter** those entries with a predicate (“**and then**”), and how that relates to **implicit get** vs **`getKeys()`** / **`getValues()`** / **`getKeyValues()`** / **`getTree()`**.

**See also:** [cheat-sheet.md](cheat-sheet.md), [path-and-selectors.md](path-and-selectors.md), [path-operations.md](path-operations.md), [DECISIONS.md](DECISIONS.md), [open-issues.md](open-issues.md).

---

## 0. AEL recap (what you need for the rest of this page)

- **`$`** means the **current record**. **`$.binName`** starts a **path**: read data from bin `binName`.
- A **map** bin holds key → value pairs. After `$.myMap`, you navigate with **dot + key** (e.g. `$.myMap.price`) or with **braced selectors** inside `{…}` (see [path-and-selectors.md](path-and-selectors.md)) to pick one or many entries by index, key, value, or rank.
- A **list** bin is ordered; navigation uses **`[…]`** selectors.
- In **path filters** (see [path-operations.md](path-operations.md)), **`@`** is the **current element** in an iteration, **`@key`** the current map key (string), **`@index`** the current list index (integer). You write boolean predicates with the same operators as elsewhere (`>`, `==`, `and`, …).

---

## 1. Map key list inside `{…}`

To select a **fixed set of map keys** as one step in a path, use the **`@`** dimension inside braces (comma-separated keys):

```text
$.bin.{@"a","b","c"}
```

**Meaning:** “From map `bin`, only consider entries whose keys are `a`, `b`, or `c`.”  
Keys can be quoted when needed: `{@"order-1","order-2"}`.

This is **not** the same as three separate path alternatives; it is a **single multi-key selection** usable as the prefix of a longer path.

---

## 2. “And then”: key set, then predicate (syntax + meaning)

**Idea in plain language:** first **narrow** the map to certain keys; **then**, among those entries, keep only those where a **condition** on each entry’s value (or key, etc.) holds.

**Syntax pattern** (illustrative; the **chain** token between the key list and the filter is not fully standardized — see [open-issues.md](open-issues.md) §1):

```text
<path-to-map-bin> . { @"<key1>", "<key2>", ... } <CHAIN> [ ? ( <predicate> ) ]
```

- **`<path-to-map-bin>`** — e.g. `$.scores`, `$.store.stationery`.
- **`{ @"k1", "k2" }`** — key-list selector (§1).
- **`<CHAIN>`** — a **composition operator** between the key-list segment and the **filter** segment (see [open-issues.md](open-issues.md) §1). The **meaning** is: **after** restricting the map to those keys, **run** the bracketed filter **on that current-level selection** — not “iterate all children” (that is the separate **`.*[?( … )]`** pattern in path expressions).
- **`[?( … )]`** — filter segment: **`?(` … `)`** wraps a **boolean** expression. Inside it, **`@`** is typically the **value** of the current candidate entry; **`@key`** / **`@index`** are available when the iteration context supplies them (see [path-operations.md](path-operations.md)).

**Concrete example** (pseudocode chain `&` — substitute whatever your engine documents):

```text
$.scores.{@"alice","bob","carol"} & [?(@ > 100)]
```

**Reading:** “On map `scores`, look only at keys `alice`, `bob`, and `carol`; keep entries whose **value** is greater than `100`.”

**Why not only a key list?** A plain key list answers “these keys.” **And then** adds “**and** only if this predicate passes,” which is strictly stronger when some of those keys’ values should be excluded.

**Contrast with `.*[?( … )]`:** In **`$.store.book.*[?(@.price > 10)]`**, the **`.*`** walks **every child** (each book), and the predicate picks **which children** match. That is **not** the same as **`{@"a","b","c"} & [?(@ > 100)]`**: there, the set is **already** only keys `a`, `b`, `c`; **`&[?( … )]`** filters **those entries at the current map level**, not “any child under the bin.” See [open-issues.md](open-issues.md) §1.

---

## 3. Implicit get vs explicit getters (target surface)

When a path **ends** without a special terminal, the language applies **implicit get**: for typical multi-value paths that means “**give me the values**” as the default result shape.

| Intent | What to write |
|--------|----------------|
| **Values** (default) | End the path after selectors / iteration — **no** `getValues()` required. **`getValues()`** is allowed but **redundant** with implicit get; **prefer omitting it** unless it helps readers or tooling. |
| **Keys** | Use **`getKeys()`** when you need the key list as the result. |
| **Key + value pairs** | **`getKeyValues()`** |
| **Subtree / tree-shaped result** | **`getTree()`** |

**Plural** names (`getKeys`, `getValues`) are intentional even when a single key or value is returned.

Some toolchains may emit **`.select(return: …)`** as a **bridge** to the same result shapes as the getter terminals; see [path-operations.md](path-operations.md) and [open-issues.md](open-issues.md) §3.

**Worked examples (values vs keys):**

```text
/* Default: values at matched paths — implicit get, no trailing call */
$.store.stationery.{@"pen","pencil"}.*

/* Explicit keys only */
$.store.stationery.{@"pen","pencil"}.getKeys()
```

(The second assumes a path shape your engine supports after a multi-key segment; if your compiler requires an extra iteration segment, follow [path-operations.md](path-operations.md).)

---

## 4. Punctuator between key-list stage and filter (`<CHAIN>`)

Only the **composition** (key list **then** filter) is fixed at the language level; the **exact character** for `<CHAIN>` (`&`, `*`, …) may vary by grammar version — [open-issues.md](open-issues.md) §1.

---

## 5. Nested sub-expressions in filters

**Normative:** do **not** nest a sub-expression inside another sub-expression used in a filter or path argument. Use **`let`** to name intermediate results, or split expressions, instead of stacking `[?( … [?( … )] … )]`.

---

## 6. Longer example (filter + compare)

Suppose `alerts` is a map from alert id (string key) to a numeric severity. You want “any of keys `cpu`, `disk`, or `net` has severity ≥ 90”:

```text
$.alerts.{@"cpu","disk","net"} & [?(@ >= 90)].count() > 0
```

Again, replace **`&`** if your implementation documents a different chain token. **`@`** is each candidate entry’s **value** (here: severity). **`.count()`** semantics for multi-select tails follow [DECISIONS.md](DECISIONS.md).
