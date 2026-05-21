# AEL v2 — open specification items

**Last reviewed:** 2026-05-20.

These points are **not fully specified** in the canonical text in `docs/ael-v2/` alone, or they depend on **release-level** choices.

---

## 1. Punctuator between key-list selector and filter (`&` vs `*`)

Two different constructions both use a `[?( … )]` filter segment, but the **scope** of what is filtered is **not** the same. Both are valid; they must not be confused.

### A) Iterate all children, then filter — `.*[?(cond)]`

After a **wildcard path segment** `.*`, the engine walks **every child** at that level. The following `[?(cond)]` keeps only **those children** for which the predicate is true.

```text
$.store.book.*[?(@.price > 10)]
```

**Reading:** In bin `store`, take the list `book`, consider **each book** (each child), and keep only books whose **`price`** is greater than 10. Here **`@`** is each **child** (e.g. a map representing one book); **`@.price`** is a field on that child.

So **`.*[?(cond)]`** means: “**among all children**, select any that match the criteria.”

### B) Current-level filter after a key list — `&[?(cond)]` (illustrative chain token)

After a **key-list** selector `{@"a","b","c"}` (or similar), the path has already **restricted** the map to those keys. A chain such as **`&[?(cond)]`** applies the predicate **at that same level** — to **those entries’ values** (or keys, per the predicate) — **without** introducing a new “iterate all children” step. It does **not** mean “filter arbitrary children under the map”; the set under test is exactly the key-list selection.

```text
$.scores.{@"a","b","c"}&[?(@ > 100)]
```

**Reading:** On map `scores`, only keys `a`, `b`, and `c`; among **those** entries, keep only where the **value** `(@)` is greater than 100.

So **`&[?(cond)]`** (after the key-list segment) means: “**on the current selection**, apply this filter” — not “search all children of the map.”

### Open (notation only)

The **exact character** for the composition between key-list and filter (`&` vs other tokens, and lexer rules) is not fully pinned — including disambiguation from **bitwise** `&`. The **semantic** split above (child iteration vs current-level filter) is the part that is settled.

---

## 2. NO_FAIL / flag placement: before `()` vs after `()`

Compact flags (e.g. `NO_FAIL`) on path operations: **before** `()` vs **after** `()` remains unsettled.

**Open:** pick one convention and apply it across all path terminators that take flags.

---

## 3. Read surface: compiler, tooling, and `Exp` mapping

**Already decided (normative):** the AEL **read** surface is **implicit get** for ordinary value reads, plus **`getKeys()`**, **`getValues()`**, **`getKeyValues()`**, and **`getTree()`** when the result is not “values only” ([DECISIONS.md](DECISIONS.md)). There is **no** remaining language-level fork between “only `.select(return: …)`” and “getters + implicit get” — getters + implicit get **are** v2.

**Still open (per product release / implementation):**

1. **Compiler contract** — which spellings a given compiler version **must** accept, which are **optional** aliases, and deprecation timelines (e.g. whether **`getValues()`** is accepted everywhere or discouraged except for readability).
2. **Tooling emission** — whether IDEs, codegen, or migrations may emit an optional **`.select(return: …)`** (or similar) as an intermediate or explicit form, and how that normalizes to the normative getter / implicit-get tree.
3. **`Exp` lowering** — concrete mapping from each read shape (implicit get vs each getter) to **`Exp`** / wire helpers for that release, including any gaps where the spec allows a shape but the server or client library does not yet expose a single obvious builder.

---

## 4. CDT count primitive (`count()` lowering)

**Rule:** **`count()`** on a single collection-valued path should compile without authors adding **`:MAP`** / **`:LIST`** **only** to distinguish list from map. The expression tree uses a **CDT count**–style primitive (name on wire / exact `Exp` helper TBD).

**Open:** final opcode or **`Exp`** builder surface, and any edge cases where a suffix is still required for **parse** disambiguation vs **semantic** count.
