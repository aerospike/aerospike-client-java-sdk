# Aerospike Expression Language (AEL) v2 — canonical reference

**Last reviewed:** 2026-05-20.

AEL is a **text DSL** compiled to Aerospike **expression trees** for server-side evaluation: **filters** (queries, batch predicates), **read expressions** (derived bins), and **write expressions** (computed bin values).

**Within this folder:** [DECISIONS.md](DECISIONS.md) is the tie-breaker for any inconsistency. [SOURCES.md](SOURCES.md) lists source precedence for this specification. [open-issues.md](open-issues.md) lists items that remain intentionally unset.

---

## How to read these docs

If you are **new to AEL**, start with **[cheat-sheet.md](cheat-sheet.md)** for syntax, tables, and worked examples, then use the pages below for full rules and edge cases.

**Guiding principle:** each topic page should stand on its own enough that you can learn that slice of the language without having already memorized the rest. Where a concept depends on another, the page should include a **short recap** (what a path is, what `@` means in a filter, etc.), the **syntax pattern**, and at least one **example** before going deeper.

---

## Lexical conventions

| Category | Rule |
|----------|------|
| Language constants | `NIL`, `INF`, and wildcard `*` **in literal positions** are **UPPERCASE** |
| Reserved words | `and`, `or`, `let`, `when`, `then`, `default`, `true`, `false`, … are **lowercase** |
| Path / loop typing | `$.bin:T` strict; `$.bin:local:T` loose per-branch; `@:T` / `@.field:T` in filters (see [DECISIONS.md](DECISIONS.md)) |
| No-arg record helpers | Optional **`():T`** after **`()`** on helpers such as **`$.key():INT`**; omit **`:T`** when inference suffices ([type-inference.md](type-inference.md) §6) |

---

## Reading order

1. [cheat-sheet.md](cheat-sheet.md) — quick syntax, tables, examples (**start here** if you are new)  
2. [DECISIONS.md](DECISIONS.md) — normative tie-breakers  
3. [path-and-selectors.md](path-and-selectors.md) — `{…}` / `[…]` CDT selectors  
4. [literals-and-types.md](literals-and-types.md)  
5. [operators-and-precedence.md](operators-and-precedence.md)  
6. [type-inference.md](type-inference.md)  
7. [path-operations.md](path-operations.md) — iteration, filters, modify/remove, loop variables  
8. [map-keys-values-and-filter-chains.md](map-keys-values-and-filter-chains.md) — key lists, “and then”, implicit get vs `getKeys()` / `getValues()` / …  
9. [sub-expressions.md](sub-expressions.md)  
10. [list-map-functions.md](list-map-functions.md)  
11. [string-functions.md](string-functions.md), [bit-operations.md](bit-operations.md), [hll-and-geo.md](hll-and-geo.md)  
12. [open-issues.md](open-issues.md)  

---

## Core expression shapes (concise)

**Record and bins:** paths start with **`$.`**, then bin names and dot-separated segments.

**Conditionals:**

```text
when (cond1 => val1, cond2 => val2, default => val3)
```

**Local bindings:**

```text
let (x = expr1, y = expr2) then (exprUsing ${x} ${y})
```

**Metadata:** function-like forms on the record, e.g. `$.ttl()`, `$.setName()`, `$.recordSize()`, … (full list and semantics in [literals-and-types.md](literals-and-types.md) §13 and [operators-and-precedence.md](operators-and-precedence.md) where combined with comparisons).

---

## Quick examples (v2 selector spelling)

```text
$.age > 21 and $.status == 'active'
$.scores.{#-1}:INT
$.bin:LIST.*
$.store.book.*[?(@.price:FLOAT < 10)]
$.name =~ /^Alice/i
when ($.tier == 1 => 'gold', default => 'bronze')
let (total = $.price * $.qty) then (${total} > 1000)
```

The path **`$.bin:LIST.*`** ends after iteration: **implicit get** is the default for “values at this path” (no extra method required). When you need keys, key–value pairs, or a tree shape, or when to prefer an explicit `getValues()` spell-out, see [map-keys-values-and-filter-chains.md](map-keys-values-and-filter-chains.md).

---

## Prepared statements

Placeholders `?0`, `?1`, … stand for bound parameters at execution time.
