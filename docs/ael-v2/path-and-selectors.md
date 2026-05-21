# Path operands, wildcards, and CDT selectors (AEL v2)

**Normative** selector rules and tables below; [DECISIONS.md](DECISIONS.md) resolves conflicts with any other wording in this folder.

If you are **new to AEL**, read [cheat-sheet.md](cheat-sheet.md) first for a compact tour of paths, literals, and operators, then return here for the full selector grammar. This page assumes you know that **`$.bin`** reads a bin and that **maps** use `{…}` and **lists** use `[…]` in path position to choose elements ([aerospike-expression-language-v2.md](aerospike-expression-language-v2.md) for record/path basics).

**See also:** [map-keys-values-and-filter-chains.md](map-keys-values-and-filter-chains.md), [path-operations.md](path-operations.md), [type-inference.md](type-inference.md).

---

## 1. Design rules (selector bracket grammar)

These rules apply to both **`{…}`** (map) and **`[…]`** (list) selector brackets:

1. The first character after `{` or `[` establishes the **dimension**: *(none)* = **index**, `@` = **map key** (in `{…}` only), `=` = **value**, `#` = **rank**.
2. **`:`** always means **range** inside a selector. **`..` is never used inside `{…}` / `[…]`**; `..` is reserved for a future **recursive-descent** path operator (JSONPath-style).
3. **`,`** always means **list** (multi-select or explicit value/key lists).
4. **`~`** always means **relative to** the following bound (relative rank / relative index forms).
5. **`!`** immediately after `{` or `[` means **inverted** selection (including `{!#…}` rank inversions).
6. **`..` is never used inside selectors** (same as rule 2, emphasized).

---

## 2. Map selectors `{…}`

### 2.1 Selector table (map)

| Dimension | Singular | Range | Open-start | Open-end | List | Inverted range | Inverted list |
|-----------|----------|-------|------------|----------|------|----------------|---------------|
| **Index** | `{1}` | `{1:5}` | `{:5}` | `{1:}` | — | `{!1:5}` | — |
| **Key** | bare `a` or `{@a}` | `{@a:d}` | `{@:d}` | `{@a:}` | `{@a,b,c}` | `{!@a:d}` | `{!@a,b,c}` |
| **Value** | `{=a}` | `{=a:d}` | `{=:d}` | `{=a:}` | `{=a,b,c}` | `{!=a:d}` | `{!=1,2,3}` |
| **Rank** | `{#1}` | `{#1:5}` | `{#:5}` | `{#1:}` | — | `{!#1:5}` | — |

### 2.2 Key singular and invertibility

`{@a}` denotes a **single** key item (equivalent to path segment `a` in typical cases): it yields **0 or 1** elements and does not support inversion as a non-list. Use **`{@a,}`** for a one-element **list** shape, invertible as **`{!@a,}`**. Similarly, **`{1,}`** is a one-element index list invertible with **`{!1,}`**.

### 2.3 Relative operations (map)

**Rank-relative:**

| | |
|--|--|
| Range | `{#-1:1~10}` |
| Open-end relative | `{#-2:~10}` |
| Inverted relative | `{!#-1:~f}` |

**Index relative to key:**

| | |
|--|--|
| Range | `{0:1~a}` |
| Open-end | `{0:~a}` |
| Inverted | `{!0:1~a}` |

---

## 3. List selectors `[…]`

### 3.1 Selector table (list)

| Dimension | Singular | Range | Open-start | Open-end | List | Inverted range | Inverted list |
|-----------|----------|-------|------------|----------|------|----------------|---------------|
| **Index** | `[1]` | `[1:5]` | `[:5]` | `[1:]` | — | `[!1:5]` | — |
| **Value** | `[=a]` | `[=a:d]` | `[=:d]` | `[=a:]` | `[=a,b,c]` | `[!=a:d]` | `[!=a,b,c]` |
| **Rank** | `[#1]` | `[#1:5]` | `[#:5]` | `[#-3:]` | — | `[!#1:5]` | — |

### 3.2 Rank-relative (list)

| | |
|--|--|
| | `[#-3:-1~b]` |
| | `[#-2:~b]` |
| | `[!#-3:-1~b]` |

---

## 4. Record path prefix

Paths begin with **`$.`**, then bin name and dot-separated segments.

**Path typing suffixes:** **`$.bin:T`** (strict) and **`$.bin:local:T`** (loose) — [DECISIONS.md](DECISIONS.md).

---

## 5. Wildcard path segment `*`

A bare **`*`** **between dots** iterates **all children** at that map or list level. This is **not** the same token as wildcard **`*` inside a list/map literal** — see [path-operations.md](path-operations.md) for the disambiguation table.

---

## 6. Interval semantics

For **`start:end`** **index** and **rank** ranges, the usual AEL convention is **begin-inclusive, end-exclusive**. **Key** and **value** intervals follow the same pattern where the underlying CDT operation is interval-based. **Always** align with the specific list/map operation invoked at the end of the path when precise edge behavior matters.

---

## 7. Single-select vs multi-select (trailing comma)

`{@a}` is **single-select** on a key; `{@a,b}` is **multi-select**. **`{@a,}`** (trailing comma) is **multi-select with exactly one key**, producing list-shaped results and well-defined inversion (`{!@a,}`). The same **trailing-comma** convention applies to analogous list/map selector forms in the tables above.

---

## 8. List selector: index `[:…]` vs value `[=…]` (lexing)

**`[:5]`** (index open-start) and **`[=:d]`** (value open-start) look alike in prose, but the grammar is **unambiguous**:

1. After **`[`**, skip **whitespace**.
2. Let **c** be the next character.
3. If **c** is **`=`**, the selector is **value-dimension** (`[=…]` row of §3.1). In particular, **`[=:bound]`** is value open-start / value range syntax.
4. If **c** is **`#`**, the selector is **rank-dimension**.
5. If **c** is **`!`**, the selector is **inverted**; consume **`!`** and re-apply steps 2–5 for the remainder (see inverted forms in §3.1).
6. **Otherwise** (including **`:`**, a digit, **`-`**, …), the selector is **index-dimension** (`[1]`, `[1:5]`, `[:5]`, `[1:]`, …).

Implementations should include lexer tests for **`[:n]`** vs **`[=:n]`** so the **`=`** probe is not regressed.
