# AEL v2 cheat sheet

One-page **syntax**, **tables**, and **examples**. Full rules: [path-and-selectors.md](path-and-selectors.md), [literals-and-types.md](literals-and-types.md), [operators-and-precedence.md](operators-and-precedence.md), [path-operations.md](path-operations.md), [map-keys-values-and-filter-chains.md](map-keys-values-and-filter-chains.md), [DECISIONS.md](DECISIONS.md).

---

## Record, bins, paths

| Form | Meaning |
|------|---------|
| `$` | Current record |
| `$.bin` | Bin named `bin` |
| `$.bin.segment` | Navigate: map key or next path piece |
| `$.bin:INT` | Path **suffix**: treat bin `bin` as an `INT` |
| `$.bin:MAP`, `$.bin:LIST` | Whole-value **map** / **list** typing — **not** `$.bin.{}` / `$.bin.[]` |
| `$.bin.count()` | Size of one collection — **CDT count** lowering; **no** `:MAP`/`:LIST` needed **just** for count ([DECISIONS.md](DECISIONS.md)) |
| `$.bin:local:FLOAT` | **Loose** typing for that occurrence only |
| `@:INT`, `@.price:FLOAT` | In filters / modify: type the loop value **`@`** or a field read |
| `$.key()`, `$.key():INT` | Record **key** / digest; optional **`:T`** after **`()`** if inference does not fix the type ([type-inference.md](type-inference.md) §6) |
| `$.ttl()`, `$.setName()`, … | Record **metadata** (see [literals-and-types.md](literals-and-types.md) §13) |

**Implicit get:** if the path ends without `getKeys()` / `getTree()` / …, the default is usually “**values here**”. **`getValues()`** is redundant with that default — omit it unless it helps readability ([map-keys-values-and-filter-chains.md](map-keys-values-and-filter-chains.md)).

---

## Literals (quick)

| Kind | Examples |
|------|----------|
| Int | `42`, `-1`, `0xff`, `0b1010` |
| Float | `3.14`, `.5` — not `10.` |
| String | `'hi'`, `"a\nb"` |
| Bool | `true`, `false` |
| Blob | `x'cafe'`, `X'cafe'` |
| Base64 | `b64'...'`, `B64'...'` |
| List | `[1, 2, a]` |
| Map | `{k: 1, "x": y}` |
| CDT sentinels | `NIL`, `INF`; literal wildcard `*` **uppercase** in list/map literals |

---

## Comments

**Block only:** C-style **`/* … */`**, anywhere **whitespace** is allowed. **No** nested `/* */`. (Regex flag **`x`** can use **`#`** to end-of-line **inside** the pattern only — [operators-and-precedence.md](operators-and-precedence.md).) Full rules: [literals-and-types.md](literals-and-types.md) §11.

```text
/* whole-line */
$.age > 21  /* trailing */
```

---

## Operators (quick)

| Class | Tokens / forms |
|-------|------------------|
| Compare | `==`, `!=`, `<`, `<=`, `>`, `>=` |
| Membership | `value in listExpr` |
| Regex | `string =~ /pattern/flags` — flags `i` `m` `s` `x` `w` ([operators-and-precedence.md](operators-and-precedence.md)) |
| Logic | `a and b`, `a or b`, `not(x)`, `exclusive(a, b, …)` |
| Math | `+`, `-`, `*`, `/`, `%`, `**` (float, right-assoc) |
| Bitwise | `&`, `|`, `^`, `~`, `<<`, `>>`, `>>>` |

**`when` / `let`:**

```text
when (c1 => v1, c2 => v2, default => v3)
let (a = expr1, b = expr2) then (expr)
```

---

## Map selectors `{…}` (CDT on a **map**)

**Design rules:** first char inside `{` sets **dimension**: *(none)* = index, `@` = key, `=` = value, `#` = rank. **`:`** = range inside brackets. **`,`** = list. **`~`** = relative. **`!` right after `{`** = inverted. **`..` not inside `{…}`** (reserved for future path recursion).

| Dimension | Singular | Range | Open-start | Open-end | List | Inverted range | Inverted list |
|-----------|----------|-------|------------|----------|------|----------------|---------------|
| **Index** | `{1}` | `{1:5}` | `{:5}` | `{1:}` | — | `{!1:5}` | — |
| **Key** | bare `a` or `{@a}` | `{@a:d}` | `{@:d}` | `{@a:}` | `{@a,b,c}` | `{!@a:d}` | `{!@a,b,c}` |
| **Value** | `{=a}` | `{=a:d}` | `{=:d}` | `{=a:}` | `{=a,b,c}` | `{!=a:d}` | `{!=1,2,3}` |
| **Rank** | `{#1}` | `{#1:5}` | `{#:5}` | `{#1:}` | — | `{!#1:5}` | — |

**Relative (map):** `{#-1:1~10}`, `{#-2:~10}`, `{!#-1:~f}` — **index-from-key:** `{0:1~a}`, `{0:~a}`, `{!0:1~a}`.

**Trailing comma:** `{@k,}` = multi-select with one key (invertible); contrast `{@k}` singular.

---

## List selectors `[…]` (CDT on a **list**)

| Dimension | Singular | Range | Open-start | Open-end | List | Inverted range | Inverted list |
|-----------|----------|-------|------------|----------|------|----------------|---------------|
| **Index** | `[1]` | `[1:5]` | `[:5]` | `[1:]` | — | `[!1:5]` | — |
| **Value** | `[=a]` | `[=a:d]` | `[=:d]` | `[=a:]` | `[=a,b,c]` | `[!=a:d]` | `[!=a,b,c]` |
| **Rank** | `[#1]` | `[#1:5]` | `[#:5]` | `[#-3:]` | — | `[!#1:5]` | — |

**Rank-relative (list):** `[#-3:-1~b]`, `[#-2:~b]`, `[!#-3:-1~b]`.

**Intervals:** index/rank `start:end` is usually **begin-inclusive, end-exclusive**; confirm for the specific op when it matters ([path-and-selectors.md](path-and-selectors.md) §6).

---

## Wildcard path segment `*`

| Position | Meaning |
|----------|---------|
| `$.map.*` | Iterate **all children** of `map` (path segment) |
| `[1, *]` | Wildcard **inside a list literal** (value position) — different role from path `*`; see [path-operations.md](path-operations.md) (**Disambiguation from `*` as WILDCARD value**). For `NIL` / `INF` / `*` in literals, see also [literals-and-types.md](literals-and-types.md) §9. |

---

## Iteration + filter (path expressions)

**1) Wildcard, then filter** — `.*` visits **every child** at this segment; `[?( … )]` keeps only those where the predicate is true (`@` is each child, e.g. a book with `@.price`):

```text
$.store.book.*[?(@.price > 10)]
```

**2) Key list, then filter** — the map is already limited to those keys; `&` attaches a filter **on that selection** at the current level (`@` is each entry’s value, not “any child under the bin”):

```text
$.scores.{@"a","b","c"}&[?(@ > 100)]
```

| In predicates | Typical meaning |
|----------------|------------------|
| `@` | Current value (child under `.*`, or map entry value after `{@…}` + `&`) |
| `@key` | Current map key (when iterating map children) |
| `@index` | Current list index |

More detail: [open-issues.md](open-issues.md) §1.

---

## Getters (after multi-select / iteration)

| Need | Typical surface |
|------|-----------------|
| Values (default) | *end path* — no `getValues()` |
| Keys | `getKeys()` |
| Key+value | `getKeyValues()` |
| Tree | `getTree()` |

Details: [map-keys-values-and-filter-chains.md](map-keys-values-and-filter-chains.md).

---

## Tiny real-world filters

| Expression | One-line |
|------------|----------|
| `$.age >= 21 and $.country == 'US'` | Record must be an adult **and** have country `US`. |
| `$.ttl() < 3600` | Record expires in under an hour (TTL in seconds). |
| `$.tags.*[?(@ =~ /^prod/i)]` | Any element in `tags` starting with `prod` (case insensitive). |
| `$.wallet.{@"gold","silver"} > 0` | Both **gold** and **silver** balances in map `wallet` are strictly positive. |
| `let (t = $.price * $.qty) then (${t} > 1000)` | Line total exceeds 1000 using a bound local `t`. |

---

## Prepared parameters

```text
$.score > ?0 and $.region == ?1
```

`?0`, `?1`, … are bound when the expression is executed.
