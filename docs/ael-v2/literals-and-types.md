# Literals, types, and special values (AEL v2)

Normative casing for constants vs reserved words: [DECISIONS.md](DECISIONS.md).

**See also:** [cheat-sheet.md](cheat-sheet.md), [type-inference.md](type-inference.md), [path-and-selectors.md](path-and-selectors.md), [aerospike-expression-language-v2.md](aerospike-expression-language-v2.md).

If you are **new to AEL**, start with [cheat-sheet.md](cheat-sheet.md) for a quick overview of numbers, strings, lists, maps, and booleans in expressions. This page spells out every literal form and related rules in full.

---

## 1. Integers

Decimal digits, optional leading sign (`+` / `-`), no space between sign and digits. Hexadecimal and binary literals are supported:

```text
42
-1
+123
0xff
0b1010
```

---

## 2. Floating-point

A single decimal point; the point must not be the last character. A leading dot is allowed (e.g. `.22`). **`10.`** is invalid — use **`10.0`**.

---

## 3. Strings

Use **single** or **double** quotes when the text is not a simple identifier. Inside strings, the following escapes are defined:

| Escape | Meaning |
|--------|---------|
| `\\` | Backslash |
| `\n` | Newline |
| `\t` | Tab |
| `\"` | Double quote (inside double-quoted strings) |
| `\'` | Single quote (inside single-quoted strings) |

A double quote inside single quotes, or a single quote inside double quotes, does not need escaping.

---

## 4. Booleans

Reserved words, **lowercase:** `true`, `false`.

---

## 5. BLOB literals

Even-length hexadecimal, with `x` or `X` prefix:

```text
X'ffee'
x'102030405060708090abcdef'
```

---

## 6. Base64 literals

Base64-encoded byte data may be written as:

```text
b64'SGVsbG8='
B64'SGVsbG8='
```

If the payload is not valid base64, the expression is a **parse error**.

---

## 7. List literals

Square brackets, comma-separated elements; elements may be any supported literal or nested list/map.

```text
[1, 2, "abc", 12.7]
[]
```

---

## 8. Map literals

Curly braces, comma-separated `key: value` pairs. Keys may be strings, integers, or BLOB literals; values may be any supported type.

```text
{name: "Tim", age: 312, 1: [2, 3, 4]}
```

---

## 9. Special CDT comparison values (constants)

**UPPERCASE** only for these language-level constants:

| Token | Role |
|-------|------|
| `NIL` | Lowest value in CDT ordering for comparisons |
| `INF` | Highest value in CDT ordering for comparisons |
| `*` | Wildcard **value** inside list/map **literals** only — not the same as a `*` **path segment** (see [path-operations.md](path-operations.md)) |

---

## 10. Type names (path suffix `:T`, loop suffix `@:T`)

Named types appear in **path suffixes** — on a bin (**`$.bin:T`**), after a selector that yields a scalar (**`$.l.[0]:INT`**, **`$.m.k:STRING`**), on record helpers (**`$.key():INT`**), and in **loop-variable** positions (**`@:T`**, **`@.field:T`**). The same name set is used everywhere, including at least:

`INT`, `STRING`, `FLOAT`, `BOOL`, `BLOB`, `HLL`, `LIST`, `MAP`, `GEO`.

**`MAP` / `LIST` on paths:** use **`$.bin:MAP`** / **`$.bin:LIST`** when the language must treat the value as a map or list (e.g. whole-value comparison). **Do not** use empty **`$.bin.{}`** / **`$.bin.[]`** for that. **`count()`** on a single collection does **not** require **`{}`** / **`[]`** or **`:MAP`/`:LIST`** solely to disambiguate list vs map ([DECISIONS.md](DECISIONS.md), [open-issues.md](open-issues.md) §4).

---

## 11. Comments

**Block comments** use C-style `/* … */`. They may appear wherever whitespace is allowed. **Nested** `/* */` comments are **not** allowed.

```text
/* whole-line */
$.age > 21  /* trailing */
```

---

## 12. Whitespace

Spaces, tabs, and newlines are allowed between tokens. These are equivalent:

```text
$.a.b
$. a . b
```

---

## 13. Record metadata and key functions (names only)

Metadata and record helpers invoked as **`$.name()`** (for example TTL, set name, record size, digest modulo) share the same identifier and **`()`** rules as other function-like forms. The record key / digest helper **`$.key()`** may take an optional **`:T`** immediately **after** **`()`** — **`$.key():INT`** — when the static type is not inferable; omit **`:T`** when context fixes the type ([type-inference.md](type-inference.md) §6). Full signatures and return types are covered alongside paths and operators in [aerospike-expression-language-v2.md](aerospike-expression-language-v2.md) and [operators-and-precedence.md](operators-and-precedence.md).
