# AEL — Specification gaps vs this repository

This document tracks **remaining gaps** between long-form / PDF AEL design material, **`Condition.g4`**, and the Java compiler (**`ExpressionConditionVisitor`** and related `ael` classes). It is aimed at engineers extending the parser or visitor.

**Primary reference (aligned to implementation):** **`ael-documentation.md`** — revised so normative text matches this repo’s grammar and visitor unless explicitly labeled otherwise.

**Legacy labels in sections below:** **AEL-DOC** refers to historical `ael-documentation.md`–style prose or external AEL reference; **PDF-SPEC** is the CORE Expression DSL PDF. Where those two differ, the note is still useful for product direction even though user-facing docs now follow code.

---

## Summary

| # | Issue | Severity | User doc |
|---|--------|----------|----------|
| 1 | `NAME_IDENTIFIER` allows digit-start → digit-only map segments resolve as **string** keys | High | Documented in **§5.1** |
| 2 | `exists()` parses but **no visitor** → suffix dropped / wrong semantics | High | — (omitted from user doc); see `BinExpressionsTests#existsPathSuffixIsIgnoredCompilesAsPlainPath` |
| 3 | Mutation path suffixes parse; **no visitor** → not compiled | Medium | **§8.4** — not executable |
| 4 | `type()` path function missing from grammar | Medium | Not advertised as supported |
| 5 | `put()` map mutation missing from grammar | Medium | **§8.4** |
| 6 | Open-start ranges (`{-d}`, `{:5}`, `{#:4}`, `[:5]`) missing from grammar | Medium | Called out as unsupported |
| 7 | Block comments (`/* */`) missing from grammar | Low | **§17** — not supported |
| 8 | `error`/`unknown` keywords treated as bin names | Medium | **§14.3** |
| 9 | `WILDCARD`/`NIL`/`INF` not handled as special values | Medium | — (omitted from user doc) |
| 10 | `$.key()` metadata function missing from grammar | Medium | **§13** |
| 11 | String escape sequences not implemented | Medium | **§2.3** |
| 12 | Value ranges limited to integers only (semantic layer) | Medium | **§5.2** value range note |
| 13 | Float/list/map/boolean not in `valueIdentifier` for CDT selectors | Medium | Examples adjusted in **§19** |
| 14 | Bitwise operator precedence differs from PDF-SPEC | Low (matches current grammar) | **§16** |

---

## Spec-vs-Spec Differences

The following are differences between the **AEL-DOC** and the **PDF-SPEC** themselves.

### S1. Bitwise operator precedence

**PDF-SPEC** defines three separate precedence levels for bitwise operators:

| Level | Operator | Description |
|---|---|---|
| 7 | `&` | Bitwise AND (most binding) |
| 8 | `^` | Bitwise XOR |
| 9 | `\|` | Bitwise OR (least binding) |

**AEL-DOC** (§16) groups all three at a single level:

| Level | Operators |
|---|---|
| 4 | `&`, `\|`, `^` |

**Implementation** matches AEL-DOC — all three are in one `bitwiseExpression` rule
with left-associativity.

**Impact:** `1 | 2 & 4` parses as `(1 | 2) & 4` in the implementation, whereas PDF-SPEC
precedence would parse it as `1 | (2 & 4)`.

---

## Implementation Issues

### 1. `NAME_IDENTIFIER` Accepts Digit-Starting Tokens

**Historical spec:** bare integer `1` in a path position was described as an **integer** map key; string key
`"1"` used quotes: `$.m."1"`.

**Implementation (`Condition.g4` line 554):**

```
NAME_IDENTIFIER: [a-zA-Z0-9_]+;
```

Identifiers can start with digits (e.g. `1`, `123abc`). When a map has integer keys,
`$.m.1` parses `1` as `NAME_IDENTIFIER` which resolves to **string** key `"1"` rather
than **integer** key `1`.

**Impact:**

| Expression | Spec Expects | Actual Behaviour |
|---|---|---|
| `$.m.1` (map has int key 1) | Integer key → found | String key → `null` |
| `$.m.1` (map has both int 1 and string "1") | Integer key value | String key value |

---

### 2. `exists()` Path Function Silently Ignored

**Historical spec:** described `exists()` as returning a boolean for bins or nested elements.

**Implementation:**
The grammar defines `pathFunctionExists`, but `ExpressionConditionVisitor` never
overrides `visitPathFunctionExists`. The `PathFunction.PathFunctionType` enum has no
`EXISTS` value — only `GET`, `COUNT`, `SIZE`, `CAST`. The default visitor returns `null`,
which causes:

1. `visitPathFunctionIfPresent` returns `null`.
2. `visitPath` falls through to `return basePath.getBinPart()`.
3. The `exists()` call is silently dropped — the expression degrades to reading the
   bin value directly.

**Impact:**

| Expression | Spec Expects | Actual Behaviour |
|---|---|---|
| `$.binA.exists()` (binA=42) | `true` (boolean) | `42` (raw value — exists() dropped) |
| `$.binB.exists()` (binB missing) | `false` (boolean) | Error — reads non-existent bin |
| `$.mapbin.a.exists()` | `true`/`false` | Map key lookup (exists() dropped) |

**Documentation:** `exists()` is not described in `ael-documentation.md`; regression coverage is `BinExpressionsTests#existsPathSuffixIsIgnoredCompilesAsPlainPath`.

---

### 3. Mutation Path Functions — Grammar Stubs, No Visitor

**Historical spec:** defined CDT mutation functions: `remove()`, `insert()`, `set()`, `append()`,
`increment()`, `clear()`, `sort()` (list), and `put()` (map).

**Implementation (`Condition.g4` lines 514–525):**
The grammar accepts the following as `pathFunction` alternatives:
`remove()`, `insert()`, `set()`, `append()`, `increment()`, `clear()`, `sort()`.

But **no visitor methods** exist for any of them. Visiting these returns `null` and
they are silently ignored (suffix has no effect on the compiled expression).

**Note:** `put()` is also missing from the grammar entirely (see issue 5).

**Impact:** Mutation suffixes are **not** compiled to `Exp` / CDT modify operations; treating parsed input as executable writes would be incorrect.

**Documentation:** `ael-documentation.md` §8.4 — parser vs compiler distinction.

---

### 4. `type()` Path Function — Missing from Grammar

**Historical spec (`type()` as path function):**

```
$.binA.type() == INT
```

Returns the data type of the element as one of `INT`, `STR`, etc.

**Implementation:**
There is no `type()` path function in the grammar. The word `type` only appears as a
parameter name inside `get()`: `PATH_FUNCTION_PARAM_TYPE: 'type'`.

**Impact:** `$.binA.type() == INT` fails to parse.

---

### 5. `put()` Map Mutation — Missing from Grammar

**AEL-DOC (§8.6):**
`put()` is listed as a map mutation function that inserts or updates a key-value pair.

**Implementation:**
The `pathFunction` rule does not include `put()`. It includes `set()` (list-only), but
`put()` (map write) is absent.

**Impact:** Map write operations cannot be expressed in AEL.

---

### 6. Open-Start Ranges — Missing from Grammar

**AEL-DOC (§5.2, §6.2):**

```
$.m.{-d}          keys from the start up to "d"
$.m.{:5}          index from start to 5
$.m.{#:4}         rank from start to 4
$.l.[:5]          list index from start to 5
```

**Implementation:**

- `keyRangeIdentifier`: only `mapKey '-' mapKey` and `mapKey '-'` (open end). No
  `'-' mapKey` form for open start.
- `indexRangeIdentifier`: only `start ':' end` and `start ':'` (open end). No
  `':' end` form for open start.
- `rankRangeIdentifier`: same — no open-start form.

**Impact:** Expressions like `$.m.{-d}`, `$.m.{:5}`, `$.l.[:3]` fail to parse.
Workaround: use explicit start values (e.g. `$.m.{0:5}`).

**Documentation:** `ael-documentation.md` §5.2 / §6.2 — open-start forms listed as not in `Condition.g4`.

---

### 7. Block Comments — Missing from Grammar

**AEL-DOC (§17):**

```
/* This is a comment */
$.age > 21 /* filter adults */
```

**Implementation:**
No comment-skip rule. Only whitespace is skipped:

```
WS: [ \t\r\n]+ -> skip;
```

**Impact:** Expressions containing `/* ... */` fail to parse.

**Documentation:** `ael-documentation.md` §17 states that block comments are not supported.

---

### 8. `error`/`unknown` Keywords — Not Explicit in Grammar

**AEL-DOC (§14.3):**

```
when ($.status in ["GOLD", "PLATINUM"] => true, default => error)
```

`error` and `unknown` are interchangeable keywords that produce a runtime exception.

**Implementation:**
These keywords are not defined as grammar tokens. They match `NAME_IDENTIFIER` and
are treated as ordinary identifiers (bin names or string map keys) — no special
handling exists in the visitor.

**Impact:** `default => error` silently treats `error` as a bin reference rather than
raising a runtime exception.

---

### 9. `WILDCARD`, `NIL`, `INF` Special Values — Not Handled

**Historical spec** described special list/map sentinels:

| Value | Description (not in this client) |
|---|---|
| `*` (WILDCARD) | Matches any value from that point |
| `NIL` | Lowest possible CDT comparison value |
| `INF` | Highest possible CDT comparison value |

**Implementation:**
- `NIL` and `INF` match `NAME_IDENTIFIER` but are treated as ordinary string
  identifiers — no visitor logic maps them to `Value.getAsNull()` or `Value.INFINITY`.
- `*` (WILDCARD) is not a `NAME_IDENTIFIER` match and has no lexer token.

**Impact:**
- `$.m.{NIL-d}` treats `NIL` as string key `"NIL"` instead of lowest CDT value.
- `$.m.{a-INF}` treats `INF` as string key `"INF"` instead of highest CDT value.
- `*` fails to parse in value contexts.

---

### 10. `$.key()` Metadata Function — Missing from Grammar

**AEL-DOC (§13):**

```
$.key(STR)       user key stored with the record (string)
$.key(INT)       user key stored with the record (integer)
$.key(BLOB)      user key stored with the record (blob)
```

**PDF-SPEC** also specifies `$.key(<STR, INT, BLOB>)`.

**Implementation:**
The `METADATA_FUNCTION` token includes `keyExists()` but has no `key(...)` variant.
`MetadataOperand.constructMetadataExp` has no `key` case.

**Impact:** `$.key(STR)` and similar expressions fail to parse.

---

### 11. String Escape Sequences Not Implemented

**AEL-DOC (§2.3):**
Documents the following escape sequences inside quoted strings:

| Escape | Meaning |
|---|---|
| `\\` | Literal backslash |
| `\n` | Newline |
| `\t` | Tab |
| `\"` | Double quote (inside double-quoted strings) |
| `\'` | Single quote (inside single-quoted strings) |

**PDF-SPEC** documents the same escapes.

**Implementation:**
The `QUOTED_STRING` lexer rule matches any characters between quotes except the
closing quote character:

```
QUOTED_STRING: ('\'' (~'\'')* '\'') | ('"' (~'"')* '"');
```

This means `\'` inside a single-quoted string terminates the string early (the `\`
is a literal character, and `'` ends the string). The `unquote()` utility simply
strips the outer quote characters — it performs no escape processing.

**Impact:**
- `'it\'s'` fails to parse (string ends at `'it\'`, leaving `s'` dangling).
- `"line1\nline2"` produces the literal string `line1\nline2` (8 characters), not
  a string with an actual newline.
- `"path\\to\\file"` produces `path\\to\\file` (literal backslashes preserved, not
  collapsed).

**Documentation:** `ael-documentation.md` §2.3 describes the actual quoting behavior (no escape processing).

---

### 12. Value Ranges Limited to Integers Only

**AEL-DOC (§5.2, §6.2):**
Value ranges use `valueIdentifier` which includes strings and integers:

```
{=111:334}      Map value range from 111 to 334
[=5:8]          List value range from 5 to 8
```

**PDF-SPEC** shows string value ranges:

```
{=d:f}  → getByValueRange(MapReturnType.ORDERED_MAP, "d", "f", "m")
```

**Implementation:**
`MapValueRange.java` and `ListValueRange.java` both call
`requireIntValueIdentifier()`, which throws an `AelParseException` if the parsed
value is not an `Integer`:

```java
public static Integer requireIntValueIdentifier(ValueIdentifierContext ctx) {
    Object result = parseValueIdentifier(ctx);
    if (result instanceof Integer intValue) {
        return intValue;
    }
    throw new AelParseException(
            "Value range requires integer operands, got: %s".formatted(ctx.getText()));
}
```

The grammar's `valueRangeIdentifier` rule accepts `valueIdentifier` (which includes
`NAME_IDENTIFIER` and `QUOTED_STRING`), so the parser will accept string value
ranges, but the semantic layer rejects them.

**Impact:** String value ranges like `{=d:f}` or `{="apple":"mango"}` parse
successfully but fail at the visitor level. Only integer value ranges work.

---

### 13. Float/List Values Not Supported in CDT Value Positions

**AEL-DOC (§5.2):**
Relative rank range examples use list values containing floats and special values:

```
{#-1:1~10}                    relative to integer value 10
```

```
$.scores.[#-1:1~[10.0, NIL]]  relative to list value [10.0, NIL]
```

**Implementation:**
The `relativeValue` grammar rule is:

```
relativeValue: '~' valueIdentifier;
```

And `valueIdentifier` only supports: `NAME_IDENTIFIER`, `QUOTED_STRING`, `signedInt`,
`IN`, `BLOB_LITERAL`, `B64_LITERAL`.

It does **not** include:
- Float literals (`FLOAT`, `LEADING_DOT_FLOAT`)
- List constants (`listConstant`)
- Map constants (`orderedMapConstant`)
- Boolean literals

The same limitation applies to value lists (`{=...,...}`, `[=...,...]`) and
value range endpoints.

Additionally, `ParsingUtils.objectToExp()` only handles `String`, `Integer`, and
`byte[]` — not `Double`, `Boolean`, `List`, or `Map`.

**Impact:**
- `[#-1:1~[10.0, NIL]]` (from AEL-DOC §19) cannot be parsed.
- `{=3.14,2.71}` (float value list) cannot be parsed.
- `[=true,false]` (boolean value list) cannot be parsed.
- Float, list, map, and boolean values cannot appear in any CDT selector position
  that uses `valueIdentifier`.

---

### 14. Bitwise Operator Precedence Differs from PDF-SPEC

**PDF-SPEC (p.22-23):**
Three separate precedence levels:

| Level | Operator |
|---|---|
| 7 | `&` (most binding) |
| 8 | `^` |
| 9 | `\|` (least binding) |

**AEL-DOC (§16):**
Single level: `&`, `\|`, `^` at level 4.

**Implementation (`Condition.g4`):**
All three are alternatives in one `bitwiseExpression` rule:

```
bitwiseExpression
    : shiftExpression
    | bitwiseExpression '&' shiftExpression
    | bitwiseExpression '|' shiftExpression
    | bitwiseExpression '^' shiftExpression
    ;
```

The implementation matches AEL-DOC. Since AEL-DOC is the golden standard, this is
noted for awareness but not considered a bug.

**Impact:** Expressions mixing `&`, `|`, `^` without parentheses evaluate
left-to-right at equal precedence rather than with `&` binding tightest. Users
relying on C/Java-style precedence (`&` > `^` > `|`) may get unexpected results.

---

## Maintainer notes

- **`docs/ael-documentation.md`** — user-facing reference; **must** track **`Condition.g4`** + **`ExpressionConditionVisitor`** (and related `ael` Java) as the source of truth.
- **`docs/ael-spec-vs-implementation.md`** (this file) — engineering backlog: PDF/legacy spec features that are still missing from grammar or visitor, or differ by design. Update the summary table when fixing or intentionally deferring an item.

