# AEL Spec vs Implementation — Current Differences

This document catalogues current incongruities between the two specification documents
and the implementation in the `ael` package:

- **`ael-documentation.md`** — the golden-standard specification (referred to as **AEL-DOC**)
- **CORE-Expression DSL PDF** — the original design document (referred to as **PDF-SPEC**)

Where the two spec documents differ from each other, those differences are noted.
AEL-DOC takes precedence.

---

## Summary

| # | Issue | Severity |
|---|---|---|
| 1 | `NAME_IDENTIFIER` allows digit-start → wrong map key type | High |
| 2 | `exists()` silently dropped by visitor | High |
| 3 | Mutation functions are grammar stubs with no visitor | Medium |
| 4 | `deviceSize`/`memorySize` aliased to `recordSize` | Low (server ≥ 7.0) |
| 5 | `type()` path function missing from grammar | Medium |
| 6 | `put()` map mutation missing from grammar | Medium |
| 7 | Open-start ranges (`{-d}`, `{:5}`, `{#:4}`) missing | Medium |
| 8 | Block comments (`/* */`) missing from grammar | Low |
| 9 | `error`/`unknown` keywords treated as bin names | Medium |
| 10 | `WILDCARD`/`NIL`/`INF` not handled as special values | Medium |
| 11 | `$.key()` metadata function missing from grammar | Medium |
| 12 | String escape sequences not implemented | Medium |
| 13 | Value ranges limited to integers only | Medium |
| 14 | Float/list values not supported in CDT value positions | Medium |
| 15 | Bitwise operator precedence differs from PDF-SPEC | Low (matches AEL-DOC) |
| 16 | B64 literal syntax is an undocumented extension | Low |

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

### S2. Index/rank range end semantics (inclusive vs exclusive)

**PDF-SPEC** uses inclusive end semantics in its examples:
- `{0:1}` → `getByIndexRange(start=0, count=2)` where count = end−start+1
- `[3:7]` → "count 5 (7−3+1)"

**AEL-DOC** (§20, Rule 10) states all ranges use exclusive end:
> "Index ranges in the AEL are also exclusive on the end."

However, AEL-DOC §5.2 contains a conflicting description for map index ranges:
`{1:3}` is described as "Index 1 through 3 (inclusive)" while `{-3:-1}` is described
as "Last 3 entries". Both descriptions imply inclusive end, contradicting Rule 10.

**Implementation** uses exclusive end throughout: `count = end − start`.

### S3. Leading-dot floats

**PDF-SPEC** explicitly lists `.37` as a valid float literal.

**AEL-DOC** (§2.2) shows `0.37` but does not mention the leading-dot form.

**Implementation** supports both — the grammar has a `LEADING_DOT_FLOAT: '.' [0-9]+`
rule.

### S4. Unicode restriction

**PDF-SPEC** states: "Note that non-ASCII unicode characters are not supported."

**AEL-DOC** does not mention this restriction.

**Implementation** does not enforce any restriction — the `QUOTED_STRING` lexer rule
accepts any character, including non-ASCII unicode.

### S5. B64 literals

Neither spec document mentions B64-encoded BLOB literals.

**Implementation** supports `b64'...'` / `B64'...'` syntax via the `B64_LITERAL` lexer
rule, with tests in `BlobTests.java` and `CtxTests.java`. This is an undocumented
extension.

---

## Implementation Issues

### 1. `NAME_IDENTIFIER` Accepts Digit-Starting Tokens

**AEL-DOC (§5.1):**
Bare integer `1` in a path position is an **integer** map key. To access string key
`"1"`, use quotes: `$.m."1"`.

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

**AEL-DOC (§8.3):**

```
$.binA.exists() and $.binB.exists()
$.mapbin.a.exists()
```

Should check whether a bin or element exists, returning a boolean.

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

---

### 3. Mutation Path Functions — Grammar Stubs, No Visitor

**AEL-DOC (§8.6):**
Defines CDT mutation functions: `remove()`, `insert()`, `set()`, `append()`,
`increment()`, `clear()`, `sort()` (list), and `put()` (map).

**Implementation (`Condition.g4` lines 514–525):**
The grammar accepts the following as `pathFunction` alternatives:
`remove()`, `insert()`, `set()`, `append()`, `increment()`, `clear()`, `sort()`.

But **no visitor methods** exist for any of them. Visiting these returns `null` and
they are silently ignored — identical to the `exists()` issue.

**Note:** `put()` is also missing from the grammar entirely (see issue 6).

**Impact:** All mutation operations via AEL expressions are no-ops. Expressions like
`$.listBin.[=30].remove()` silently return the original data without modification.

---

### 4. Metadata Functions Aliasing

**AEL-DOC (§13):**
`$.deviceSize()`, `$.memorySize()`, and `$.recordSize()` are listed as separate
metadata functions returning different size values.

**Implementation (`MetadataOperand.java` line 46):**

```java
case "deviceSize", "recordSize", "memorySize" -> Exp.recordSize();
```

All three are aliased to `Exp.recordSize()`. The upstream `Exp.java` has distinct
(deprecated) methods `Exp.deviceSize()` and `Exp.memorySize()` for pre-7.0 servers.

**Impact:** On Aerospike server < 7.0, `$.deviceSize()` and `$.memorySize()` return
the wrong value (total record size instead of device-only or memory-only size). On
server ≥ 7.0 all three are equivalent.

---

### 5. `type()` Path Function — Missing from Grammar

**AEL-DOC (§8.5):**

```
$.binA.type() == INT
```

Returns the data type of the element as one of `INT`, `STR`, etc.

**Implementation:**
There is no `type()` path function in the grammar. The word `type` only appears as a
parameter name inside `get()`: `PATH_FUNCTION_PARAM_TYPE: 'type'`.

**Impact:** `$.binA.type() == INT` fails to parse.

---

### 6. `put()` Map Mutation — Missing from Grammar

**AEL-DOC (§8.6):**
`put()` is listed as a map mutation function that inserts or updates a key-value pair.

**Implementation:**
The `pathFunction` rule does not include `put()`. It includes `set()` (list-only), but
`put()` (map write) is absent.

**Impact:** Map write operations cannot be expressed in AEL.

---

### 7. Open-Start Ranges — Missing from Grammar

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

---

### 8. Block Comments — Missing from Grammar

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

---

### 9. `error`/`unknown` Keywords — Not Explicit in Grammar

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

### 10. `WILDCARD`, `NIL`, `INF` Special Values — Not Handled

**AEL-DOC (§2.8):**

| Value | Description |
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

### 11. `$.key()` Metadata Function — Missing from Grammar

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

### 12. String Escape Sequences Not Implemented

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

---

### 13. Value Ranges Limited to Integers Only

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

### 14. Float/List Values Not Supported in CDT Value Positions

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

### 15. Bitwise Operator Precedence Differs from PDF-SPEC

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

### 16. B64 Literal Syntax Is an Undocumented Extension

**AEL-DOC and PDF-SPEC:**
Neither document mentions Base64-encoded BLOB literals.

**Implementation:**
The grammar supports `b64'...'` / `B64'...'` via:

```
B64_LITERAL: [bB] '64\'' [A-Za-z0-9+/=]* '\'';
```

The visitor parses these in `visitBlobOperand` and `parseValueIdentifier`. Tests
exist (`BlobTests.java`, `CtxTests.java`).

**Impact:** None on correctness — this is a useful extension. Should be documented
in AEL-DOC for completeness.

---

## Internal Inconsistency in AEL-DOC

### Map Index Range End Semantics

AEL-DOC §5.2 describes map index ranges with inclusive end semantics:

> `{1:3}` — Index 1 through 3 (inclusive)
> `{-3:-1}` — Last 3 entries

But AEL-DOC §20 (Rule 10) states:

> Index ranges in the AEL are also **exclusive** on the end.

The implementation uses exclusive end: `count = end − start`. List index ranges,
list rank ranges, map rank ranges, and key ranges all consistently use exclusive end
semantics throughout both the docs and implementation.

The §5.2 map index range table descriptions should be updated to say "(exclusive)"
for consistency with Rule 10 and the implementation.
