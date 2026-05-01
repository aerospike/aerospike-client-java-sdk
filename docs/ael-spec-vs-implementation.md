# AEL — Specification gaps vs this repository

This document tracks **remaining gaps** between long-form / PDF AEL design material, **`Condition.g4`**, and the Java compiler (**`ExpressionConditionVisitor`** and related `ael` classes). It is aimed at engineers extending the parser or visitor.

**Primary reference (aligned to implementation):** **`ael-documentation.md`** — revised so normative text matches this repo’s grammar and visitor unless explicitly labeled otherwise.

**Legacy labels:** **AEL-DOC** / **PDF-SPEC** refer to historical prose or the CORE Expression DSL PDF. Where those two differ, the note is still useful for product direction.

---

## Recently addressed (no longer gaps here)

| Topic | Status in this tree |
|-------|---------------------|
| **Integer digit map keys** | After `$.m`, the pattern `'.' (mapPart \| listPart)` consumes `.` then `mapKey`; all-digit keys use the **`INT`** lexer rule (defined before `NAME_IDENTIFIER`), so `$.m.1` compiles to an **integer** key. Quoted `$.m."1"` is a **string** key. Coverage: `MapKeyTypingTests`, `MapExpressionsTests` (duplicate “leading-dot float” test removed). |
| **`exists()` path function** | `pathFunctionExists`, `PathFunction.PathFunctionType.EXISTS`, `visitPathFunctionExists`, and `visitPath` emit `Exp.binExists` / CDT `*ReturnType.EXISTS`. Coverage: `ExistsFunctionTests`. |
| **`error` / `unknown` keywords** | `unknownExpression: 'unknown' \| 'error'`; `UnknownOperand` → `Exp.unknown()`. |

---

## Summary — open gaps

| # | Issue | Severity | User doc |
|---|--------|----------|----------|
| 1 | Mutation path suffixes parse; **no visitor** → not compiled | Medium | **§8.5** |
| 2 | `type()` path function missing from grammar | Medium | Not advertised as supported |
| 3 | `put()` map mutation missing from grammar | Medium | **§8.5** |
| 4 | Open-start ranges (`{-d}`, `{:5}`, `{#:4}`, `[:5]`) missing from grammar | Medium | **§5.2** / **§6.2** |
| 5 | Block comments (`/* */`) missing from grammar | Low | **§17** |
| 6 | `WILDCARD` / `NIL` / `INF` not handled as special values | Medium | — |
| 7 | `$.key(STR)` / `$.key(INT)` / `$.key(BLOB)` metadata missing | Medium | **§13** |
| 8 | String escape sequences not implemented | Medium | **§2.3** |
| 9 | Value ranges limited to integers only (semantic layer) | Medium | **§5.2** value range |
| 10 | Float/list/map/boolean not in `valueIdentifier` for CDT selectors | Medium | **§19** / relative-value examples |
| 11 | Purely **numeric bin names** may fail (digits lex as `INT`, not `binPart`) | Low | **§5.1** |
| 12 | Bitwise operator precedence differs from PDF-SPEC | Low (matches current grammar) | **§16** |

---

## Spec-vs-Spec Differences

### S1. Bitwise operator precedence

**PDF-SPEC** uses three levels (`&` tightest, then `^`, then `|`).

**AEL-DOC** (§16) and **this implementation** use one `bitwiseExpression` rule for `&`, `|`, `^` with left associativity.

**Impact:** `1 | 2 & 4` parses as `(1 | 2) & 4` here, not `1 | (2 & 4)`.

---

## Implementation issues (detail)

### 1. Mutation path functions — grammar only

`remove()`, `insert()`, `set()`, `append()`, `increment()`, `clear()`, `sort()` appear in `pathFunction` but have **no** visitor implementations; compiled output ignores them as path functions.

`put()` is **absent** from the grammar.

**Documentation:** `ael-documentation.md` §8.5.

---

### 2. `type()` path function — missing

No `type()` production under `pathFunction`; `type` is only the `get()` parameter name.

---

### 3. Open-start ranges — missing

`keyRangeIdentifier`, `indexRangeIdentifier`, and `rankRangeIdentifier` have no open-start (`-key`, `:end`, …) forms.

**Workaround:** explicit starts (e.g. `{0:5}`).

---

### 4. Block comments — missing

Only `WS` is skipped; `/* */` fails to lex/parse.

---

### 5. `WILDCARD`, `NIL`, `INF`

No lexer/visitor mapping to CDT sentinels; `NIL`/`INF` behave as ordinary identifiers where allowed. `*` is not a general wildcard token in value positions.

**Regression-style coverage:** `MapExpressionsTests#nameIdentifierResemblingSpecSentinelIsOrdinaryMapKey`, `asteriskInListLiteralIsNotWildcardValue`.

---

### 6. `$.key(...)` metadata — missing

`METADATA_FUNCTION` has no `key(...)`; `MetadataOperand` has no `key` case.

---

### 7. String escape sequences — not implemented

`QUOTED_STRING` is raw; `unquote` strips delimiters only.

**Documentation:** `ael-documentation.md` §2.3.

---

### 8. Value ranges — integers only

`MapValueRange` / `ListValueRange` use `requireIntValueIdentifier()`; string endpoints parse but fail in the visitor.

---

### 9. Rich `valueIdentifier` / `objectToExp`

`relativeValue` / lists of values do not admit float literals, nested list/map literals, or booleans. `ParsingUtils.objectToExp` supports `String`, `Long`, `Integer`, `byte[]` only — not `Double`, `Boolean`, `List`, or `Map`.

---

### 10. Numeric-only bin names

`binPart` does not accept `INT`; an all-digit bin name may tokenize as `INT` and fail the parser.

---

### 11. Bitwise precedence vs PDF

Same as **S1** above.

---

## Maintainer notes

- **`docs/ael-documentation.md`** — user-facing reference; track **`Condition.g4`** + **`ExpressionConditionVisitor`** as source of truth.
- **`docs/ael-spec-vs-implementation.md`** (this file) — engineering backlog. Update the summary table when fixing or deferring an item.
