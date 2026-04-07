# AEL Spec vs Implementation — Identified Differences

This document catalogues incongruities between the
[Expression AEL specification](../CORE-Expression%20AEL.pdf) and its current
implementation in the `ael` package. Items 2b–2f were identified via runtime
testing; items 2g–2n were identified by comparing the spec documentation
against the `Condition.g4` grammar. Each section describes what the spec
requires, what the implementation actually does, and (where applicable)
references the reproducing test in `examples/.../OperationDifferences.java`.

## Summary

| ID | Issue | Severity | Status |
|---|---|---|---|
| 2b | `NAME_IDENTIFIER` allows digit-start → wrong map key type | High | Open — `test2b` |
| 2d | `exists()` silently dropped by visitor | High | Open — `test2d` |
| 2e | Mutation functions are grammar stubs with no visitor | Medium | Open — `test2e` |
| 2f | `deviceSize`/`memorySize` aliased to `recordSize` | Low (server >= 7.0) | Open — requires pre-7.0 server |
| 2g | `type()` path function missing from grammar | Medium | Open — parse failure |
| 2h | `put()` map mutation missing from grammar | Medium | Open — parse failure |
| 2i | Open-start ranges (`{-d}`, `{:5}`, `{#:4}`) missing | Medium | Open — parse failure |
| 2j | BLOB literals (`X'ffee'`) missing from grammar | Medium | Open — parse failure |
| 2k | Block comments (`/* */`) missing from grammar | Low | Open — parse failure |
| 2l | `error`/`unknown` keywords treated as bin names | Medium | Open — silently wrong behaviour |
| 2m | `WILDCARD`/`NIL`/`INF` not handled as special values | Medium | Open — `NIL`/`INF` treated as string keys |
| 2n | `$.key()` metadata function missing from grammar | Medium | Open — parse failure |

---

## 2b. `NAME_IDENTIFIER` Accepts Digit-Starting Tokens

**Spec:**
Unquoted identifiers must match `^[A-Za-z]\w*$` (must start with a letter).

**Implementation (`Condition.g4` line 546):**

```
NAME_IDENTIFIER: [a-zA-Z0-9_]+;
```

**Description:**
The grammar allows identifiers that start with digits (e.g. `1`, `123abc`).
This means that when a map has integer keys, the AEL expression `$.m.1`
is parsed with `1` matching `NAME_IDENTIFIER`, which resolves it as a
**string** key lookup for `"1"` rather than an **integer** key lookup for `1`.

**Impact:**

Given a map `{1(int): "val_from_int_key_1"}`:

| Expression | Spec Expects | Actual Behaviour |
|---|---|---|
| `$.m.1` | Integer key `1` → `"val_from_int_key_1"` | String key `"1"` → `null` (not found) |

Given a map with both `1(int)` and `"1"(string)` keys:

| Expression | Spec Expects | Actual Behaviour |
|---|---|---|
| `$.m.1` | Integer key `1` → `"INTEGER_KEY_1"` | String key `"1"` → `"STRING_KEY_1"` |

**Test:** `test2b_NameIdentifierTooPermissive`

- **[A]** Map with only integer keys — `$.m.1.get(type: STRING)` returns
  `null` because the AEL looks up string key `"1"`.
- **[B]** Map with both integer key `1` and string key `"1"` — returns the
  string key's value instead of the integer key's value.

---

## 2d. `exists()` Path Function Silently Ignored

**Spec (p.10):**

```
$.a.exists() and $.b.exists()
```

Should check whether bins `a` and `b` exist, mapping to `Exp.binExists()`.

**Implementation:**
The grammar defines the rule:

```
pathFunctionExists: PATH_FUNCTION_EXISTS;
PATH_FUNCTION_EXISTS: 'exists' '()';
```

But `ExpressionConditionVisitor` never overrides `visitPathFunctionExists`.
The default ANTLR visitor returns `null`, which means:

1. `visitPathFunctionIfPresent` calls `visit(ctx.pathFunction())`, which
   dispatches to the default visitor and returns `null`.
2. In `visitPath`, the check
   `pathFunction != null && pathFunction.getPathFunctionType() == COUNT`
   evaluates to `false`.
3. `overrideBinWithPathFunction` receives `null` and does nothing.
4. Falls through to `return basePath.getBinPart()` — the `exists()` call
   is silently dropped and the expression degrades to reading the bin value
   directly.

**Impact:**

| Expression | Spec Expects | Actual Behaviour |
|---|---|---|
| `$.binA.exists()` (binA=42) | `true` (boolean) | `42` (raw bin value — exists() dropped) |
| `$.binB.exists()` (binB missing) | `false` (boolean) | Error — attempts to read non-existent bin |
| `$.binA.exists() and $.flag` | `true and true` = passes filter | Degrades to `42 and true` — passes, but for wrong reason (truthy int) |

**Test:** `test2d_ExistsSilentlyIgnored`

- **[A]** `$.binA.exists() and $.flag` as a where clause — filter passes,
  but only because int `42` is treated as truthy. The `exists()` call
  contributed nothing.
- **[B]** `$.binB.exists()` as a where clause — should evaluate to `false`
  but instead errors because reading a missing bin as boolean fails
  server-side.
- **[C]** `$.binA.exists()` as a read expression — returns `42` (Long)
  instead of `true` (Boolean), proving `exists()` was silently dropped.

---

## 2e. Mutation Path Functions — Grammar Stubs, No Implementation

**Spec (p.10-11):**
These path functions are defined for CDT mutation:

| Function | List | Map | Maps to |
|---|---|---|---|
| `remove()` | Yes | Yes | `ListExp.removeByXxx()` / `MapExp.removeByXxx()` |
| `insert()` | Yes | — | `ListExp.insert()` |
| `set()` | Yes | — | `ListExp.set()` |
| `append()` | Yes | — | `ListExp.append()` |
| `increment()` | Yes | Yes | `ListExp.increment()` / `MapExp.increment()` |
| `clear()` | Yes | Yes | `ListExp.clear()` / `MapExp.clear()` |
| `sort()` | Yes | — | `ListExp.sort()` |
| `put()` | — | Yes | `MapExp.put()` |

**Implementation (`Condition.g4` lines 506-518):**

```
pathFunction
    : pathFunctionCast
    | pathFunctionExists
    | pathFunctionGet
    | pathFunctionCount
    | 'remove' '()'
    | 'insert' '()'
    | 'set' '()'
    | 'append' '()'
    | 'increment' '()'
    | 'clear' '()'
    | 'sort' '()'
    ;
```

The grammar accepts these tokens, but there are **no visitor methods** in
`ExpressionConditionVisitor` for any of them. Like `exists()`, visiting
these returns `null` and they are silently ignored. Note that `put()` is
also missing from the grammar entirely.

**Impact:**

| Write Expression | Spec Expects | Actual Behaviour |
|---|---|---|
| `$.listBin.[].sort()` | Sorted list `[10,20,30,40,50]` | Error or returns unsorted original |
| `$.listBin.[=30].remove()` | List without 30: `[50,10,40,20]` | Error or returns original list |
| `$.listBin.[].clear()` | Empty list `[]` | Error or returns original list |

The original `listBin` data is never modified — confirming all mutation
operations are no-ops.

**Test:** `test2e_MutationOperationsIgnored`

- **[A]** `$.listBin.[].sort()` via write expression — sort() dropped.
- **[B]** `$.listBin.[=30].remove()` via write expression — remove() dropped.
- **[C]** `$.listBin.[].clear()` via write expression — clear() dropped.
- **[D]** Verifies original `listBin` is unchanged, confirming mutations
  had no effect.

---

## 2f. Metadata Functions Aliasing

**Spec:**
The AEL metadata functions `deviceSize()`, `memorySize()`, and
`recordSize()` should map to their respective upstream `Exp` methods:

| AEL Function | Expected Upstream Method |
|---|---|
| `$.deviceSize()` | `Exp.deviceSize()` (deprecated, pre-7.0) |
| `$.memorySize()` | `Exp.memorySize()` (deprecated, pre-7.0) |
| `$.recordSize()` | `Exp.recordSize()` |

**Implementation (`MetadataOperand.java` line 46):**

```java
case "deviceSize", "recordSize", "memorySize" -> Exp.recordSize();
```

**Description:**
All three metadata functions are aliased to `Exp.recordSize()`. The
upstream `Exp.java` has distinct (deprecated) methods `Exp.deviceSize()`
and `Exp.memorySize()` for pre-7.0 servers. If the AEL is intended to work
with servers older than 7.0, this aliasing produces incorrect results —
`$.deviceSize()` and `$.memorySize()` would return the total record size
rather than their respective device/memory sizes.

**Impact:**
On Aerospike server < 7.0, `$.deviceSize()` and `$.memorySize()` return
the wrong value (total record size instead of device-only or memory-only
size). On server >= 7.0 all three are equivalent, so the bug is latent.

**Test:** Not included in `OperationDifferences.java` (requires a pre-7.0
server to demonstrate the difference).

---

## 2g. `type()` Path Function — Missing from Grammar

**Spec:**

```
$.binA.type() == INT
```

Returns the data type of the element as an integer constant.

**Implementation:**
The `type()` function is not defined in the `pathFunction` rule in `Condition.g4`.
There is no lexer token or parser rule for it.

**Impact:**
`$.binA.type() == INT` fails to parse. Users cannot inspect bin types at runtime.

---

## 2h. `put()` Map Mutation — Missing from Grammar

**Spec:**

```
$.mapBin.key.put(value)
```

Maps to `MapExp.put()` which upserts a key-value pair into a map (creates if
missing, overwrites if exists).

**Implementation:**
The grammar defines `set()` but not `put()`. These are **not equivalent**:
- `set()` maps to `ListExp.set()` — overwrites a value at a specific list index
- `put()` maps to `MapExp.put()` — upserts a key-value pair into a map

There is no map-level write function in the grammar.

**Impact:**
Map write operations (inserting or updating key-value pairs) cannot be expressed.

---

## 2i. Open-Start Ranges — Missing from Grammar

**Spec:**

```
$.m.{-d}          keys from the start up to "d"
$.m.{:5}          index from start to 5
$.m.{#:4}         rank from start to 4
$.l.[:5]          list index from start to 5
```

**Implementation (`Condition.g4`):**
- `keyRangeIdentifier`: only `mapKey '-' mapKey` and `mapKey '-'` (open end).
  No `'-' mapKey` alternative for open start.
- `indexRangeIdentifier`: only `start ':' end` and `start ':'` (open end).
  No `':' end` alternative for open start.
- `rankRangeIdentifier`: same — no open-start form.

**Impact:**
Expressions like `$.m.{-d}`, `$.m.{:5}`, `$.l.[:3]` fail to parse. Users must
work around this by using explicit start values (e.g., `$.m.{0:5}` instead of
`$.m.{:5}`).

---

## 2j. BLOB Literals — Missing from Grammar

**Spec:**

```
X'ffee'
x'102030405060708090abcdef'
```

Hex-encoded byte sequences as literal values.

**Implementation:**
There is no lexer rule for BLOB/hex literals in `Condition.g4`. The only literal
types tokenized are `INT`, `FLOAT`, `QUOTED_STRING`, `TRUE`, `FALSE`.

**Impact:**
BLOB literals cannot be used in expressions. Users cannot compare bin values to
byte sequences or use BLOB constants in value lists/ranges.

---

## 2k. Block Comments — Missing from Grammar

**Spec:**

```
/* This is a comment */
$.age > 21 /* filter adults */
```

C-style block comments.

**Implementation:**
There is no comment-skip rule in `Condition.g4`. The only skip rule is
`WS: [ \t\r\n]+ -> skip`.

**Impact:**
Expressions containing `/* ... */` comments fail to parse.

---

## 2l. `error`/`unknown` Keywords — Not Explicit in Grammar

**Spec:**

```
when ($.status in ["GOLD", "PLATINUM"] => true, default => error)
```

`error` and `unknown` are interchangeable keywords that produce a runtime
exception when evaluated.

**Implementation:**
These keywords are not defined as grammar tokens or rules. They match
`NAME_IDENTIFIER` but are **not** recognised by the visitor — no special
handling exists for either string. They are treated as ordinary identifiers
(e.g., bin names or string map keys) rather than as error-producing
expressions.

**Impact:**
`when (... default => error)` silently treats `error` as a bin reference
named `error` rather than raising a runtime exception. The intent of forcing
a guaranteed failure on unexpected branches is lost.

---

## 2m. `WILDCARD`, `NIL`, `INF` Special Values — Not Explicit in Grammar

**Spec:**

| Value | Description |
|---|---|
| `*` (WILDCARD) | Matches any value from that point |
| `NIL` | Lowest possible CDT comparison value |
| `INF` | Highest possible CDT comparison value |

**Implementation:**
`NIL` and `INF` match `NAME_IDENTIFIER: [a-zA-Z0-9_]+` but are **not**
recognised by the visitor — no special handling maps them to
`Value.getAsNull()` or `Value.INFINITY`. They are treated as ordinary
identifiers (bin names or string map keys). `*` (WILDCARD) is not a
`NAME_IDENTIFIER` match and would need its own lexer token, which does not
exist.

**Impact:**
- `$.m.{NIL-d}` treats `NIL` as a string key `"NIL"` rather than the
  lowest CDT comparison value.
- `$.m.{a-INF}` treats `INF` as a string key `"INF"` rather than the
  highest CDT comparison value.
- `*` (WILDCARD) fails to parse in value contexts where it is expected.

---

## 2n. `$.key()` Metadata Function — Missing from Grammar

**Spec:**

```
$.key(STR)       user key stored with the record (string)
$.key(INT)       user key stored with the record (integer)
$.key(BLOB)      user key stored with the record (blob)
```

**Implementation:**
The `METADATA_FUNCTION` token in `Condition.g4` lists `ttl()`, `voidTime()`,
`lastUpdate()`, `sinceUpdate()`, `setName()`, `keyExists()`, `isTombstone()`,
`deviceSize()`, `memorySize()`, `recordSize()`, and `digestModulo(INT)`.
There is no `key(...)` variant.

**Impact:**
`$.key(STR)` and similar expressions fail to parse. Users cannot access the
stored user key.
