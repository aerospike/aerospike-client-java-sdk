# DSL Spec vs Implementation — Identified Differences

This document catalogues six incongruities between the
[Expression DSL specification](../CORE-Expression%20DSL.pdf) and its current
implementation in the `dsl` package. Each section describes what the spec
requires, what the implementation actually does, and (where applicable)
references the reproducing test in
`examples/.../OperationDifferences.java`.

---

## 2a. `let...then` vs `with...do` Keyword Mismatch

**Spec (p.18):**

```
let (x = 1, y = ${x} + 1) then (${y} + ${x})
```

**Implementation (`Condition.g4` line 24):**

```
'with' '(' variableDefinition (',' variableDefinition)* ')' 'do' '(' expression ')'   # WithExpression
```

**Description:**
The spec defines local variable scoping with the keywords `let` and `then`.
The grammar instead uses `with` and `do`. The spec's keywords are more
conventional (`let` is widely understood as variable binding in functional
languages) and were the agreed-upon target, but the grammar has not been
updated.

**Impact:**

- `let (x = $.x, y = $.y) then (${x} + ${y})` — fails to parse.
- `with (x = $.x, y = $.y) do (${x} + ${y})` — works, but contradicts the spec.

**Test:** `test2a_LetThenVsWithDo` (currently commented out in `main`
because the `with...do` form is still the only working syntax).

---

## 2b. `NAME_IDENTIFIER` Accepts Digit-Starting Tokens

**Spec:**
Unquoted identifiers must match `^[A-Za-z]\w*$` (must start with a letter).

**Implementation (`Condition.g4` line 506):**

```
NAME_IDENTIFIER: [a-zA-Z0-9_]+;
```

**Description:**
The grammar allows identifiers that start with digits (e.g. `1`, `123abc`).
This means that when a map has integer keys, the DSL expression `$.m.1`
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
  `null` because the DSL looks up string key `"1"`.
- **[B]** Map with both integer key `1` and string key `"1"` — returns the
  string key's value instead of the integer key's value.

---

## 2c. `>>` Operator Semantics Reversed

**Spec:**

| Operator | Meaning | Java Equivalent |
|---|---|---|
| `>>` | Arithmetic right shift (sign-preserving) | `>>` / `Exp.arshift()` |
| `>>>` | Logical right shift (zero-fill) | `>>>` / `Exp.rshift()` |

**Implementation (`VisitorUtils.java` line 157):**

```java
case R_SHIFT -> Exp::rshift;   // logical right shift!
```

**Description:**
The DSL's `>>` operator is wired to `Exp.rshift()`, which performs a
**logical** (zero-fill) right shift. Per the spec and standard Java
convention, `>>` should be **arithmetic** (sign-preserving) via
`Exp.arshift()`. Additionally, the spec's `>>>` operator for logical right
shift does not exist in the grammar at all.

**Impact:**

For negative numbers the result is completely wrong:

```
-8 >> 1
  Spec / Java:    -4     (arithmetic, sign bit preserved)
  DSL actual:     9223372036854775804  (logical, zero-fill)
```

For positive numbers the results are identical (both shift variants produce
the same output), masking the bug in simple tests.

**Test:** `test2c_RightShiftReversed`

- **[A]** Negative number `-8 >> 1`: expects `-4` (arithmetic), gets
  `9223372036854775804` (logical).
- **[B]** Positive number `16 >> 1`: expects `8`, gets `8` — correct by
  coincidence (both shift types agree for positive values).
- **[C]** Attempts `$.intBin >>> 1` — fails to parse because `>>>` is
  not in the grammar.

**Net result:** `>>` does logical when it should do arithmetic, and `>>>`
doesn't exist at all.

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

1. `overrideWithPathFunction` receives `null` and does nothing.
2. In `visitPath`, the condition
   `!cdtParts.isEmpty() || ctx.pathFunction() != null && ctx.pathFunction().pathFunctionCount() != null`
   evaluates to `false`.
3. Falls through to `return basePath.getBinPart()` — the `exists()` call
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

**Implementation (`Condition.g4` lines 473-479):**

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
The DSL metadata functions `deviceSize()`, `memorySize()`, and
`recordSize()` should map to their respective upstream `Exp` methods:

| DSL Function | Expected Upstream Method |
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
and `Exp.memorySize()` for pre-7.0 servers. If the DSL is intended to work
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

## Summary Table

| ID | Issue | Severity | Testable? |
|---|---|---|---|
| 2a | `let...then` vs `with...do` keywords | Medium | Yes — `test2a` |
| 2b | `NAME_IDENTIFIER` allows digit-start → wrong map key type | High | Yes — `test2b` |
| 2c | `>>` wired to logical shift, `>>>` missing | High | Yes — `test2c` |
| 2d | `exists()` silently dropped by visitor | High | Yes — `test2d` |
| 2e | Mutation functions are grammar stubs with no visitor | Medium | Yes — `test2e` |
| 2f | `deviceSize`/`memorySize` aliased to `recordSize` | Low (server >= 7.0) | Requires pre-7.0 server |
