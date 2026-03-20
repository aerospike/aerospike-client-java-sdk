# Changes required for DSL v2

## Table of contents

1. [Regex filtering: `=~` operator](#regex-filtering--operator)
2. [String library functions](#string-library-functions)
3. [Advanced type inference](#advanced-type-inference)
   - [Concrete types](#concrete-types)
   - [Intermediate type states](#intermediate-type-states)
   - [Removal of default type fallback](#removal-of-default-type-fallback)
   - [1. Inference from literals](#1-inference-from-literals)
   - [2. Inference from operators](#2-inference-from-operators)
   - [3. Inference from functions](#3-inference-from-functions)
   - [4. Inference from `when` expressions](#4-inference-from-when-expressions)
   - [5. Inference from `let … then` expressions](#5-inference-from-let--then-expressions)
   - [6. Inference from record metadata functions](#6-inference-from-record-metadata-functions)
   - [7. Inference from CDT operations](#7-inference-from-cdt-operations)
   - [8. Cross-expression propagation (deferred validation)](#8-cross-expression-propagation-deferred-validation)
   - [9. Path expression inference](#9-path-expression-inference-new-for-v2)
   - [10. Inference from sub-expressions in path parameters](#10-inference-from-sub-expressions-in-path-parameters)
   - [11. Explicit type annotations](#11-explicit-type-annotations)
   - [12. Error conditions](#12-error-conditions)
   - [13. Implementation strategy](#13-implementation-strategy)
4. [Path Expression support](#path-expression-support)
5. [Support sub-expressions for all parameters (except Regex)](#support-sub-expressions-for-all-parameters-except-regex)
   - [Problem](#problem)
   - [Principle](#principle)
   - [Fixed-parameter positions](#fixed-parameter-positions)
   - [List-parameter positions](#list-parameter-positions)
   - [Disambiguation](#disambiguation)
   - [Type constraints per position](#type-constraints-per-position)
   - [Output type of path elements](#output-type-of-path-elements)
   - [Examples with real-world motivation](#examples-with-real-world-motivation)
   - [Grammar changes (summary)](#grammar-changes-summary)
6. [Full syntax of all list / map functions](#full-syntax-of-all-list--map-functions)
   - [Read operations — coverage summary](#read-operations--coverage-summary)
   - [Read operations — what needs attention](#read-operations--what-needs-attention)
   - [Remove operations — coverage summary](#remove-operations--coverage-summary)
   - [Mutation functions — MISSING: parameters and verb-based write semantics](#mutation-functions--missing-parameters-and-verb-based-write-semantics)
   - [Optional modifiers: noFail and order](#optional-modifiers-nofail-and-order)
   - [Mutation functions — grammar changes needed](#mutation-functions--grammar-changes-needed)
   - [Complete function reference](#complete-function-reference)
7. [Support for HLL and Geo](#support-for-hll-and-geo)
8. [Support for mapKeys and mapValues](#support-for-mapkeys-and-mapvalues)
9. [String library functions](#string-library-functions-1)

---

## Regex filtering: `=~` operator

The `=~` operator applies a POSIX regex match. It maps to `Exp.regexCompare()`.
The pattern uses `/pattern/flags` syntax, following the Perl/JavaScript/Ruby convention
that is the most widely recognised regex notation across programming languages.

**Syntax:**
```
expression =~ /regex_pattern/
expression =~ /regex_pattern/flags
```

The left hand expression must evaluate to a String type.

**Flag letters:**

| Flag | `RegexFlag` constant | Value | Meaning |
|------|---------------------|-------|---------|
| `i` | `RegexFlag.ICASE` | `2` | Case-insensitive matching |
| `x` | `RegexFlag.EXTENDED` | `1` | Use POSIX extended regex syntax |
| `n` | `RegexFlag.NOSUB` | `4` | Don't report match positions |
| `m` | `RegexFlag.NEWLINE` | `8` | `.` doesn't match newline; `^`/`$` match at line boundaries |

Flags compose by concatenation: `/pattern/im` means `RegexFlag.ICASE | RegexFlag.NEWLINE`.
No flags means `RegexFlag.NONE` (`0`).

**Examples:**
```
$.name =~ /^Alice/i                                name starts with Alice, case insensitive
$.store.book.*[?(@.title =~ /Lord.*/)]                title matches "Lord..."
$.store.book.*[?(@.author =~ /j\.r\.r\./i)]          case-insensitive match
$.store.stationery.*[?(@key =~ /pen.*/)]              keys starting with "pen"
$.store.book.*[?(@.title =~ /^the/im)]                case-insensitive, newline-aware
```

**Exp equivalent for `$.name =~ /^Alice/i`:**
```java
Exp.regexCompare("^Alice", RegexFlag.ICASE, 
    Exp.stringBin("name")
)
```

**Exp equivalent for `@key =~ /pen.*/`:**
```java
CTX.allChildrenWithFilter(
    Exp.regexCompare("pen.*", RegexFlag.NONE,
        Exp.stringLoopVar(LoopVarPart.MAP_KEY))
)
```

**Exp equivalent for `@.author =~ /j\.r\.r\./i`:**
```java
CTX.allChildrenWithFilter(
    Exp.regexCompare("j\\.r\\.r\\.", RegexFlag.ICASE,
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
            Exp.val("author"),
            Exp.mapLoopVar(LoopVarPart.VALUE)))
)
```

Note: `=~` is a general DSL operator, not limited to path expressions. It works
anywhere you have a string expression on the left:

## String library functions

## Advanced type inference

The DSL should derive types of bins, map keys, map values, list values, and variables
wherever possible. When the parser encounters a bin or variable for the first time
its type is UNKNOWN. As parsing progresses, context reveals the type. If a bin or
variable is later encountered with a conflicting type, the parser must raise an error.

### Concrete types

`BOOLEAN`, `INTEGER`, `FLOAT`, `STRING`, `GEO`, `HLL`, `MAP`, `LIST`, `BLOB`

These are the only types that are valid at the end of parsing. Every bin, variable,
and sub-expression must resolve to one of these before an `Exp` can be emitted.

### Intermediate type states

During parsing, three intermediate states exist. These represent partial knowledge
that must be fully resolved before the parse completes.

| State | Meaning | Example origin | Must resolve to |
|---|---|---|---|
| `UNKNOWN` | No type information yet. Every bin and variable starts here. | `$.a` first encountered | Any concrete type |
| `SAME_TYPE` | Two or more items must share the same type, but we don't yet know what that type is. Created by comparison operators when both sides are `UNKNOWN`. | `$.a == $.b` (no other context yet) | Any concrete type (once one side resolves, the other inherits) |
| `NUMERIC` | Known to be either `INTEGER` or `FLOAT`, but not yet determined which. Created by arithmetic operators when both sides are `UNKNOWN`. | `$.a + $.b` (no literals yet) | `INTEGER` or `FLOAT` |

**None of these intermediate states are valid at the end of parsing.** If any bin,
variable, or expression still has an intermediate state after the full expression
has been parsed and all deferred validations re-applied, the parser must raise an
error listing the unresolved items with a suggestion to add explicit type annotations
(e.g., `.get(type: INT)`).

Examples of resolution:
```
$.a + $.b                        a, b: NUMERIC (from +)
$.a + $.b > 10                   a, b: INTEGER (NUMERIC resolved by literal 10)

$.a == $.b                       a, b: SAME_TYPE (from ==)
$.a == $.b and $.b > 'hello'     a, b: STRING (SAME_TYPE resolved by literal 'hello')

$.a + $.b                        a, b: NUMERIC — ERROR if no further context resolves them
$.a == $.b                       a, b: SAME_TYPE — ERROR if no further context resolves them
```

### Removal of default type fallback

**The current implementation silently assigns default types when inference fails:**
- Map value access (e.g., `$.m.name`): defaults to `STRING`
- All other cases (list elements, bins): defaults to `INTEGER`

This is in `TypeUtils.getDefaultType()` and `BinPart` (which sets `Exp.Type.INT`
in its constructor).

**This must be removed in v2.** Silent defaults cause unpredictable runtime behaviour:
if the guessed type happens to match the actual data, the expression works; if not,
the server returns a `ParameterError` with no indication that the DSL parser made
an arbitrary choice. The user has no way to know that the type was guessed rather
than inferred.

Instead, when the parser cannot determine a concrete type, it must raise a parse-time
error with a clear message, e.g.:

```
Cannot determine type of bin 'a'. Add an explicit type annotation:
  $.a.get(type: INT)     for integer
  $.a.get(type: STRING)  for string
  $.a.get(type: FLOAT)   for float
```

This shifts the failure from an opaque runtime error to an actionable parse-time
diagnostic.

### 1. Inference from literals

When a bin or variable appears in an expression with a literal, the literal's type
propagates to the unknown side.

| Expression | Inferred types | Rule |
|---|---|---|
| `$.a == 'hello'` | `a` → STRING | String literal on one side of `==` |
| `$.a > 10` | `a` → INTEGER | Integer literal on one side of comparison |
| `$.a + 3.14` | `a` → FLOAT | Float literal in arithmetic |
| `$.a == true` | `a` → BOOLEAN | Boolean literal on one side of `==` |
| `$.a + 1` | `a` → INTEGER | Integer literal in additive expression |
| `$.a * 2.5` | `a` → FLOAT | Float literal in multiplicative expression |

### 2. Inference from operators

Each operator constrains its operands and/or produces a known result type.

**Logical operators (`and`, `or`):**
Both operands must be BOOLEAN; result is BOOLEAN.
```
$.a and $.b          →  a: BOOLEAN, b: BOOLEAN
$.a or true          →  a: BOOLEAN
```

**Comparison operators (`==`, `!=`, `>`, `>=`, `<`, `<=`):**
Both operands must be the same type; result is BOOLEAN.
If one side has a known type and the other is UNKNOWN, the UNKNOWN side inherits.
```
$.a == $.b           →  if either has a known type, the other gets it
$.a > 10             →  a: INTEGER (from literal); result: BOOLEAN
$.a != 'hello'       →  a: STRING; result: BOOLEAN
```

**Arithmetic operators (`+`, `-`, `*`, `/`, `%`):**
Both operands must be numeric (INTEGER or FLOAT) and the same type.
All operands in a chain are unified to the same numeric type.
```
$.a + $.b + $.c + 1  →  a, b, c: INTEGER (from literal 1)
$.a * $.b + 2.0      →  a, b: FLOAT (from literal 2.0)
$.a + $.b            →  both must be numeric; if one is known, the other inherits
```

**Exponent (`**`):**
Both operands must be FLOAT; result is FLOAT.
```
$.a ** $.b           →  a: FLOAT, b: FLOAT
$.a ** 2             →  ERROR: 2 is INTEGER but ** requires FLOAT; use 2.0
```

**Shift operators (`<<`, `>>`, `>>>`):**
Both operands must be INTEGER; result is INTEGER.
```
$.a << 2             →  a: INTEGER
$.a >> $.b           →  a: INTEGER, b: INTEGER
```

**Bitwise operators (`&`, `|`, `^`):**
Both operands must be INTEGER; result is INTEGER.
```
$.a & 0xFF           →  a: INTEGER
$.a | $.b            →  a: INTEGER, b: INTEGER
```

**Unary operators:**
- `not(expr)` — operand must be BOOLEAN; result is BOOLEAN
- `~expr` — operand must be INTEGER; result is INTEGER
- `+expr`, `-expr` — operand must be numeric; result keeps operand type

```
not($.a)             →  a: BOOLEAN
~$.a                 →  a: INTEGER
-$.a                 →  a: numeric (INTEGER or FLOAT, preserved)
```

### 3. Inference from functions

Each function constrains its parameters and produces a known return type.

| Function | Parameter types | Return type | Inference |
|---|---|---|---|
| `abs(x)` | numeric | same as `x` | If `x` is UNKNOWN, stays numeric |
| `ceil(x)` | FLOAT | FLOAT | `x` → FLOAT |
| `floor(x)` | FLOAT | FLOAT | `x` → FLOAT |
| `log(x, base)` | FLOAT, FLOAT | FLOAT | `x` → FLOAT, `base` → FLOAT |
| `min(a, b, …)` | numeric, all same | same as args | All unified to same type |
| `max(a, b, …)` | numeric, all same | same as args | All unified to same type |
| `count(x)` | INTEGER | INTEGER | `x` → INTEGER |
| `exclusive(a, b, …)` | BOOLEAN, all | BOOLEAN | All args → BOOLEAN |
| `lscan(x, bool)` | INTEGER, BOOLEAN | INTEGER | `x` → INTEGER, `bool` → BOOLEAN |
| `rscan(x, bool)` | INTEGER, BOOLEAN | INTEGER | `x` → INTEGER, `bool` → BOOLEAN |
| `int(x)` | FLOAT | INTEGER | `x` → FLOAT (cast) |
| `float(x)` | INTEGER | FLOAT | `x` → INTEGER (cast) |
| `geo(x)` | STRING | GEO | `x` → STRING (cast) |
| `contains(a, b)` | GEO, GEO | BOOLEAN | `a` → GEO, `b` → GEO |

**Regex:**
```
$.name =~ /pattern/i    →  name: STRING; result: BOOLEAN
```
The left operand of `=~` must be STRING; result is always BOOLEAN.

### 4. Inference from `when` expressions

All action expressions (the right side of `=>`) must be the same type. The `when`
expression's type is that unified action type. Condition expressions (left of `=>`)
must all be BOOLEAN.

```
when (
    $.b == 1 => $.a1,
    $.b == 2 => $.a2,
    $.b == 3 => $.a3,
    default  => 'hello'
)
```
Inference chain:
- `$.b == 1` → `b`: INTEGER (from literal `1`); condition is BOOLEAN ✓
- `default => 'hello'` → action type is STRING (from literal `'hello'`)
- All actions must be the same type → `a1`, `a2`, `a3`: STRING
- The `when` expression's type is STRING

So if this `when` appears in `$.a == when(...)`, then `a` is also STRING.

### 5. Inference from `let … then` expressions

Variable definitions in a `let` block inherit their type from the assigned expression.
The `then` expression can use these variables and their known types propagate.

```
let (x = $.a + 1, y = ${x} * 2) then (${y} > 10)
```
Inference chain:
- `$.a + 1` → `a`: INTEGER → `x`: INTEGER
- `${x} * 2` → already INTEGER → `y`: INTEGER
- `${y} > 10` → confirmed INTEGER; result: BOOLEAN

### 6. Inference from record metadata functions

| Function | Return type |
|---|---|
| `$.ttl()`, `$.voidTime()`, `$.lastUpdateTime()`, `$.timeSinceLastUpdate()` | INTEGER |
| `$.deviceSize()`, `$.memorySize()`, `$.recordSize()` | INTEGER |
| `$.keyExists()`, `$.isTombstone()` | BOOLEAN |
| `$.setName()` | STRING |
| `$.key(type: INT)` | depends on type parameter |
| `$.type()` | INTEGER |
| `$.exists()` | BOOLEAN |

### 7. Inference from CDT operations

When a bin is used with list or map functions, its type is inferred:
```
$.a.get(index: 0, type: INT)       →  a: LIST (list function used)
$.a.getByKey(key: 'x', type: INT)  →  a: MAP  (map function used)
$.a.size()                          →  a: LIST or MAP (ambiguous until resolved)
$.a.sort().get(index: 0, type: INT) →  a: LIST (sort is list-only, confirms LIST)
```

The `type:` parameter on CDT read functions explicitly sets the return type.

### 8. Cross-expression propagation (deferred validation)

Types may not be known when first encountered. The parser must defer validation
and re-check after the full expression is parsed.

```
$.b == $.d or $.c > $.b
```
- First pass: `$.b == $.d` → both UNKNOWN, defer SAME_TYPE validation
- Continue: `$.c > $.b` → both UNKNOWN, no inference yet
- If later `$.c` gets a type (e.g., from `$.c + 1` elsewhere), `c` → INTEGER
- Re-apply deferred: `$.c > $.b` → `b`: INTEGER (from `c`)
- Re-apply deferred: `$.b == $.d` → `d`: INTEGER (from `b`)

More complex example:
```
$.a == when($.b == 1 => $.a1, $.b == 2 => $.a2, default => 'hello')
    and $.c
    and $.a + 1 == $.d
```
Inference chain:
1. `$.b == 1` → `b`: INTEGER
2. `default => 'hello'` → action type STRING → `a1`, `a2`: STRING
3. `when(...)` result: STRING → `$.a == when(...)` → `a`: STRING
4. Wait — `$.a + 1` requires `a` to be INTEGER (from literal `1`), but step 3 set `a` to STRING
5. **ERROR**: Bin `a` used as both STRING (from `==` with `when`) and INTEGER (from `+ 1`)

Correct version:
```
$.a == when($.b == 1 => $.a1, $.b == 2 => $.a2, default => 5)
    and $.c
    and $.a + 1 == $.d
```
1. `$.b == 1` → `b`: INTEGER
2. `default => 5` → action type INTEGER → `a1`, `a2`: INTEGER
3. `when(...)` result: INTEGER → `a`: INTEGER
4. `and $.c` → `c`: BOOLEAN (from `and`)
5. `$.a + 1` → `a` already INTEGER ✓ → result INTEGER
6. `($.a + 1) == $.d` → `d`: INTEGER

### 9. Path expression inference (new for v2)

Within `*[?(...)]` filters and `.modify()` expressions, `@` (loop variable) follows
the same inference rules as bins:

```
*[?(@.price > 10.0)]        →  @.price: FLOAT
*[?(@key == 'revenue')]      →  @key: STRING (always STRING for map keys)
*[?(@index < 3)]             →  @index: INTEGER (always INTEGER for list indices)
*.price.modify(@ * 0.9)      →  @: FLOAT (from literal 0.9)
*[?(@ > $.threshold.get(type: INT))]  →  @: INTEGER (from explicit type on other side)
```

### 10. Inference from sub-expressions in path parameters

When a sub-expression `(expr)` appears inside a path element, the structural position
constrains the expression's type (input), and surrounding context constrains the
path element's return type (output). Both directions propagate simultaneously.

**Input constraints from position:**
```
$.m.{($.idx)}            →  idx: INTEGER (map-by-index requires INTEGER)
$.m.{#($.r)}             →  r: INTEGER (map-by-rank requires INTEGER)
$.m.($.key)              →  key: STRING, INTEGER, or BLOB (valid map key types)
$.l.[($.i)]              →  i: INTEGER (list-by-index requires INTEGER)
$.m.{($.lo):($.hi)}      →  lo: INTEGER, hi: INTEGER (index range bounds)
$.m.{($.s)-($.e)}        →  s and e: SAME_TYPE, must be STRING, INTEGER, or BLOB
```

**Output constraints from context:**
```
$.m.{($.idx)} == 'hello' →  idx: INTEGER (from position), output: STRING (from literal)
$.m.{($.idx)} > 10       →  idx: INTEGER (from position), output: INTEGER (from literal)
$.m.($.key) + 1           →  key: STRING/INTEGER/BLOB (from position), output: INTEGER (from + 1)
$.l.[($.i)] == true       →  i: INTEGER (from position), output: BOOLEAN (from literal)
```

**Bidirectional inference in a single expression:**
```
$.m.{($.idx)} == $.other
```
- `$.idx` → INTEGER (from the `{...}` index position)
- `$.m.{($.idx)}` output and `$.other` → SAME_TYPE (from `==`)
- If `$.other` is resolved elsewhere (e.g., `$.other > 3.14`), then both the output
  and `$.other` become FLOAT

**List-vs-singular disambiguation feeds back into inference:**
```
$.m.{($.x)}.count()
```
- `.count()` only applies to collections → the result of `{($.x)}` must be a
  collection → `$.x` must be LIST (key list form, not index form)

### 11. Explicit type annotations

When inference is not possible (e.g., both sides of a comparison are bins with no
literal context), the user can provide explicit type annotations using `.get(type: ...)`
or cast functions like `.asInt()` and `.asFloat()`.

```
$.a.get(type: INT) == $.b       a explicitly typed as INTEGER; b inherits INTEGER
$.a.get(type: FLOAT) > $.b     a explicitly typed as FLOAT; b inherits FLOAT
$.a.asInt() + $.b               a cast to INTEGER; b inherits INTEGER
```

Note that only one side needs the explicit annotation — the other side is inferred
via the same-type rule on the comparison or arithmetic operator. Annotating both
sides is valid but redundant:
```
$.a.get(type: FLOAT) > $.b.get(type: FLOAT)   valid but only one side needed
```

For CDT read functions, the `type:` parameter sets the return type of the read:
```
$.myList.get(index: 0, type: INT)              returns INTEGER from list position 0
$.myMap.getByKey(key: 'x', type: STRING)       returns STRING for key 'x'
```

### 12. Error conditions

The parser must raise an error when:

| Condition | Example | Error |
|---|---|---|
| Conflicting types for same bin | `$.a > 1 and $.a == 'hello'` | `a` is both INTEGER and STRING |
| Conflicting types for same variable | `let (x = 1) then (${x} == 'hi')` | `x` is INTEGER but compared to STRING |
| Unresolved UNKNOWN | `$.a == $.b` (no other context) | Neither `a` nor `b` can be typed; suggest `.get(type: ...)` |
| Unresolved SAME_TYPE | `$.a == $.b` where neither resolves | Both linked but no concrete type discovered |
| Unresolved NUMERIC | `$.a + $.b` (no literals in scope) | Known to be numeric but INTEGER vs FLOAT not determined |
| Wrong type for operator | `$.a and 5` | `and` requires BOOLEAN, got INTEGER |
| Mixed numeric types without cast | `$.a + $.b` where `a` is INT, `b` is FLOAT | Use `float($.a) + $.b` or `$.a + int($.b)` |
| Non-numeric in arithmetic | `$.a + 'hello'` | `+` requires numeric |
| Non-boolean in logical | `'hello' or true` | `or` requires BOOLEAN |
| Non-string in regex | `5 =~ /pat/` | `=~` requires STRING on left |
| Non-integer in bitwise | `3.14 & 0xFF` | `&` requires INTEGER |
| Exponent with integer | `$.a ** 2` | `**` requires FLOAT; use `2.0` |

### 13. Implementation strategy

1. **First pass (during parse):** Apply type inference rules as expressions are parsed.
   Assign intermediate states (`UNKNOWN`, `SAME_TYPE`, `NUMERIC`) where concrete types
   cannot yet be determined. Propagate concrete types immediately when available.
2. **Deferred validations:** When both sides of an operator are in intermediate states,
   record the constraint (e.g., "these two must be SAME_TYPE", "these two must be NUMERIC").
   Do not guess or assign a default.
3. **Second pass (post-parse):** Re-apply all deferred validations. Types discovered
   later in the parse can now resolve intermediate states recorded earlier.
4. **Final validation:** Check that every bin, variable, and sub-expression has resolved
   to a concrete type. If any remain in an intermediate state (`UNKNOWN`, `SAME_TYPE`,
   or `NUMERIC`), raise a parse error listing the unresolved items with a suggestion
   to add explicit type annotations. **Do not fall back to a default type.**
5. **CDT function resolution:** After type inference, resolve generic CDT functions
   (e.g., `size()`) to list-specific or map-specific variants based on the inferred
   bin type.

## Path Expression support

## Support sub-expressions for all parameters (except Regex)

### Problem

The current DSL only accepts literal constants in path element parameters: key names,
index integers, value literals, rank integers, and the items inside range bounds and
lists. Real-world use often requires these values to come from other bins, variables,
or computed expressions. For example, "give me map entries from key `$.startKey` to
key `$.endKey`" is not expressible today.

### Principle

Anywhere a literal constant currently appears as a parameter inside a path element,
a parenthesised expression `(expr)` can appear instead. The parentheses are the
universal sub-expression marker. They already exist in the DSL grammar for grouping
(`(expression)` in the `operand` rule), so this is a natural extension.

The structural position — determined by prefixes (`=`, `#`, `!`), separators (`-`, `:`,
`~`), and surrounding delimiters (`{}`, `[]`) — continues to determine the operation.
The `(expr)` simply replaces the literal in each slot.

**What stays literal:** The `type:` and `return:` parameters of `get()` remain enum
keywords (`INT`, `STRING`, `VALUE`, `COUNT`, etc.). These are compile-time constants,
not data-dependent. Regex patterns in `=~` also remain literal (as noted in the heading).

### Fixed-parameter positions

Each individual parameter slot can independently be a literal or a `(expr)`.
Literals and expressions can be mixed freely within the same construct.

#### Singular elements

| Current (literal) | With expression | Semantics |
|---|---|---|
| `$.m.name` | `$.m.($.keyBin)` | Map key lookup — expression must evaluate to STRING, INTEGER, or BLOB |
| `$.m.{1}` | `$.m.{($.idx)}` | Map by index — expression must be INTEGER |
| `$.m.{=val}` | `$.m.{=($.v)}` | Map by value — the expression's resolved type is used for comparison |
| `$.m.{#1}` | `$.m.{#($.r)}` | Map by rank — expression must be INTEGER |
| `$.l.[3]` | `$.l.[($.idx)]` | List by index — expression must be INTEGER |
| `$.l.[=val]` | `$.l.[=($.v)]` | List by value — the expression's resolved type is used for comparison |
| `$.l.[#1]` | `$.l.[#($.r)]` | List by rank — expression must be INTEGER |

#### Ranges

Each bound in a range can independently be literal or `(expr)`. Open-ended ranges
(omitted bound) remain supported.

**Key ranges:**
```
$.m.{a-c}                              literal: keys "a" up to "c"
$.m.{($.start)-($.end)}                both bounds from expressions
$.m.{a-($.end)}                        literal start, expression end
$.m.{($.start)-}                       expression start, open end
$.m.{-($.end)}                         open start, expression end
$.m.{!($.start)-($.end)}               inverted key range from expressions
```

**Index ranges:**
```
$.m.{1:3}                              literal
$.m.{($.s):($.e)}                      both from expressions
$.m.{0:($.count)}                      literal start, expression end
$.m.{($.offset):}                      expression start, open end
$.l.[($.s):($.e)]                      list index range from expressions
$.l.[!($.s):($.e)]                     inverted list index range
```

**Value ranges:**
```
$.m.{=5:8}                             literal
$.m.{=($.lo):($.hi)}                   both from expressions
$.m.{=($.lo):}                         expression start, open end
$.l.[=($.lo):($.hi)]                   list value range from expressions
$.l.[!=($.lo):($.hi)]                  inverted
```

**Rank ranges:**
```
$.m.{#0:3}                             literal
$.m.{#($.s):($.e)}                     both from expressions
$.l.[#($.s):($.e)]                     list rank range from expressions
```

**Relative ranges (rank-relative and key-relative):**

The reference value (after `~`) can also be an expression:
```
$.m.{#-1:1~10}                         literal: relative to value 10
$.m.{#($.s):($.e)~($.ref)}            all three from expressions
$.m.{#-1:1~($.pivot)}                  literal bounds, expression reference
$.m.{0:1~($.refKey)}                   key-relative index, expression reference
```

### List-parameter positions

For constructs that accept a variable-length comma-separated list of literals
(`{a,b,c}`, `{=1,2,3}`, `[=a,b,c]`, etc.), the entire list can be replaced by a
single `(expr)` that evaluates to a `LIST`.

| Current (literals) | With expression | Semantics |
|---|---|---|
| `$.m.{a,b,c}` | `$.m.{($.keys)}` | Key list — `$.keys` must be LIST |
| `$.m.{!a,b,c}` | `$.m.{!($.keys)}` | Inverted key list |
| `$.m.{=1,2,3}` | `$.m.{=($.vals)}` | Value list — `$.vals` must be LIST |
| `$.m.{!=1,2,3}` | `$.m.{!=($.vals)}` | Inverted value list |
| `$.l.[=a,b,c]` | `$.l.[=($.vals)]` | List value list |
| `$.l.[!=a,b,c]` | `$.l.[!=($.vals)]` | Inverted list value list |

It is all-or-nothing: either every item is a literal (comma-separated, as today), or
the entire list is a single expression. You cannot mix individual literal items with
expression items in a comma list.

### Disambiguation

Two syntactic positions have a potential collision between the singular and list forms
when a single `(expr)` appears:

| Syntax | Singular interpretation | List interpretation |
|---|---|---|
| `{(expr)}` | Map by index (expr → INTEGER) | Key list (expr → LIST) |
| `{=(expr)}` | Map by value (expr → scalar) | Value list (expr → LIST) |
| `[=(expr)]` | List by value (expr → scalar) | Value list (expr → LIST) |

**Resolution: the expression's type disambiguates.** If the expression resolves to
`LIST`, it is the list form. If it resolves to `INTEGER`, `STRING`, `FLOAT`, etc., it
is the singular form. This is consistent with the type inference system — if the type
cannot be determined, the parser raises an error:

```
Cannot determine whether '{=($.x)}' is a single-value lookup or a value-list lookup.
  If $.x is a single value: $.x.get(type: INT)
  If $.x is a list:         $.x.get(type: LIST)
```

Note that inverted forms (`{!(expr)}`, `{!=(expr)}`, `[!=(expr)]`) have no singular
counterpart in the current grammar, so there is no ambiguity — these are always the
list form.

### Type constraints per position

| Position | Expression must evaluate to |
|---|---|
| Map key (dot position) | STRING, INTEGER, or BLOB |
| Map index `{...}` | INTEGER |
| Map value `{=...}` (singular) | Any type (the expression's own resolved type is used for comparison) |
| Map rank `{#...}` | INTEGER |
| Key range bound | STRING, INTEGER, or BLOB (both bounds same type) |
| Index range bound | INTEGER |
| Value range bound | Any scalar (both bounds same type) |
| Rank range bound | INTEGER |
| Relative reference `~(expr)` | Same type as the values being ranked/compared |
| Key list `{(expr)}` | LIST |
| Value list `{=(expr)}` / `[=(expr)]` | LIST |
| List index `[...]` | INTEGER |
| List value `[=...]` (singular) | Any type (the expression's own resolved type is used for comparison) |
| List rank `[#...]` | INTEGER |

### Output type of path elements

The table above covers the **input** type — what the parameter expression must evaluate
to. But path elements also produce an **output** type (what the overall expression
returns), which depends on the data stored in the database and cannot be determined
from the DSL alone.

This is not new to sub-expressions — `$.m.{1}` with a literal has the same issue.
The output type is resolved by the existing mechanisms:

1. **Inference from context:** `$.m.{($.idx)} == 'hello'` → output is STRING
2. **Explicit annotation:** `$.m.{($.idx)}.get(type: INT)` → output is INTEGER
3. **Propagation:** if the output is compared to something whose type is already known

If the output type cannot be resolved, the parser raises an error suggesting
`.get(type: ...)`.

Note that context inference often constrains **both** the input and output types
simultaneously. For example:

```
$.m.{($.idx)} == 'hello'
```
This single expression tells us:
- `$.idx` → INTEGER (because `{...}` is a map-by-index position, which requires INTEGER)
- The map operation's output → STRING (because `== 'hello'` requires STRING on both sides)

See [Inference from sub-expressions in path parameters](#10-inference-from-sub-expressions-in-path-parameters)
in the type inference section for the full set of rules.

### Examples with real-world motivation

**Dynamic key lookup** — key name stored in another bin:
```
$.config.($.settingName) > 0
```
Equivalent Exp:
```java
Exp.gt(
    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
        Exp.stringBin("settingName"),
        Exp.mapBin("config")),
    Exp.val(0)
)
```

**Dynamic range** — filter transactions between two timestamp bins:
```
$.txns.{($.rangeStart)-($.rangeEnd)}
```
Equivalent Exp:
```java
MapExp.getByKeyRange(MapReturnType.ORDERED_MAP,
    Exp.stringBin("rangeStart"),
    Exp.stringBin("rangeEnd"),
    Exp.mapBin("txns"))
```

**Top N by rank** — number of results comes from a bin:
```
$.scores.{#(-($.topN)):}.count()
```
Equivalent Exp:
```java
MapExp.getByRankRange(MapReturnType.COUNT,
    Exp.neg(Exp.intBin("topN")),
    Exp.mapBin("scores"))
```

**Value list from another bin** — find list elements matching a set of values from
a separate bin:
```
$.tags.[=($.allowedTags)].count() > 0
```
Equivalent Exp:
```java
Exp.gt(
    ListExp.getByValueList(ListReturnType.COUNT,
        Exp.listBin("allowedTags"),
        Exp.listBin("tags")),
    Exp.val(0)
)
```

**Variable from `let` block** — computed threshold used in a path:
```
let (threshold = $.basePrice * 1.1)
then ($.products.{=(${threshold}):}.count())
```

**Nested expression** — arithmetic inside a parameter:
```
$.data.[($.offset + 1):($.offset + $.pageSize)]
```

### Grammar changes (summary)

Introduce a new rule for sub-expressions within path elements:

```
subExpr: '(' expression ')';
```

Then extend every parameter position to accept `subExpr` as an alternative:

```
mapKey: NAME_IDENTIFIER | QUOTED_STRING | subExpr;

mapIndex: '{' (INT | subExpr) '}';
mapValue: '{=' (valueIdentifier | subExpr) '}';
mapRank: '{#' (INT | subExpr) '}';

listIndex: '[' (INT | subExpr) ']';
listValue: '[=' (valueIdentifier | subExpr) ']';
listRank: '[#' (INT | subExpr) ']';
```

For ranges, each bound becomes `literal | subExpr`:
```
keyRangeIdentifier
    : (mapKey | subExpr) '-' (mapKey | subExpr)
    | (mapKey | subExpr) '-'
    | '-' (mapKey | subExpr)
    ;

indexRangeIdentifier
    : (start | subExpr) ':' (end | subExpr)
    | (start | subExpr) ':'
    | ':' (end | subExpr)
    ;
```

For list parameters, the list identifier accepts a single `subExpr` alternative:
```
keyListIdentifier
    : mapKey (',' mapKey)*
    | subExpr
    ;

valueListIdentifier
    : valueIdentifier (',' valueIdentifier)+
    | subExpr
    ;
```

The `relativeValue` rule extends similarly:
```
relativeValue: '~' (valueIdentifier | subExpr);
```

## Full syntax of all list / map functions

This section compares the full API surface of `MapExp` and `ListExp` against the
current DSL spec and grammar, identifying what is missing or incomplete.

### Read operations — coverage summary

The DSL path element syntax maps to `getBy*` methods. The current coverage is
**complete** for all read operations:

| MapExp method | DSL path syntax | Status |
|---|---|---|
| `getByKey` | `$.m.key`, `$.m.'key'`, `$.m.1` | ✓ Covered |
| `getByKeyRange` | `$.m.{a-c}`, `$.m.{a-}`, `$.m.{-c}` | ✓ Covered |
| `getByKeyList` | `$.m.{a,b,c}` | ✓ Covered |
| `getByKeyRelativeIndexRange` (open) | `$.m.{0:~key}` | ✓ Covered |
| `getByKeyRelativeIndexRange` (bounded) | `$.m.{0:1~key}` | ✓ Covered |
| `getByValue` | `$.m.{=val}` | ✓ Covered |
| `getByValueRange` | `$.m.{=5:8}`, `$.m.{=5:}` | ✓ Covered |
| `getByValueList` | `$.m.{=1,2,3}` | ✓ Covered |
| `getByValueRelativeRankRange` (open) | `$.m.{#-1:~ref}` | ✓ Covered |
| `getByValueRelativeRankRange` (bounded) | `$.m.{#-1:1~ref}` | ✓ Covered |
| `getByIndex` | `$.m.{1}` | ✓ Covered |
| `getByIndexRange` (open) | `$.m.{1:}` | ✓ Covered |
| `getByIndexRange` (bounded) | `$.m.{1:3}` | ✓ Covered |
| `getByRank` | `$.m.{#1}` | ✓ Covered |
| `getByRankRange` (open) | `$.m.{#1:}` | ✓ Covered |
| `getByRankRange` (bounded) | `$.m.{#1:3}` | ✓ Covered |
| `size` | `$.m.count()`, `$.m.{}.count()` | ✓ Covered |

| ListExp method | DSL path syntax | Status |
|---|---|---|
| `getByIndex` | `$.l.[1]` | ✓ Covered |
| `getByIndexRange` (open) | `$.l.[1:]` | ✓ Covered |
| `getByIndexRange` (bounded) | `$.l.[1:3]` | ✓ Covered |
| `getByValue` | `$.l.[=val]` | ✓ Covered |
| `getByValueRange` | `$.l.[=5:8]`, `$.l.[=5:]` | ✓ Covered |
| `getByValueList` | `$.l.[=1,2,3]` | ✓ Covered |
| `getByValueRelativeRankRange` (open) | `$.l.[#-1:~ref]` | ✓ Covered |
| `getByValueRelativeRankRange` (bounded) | `$.l.[#-1:1~ref]` | ✓ Covered |
| `getByRank` | `$.l.[#1]` | ✓ Covered |
| `getByRankRange` (open) | `$.l.[#1:]` | ✓ Covered |
| `getByRankRange` (bounded) | `$.l.[#1:3]` | ✓ Covered |
| `size` | `$.l.count()`, `$.l.[].count()` | ✓ Covered |

Inverted variants (`!` prefix) are also covered: `{!a-c}`, `{!=1,2,3}`, `[!0:3]`, etc.

The `return:` parameter on `get()` handles `MapReturnType`/`ListReturnType` selection
(VALUE, COUNT, INDEX, RANK, KEY, KEY_VALUE, EXISTS, ORDERED_MAP, UNORDERED_MAP,
REVERSE_INDEX, REVERSE_RANK, NONE).

### Read operations — what needs attention

The `get()` function `return:` parameter currently only appears on an explicit `get()`
call. But the DSL spec says the default return type depends on context (single element
→ VALUE, multi-element map → ORDERED_MAP, multi-element list → VALUE). This implicit
behaviour needs to be documented as a formal rule so that:

```
$.m.{a-c}                      →  getByKeyRange(ORDERED_MAP, ...)  (implicit)
$.m.{a-c}.get(return: VALUE)   →  getByKeyRange(VALUE, ...)        (explicit override)
$.m.{a-c}.get(return: COUNT)   →  getByKeyRange(COUNT, ...)        (explicit override)
$.l.[0:3]                      →  getByIndexRange(VALUE, ...)       (implicit)
$.l.[0:3].get(return: INDEX)   →  getByIndexRange(INDEX, ...)       (explicit override)
```

### Remove operations — coverage summary

The `remove()` function combined with path context covers all `removeBy*` methods.
The path element determines what to select, and `remove()` applies the removal:

| API method | DSL syntax | Status |
|---|---|---|
| `removeByKey` | `$.m.key.remove()` | ✓ Concept covered |
| `removeByKeyRange` | `$.m.{a-c}.remove()` | ✓ Concept covered |
| `removeByKeyList` | `$.m.{a,b,c}.remove()` | ✓ Concept covered |
| `removeByKeyRelativeIndexRange` | `$.m.{0:1~key}.remove()` | ✓ Concept covered |
| `removeByValue` | `$.m.{=val}.remove()` | ✓ Concept covered |
| `removeByValueRange` | `$.m.{=5:8}.remove()` | ✓ Concept covered |
| `removeByValueList` | `$.m.{=1,2,3}.remove()` | ✓ Concept covered |
| `removeByValueRelativeRankRange` | `$.m.{#-1:1~ref}.remove()` | ✓ Concept covered |
| `removeByIndex` | `$.m.{1}.remove()`, `$.l.[1].remove()` | ✓ Concept covered |
| `removeByIndexRange` | `$.m.{1:3}.remove()`, `$.l.[1:3].remove()` | ✓ Concept covered |
| `removeByRank` | `$.m.{#1}.remove()`, `$.l.[#1].remove()` | ✓ Concept covered |
| `removeByRankRange` | `$.m.{#1:3}.remove()`, `$.l.[#1:3].remove()` | ✓ Concept covered |

Inverted removes use the `!` prefix: `$.m.{!a-c}.remove()` → removeByKeyRange with
INVERTED, which removes everything **outside** the range.

**Note on expression context:** In `MapExp`/`ListExp`, all write operations (including
remove) return the **modified collection**, not the removed items. The `returnType`
parameter on remove methods in expression context only accepts `NONE` (remove matched)
or `INVERTED` (remove unmatched). This differs from operation context (`MapOperation`)
where remove can return COUNT, VALUE, etc. The DSL's `remove()` in expression context
therefore does not need a `return:` parameter — the `!` prefix handles inversion.

### Mutation functions — MISSING: parameters and verb-based write semantics

**This is the primary gap.** The grammar defines all mutation functions as parameterless
stubs (`'insert' '()'`, `'set' '()'`, etc.) but the API methods require parameters
(values to insert, amounts to add, etc.).

#### Design principle: verbs carry write intent

Following the fluent client's `CdtGetOrRemoveBuilder` pattern, the **function name
encodes the write mode** instead of using explicit `writeFlags:` parameters. The key
or index comes from the path navigation, not the function arguments:

| Verb | Write mode | Maps to (MapExp) | Maps to (ListExp) |
|---|---|---|---|
| `setTo(value)` | Unconditional (DEFAULT) | `MapExp.put(DEFAULT, key, value, bin)` | `ListExp.set(DEFAULT, index, value, bin)` |
| `insert(value)` | CREATE_ONLY (fail if exists) | `MapExp.put(CREATE_ONLY, key, value, bin)` | `ListExp.insert(DEFAULT, index, value, bin)` |
| `update(value)` | UPDATE_ONLY (fail if missing) | `MapExp.put(UPDATE_ONLY, key, value, bin)` | N/A (not applicable for lists) |
| `add(amount)` | Numeric increment | `MapExp.increment(DEFAULT, key, amount, bin)` | `ListExp.increment(DEFAULT, index, amount, bin)` |

For maps, `setTo` is an upsert — it creates the key if it doesn't exist, or overwrites
it if it does. For lists, `setTo` overwrites the value at the given index. The name
`setTo` was chosen over `upsert` because it reads naturally for both data structures
(`$.m.name.setTo('Alice')`, `$.l.[0].setTo(99)`) and `insert`/`update` cover the
cases where create-only or update-only semantics are needed.

#### Map mutation functions

| Function | Current grammar | Proposed syntax |
|---|---|---|
| `setTo` | `set()` ← stub | `$.m.key.setTo(value_expr)` |
| `insert` | `insert()` ← stub | `$.m.key.insert(value_expr)` |
| `update` | not in grammar | `$.m.key.update(value_expr)` |
| `putItems` | not in grammar | `$.m.putItems(map_expr)` |
| `add` | `increment()` ← stub | `$.m.key.add(amount_expr)` |
| `clear` | `clear()` | `$.m.clear()` ✓ |

The key is always provided by the path context. The function takes only the value.

**Map write examples:**
```
$.m.name.setTo('Alice')                  unconditional: set key "name" to "Alice"
$.m.name.insert('Alice')                 create only: fail if "name" already exists
$.m.name.update('Alice')                 update only: fail if "name" doesn't exist
$.m.($.keyBin).setTo($.valueBin)         key and value from other bins
$.m.putItems({a: 1, b: 2, c: 3})        insert/update multiple literal entries
$.m.putItems(($.mapBin))                 insert/update entries from another bin
```

Exp equivalents:
```java
MapExp.put(new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.DEFAULT),
    Exp.val("name"), Exp.val("Alice"), Exp.mapBin("m"))
MapExp.put(new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.CREATE_ONLY),
    Exp.val("name"), Exp.val("Alice"), Exp.mapBin("m"))
MapExp.put(new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.UPDATE_ONLY),
    Exp.val("name"), Exp.val("Alice"), Exp.mapBin("m"))
MapExp.putItems(MapPolicy.Default, mapExp, Exp.mapBin("m"))
```

**Map add (increment) examples:**
```
$.m.counter.add(1)                       increment map key "counter" by 1
$.m.counter.add(-($.decrement))          decrement by a bin value
$.m.balance.add($.amount)                add amount from another bin
```

Exp equivalent:
```java
MapExp.increment(MapPolicy.Default, Exp.val("counter"), Exp.val(1), Exp.mapBin("m"))
```

#### List mutation functions

| Function | Current grammar | Proposed syntax |
|---|---|---|
| `append` | `append()` ← stub | `$.l.append(value_expr)` |
| `appendItems` | not in grammar | `$.l.appendItems(list_expr)` |
| `insert` | `insert()` ← stub | `$.l.[3].insert(value_expr)` |
| `insertItems` | not in grammar | `$.l.[3].insertItems(list_expr)` |
| `setTo` | `set()` ← stub | `$.l.[3].setTo(value_expr)` |
| `add` | `increment()` ← stub | `$.l.[3].add(amount_expr)` |
| `clear` | `clear()` | `$.l.clear()` ✓ |
| `sort` | `sort()` ← stub | `$.l.sort()` or `$.l.sort(dropDuplicates: true)` |

For lists, `insert` means "insert at this position, shifting existing elements right"
while `setTo` means "overwrite the value at this position". Both take the index from
the path context.

**List examples:**
```
$.l.append(42)                           append to end
$.l.append($.valueBin)                   append value from another bin
$.l.appendItems([1, 2, 3])              append multiple literal values
$.l.appendItems(($.otherList))           append items from another list bin
$.l.[0].insert('first')                  insert at index 0, shifting existing
$.l.[3].insertItems([10, 20])           insert multiple at index 3
$.l.[0].setTo(99)                        overwrite first element
$.l.[-1].setTo('last')                   overwrite last element
$.l.[0].add(10)                          increment first element by 10
$.l.[2].add(-($.delta))                  decrement third element by a bin value
```

Exp equivalents:
```java
ListExp.append(ListPolicy.Default, Exp.val(42), Exp.listBin("l"))
ListExp.appendItems(ListPolicy.Default, Exp.val(List.of(1, 2, 3)), Exp.listBin("l"))
ListExp.insert(ListPolicy.Default, Exp.val(0), Exp.val("first"), Exp.listBin("l"))
ListExp.set(ListPolicy.Default, Exp.val(0), Exp.val(99), Exp.listBin("l"))
ListExp.increment(ListPolicy.Default, Exp.val(0), Exp.val(10), Exp.listBin("l"))
```

**List sort examples:**
```
$.l.sort()                               sort preserving duplicates
$.l.sort(dropDuplicates: true)           sort and remove duplicates
```

Exp equivalents:
```java
ListExp.sort(ListSortFlags.DEFAULT, Exp.listBin("l"))
ListExp.sort(ListSortFlags.DROP_DUPLICATES, Exp.listBin("l"))
```

### Optional modifiers: noFail and order

While the verb name carries the primary write intent, two optional modifiers may be
needed in some cases:

**`noFail`** — suppress errors when the write mode condition isn't met:
```
$.m.key.insert(value, noFail: true)      CREATE_ONLY + NO_FAIL: silently skip if exists
$.m.key.update(value, noFail: true)      UPDATE_ONLY + NO_FAIL: silently skip if missing
$.l.append(value, noFail: true)          NO_FAIL on append
```

**`order`** — specify the collection ordering when it matters:
```
$.l.append(value, order: ORDERED)        insert into correct sorted position
$.m.key.setTo(value, order: KEY_ORDERED) set with key-ordered map policy
```

These are optional named parameters, not write flags. The defaults (no `noFail`,
`UNORDERED` order) apply when omitted.

### Mutation functions — grammar changes needed

The parameterless stubs need to become proper parameterised rules:

```
pathFunction
    : pathFunctionCast
    | pathFunctionExists
    | pathFunctionGet
    | pathFunctionCount
    | pathFunctionRemove
    | pathFunctionSetTo
    | pathFunctionInsert
    | pathFunctionUpdate
    | pathFunctionAdd
    | pathFunctionPutItems
    | pathFunctionAppend
    | pathFunctionAppendItems
    | pathFunctionInsertItems
    | pathFunctionClear
    | pathFunctionSort
    ;

pathFunctionRemove: 'remove' '()';

pathFunctionSetTo: 'setTo' '(' expression mutationOpts? ')';

pathFunctionInsert: 'insert' '(' expression mutationOpts? ')';

pathFunctionUpdate: 'update' '(' expression mutationOpts? ')';

pathFunctionAdd: 'add' '(' expression mutationOpts? ')';

pathFunctionPutItems: 'putItems' '(' expression mutationOpts? ')';

pathFunctionAppend: 'append' '(' expression mutationOpts? ')';

pathFunctionAppendItems: 'appendItems' '(' expression mutationOpts? ')';

pathFunctionInsertItems: 'insertItems' '(' expression mutationOpts? ')';

pathFunctionClear: 'clear' '()';

pathFunctionSort: 'sort' '(' sortParams? ')';

mutationOpts: (',' mutationOpt)+;
mutationOpt
    : 'noFail' ':' booleanOperand
    | 'order' ':' cdtOrder
    ;

cdtOrder: 'UNORDERED' | 'ORDERED' | 'KEY_ORDERED' | 'KEY_VALUE_ORDERED';

sortParams: 'dropDuplicates' ':' booleanOperand;
```

### Complete function reference

For reference, this is the full list of DSL path functions after these changes:

**Read functions:**

| Function | Parameters | Description |
|---|---|---|
| `get()` | `type:`, `return:` | Explicit type and return type override |
| `count()` | none | Element count (maps to `size()` for full bins) |
| `exists()` | none | Returns BOOLEAN |
| `asInt()` | none | Cast to INTEGER |
| `asFloat()` | none | Cast to FLOAT |
| `type()` | none | Returns the data type as INTEGER |

**Write functions (map) — key from path context:**

| Function | Parameters | Write mode | Description |
|---|---|---|---|
| `setTo(value)` | value expr | DEFAULT | Unconditional create-or-overwrite |
| `insert(value)` | value expr | CREATE_ONLY | Create, fail if exists |
| `update(value)` | value expr | UPDATE_ONLY | Update, fail if missing |
| `add(amount)` | amount expr | DEFAULT | Increment numeric value |
| `putItems(map)` | map expr | DEFAULT | Insert/update multiple entries |
| `remove()` | none | — | Remove matched elements |
| `clear()` | none | — | Remove all elements |

**Write functions (list) — index from path context where applicable:**

| Function | Parameters | Description |
|---|---|---|
| `append(value)` | value expr | Append to end of list |
| `appendItems(list)` | list expr | Append multiple to end |
| `insert(value)` | value expr | Insert at index, shifting elements right |
| `insertItems(list)` | list expr | Insert multiple at index |
| `setTo(value)` | value expr | Overwrite value at index |
| `add(amount)` | amount expr | Increment numeric value at index |
| `remove()` | none | Remove matched elements |
| `clear()` | none | Remove all elements |
| `sort()` | optional `dropDuplicates: true` | Sort the list |

All write functions accept optional `noFail: true` and `order: ...` modifiers.

## Support for HLL and Geo

## Support for mapKeys and mapValues

## String library functions

