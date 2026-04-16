# Advanced type inference

The AEL should derive types of bins, map keys, map values, list values, and variables
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
the server returns a `ParameterError` with no indication that the AEL parser made
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
4. `($.a == when(...)) and ...` → the `==` comparison returns BOOLEAN, which is valid
   for `and`. But `and $.c` → `c`: BOOLEAN
5. `$.a + 1` requires `a` to be INTEGER (from literal `1`), but step 3 set `a` to STRING
6. **ERROR**: Bin `a` used as both STRING (from `==` with `when`) and INTEGER (from `+ 1`)

Note that if we changed the example slightly so `a` was not reused:

```
when($.b == 1 => $.a1, $.b == 2 => $.a2, default => 'hello')
    and $.c
```

Here the `when(...)` result is STRING, and `and` requires both operands to be BOOLEAN.
A STRING is not BOOLEAN, so this is also an **ERROR**: `when` result type STRING is
incompatible with `and` which requires BOOLEAN operands.

Correct version:
```
$.a == when($.b == 1 => $.a1, $.b == 2 => $.a2, default => 5)
    and $.c
    and $.a + 1 == $.d
```
1. `$.b == 1` → `b`: INTEGER
2. `default => 5` → action type INTEGER → `a1`, `a2`: INTEGER
3. `when(...)` result: INTEGER → `$.a == when(...)` → `a`: INTEGER
4. `$.a == when(...)` returns BOOLEAN → valid operand for `and` ✓
5. `and $.c` → `c`: BOOLEAN (required by `and`)
6. `$.a + 1` → `a` already INTEGER ✓ → result INTEGER
7. `($.a + 1) == $.d` → `d`: INTEGER

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

### 13. Possible implementation strategy

1. **First pass (during parse):** Apply type inference rules as expressions are parsed.
   Assign intermediate states (`UNKNOWN`, `SAME_TYPE`, `NUMERIC`) where concrete types
   cannot yet be determined. Propagate concrete types immediately when available.
2. **Deferred validations:** When both sides of an operator are in intermediate states,
   record the constraint (e.g., "these two must be SAME_TYPE", "these two must be NUMERIC").
   Avoid guessing or assigning a default.
3. **Second pass (post-parse):** Re-apply all deferred validations. Types discovered
   later in the parse can now resolve intermediate states recorded earlier.
4. **Final validation:** Check that every bin, variable, and sub-expression has resolved
   to a concrete type. If any remain in an intermediate state (`UNKNOWN`, `SAME_TYPE`,
   or `NUMERIC`), raise a parse error listing the unresolved items with a suggestion
   to add explicit type annotations. **Avoid falling back to a default type.**
5. **CDT function resolution:** After type inference, resolve generic CDT functions
   (e.g., `size()`) to list-specific or map-specific variants based on the inferred
   bin type.

