# Support sub-expressions for all parameters (except Regex)

### Problem

The current AEL only accepts literal constants in path element parameters: key names,
index integers, value literals, rank integers, and the items inside range bounds and
lists. Real-world use often requires these values to come from other bins, variables,
or computed expressions. For example, "give me map entries from key `$.startKey` to
key `$.endKey`" is not expressible today.

### Principle

Anywhere a literal constant currently appears as a parameter inside a path element,
a parenthesised expression `(expr)` can appear instead. The parentheses are the
universal sub-expression marker. They already exist in the AEL grammar for grouping
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
from the AEL alone.

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

See [Inference from sub-expressions in path parameters](type-inference.md#10-inference-from-sub-expressions-in-path-parameters)
for the full set of rules.

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

