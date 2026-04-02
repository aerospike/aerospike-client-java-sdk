# Aerospike Expression AEL Reference

## Overview

The Aerospike Expression AEL is a text-based domain-specific language that compiles to Aerospike Expression objects. It provides a concise, readable syntax for:

- **Filter expressions** — `WHERE` clauses on queries, batch operations, and single-key operations.
- **Read expressions** — computing derived (synthetic) bins from existing data.
- **Write expressions** — computing values to store in bins.

A AEL expression is a string that is parsed, compiled to an `Exp` tree, and then evaluated by the Aerospike server against each record.

---

## Quick Reference

| Purpose | Example |
|---------|---------|
| Compare a bin | `$.age > 21` |
| Compare two bins | `$.price.get(type: INT) > $.discount.get(type: INT)` |
| Logical filter | `$.age > 21 and $.status == 'active'` |
| Map key access | `$.profile.name == 'Tim'` |
| List index access | `$.scores.[0] > 90` |
| Nested CDT path | `$.data.users.[2].email == 'a@b.com'` |
| Arithmetic | `($.price * $.qty) > 1000` |
| Metadata | `$.ttl() < 3600` |
| Conditional | `when ($.tier == 1 => 'gold', $.tier == 2 => 'silver', default => 'bronze')` |
| Variable binding | `let (total = $.price * $.qty) then (${total} > 1000)` |

---

## 1. Record Reference

Every expression path starts with `$` followed by `.` to reference the current record.

```
$.binName          -- access a bin
$.metadata()       -- access record metadata
```

The `$` symbol represents the record. Everything after `$.` is a path into the record's data.

---

## 2. Literals and Data Types

### 2.1 Integers

Plain decimal digits, optionally preceded by a sign with no spaces:

```
42
-1
+123
0xff          -- hexadecimal (case-insensitive)
0b1010        -- binary
```

### 2.2 Floating-Point Numbers

Digits with exactly one decimal point (the decimal point must not be the last character):

```
3.14
-34.1
0.37
```

Note: `10.` is **not valid**; use `10.0` instead.

### 2.3 Strings

Simple identifiers matching `^[A-Za-z]\w*$` do not require quoting in value positions. Otherwise, use single or double quotes:

```
"hello world"
'hello world'
```

Escape sequences inside quoted strings:

| Escape | Meaning |
|--------|---------|
| `\\` | Literal backslash |
| `\n` | Newline |
| `\t` | Tab |
| `\"` | Double quote (only needed inside double-quoted strings) |
| `\'` | Single quote (only needed inside single-quoted strings) |

A double quote inside single quotes needs no escaping and vice versa: `'"'` equals `"\""`.

### 2.4 Booleans

```
true
false
```

### 2.5 BLOBs

Hex-encoded byte sequences:

```
X'ffee'
x'102030405060708090abcdef'
```

There must be an even number of hex characters.

### 2.6 Lists

Square brackets with comma-separated values:

```
[1, 2, "abc", 12.7]
[1, [2, 3], {a: 4}]
[]
```

List elements can be any supported type: integers, floats, strings, booleans, BLOBs, lists, or maps.

### 2.7 Maps

Curly braces with `key: value` pairs:

```
{name: "Tim", age: 312, 1: [2, 3, 4]}
```

Map keys can be strings, integers, or BLOBs. Map values can be any supported type.

### 2.8 Special Values

| Value | Description | Use Case |
|-------|-------------|----------|
| `*` (WILDCARD) | Matches any value from that point | `[1, *]` matches `[1]`, `[1, 2, 34]` |
| `NIL` | Lowest possible CDT comparison value | Range start: `[#-1:1~[10.0, NIL]]` |
| `INF` | Highest possible CDT comparison value | Range end marker |

---

## 3. Path Expressions

Paths are the core of the AEL. A path is a dot-separated chain of elements that navigates from a record's bin into nested data structures (maps and lists).

### Structure

```
$.<bin>.<context1>.<context2>...<leaf>.<function>
```

- **Bin**: The top-level bin name (first element after `$.`).
- **Context elements**: Intermediate navigation into nested CDTs.
- **Leaf element**: The final element being targeted.
- **Function**: An optional terminal operation (`get()`, `count()`, `exists()`, `asInt()`, `asFloat()`).

### How the AEL determines bin type

The AEL deduces whether a bin is a Map or List from the **first context element** after the bin name:

- Starts with `{` or is a bare/quoted identifier → **Map bin**
- Starts with `[` → **List bin**
- No context at all → **Scalar bin**

---

## 4. Scalar Bin Access

For scalar bins (integer, string, float, boolean), the path is just the bin name:

```
$.age > 21
$.name == 'Tim'
$.score >= 95.5
$.active == true
```

### Type Inference

When comparing a bin to a literal, the type is inferred from the literal:

```
$.age > 21                    -- type inferred as INT from literal 21
$.name == 'hello'             -- type inferred as STRING from literal 'hello'
$.ratio > 3.14                -- type inferred as FLOAT from literal 3.14
$.flag == true                -- type inferred as BOOL from literal true
```

### Explicit Type Annotation

When comparing two bins (no literal to infer from), you must annotate at least one side:

```
$.binA.get(type: INT) == $.binB.get(type: INT)
$.binA.get(type: STRING) == $.binB.get(type: STRING)
```

Available type values: `INT`, `STRING`, `FLOAT`, `BOOL`, `BLOB`, `HLL`, `LIST`, `MAP`, `GEO`.

### Type Casting

Convert between integer and float:

```
$.floatBin.asInt()            -- cast float to integer
$.intBin.asFloat()            -- cast integer to float
$.intBin.get(type: INT) > $.floatBin.asInt()
```

---

## 5. Map Path Elements

Maps are key-value structures. The AEL navigates into maps using dot-separated path elements. Map keys can be strings, integers, or BLOBs.

### 5.1 Singular Map Elements (Context or Leaf)

These select a single element. They can appear anywhere in the path.

| Syntax | Description | Example |
|--------|-------------|---------|
| `key` | Map key (unquoted string identifier) | `$.m.name` |
| `'key'` or `"key"` | Map key (quoted string) | `$.m.'special-key'` |
| `1` | Map key (integer 1) | `$.m.1` |
| `{1}` | Map by **index** 1 | `$.m.{1}` |
| `{=1}` | Map by **value** 1 | `$.m.{=1}` |
| `{=bb}` | Map by **value** "bb" | `$.m.{=bb}` |
| `{#1}` | Map by **rank** 1 | `$.m.{#1}` |

**String key notes**: Unquoted identifiers work for simple names (`name`, `user_id`). Use quotes for keys containing special characters (`'127.0.0.1'`, `"special-key"`), keys that are purely numeric strings (`"66"`), or reserved words.

**Integer key notes**: A bare integer like `1` in a path position is an integer map key. To access a string key `"1"`, use quotes: `$.m."1"`.

### 5.2 Plural Map Elements (Leaf Only)

These select multiple elements and can **only** appear at the end of a path. They return a collection.

#### Key Range

Selects map entries by a range of keys (begin inclusive, end exclusive):

| Syntax | Description |
|--------|-------------|
| `{a-c}` | Keys from "a" up to (but not including) "c" |
| `{a-}` | Keys from "a" to the end |
| `{-d}` | Keys from the start up to "d" |
| `{!g-z}` | Keys **not** in range "g" to "z" (inverted) |

```
$.m.{a-d}                    -- keys a, b, c (not d)
$.m.{!a-d}                   -- all keys except a, b, c
$.m.{5-}                     -- integer keys from 5 onward
```

#### Key List

Selects entries by an explicit set of keys:

| Syntax | Description |
|--------|-------------|
| `{a,b,c}` | Keys a, b, and c |
| `{!a,b,c}` | All keys **except** a, b, c (inverted) |

```
$.m.{name,age,email}
$.m.{!temp,internal}          -- everything except these keys
```

#### Index Range

Selects entries by index position. Ranges use `start:end` where both are inclusive. Negative indices count from the end.

| Syntax | Description |
|--------|-------------|
| `{1:3}` | Index 1 through 3 (inclusive) |
| `{-3:-1}` | Last 3 entries |
| `{1:}` | From index 1 to the end |
| `{:5}` | From start to index 5 (exclusive) |
| `{!2:4}` | Entries **outside** index 2-4 (inverted) |

```
$.m.{0:2}                    -- first 3 entries
$.m.{-3:}                    -- last 3 entries
```

#### Value List

Selects entries whose values match:

| Syntax | Description |
|--------|-------------|
| `{=1,2,3}` | Entries with values 1, 2, or 3 |
| `{!=a,b,c}` | Entries whose values are **not** a, b, or c |

#### Value Range

Selects entries by a range of values (begin inclusive, end exclusive):

| Syntax | Description |
|--------|-------------|
| `{=111:334}` | Values from 111 up to 334 |
| `{=111:}` | Values from 111 to the end |
| `{!=10:20}` | Values **outside** 10-20 (inverted) |

#### Rank Range

Selects entries by rank (the sorted position of the value). Rank 0 is the lowest value.

| Syntax | Description |
|--------|-------------|
| `{#0:3}` | Rank 0 through 3 (exclusive) |
| `{#-3:}` | Top 3 ranked entries |
| `{#:4}` | From lowest to rank 4 (exclusive) |
| `{!#0:3}` | Entries **outside** rank 0-3 (inverted) |

#### Relative Rank Range

Selects entries relative to the rank of a given value:

| Syntax | Description |
|--------|-------------|
| `{#-1:1~10}` | Rank relative to value 10, from -1 to +1 |
| `{#-2:~10}` | From 2 ranks below value 10 to the end |
| `{!#-1:~f}` | **Not** in relative rank range of value "f" |

Format: `{#start:end~referenceValue}` or `{#start:~referenceValue}` (open-ended).

#### Key-Relative Index Range

Selects entries by index position relative to a given key:

| Syntax | Description |
|--------|-------------|
| `{0:1~a}` | Index 0 to 1 relative to key "a" |
| `{0:~a}` | From index 0 relative to key "a" to the end |
| `{!0:1~a}` | **Not** in relative index range of key "a" |

### 5.3 Map Top-Level Operations

Use `{}` to reference the map bin itself (for operations like `count`):

```
$.mapBin.{}.count()           -- size of the map
$.mapBin.count()              -- equivalent shorthand
```

---

## 6. List Path Elements

Lists are ordered sequences accessed by index. The AEL uses square brackets `[]`.

### 6.1 Singular List Elements (Context or Leaf)

| Syntax | Description | Example |
|--------|-------------|---------|
| `[1]` | List by **index** 1 | `$.l.[1]` |
| `[=1]` | List by **value** 1 | `$.l.[=1]` |
| `[#1]` | List by **rank** 1 | `$.l.[#1]` |

Negative indices count from the end: `[-1]` is the last element, `[-3]` is third from last.

### 6.2 Plural List Elements (Leaf Only)

#### Index Range

| Syntax | Description |
|--------|-------------|
| `[1:3]` | Index 1 through 3 (exclusive) |
| `[-3:-1]` | From third-to-last to last (exclusive) |
| `[1:]` | From index 1 to the end |
| `[:5]` | From start to index 5 (exclusive) |
| `[!2:4]` | Entries **outside** index 2-4 (inverted) |

#### Value List

| Syntax | Description |
|--------|-------------|
| `[=a,b,c]` | Elements with values a, b, or c |
| `[!=a,b,c]` | Elements whose values are **not** a, b, or c |
| `[=1,2,3]` | Elements with values 1, 2, or 3 |

#### Value Range

| Syntax | Description |
|--------|-------------|
| `[=5:8]` | Values from 5 up to 8 (exclusive) |
| `[=111:]` | Values from 111 to the end |
| `[!=10:20]` | Values **outside** 10-20 (inverted) |

#### Rank Range

| Syntax | Description |
|--------|-------------|
| `[#0:3]` | Rank 0 through 3 (exclusive) |
| `[#-3:]` | Top 3 ranked elements |
| `[!#0:3]` | Elements **outside** rank 0-3 (inverted) |

#### Relative Rank Range

| Syntax | Description |
|--------|-------------|
| `[#-3:-1~b]` | Rank range relative to value "b" |
| `[!#-3:-1~b]` | **Not** in relative rank range (inverted) |
| `[#-3:~b]` | From 3 ranks below "b" to the end |

### 6.3 List Top-Level Operations

Use `[]` to reference the list bin itself:

```
$.listBin.[].count()          -- size of the list
$.listBin.[] == [1, 2, 3]     -- compare entire list to a literal
```

---

## 7. Nested Path Examples

Paths can chain map and list accessors to navigate deeply nested structures.

### Map inside Map

```
$.profile.address.city == 'Denver'
```

This navigates: bin `profile` → map key `address` → map key `city`.

### List inside Map

```
$.profile.scores.[0] > 90
```

Navigates: bin `profile` → map key `scores` → list index 0.

### Map inside List

```
$.records.[2].name == 'Tim'
```

Navigates: bin `records` → list index 2 → map key `name`.

### Deep Nesting

```
$.data.users.[0].addresses.[1].city.get(type: STRING) == 'Austin'
```

Navigates: bin `data` → key `users` → index 0 → key `addresses` → index 1 → key `city`.

### Mixed CDT with Plural Leaf

```
$.mapBin.items.{a-d}                -- key range on a nested map
$.listBin.[0].[1:3]                  -- index range on a nested list
$.mapBin.settings.{=1,2,3}          -- value list on a nested map
```

---

## 8. Path Functions

Path functions are terminal operations appended to the end of a path.

### 8.1 `get()`

Explicitly sets the return type and/or data type. Accepts named parameters:

```
$.binName.get(type: INT)
$.binName.get(return: COUNT)
$.binName.get(type: INT, return: VALUE)
$.listBin.[0].get(type: STRING, return: VALUE)
$.mapBin.a.get(return: ORDERED_MAP)
```

**Parameters:**

| Parameter | Values | Description |
|-----------|--------|-------------|
| `type` | `INT`, `STRING`, `FLOAT`, `BOOL`, `BLOB`, `HLL`, `LIST`, `MAP`, `GEO` | Expected data type of the result |
| `return` | `VALUE`, `COUNT`, `INDEX`, `RANK`, `NONE`, `EXISTS`, `KEY`, `KEY_VALUE`, `ORDERED_MAP`, `UNORDERED_MAP`, `REVERSE_INDEX`, `REVERSE_RANK` | What metadata or transformation to return |

**Default behaviors:**

- The default operation at the end of any path is an implicit `get`.
- For single-element map/list access, default return type is `VALUE`.
- For multi-element map access, default return type is `ORDERED_MAP`.
- For multi-element list access, default return type is `VALUE`.
- The `type` parameter is inferred from the comparison operand when possible.

### 8.2 `count()`

Returns the count of elements:

```
$.listBin.[].count()          -- total size of the list
$.mapBin.{}.count()           -- total size of the map
$.listBin.[=4].count() > 0    -- how many elements equal 4
$.mapBin.a.{}.count() == 5    -- size of nested map at key 'a'
```

When used on a full bin (`[]` or `{}`), this maps to the efficient `size()` operation. When used on a subset (e.g., `[=4]`), it maps to a `get` with `return: COUNT`.

### 8.3 `exists()`

Checks whether a bin or element exists, returning a boolean:

```
$.binA.exists() and $.binB.exists()
$.mapbin.a.exists()
```

### 8.4 `asInt()` and `asFloat()`

Cast between integer and float types:

```
$.floatBin.asInt()            -- read a float bin as an integer
$.intBin.asFloat()            -- read an integer bin as a float
$.intBin.get(type: INT) > $.floatBin.asInt()
```

### 8.5 `type()`

Returns the data type of the element as one of: `INT`, `STR`, `HLL`, `BLOB`, `FLOAT`, `BOOL`, `LIST`, `MAP`, `GEO`.

```
$.binA.type() == INT
```

### 8.6 CDT Mutation Functions

These functions perform mutations on list or map data within write expressions.

**List mutation functions:**

| Function | Description |
|----------|-------------|
| `remove()` | Remove matched elements. Default return type: `COUNT` |
| `insert()` | Insert an element at the given position |
| `set()` | Set an element at the given position |
| `append()` | Append an element to the list |
| `increment()` | Increment a numeric element |
| `clear()` | Remove all items from the list |
| `sort()` | Sort the list |

**Map mutation functions:**

| Function | Description |
|----------|-------------|
| `remove()` | Remove matched entries. Default return type: `COUNT` |
| `put()` | Insert or update a key-value pair |
| `increment()` | Increment a numeric value by key |
| `clear()` | Remove all items from the map |

**Remove examples:**

```
$.listbin.[=a:z].remove(return: NONE)
$.listbin.[=4].remove()                  -- removes elements with value 4, returns count
```

If no `return` parameter is specified for `remove`, the default is `COUNT`.

---

## 9. Comparison Operators

All comparison operators work with infix notation. Operands can be bin paths, literals, or arithmetic expressions.

| Operator | Meaning | Example |
|----------|---------|---------|
| `==` | Equal | `$.name == 'Tim'` |
| `!=` | Not equal | `$.status != 'inactive'` |
| `>` | Greater than | `$.age > 21` |
| `>=` | Greater than or equal | `$.score >= 90` |
| `<` | Less than | `$.price < 100` |
| `<=` | Less than or equal | `$.ttl() <= 3600` |

The literal can be on either side: `100 < $.age` is equivalent to `$.age > 100`.

### The `in` Operator

The `in` operator checks whether a value exists within a list. The format is `expression in expression`, where the right-hand side must be a list. Precedence is the same as standard comparison operators (higher than logical operators).

```
"gold" in $.allowedStatuses
$.name in ["Bob", "Mary", "Richard"]
$.itemType in $.allowedItems
$.rooms.room1.rates.rateType in ["RACK_RATE", "DISCOUNT"]
$.cost > 50 and $.status in $.allowedStatuses and "available" in $.bookableStates
```

### Comparing bins to bins

When comparing two bin references, at least one must have an explicit type:

```
$.binA.get(type: INT) == $.binB.get(type: INT)
$.binA.get(type: STRING) > $.binB.get(type: STRING)
```

### Comparing to complex values

```
$.listBin.[] == [1, 2, 3]                    -- list equality
$.mapBin.{} == {name: 'Tim', age: 312}       -- map equality
$.listBin.get(type: LIST) == [100, 200]      -- explicit list type
```

---

## 10. Logical Operators

| Operator | Syntax | Example |
|----------|--------|---------|
| AND | `expr1 and expr2` | `$.age > 21 and $.active == true` |
| OR | `expr1 or expr2` | `$.role == 'admin' or $.role == 'super'` |
| NOT | `not(expr)` | `not($.age > 65)` |
| EXCLUSIVE | `exclusive(expr1, expr2, ...)` | `exclusive($.a > 10, $.b < 5)` |

### Precedence

`and` has higher precedence than `or`. Use parentheses to control grouping:

```
$.a > 1 and $.b > 2 or $.c > 3
-- is equivalent to:
($.a > 1 and $.b > 2) or $.c > 3

-- use parentheses to change grouping:
$.a > 1 and ($.b > 2 or $.c > 3)
```

### Boolean bin shorthand

When a boolean bin appears in a logical context, the type is inferred as `BOOL`:

```
$.flag1 and $.flag2
-- equivalent to:
$.flag1.get(type: BOOL) and $.flag2.get(type: BOOL)
```

### `not()`

Wraps any expression, inverting its boolean result:

```
not($.age > 10)
not($.keyExists())
```

### `exclusive()`

Returns true if exactly one of the expressions is true:

```
exclusive($.a > 10, $.a < 99, ($.a + $.b) > 17)
exclusive($.hand == 'hook', $.leg == 'peg')
```

---

## 11. Arithmetic Operators and Functions

### 11.1 Arithmetic Operators

Arithmetic operators work on integer or float operands. Both operands must be the same numeric type (use `asInt()`/`asFloat()` to convert if needed).

| Operator | Description | Operand Types | Example |
|----------|-------------|---------------|---------|
| `+` | Addition | INT or FLOAT | `$.price + $.tax` |
| `-` | Subtraction | INT or FLOAT | `$.total - $.discount` |
| `*` | Multiplication | INT or FLOAT | `$.price * $.qty` |
| `/` | Division | INT or FLOAT | `$.total / $.count` |
| `%` | Modulus | INT only | `$.value % 3` |
| `**` | Power (right-associative) | FLOAT only | `2.0 ** 7.0` |

The `**` operator is right-associative: `4.0 ** 5.0 ** 6.0` equals `4.0 ** (5.0 ** 6.0)`.

### 11.2 Arithmetic Functions

| Function | Description | Operand Types | Example |
|----------|-------------|---------------|---------|
| `abs(value)` | Absolute value | INT or FLOAT | `abs(-12) == 12` |
| `ceil(value)` | Round floating point up | FLOAT | `ceil(12.34) == 13` |
| `floor(value)` | Round floating point down | FLOAT | `floor(12.34) == 12` |
| `log(num, base)` | Logarithm | FLOAT | `log(32.0, 2.0) == 5.0` |
| `max(v1, v2, ...)` | Maximum of values | All same type (INT or FLOAT) | `max(4, 5, 9, 6, 3) == 9` |
| `min(v1, v2, ...)` | Minimum of values | All same type (INT or FLOAT) | `min(4, 5, 9, 6, 3) == 3` |

### 11.3 Type Casting Functions

| Function | Description | Example |
|----------|-------------|---------|
| `asFloat()` | Convert preceding integer to float | `28.asFloat() == 28.0` |
| `asInt()` | Convert preceding float to integer | `27.0.asInt() == 27` |

### Precedence

See [Section 16](#16-operator-precedence-lowest-to-highest) for the full
operator precedence table. Use parentheses to clarify or override:

```
($.price * $.qty) > 1000
($.a + $.b) * $.c
(($.apples + $.bananas) + $.oranges) > 10
```

### Arithmetic in filter expressions

```
($.apples + 5) > 10
($.price * $.qty) > 1000
(5.2 + $.bananas) > 10.2
```

### Arithmetic as read expressions

When used in a `selectFrom` context, arithmetic expressions produce computed values:

```
$.age + 20                    -- returns the age plus 20
$.price * $.qty               -- returns price times quantity
```

---

## 12. Bitwise Operators and Functions

All bitwise operators and functions work on integer values only.

### 12.1 Bitwise Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `&` | Bitwise AND | `$.flags & 0x0F` |
| `\|` | Bitwise OR | `$.flags \| 0x10` |
| `^` | Bitwise XOR | `$.a ^ $.b` |
| `~` | Bitwise NOT (unary) | `~$.mask` |
| `<<` | Left shift | `$.value << 2` |
| `>>` | Arithmetic right shift (sign-preserving) | `0x20 >> 1 == 16` |
| `>>>` | Logical right shift (zero-fill) | `0b10000 >>> 2 == 4` |

**`>>` vs `>>>`**: The `>>` operator preserves the sign bit (negative numbers remain negative after shifting). The `>>>` operator fills the vacated high bits with zeros (negative numbers become large positive numbers after shifting).

### 12.2 Bitwise Functions

| Function | Description | Example |
|----------|-------------|---------|
| `countOneBits(arg)` | Number of bits in `arg` that are 1 | `countOneBits(-1) == 64` |
| `findBitLeft(arg, search)` | Scan bits from most significant to least significant, looking for `search` (true=1, false=0). Returns the bit index (MSB is 0). | `findBitLeft(30, true)` |
| `findBitRight(arg, search)` | Scan bits from least significant to most significant, looking for `search` (true=1, false=0). Returns the bit index (MSB is 0). | `findBitRight(27, false)` |

### Examples

```
($.flags & 1) == 1            -- check if lowest bit is set
(($.flags >> 6) & 1) == 1     -- check if bit 6 is set
$.visits << 1                  -- left shift (multiply by 2)
(~$.apples) != 10             -- bitwise NOT
countOneBits($.mask) > 4      -- more than 4 bits set
```

---

## 13. Record Metadata

Access record-level metadata using function syntax after `$`:

| Function | Returns | Description |
|----------|---------|-------------|
| `$.ttl()` | INT | Time-to-live in seconds |
| `$.voidTime()` | INT | Absolute expiration time (-1 = never expires) |
| `$.lastUpdate()` | INT | Last update time (nanoseconds since epoch) |
| `$.sinceUpdate()` | INT | Milliseconds since last update |
| `$.setName()` | STRING | Name of the set this record belongs to |
| `$.keyExists()` | BOOL | Whether the user key is stored with the record |
| `$.isTombstone()` | BOOL | Whether the record is a tombstone (deleted) |
| `$.deviceSize()` | INT | Storage size on device in bytes |
| `$.memorySize()` | INT | Size in memory in bytes |
| `$.recordSize()` | INT | Total record size in bytes |
| `$.digestModulo(n)` | INT | Record digest modulo `n` |
| `$.key(STR)` | STRING | User key stored with the record (string) |
| `$.key(INT)` | INT | User key stored with the record (integer) |
| `$.key(BLOB)` | BLOB | User key stored with the record (blob) |

### Examples

```
$.ttl() < 3600                                     -- expires in less than 1 hour
$.recordSize() > 1024 and $.ttl() < 300            -- large record expiring soon
$.voidTime() == -1                                  -- never expires
$.setName() == 'groupA' or $.setName() == 'groupB'
$.isTombstone() and $.ttl() < 300
$.sinceUpdate() < 7200000                           -- updated in last 2 hours
$.digestModulo(3) == 0                              -- partition sampling
$.deviceSize() > 1024 or $.ttl() < 300
$.keyExists()                                       -- boolean context
not($.keyExists())
```

---

## 14. Control Structures

### 14.1 Variable Binding: `let ... then`

Defines local variables and evaluates an expression using them. Maps to `Exp.let()`.

**Syntax:**

```
let (<varDef1>, <varDef2>, ...) then (<expression>)
```

Each variable definition has the form: `variableName = <expression>`

Variables are referenced using `${variableName}`.

**Example:**

```
let (x = $.price, y = $.qty) then (${x} * ${y})
```

This binds `x` to the `price` bin and `y` to the `qty` bin, then returns their product.

**Variables can reference earlier variables:**

```
let (x = 1, y = ${x} + 1) then (${y} + ${x})
-- result: 3
```

**With bin references:**

```
let (total = $.price * $.qty, tax = ${total} * 0.1) then (${total} + ${tax})
```

### 14.2 Conditional: `when ... default`

Evaluates conditions in order and returns the result of the first matching one. Maps to `Exp.cond()`.

**Syntax:**

```
when (<condition1> => <result1>, <condition2> => <result2>, ..., default => <result>)
```

The `default` clause is mandatory and is returned when no condition matches.

**Example:**

```
when ($.who == 1 => "bob", $.who == 2 => "fred", default => "other")
```

**Used in comparisons:**

```
$.name == (when ($.tier == 1 => 'gold', $.tier == 2 => 'silver', default => 'bronze'))
```

**With type annotations:**

```
$.a.get(type: STRING) == (when (
    $.b == 1 => $.a1.get(type: STRING),
    $.b == 2 => $.a2.get(type: STRING),
    default => "hello"
))
```

### 14.3 Error / Unknown

The `error` keyword (also available as `unknown`) evaluates to an exception when reached. Its primary use is in a `when` branch to force a runtime error when an unexpected condition is met.

`error` and `unknown` are interchangeable — `error` is syntactic sugar over `unknown`.

```
when ($.status in ["GOLD", "PLATINUM"] => true, default => error)
```

Although it can appear anywhere an expression is accepted, in practice it is only useful inside conditional branches.

### Nesting Control Structures

Control structures can be nested and used anywhere an expression is valid:

```
$.result == (when (
    $.status == 'VIP' => let (base = $.price * 0.8) then (${base}),
    default => $.price
))
```

---

## 15. Prepared Statements (Placeholders)

AEL expressions can be pre-compiled with parameter placeholders for reuse. Placeholders use the syntax `?N` where N is a zero-based index.

```
$.bin > ?0
$.name == ?0 and $.age > ?1
($.apples + ?0) > ?1
```

At execution time, concrete values are bound to each placeholder:

```java
PreparedDsl prepared = PreparedDsl.prepare("$.age > ?0 and $.name == ?1");
// Bind values at execution time:
session.query(set).where(prepared, 21, "Tim").execute();
```

---

## 16. Operator Precedence (Lowest to Highest)

| Level | Operators | Associativity |
|-------|-----------|---------------|
| 1 | `or` | Left to right |
| 2 | `and` | Left to right |
| 3 | `==`, `!=`, `>`, `>=`, `<`, `<=`, `in` | Left to right |
| 4 | `&`, `\|`, `^` | Left to right |
| 5 | `<<`, `>>`, `>>>` | Left to right |
| 6 | `+`, `-` | Left to right |
| 7 | `*`, `/`, `%` | Left to right |
| 8 | `**` | Right to left |
| 9 | `~` (unary), `not()` | Unary prefix |
| 10 | Functions, path functions, literals, `()` | — |

Use parentheses to override default precedence:

```
$.a > 1 and $.b > 2 or $.c > 3         -- (a>1 AND b>2) OR c>3
$.a > 1 and ($.b > 2 or $.c > 3)       -- a>1 AND (b>2 OR c>3)
```

---

## 17. Comments

C-style block comments are supported:

```
/* This is a comment */
$.age > 21 /* filter adults */
```

Nested comments are **not** allowed.

---

## 18. Whitespace

Whitespace (spaces, tabs, newlines) is allowed between any elements:

```
$.a.b       -- compact
$. a . b    -- with spaces (equivalent)

$.age > 21 and $.name == 'Tim'
$.age>21 and $.name=='Tim'    -- also valid
```

---

## 19. Complete Examples

### Simple filter

```
$.age > 21 and $.status == 'active'
```

### CDT navigation with comparison

```
$.orders.[0].total > 100.0
```

### Nested map/list with count

```
$.inventory.items.[=0].count() == 0
```

### Complex logical filter with metadata

```
($.recordSize() > 1024 or $.ttl() < 300) and $.setName() == 'critical'
```

### Arithmetic read expression

```
($.price * $.qty) - $.discount
```

### Map plural access with count

```
$.config.{enabled,visible}.count() == 2
```

### Inverted selection

```
$.scores.[!0:2]               -- all list elements EXCEPT index 0-2
$.tags.{!temp,draft}           -- all map keys EXCEPT 'temp' and 'draft'
```

### Variable binding with conditional

```
let (tier = when ($.spent > 10000 => 3, $.spent > 1000 => 2, default => 1))
then (${tier})
```

### Comparing a bin to a list literal

```
$.listBin.[] == [1, 2, 3]
```

### Integer map key access

```
$.m.1                          -- access integer key 1
$.m."1"                        -- access string key "1"
$.m.{1,2}                      -- key list with integer keys 1 and 2
$.m.{1-3}                      -- key range: integer keys [1, 3)
```

### Relative rank range

```
$.scores.[#-1:1~[10.0, NIL]]
```

Finds the 2 closest-ranked elements to the value `[10.0, NIL]`.

### Multi-step path with explicit return type

```
$.mapBin.a.dd.[1].{#0}.get(return: UNORDERED_MAP)
```

Navigates into nested data and returns the lowest-ranked map entry as an unordered map.

---

## 20. AEL Syntax Rules for AI Agents

This section provides precise rules for generating correct AEL expressions programmatically.

### Rule 1: Always start paths with `$.`

Every bin or metadata reference begins with `$.`:

```
CORRECT:   $.age > 21
INCORRECT: age > 21
```

### Rule 2: Map context uses dot notation, list context uses brackets

```
$.mapBin.key                    -- map access (dot + identifier)
$.listBin.[0]                   -- list access (dot + brackets)
```

### Rule 3: The first context element determines bin type

```
$.x.name      -- x is a MAP bin (first context is identifier)
$.x.[0]       -- x is a LIST bin (first context is [])
$.x > 5       -- x is a SCALAR bin (no context)
```

### Rule 4: Plural elements are leaf-only

Key ranges, index ranges, value lists, rank ranges — these can **only** appear at the end of a path, never as intermediate context:

```
CORRECT:   $.m.nested.{a-c}
INCORRECT: $.m.{a-c}.nested     -- plural in middle is invalid
```

### Rule 5: Annotate types when comparing bins to bins

```
CORRECT:   $.a.get(type: INT) == $.b.get(type: INT)
INCORRECT: $.a == $.b            -- ambiguous, type cannot be inferred
```

### Rule 6: Parenthesise sub-expressions in arithmetic comparisons

```
CORRECT:   ($.a + $.b) > 10
INCORRECT: $.a + $.b > 10       -- > binds tighter than intended
```

### Rule 7: Variable names in `let` blocks

Variable names are unquoted identifiers referenced with `${name}`:

```
CORRECT:   let (x = 1) then (${x} + 1)
CORRECT:   let (total = $.a * $.b) then (${total} > 100)
```

### Rule 8: `when` must have a `default` clause

```
CORRECT:   when ($.a == 1 => "one", default => "other")
INCORRECT: when ($.a == 1 => "one")
```

### Rule 9: The `!` prefix inverts selections

Place `!` immediately after the opening bracket/brace:

```
{!a-c}     -- inverted key range
[!0:3]     -- inverted index range
{!=1,2,3}  -- inverted value list
[!#0:3]    -- inverted rank range
```

### Rule 10: Range semantics

- Key ranges and value ranges are **begin-inclusive, end-exclusive**: `{a-d}` includes a, b, c but not d.
- Index ranges in the AEL are also **exclusive** on the end.
- Rank ranges follow the same pattern.

### Rule 11: Use `get()` to override defaults

When the default type inference or return type is wrong, override explicitly:

```
$.mapBin.key.get(type: INT)                  -- force integer type
$.listBin.[0:3].get(return: COUNT)           -- return count instead of values
$.mapBin.{a,b}.get(return: KEY_VALUE)        -- return key-value pairs
```

### Rule 12: Integers vs floats in arithmetic

Both operands in arithmetic must be the same type. Use `asInt()` or `asFloat()` to convert:

```
$.intBin + $.floatBin.asInt()
$.intBin.asFloat() + $.floatBin
```
