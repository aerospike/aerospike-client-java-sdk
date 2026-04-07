# AEL Test Specifications

Comprehensive test specifications for the Aerospike Expression AEL. Each test
has sample data, the AEL expression, and the expected output. Tests are marked:

- **Status ✓** — should work with the current implementation
- **Status ✗** — known to fail (references a known issue from `ael-spec-vs-implementation.md`)
- **Status ?** — behaviour uncertain or untested

Tests use the fluent client pattern:

```java
// Read expression (returns a value)
session.query(set.id(N)).bin("r").selectFrom("<AEL>").execute();

// Filter expression (returns boolean — record included or excluded)
session.query(set.id(N)).where("<AEL>").execute();
```

---

## Test Data

### Record 1 — Scalar bins

| Bin | Type | Value |
|-----|------|-------|
| `intBin` | INT | `42` |
| `floatBin` | FLOAT | `3.14` |
| `strBin` | STRING | `"hello"` |
| `boolBin` | BOOL | `true` |
| `negInt` | INT | `-8` |
| `zeroBin` | INT | `0` |

### Record 2 — Simple map + simple list

| Bin | Type | Value |
|-----|------|-------|
| `m` | MAP | `{"alpha": 10, "beta": 20, "gamma": 30, "delta": 40, "epsilon": 50}` |
| `l` | LIST | `[50, 10, 40, 20, 30, 60, 5]` |

### Record 3 — Nested structures

| Bin | Type | Value |
|-----|------|-------|
| `profile` | MAP | `{"name": "Alice", "address": {"city": "Austin", "state": "TX", "zip": "73301"}, "scores": [95, 87, 72, 100, 63], "tags": ["vip", "early_adopter"]}` |

### Record 4 — Deep nesting

| Bin | Type | Value |
|-----|------|-------|
| `data` | MAP | `{"users": [{"name": "Bob", "addresses": [{"city": "NYC", "zip": "10001"}, {"city": "LA", "zip": "90001"}]}, {"name": "Eve", "addresses": [{"city": "SF", "zip": "94101"}]}]}` |

### Record 5 — Integer-keyed map

| Bin | Type | Value |
|-----|------|-------|
| `m` | MAP | `{1: "one", 2: "two", 3: "three", 10: "ten", 20: "twenty"}` |

### Record 6 — Empty collections and edge values

| Bin | Type | Value |
|-----|------|-------|
| `emptyList` | LIST | `[]` |
| `emptyMap` | MAP | `{}` |
| `intBin` | INT | `0` |
| `strBin` | STRING | `""` |

### Record 7 — Arithmetic, logic, control flow

| Bin | Type | Value |
|-----|------|-------|
| `price` | INT | `100` |
| `qty` | INT | `5` |
| `discount` | FLOAT | `10.0` |
| `tier` | INT | `2` |
| `status` | STRING | `"active"` |
| `flag1` | BOOL | `true` |
| `flag2` | BOOL | `false` |
| `items` | LIST | `["gold", "silver", "bronze"]` |
| `allowed` | LIST | `["gold", "platinum"]` |

### Record 8 — Transactions (map with composite string keys)

| Bin | Type | Value |
|-----|------|-------|
| `txns` | MAP (key-ordered) | See below |

```
{
  "1672531200000,txn01": [150,   "Coffee subscription"],
  "1675209600000,txn02": [10000, "Laptop"],
  "1677628800000,txn03": [250,   "Groceries"],
  "1680307200000,txn04": [500,   "Flight ticket"],
  "1682899200000,txn05": [75,    "Books"],
  "1685577600000,txn06": [8000,  "Phone"],
  "1688169600000,txn07": [3500,  "Conference ticket"],
  "1690848000000,txn08": [45,    "Snacks"],
  "1693526400000,txn09": [12000, "Vacation"],
  "1696118400000,txn10": [600,   "Concert"],
  "1698796800000,txn11": [2750,  "Furniture"],
  "1701388800000,txn12": [9500,  "Holiday gifts"]
}
```

Timestamps: txn01=Jan 2023, txn02=Feb 2023, …, txn12=Dec 2023.

### Record 9 — Rank test map

| Bin | Type | Value |
|-----|------|-------|
| `scores` | MAP | `{"math": 85, "science": 92, "english": 78, "art": 95, "history": 88}` |

Value rank order: `english(78) < math(85) < history(88) < science(92) < art(95)`.

### Record 10 — Multiple typed bins for cross-bin tests

| Bin | Type | Value |
|-----|------|-------|
| `a` | INT | `10` |
| `b` | INT | `20` |
| `c` | FLOAT | `30.5` |
| `d` | FLOAT | `40.7` |
| `name` | STRING | `"gold"` |

### Record 11 — Type derivation

| Bin | Type | Value |
|-----|------|-------|
| `a` | INT | `10` |
| `b` | INT | `10` |
| `c` | BOOL | `true` |
| `d` | INT | `11` |
| `e` | FLOAT | `3.14` |
| `f` | STRING | `"hello"` |
| `g` | BOOL | `false` |

Bins are chosen so that cross-expression type propagation can be fully
verified: `a==b` is true (both 10), `a+1==d` is true (11), `c` is boolean,
etc.

---

## 1. Scalar Bin Access

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| S01 | Read integer bin | 1 | `$.intBin` | Read | `42` (Long) | ✓ |
| S02 | Read float bin | 1 | `$.floatBin` | Read | `3.14` (Double) | ✓ |
| S03 | Read string bin | 1 | `$.strBin.get(type: STRING)` | Read | `"hello"` | ✓ |
| S04 | Read boolean bin | 1 | `$.boolBin` | Read | `true` | ✓ |
| S05 | Read negative integer | 1 | `$.negInt` | Read | `-8` (Long) | ✓ |
| S06 | Read zero | 6 | `$.intBin` | Read | `0` (Long) | ✓ |
| S07 | Compare int bin > literal | 1 | `$.intBin > 40` | Filter | `true` (record returned) | ✓ |
| S08 | Compare int bin > literal (false) | 1 | `$.intBin > 50` | Filter | `false` (record filtered out) | ✓ |
| S09 | Compare string bin == literal | 1 | `$.strBin == 'hello'` | Filter | `true` | ✓ |
| S10 | Literal on left side | 1 | `40 < $.intBin` | Filter | `true` | ✓ |

---

## 2. Type Inference and Casting

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| T01 | asFloat() on integer bin | 1 | `$.intBin.asFloat()` | Read | `42.0` (Double) | ✓ |
| T02 | asInt() on float bin | 1 | `$.floatBin.asInt()` | Read | `3` (Long, truncated) | ✓ |
| T03 | Explicit type annotation | 10 | `$.a.get(type: INT) > $.b.get(type: INT)` | Filter | `false` (10 > 20 is false) | ✓ |
| T04 | Compare two bins same type | 10 | `$.a.get(type: INT) == $.b.get(type: INT)` | Filter | `false` | ✓ |
| T05 | Mixed type arithmetic — no cast | 10 | `$.a + $.c` | Read | Error (INT + FLOAT type mismatch) | ✓ |
| T06 | Float-to-int cast in arithmetic | 10 | `$.a + $.c.asInt()` | Read | `40` (10 + 30) | ✓ |
| T07 | Int-to-float cast in arithmetic | 10 | `$.a.asFloat() + $.c` | Read | `40.5` (10.0 + 30.5) | ✓ |
| T08 | Round-trip cast precision loss | 1 | `$.floatBin.asInt().asFloat()` | Read | `3.0` (3.14→3→3.0) | ✓ |
| T09 | No-op cast: asInt() on int | 1 | `$.intBin.asInt()` | Read | `42` | ✓ |
| T10 | No-op cast: asFloat() on float | 1 | `$.floatBin.asFloat()` | Read | `3.14` | ✓ |

---

## 2b. Type Derivation

These tests verify whether the AEL parser can derive bin types **without**
explicit `get(type: ...)` annotations, based on context clues: literals,
boolean operators, arithmetic operators, and cross-expression propagation.

Uses Record 11: `a=10(INT), b=10(INT), c=true(BOOL), d=11(INT), e=3.14(FLOAT), f="hello"(STRING), g=false(BOOL)`.

### Level 1: Literal provides the type hint

| ID | Description | AEL Expression | Context | Expected | Status |
|----|-------------|----------------|---------|----------|--------|
| TD01 | INT from literal | `$.a > 5` | Filter | `true` (10 > 5; `a` derived INT) | ✓ |
| TD02 | STRING from literal | `$.f == 'hello'` | Filter | `true` (`f` derived STRING) | ✓ |
| TD03 | FLOAT from literal | `$.e > 3.0` | Filter | `true` (`e` derived FLOAT) | ✓ |
| TD04 | BOOL from literal | `$.c == true` | Filter | `true` (`c` derived BOOL) | ✓ |

### Level 2: Boolean context implies BOOL

| ID | Description | AEL Expression | Context | Expected | Status |
|----|-------------|----------------|---------|----------|--------|
| TD05 | AND implies both BOOL | `$.c and not($.g)` | Filter | `true` | ✓ |
| TD06 | OR implies both BOOL | `$.c or $.g` | Filter | `true` | ✓ |
| TD07 | NOT implies BOOL | `not($.g)` | Filter | `true` | ✓ |

### Level 3: Arithmetic context implies numeric type

| ID | Description | AEL Expression | Context | Expected | Status |
|----|-------------|----------------|---------|----------|--------|
| TD08 | Arithmetic + INT literal | `$.a + 1` | Read | `11` (`a` derived INT) | ✓ |
| TD09 | Arithmetic + FLOAT literal | `$.e + 1.0` | Read | `4.14` (`e` derived FLOAT) | ✓ |
| TD10 | Arithmetic, no literal | `$.a + $.b` | Read | `20` — can parser derive type with no literal hint? | ? |

### Level 4: Propagation through comparison

The literal in one sub-expression provides the type, which propagates
across the `==` or `>` operator to type the other operand.

| ID | Description | AEL Expression | Context | Expected | Status |
|----|-------------|----------------|---------|----------|--------|
| TD11 | Literal propagates through arithmetic+equality | `$.a + 1 == $.d` | Filter | `true` (11 == 11; `d` derived INT from `a+1`) | ? |
| TD12 | Same propagation, `>` operator | `$.a + 1 > $.b` | Filter | `true` (11 > 10; `b` derived INT) | ? |

### Level 5: Cross-expression propagation

Type information from one clause of a boolean chain should propagate to
shared bin references in other clauses.

| ID | Description | AEL Expression | Context | Expected | Status |
|----|-------------|----------------|---------|----------|--------|
| TD13 | Full chain: `a`,`b`,`d`=INT; `c`=BOOL | `$.a == $.b and $.c and $.a + 1 == $.d` | Filter | `true` — `a` typed by `+1`, propagates to `b` via `==`; `c` typed by `and`; `d` typed by `== (a+1)` | ? |
| TD14 | Mixed types in chain | `$.f == 'hello' and $.a + 1 == $.d and $.c` | Filter | `true` — `f`=STRING, `a`/`d`=INT, `c`=BOOL | ? |

### Level 6: Nested arithmetic with propagation

| ID | Description | AEL Expression | Context | Expected | Status |
|----|-------------|----------------|---------|----------|--------|
| TD15 | `($.a * $.b) > 50` | `($.a * $.b) > 50` | Filter | `true` (100 > 50; literal 50 propagates through `>` and `*` to both `a` and `b`) | ? |
| TD16 | No literals anywhere | `($.a + $.b) == ($.d + $.b)` | Filter | `false` (20 ≠ 21) — can the parser work with zero literal hints? | ? |

### Level 7: Shared bin reference across contexts

A bin appears in two different sub-expressions. Its type in one should
inform the other.

| ID | Description | AEL Expression | Context | Expected | Status |
|----|-------------|----------------|---------|----------|--------|
| TD17 | `$.a` in comparison and arithmetic | `$.a > 0 and $.a + $.b > 15` | Filter | `true` (10>0 and 20>15; `a` INT from `>0`, `b` INT from `a+b` arithmetic) | ? |

### Level 8: Type mismatch detection

The parser should detect and reject type-incompatible operations.

| ID | Description | AEL Expression | Context | Expected | Status |
|----|-------------|----------------|---------|----------|--------|
| TD18 | INT + STRING | `$.a + $.f` | Read | Error | ? |
| TD19 | INT > STRING literal | `$.a > 'hello'` | Filter | Error or `false` | ? |

### Level 9: Explicit cast vs inference

Demonstrates that `asFloat()`/`asInt()` are needed when the parser
cannot infer type or when operand types differ.

| ID | Description | AEL Expression | Context | Expected | Status |
|----|-------------|----------------|---------|----------|--------|
| TD20 | Explicit cast resolves mismatch | `$.a.asFloat() + $.e` | Read | `13.14` (10.0 + 3.14) | ✓ |
| TD21 | No cast → type mismatch | `$.a + $.e` | Read | Error (INT + FLOAT) | ✓ |

---

## 3. Map Access

### 3.1 Singular access

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| M01 | String key (unquoted) | 2 | `$.m.alpha.get(type: INT)` | Read | `10` | ✓ |
| M02 | String key (quoted) | 2 | `$.m.'alpha'.get(type: INT)` | Read | `10` | ✓ |
| M03 | Integer key lookup | 5 | `$.m.1.get(type: STRING)` | Read | `"one"` | ✗ 2b |
| M04 | Map by index {0} | 2 | `$.m.{0}.get(type: INT)` | Read | `10` (first entry by key order) | ✓ |
| M05 | Map by value {=20} | 2 | `$.m.{=20}.get(type: STRING)` | Read | `"beta"` (key of value 20) | ✓ |
| M06 | Map by rank {#0} | 9 | `$.scores.{#0}.get(type: STRING)` | Read | `"english"` (lowest value 78) | ✓ |
| M07 | Map by rank {#-1} | 9 | `$.scores.{#-1}.get(type: STRING)` | Read | `"art"` (highest value 95) | ✓ |
| M08 | Map by negative index {-1} | 2 | `$.m.{-1}.get(type: INT)` | Read | `30` (last entry by key order = `gamma`) | ✓ |
| M09 | Map count | 2 | `$.m.{}.count()` | Read | `5` | ✓ |
| M10 | Nested map string access | 3 | `$.profile.address.city.get(type: STRING)` | Read | `"Austin"` | ✓ |

### 3.2 Plural access (ranges, lists)

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| M11 | Key range | 2 | `$.m.{alpha-delta}` | Read | Map with keys `alpha`, `beta` (begin-inclusive, end-exclusive, so `delta` excluded; but actually `alpha` < `beta` < `delta` alphabetically, `gamma` > `delta` ) — returns `{"alpha":10, "beta":20}` | ✓ |
| M12 | Key range (open-ended) | 2 | `$.m.{delta-}` | Read | `{"delta": 40, "epsilon": 50, "gamma": 30}` | ✓ |
| M13 | Key list | 2 | `$.m.{alpha,gamma}` | Read | `{"alpha": 10, "gamma": 30}` | ✓ |
| M14 | Inverted key range | 2 | `$.m.{!alpha-delta}` | Read | All keys NOT in [alpha, delta) | ✓ |
| M15 | Inverted key list | 2 | `$.m.{!alpha,gamma}` | Read | `{"beta": 20, "delta": 40, "epsilon": 50}` | ✓ |
| M16 | Index range {0:3} | 2 | `$.m.{0:3}` | Read | First 3 entries by key order | ✓ |
| M17 | Index range from end {-2:} | 2 | `$.m.{-2:}` | Read | Last 2 entries by key order | ✓ |
| M18 | Value range {=15:35} | 2 | `$.m.{=15:35}` | Read | Entries with values in [15, 35): `{"beta": 20, "gamma": 30}` | ✓ |
| M19 | Value list {=10,30,50} | 2 | `$.m.{=10,30,50}` | Read | `{"alpha": 10, "gamma": 30, "epsilon": 50}` | ✓ |
| M20 | Rank range {#0:3} | 9 | `$.scores.{#0:3}` | Read | 3 lowest: `{"english": 78, "math": 85, "history": 88}` | ✓ |
| M21 | Rank range from top {#-2:} | 9 | `$.scores.{#-2:}` | Read | 2 highest: `{"science": 92, "art": 95}` | ✓ |
| M22 | Inverted rank range | 9 | `$.scores.{!#0:2}` | Read | All except 2 lowest | ✓ |
| M23 | Relative rank range | 9 | `$.scores.{#-1:2~88}` | Read | Ranks relative to value 88: 1 below and 1 at/above | ✓ |
| M24 | Key-relative index range | 2 | `$.m.{0:2~beta}` | Read | 2 entries starting from key `beta` | ✓ |
| M25 | Count on plural | 2 | `$.m.{alpha-delta}.get(return: COUNT)` | Read | `2` | ✓ |

---

## 4. List Access

### 4.1 Singular access

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| L01 | Positive index [0] | 2 | `$.l.[0].get(type: INT)` | Read | `50` | ✓ |
| L02 | Middle index [3] | 2 | `$.l.[3].get(type: INT)` | Read | `20` | ✓ |
| L03 | Negative index [-1] (last) | 2 | `$.l.[-1].get(type: INT)` | Read | `5` (last element) | ✓ |
| L04 | Negative index [-3] | 2 | `$.l.[-3].get(type: INT)` | Read | `30` (third from last) | ✓ |
| L05 | By value [=40] | 2 | `$.l.[=40].get(type: INT)` | Read | `40` | ✓ |
| L06 | By rank [#0] (lowest) | 2 | `$.l.[#0].get(type: INT)` | Read | `5` (lowest value) | ✓ |
| L07 | By rank [#-1] (highest) | 2 | `$.l.[#-1].get(type: INT)` | Read | `60` (highest value) | ✓ |
| L08 | List count | 2 | `$.l.[].count()` | Read | `7` | ✓ |

### 4.2 Plural access (ranges, lists)

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| L09 | Index range [0:3] | 2 | `$.l.[0:3]` | Read | `[50, 10, 40]` (indices 0, 1, 2) | ✓ |
| L10 | Index range from end [-3:] | 2 | `$.l.[-3:]` | Read | `[30, 60, 5]` (last 3) | ✓ |
| L11 | Inverted index range [!0:2] | 2 | `$.l.[!0:2]` | Read | Everything except first 2: `[40, 20, 30, 60, 5]` | ✓ |
| L12 | Value list [=10,30,50] | 2 | `$.l.[=10,30,50]` | Read | `[50, 10, 30]` (elements with those values) | ✓ |
| L13 | Inverted value list | 2 | `$.l.[!=10,30,50]` | Read | `[40, 20, 60, 5]` | ✓ |
| L14 | Value range [=20:50] | 2 | `$.l.[=20:50]` | Read | Elements with values in [20, 50): `[40, 20, 30]` | ✓ |
| L15 | Rank range [#0:3] | 2 | `$.l.[#0:3]` | Read | 3 lowest: `[5, 10, 20]` | ✓ |
| L16 | Rank range from top [#-3:] | 2 | `$.l.[#-3:]` | Read | 3 highest: `[40, 50, 60]` | ✓ |
| L17 | Inverted rank range | 2 | `$.l.[!#0:2]` | Read | All except 2 lowest | ✓ |
| L18 | Relative rank range | 2 | `$.l.[#-1:2~30]` | Read | 1 below 30 and 1 at 30: ranks relative to value 30 | ✓ |
| L19 | Count on value match | 2 | `$.l.[=10].count()` | Read | `1` (one element equals 10) | ✓ |
| L20 | Count on range | 2 | `$.l.[=20:50].get(return: COUNT)` | Read | `3` (values 20, 30, 40 are in range) | ✓ |

---

## 5. Nested CDT Navigation

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| N01 | Map → Map → scalar | 3 | `$.profile.address.city.get(type: STRING)` | Read | `"Austin"` | ✓ |
| N02 | Map → Map → scalar (zip) | 3 | `$.profile.address.zip.get(type: STRING)` | Read | `"73301"` | ✓ |
| N03 | Map → List → index | 3 | `$.profile.scores.[0].get(type: INT)` | Read | `95` | ✓ |
| N04 | Map → List → negative index | 3 | `$.profile.scores.[-1].get(type: INT)` | Read | `63` (last) | ✓ |
| N05 | Map → List → count | 3 | `$.profile.scores.[].count()` | Read | `5` | ✓ |
| N06 | Map → List → rank (highest) | 3 | `$.profile.scores.[#-1].get(type: INT)` | Read | `100` | ✓ |
| N07 | Deep: Map → List → Map | 4 | `$.data.users.[0].name.get(type: STRING)` | Read | `"Bob"` | ✓ |
| N08 | Deep: Map → List → Map → List → Map | 4 | `$.data.users.[0].addresses.[1].city.get(type: STRING)` | Read | `"LA"` | ✓ |
| N09 | Deep: second list element | 4 | `$.data.users.[1].name.get(type: STRING)` | Read | `"Eve"` | ✓ |
| N10 | Deep: second user's first address | 4 | `$.data.users.[1].addresses.[0].city.get(type: STRING)` | Read | `"SF"` | ✓ |
| N11 | Map → List → range | 3 | `$.profile.scores.[0:3]` | Read | `[95, 87, 72]` | ✓ |
| N12 | Map → List with value filter | 3 | `$.profile.tags.[=vip].get(type: STRING)` | Read | `"vip"` | ✓ |
| N13 | Nested map count | 3 | `$.profile.address.{}.count()` | Read | `3` (city, state, zip) | ✓ |

---

## 6. Arithmetic

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| A01 | Integer addition | 7 | `$.price + $.qty` | Read | `105` (100 + 5) | ✓ |
| A02 | Integer subtraction | 7 | `$.price - $.qty` | Read | `95` (100 - 5) | ✓ |
| A03 | Integer multiplication | 7 | `$.price * $.qty` | Read | `500` (100 * 5) | ✓ |
| A04 | Integer division | 7 | `$.price / $.qty` | Read | `20` (100 / 5) | ✓ |
| A05 | Integer modulus | 7 | `$.price % $.qty` | Read | `0` (100 % 5) | ✓ |
| A06 | Bin + literal | 7 | `$.price + 50` | Read | `150` | ✓ |
| A07 | Parenthesized expression | 7 | `($.price * $.qty) - 100` | Read | `400` | ✓ |
| A08 | Nested parentheses | 7 | `(($.price + $.qty) * 2) - 10` | Read | `200` (210 - 10) | ✓ |
| A09 | Arithmetic in filter | 7 | `($.price * $.qty) > 400` | Filter | `true` (500 > 400) | ✓ |
| A10 | Float addition | 10 | `$.c + $.d` | Read | `71.2` (30.5 + 40.7) | ✓ |
| A11 | Division with remainder | 1 | `$.intBin / 5` | Read | `8` (42 / 5 = 8, integer division) | ✓ |
| A12 | Modulus non-zero | 1 | `$.intBin % 5` | Read | `2` (42 % 5) | ✓ |

---

## 7. Bitwise Operations

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| B01 | Bitwise AND | 1 | `$.intBin & 15` | Read | `10` (42 & 0xF = 0b101010 & 0b1111 = 0b1010) | ✓ |
| B02 | Bitwise OR | 1 | `$.intBin \| 15` | Read | `47` (42 \| 15 = 0b101010 \| 0b001111 = 0b101111) | ✓ |
| B03 | Bitwise XOR | 1 | `$.intBin ^ 15` | Read | `37` (42 ^ 15) | ✓ |
| B04 | Bitwise NOT | 1 | `~$.intBin` | Read | `-43` (~42) | ✓ |
| B05 | Left shift | 1 | `$.intBin << 2` | Read | `168` (42 << 2) | ✓ |
| B06 | Right shift (positive) | 1 | `$.intBin >> 1` | Read | `21` (42 >> 1) — positive numbers identical for arithmetic/logical | ✓ |
| B07 | Right shift (negative, arithmetic) | 1 | `$.negInt >> 1` | Read | `-4` (arithmetic, sign preserved) | ✗ 2c |
| B08 | Logical right shift >>> | 1 | `$.negInt >>> 1` | Read | `9223372036854775804` (zero-fill) | ✗ 2c |
| B09 | AND in filter context | 1 | `($.intBin & 1) == 0` | Filter | `true` (42 is even, bit 0 is 0) | ✓ |
| B10 | Check specific bit | 1 | `(($.intBin >> 3) & 1) == 1` | Filter | `true` (bit 3 of 42 = 0b101010 is 1) | ✓ |

---

## 8. Comparison Operators

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| C01 | Equal == | 1 | `$.intBin == 42` | Filter | `true` | ✓ |
| C02 | Not equal != | 1 | `$.intBin != 42` | Filter | `false` | ✓ |
| C03 | Greater > | 1 | `$.intBin > 41` | Filter | `true` | ✓ |
| C04 | Greater or equal >= | 1 | `$.intBin >= 42` | Filter | `true` | ✓ |
| C05 | Less < | 1 | `$.intBin < 43` | Filter | `true` | ✓ |
| C06 | Less or equal <= | 1 | `$.intBin <= 42` | Filter | `true` | ✓ |
| C07 | String equality | 1 | `$.strBin == 'hello'` | Filter | `true` | ✓ |
| C08 | String inequality | 1 | `$.strBin != 'world'` | Filter | `true` | ✓ |
| C09 | Float comparison | 1 | `$.floatBin > 3.0` | Filter | `true` | ✓ |
| C10 | Boolean equality | 1 | `$.boolBin == true` | Filter | `true` | ✓ |
| C11 | `in` — literal in bin list | 7 | `"gold" in $.items` | Filter | `true` (items contains "gold") | ✓ |
| C12 | `in` — literal not in bin list | 7 | `"platinum" in $.items` | Filter | `false` | ✓ |
| C13 | `in` — bin in literal list | 7 | `$.status in ["active", "pending"]` | Filter | `true` | ✓ |
| C14 | `in` — bin in bin list | 7 | `$.name in $.items` | Filter | `true` ("gold" is in items) | ? |
| C15 | Comparison boundary: equal values | 1 | `$.intBin >= 42 and $.intBin <= 42` | Filter | `true` (both boundaries hit) | ✓ |
| C16 | Literal on left | 7 | `100 == $.price` | Filter | `true` | ✓ |

> Note: C14 uses Record 10 for `$.name` ("gold") and Record 7 for `$.items`. This should be tested on a record
> that has both bins.

---

## 9. Logical Operators

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| LG01 | AND — both true | 1 | `$.intBin > 40 and $.strBin == 'hello'` | Filter | `true` | ✓ |
| LG02 | AND — one false | 1 | `$.intBin > 50 and $.strBin == 'hello'` | Filter | `false` | ✓ |
| LG03 | OR — one true | 1 | `$.intBin > 50 or $.strBin == 'hello'` | Filter | `true` | ✓ |
| LG04 | OR — both false | 1 | `$.intBin > 50 or $.strBin == 'world'` | Filter | `false` | ✓ |
| LG05 | NOT | 1 | `not($.intBin > 50)` | Filter | `true` | ✓ |
| LG06 | NOT double negation | 1 | `not(not($.intBin > 40))` | Filter | `true` | ✓ |
| LG07 | EXCLUSIVE — one true | 7 | `exclusive($.flag1, $.flag2)` | Filter | `true` (exactly one is true) | ✓ |
| LG08 | EXCLUSIVE — both true | 7 | `exclusive($.flag1, $.boolBin)` | Filter | Should test with two true values → `false` | ? |
| LG09 | Precedence: AND before OR | 7 | `$.flag1 or $.flag2 and $.flag2` | Filter | `true` (= `flag1 or (flag2 and flag2)` = `true or false`) | ✓ |
| LG10 | Parentheses override precedence | 7 | `($.flag1 or $.flag2) and $.flag2` | Filter | `false` (= `true and false`) | ✓ |
| LG11 | Boolean bin shorthand | 7 | `$.flag1 and not($.flag2)` | Filter | `true` | ✓ |
| LG12 | Three-way AND | 1 | `$.intBin > 0 and $.intBin < 100 and $.strBin == 'hello'` | Filter | `true` | ✓ |
| LG13 | Three-way OR | 1 | `$.intBin == 0 or $.intBin == 42 or $.intBin == 99` | Filter | `true` | ✓ |

---

## 10. Control Structures

### 10.1 Variable binding (`with...do` / spec: `let...then`)

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| CS01 | Simple variable | 7 | `with ('x' = $.price) do (${x} + 1)` | Read | `101` | ✓ |
| CS01s | Simple variable (spec syntax) | 7 | `let (x = $.price) then (${x} + 1)` | Read | `101` | ✗ 2a |
| CS02 | Two variables | 7 | `with ('x' = $.price, 'y' = $.qty) do (${x} * ${y})` | Read | `500` | ✓ |
| CS03 | Variable referencing earlier variable | 7 | `with ('x' = $.price, 'y' = ${x} * 2) do (${y} + ${x})` | Read | `300` (y=200, x=100) | ✓ |
| CS04 | Variable with arithmetic | 7 | `with ('total' = $.price * $.qty, 'tax' = ${total} / 10) do (${total} + ${tax})` | Read | `550` (500 + 50) | ✓ |
| CS05 | Nested with...do | 7 | `with ('x' = $.price) do (with ('y' = ${x} * 2) do (${y} + ${x}))` | Read | `300` | ✓ |
| CS06 | Variable used in comparison | 7 | `with ('total' = $.price * $.qty) do (${total} > 400)` | Filter | `true` (500 > 400) | ✓ |

### 10.2 Conditional (`when...default`)

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| CS07 | Two branches + default, match first | 7 | `when ($.tier == 1 => "gold", $.tier == 2 => "silver", default => "bronze")` | Read | `"silver"` (tier=2) | ✓ |
| CS08 | Fall to default | 7 | `when ($.tier == 5 => "diamond", default => "standard")` | Read | `"standard"` | ✓ |
| CS09 | Three branches, match middle | 7 | `when ($.tier == 1 => 100, $.tier == 2 => 200, $.tier == 3 => 300, default => 0)` | Read | `200` | ✓ |
| CS10 | When with complex conditions | 7 | `when ($.price > 200 => "expensive", $.price > 50 => "moderate", default => "cheap")` | Read | `"moderate"` (price=100) | ✓ |
| CS11 | Nested when | 7 | `when ($.tier > 0 => when ($.tier == 1 => "tier1", default => "tierN"), default => "none")` | Read | `"tierN"` | ✓ |
| CS12 | When inside comparison | 7 | `$.status == (when ($.tier == 2 => "active", default => "inactive"))` | Filter | `true` | ✓ |

### 10.3 Mixed nesting

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| CS13 | when inside with body | 7 | `with ('t' = $.tier) do (when (${t} == 1 => "gold", ${t} == 2 => "silver", default => "bronze"))` | Read | `"silver"` | ✓ |
| CS14 | with inside when branch | 7 | `when ($.tier == 2 => with ('p' = $.price) do (${p} * 2), default => 0)` | Read | `200` | ✓ |
| CS15 | Deeply nested: with → when → with | 7 | `with ('t' = $.tier) do (when (${t} == 2 => with ('p' = $.price) do (${p} + ${t}), default => 0))` | Read | `102` (100 + 2) | ✓ |
| CS16 | error/unknown in when branch | 7 | `when ($.tier == 2 => "ok", default => unknown)` | Read | `"ok"` (tier=2 matches first branch) | ✓ |
| CS17 | error hits default | 7 | `when ($.tier == 5 => "ok", default => unknown)` | Read | Runtime error (unknown reached) | ✓ |

---

## 11. Metadata

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| MD01 | TTL | 1 | `$.ttl()` | Read | INT (seconds until expiry, or -1 for never-expire) | ✓ |
| MD02 | Record size | 1 | `$.recordSize()` | Read | INT > 0 | ✓ |
| MD03 | Key exists | 1 | `$.keyExists()` | Filter | `true` or `false` (depends on key storage policy) | ✓ |
| MD04 | Set name | 1 | `$.setName() == 'ael_test_spec'` | Filter | `true` | ✓ |
| MD05 | TTL in filter | 1 | `$.ttl() > 0 or $.ttl() == -1` | Filter | `true` | ✓ |
| MD06 | Since update | 1 | `$.sinceUpdate() >= 0` | Filter | `true` | ✓ |
| MD07 | Void time | 1 | `$.voidTime()` | Read | INT (-1 or positive) | ✓ |
| MD08 | Digest modulo | 1 | `$.digestModulo(3)` | Read | `0`, `1`, or `2` | ✓ |
| MD09 | Is tombstone | 1 | `$.isTombstone()` | Filter | `false` (live record) | ✓ |
| MD10 | Metadata in logical expression | 1 | `$.recordSize() > 0 and $.ttl() != 0` | Filter | `true` | ✓ |

---

## 12. Path Functions

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| PF01 | get(type: INT) | 2 | `$.m.alpha.get(type: INT)` | Read | `10` | ✓ |
| PF02 | get(return: COUNT) on key range | 2 | `$.m.{alpha-delta}.get(return: COUNT)` | Read | `2` | ✓ |
| PF03 | get(return: KEY) on rank | 9 | `$.scores.{#-1}.get(return: KEY)` | Read | `"art"` (highest) | ✓ |
| PF04 | get(return: INDEX) | 2 | `$.m.alpha.get(return: INDEX)` | Read | `0` (first by key order) | ✓ |
| PF05 | get(return: RANK) | 9 | `$.scores.math.get(return: RANK)` | Read | `1` (second lowest: 78 < 85) | ✓ |
| PF06 | get(return: ORDERED_MAP) | 2 | `$.m.{alpha,beta}.get(return: ORDERED_MAP)` | Read | `{"alpha": 10, "beta": 20}` | ✓ |
| PF07 | get(return: EXISTS) on existing key | 2 | `$.m.alpha.get(return: EXISTS)` | Read | `true` | ✓ |
| PF08 | get(return: EXISTS) on missing key | 2 | `$.m.zzz.get(return: EXISTS)` | Read | `false` | ✓ |
| PF09 | count() on whole list | 2 | `$.l.[].count()` | Read | `7` | ✓ |
| PF10 | count() on value match | 2 | `$.l.[=50].count()` | Read | `1` | ✓ |
| PF11 | exists() on present bin | 1 | `$.intBin.exists()` | Read | `true` | ✗ 2d |
| PF12 | exists() on missing bin | 1 | `$.missingBin.exists()` | Read | `false` | ✗ 2d |

---

## 13. Special Values (INF, NIL, WILDCARD)

> Note: These special values are defined in the spec but may not be
> available as operands in the current grammar.

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| SV01 | List comparison with WILDCARD | 2 | `$.l.[] == [50, *]` | Filter | `true` (list starts with 50, wildcard matches rest) | ? |
| SV02 | Map key range with NIL start | 2 | `$.m.{NIL-delta}` | Read | All keys up to (but not including) `delta` | ? |
| SV03 | Map value range with INF end | 2 | `$.m.{=0:INF}` | Read | All entries (values from 0 to infinity) | ? |
| SV04 | List value range with NIL | 2 | `$.l.[=NIL:30]` | Read | Values from lowest possible to 30 | ? |
| SV05 | Relative rank with complex value | 2 | `$.l.[#-1:1~[10, NIL]]` | Read | Relative rank near [10, NIL] | ? |

---

## 14. Edge Cases

### 14.1 Boundary conditions

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| E01 | List index 0 (first) | 2 | `$.l.[0].get(type: INT)` | Read | `50` | ✓ |
| E02 | List index -1 (last) | 2 | `$.l.[-1].get(type: INT)` | Read | `5` | ✓ |
| E03 | Empty list count | 6 | `$.emptyList.[].count()` | Read | `0` | ✓ |
| E04 | Empty map count | 6 | `$.emptyMap.{}.count()` | Read | `0` | ✓ |
| E05 | Zero bin in arithmetic | 6 | `$.intBin + 1` | Read | `1` (0 + 1) | ✓ |
| E06 | Empty string comparison | 6 | `$.strBin == ''` | Filter | `true` (empty string equals empty string) | ✓ |
| E07 | Negative + negative | 1 | `$.negInt + $.negInt` | Read | `-16` (-8 + -8) | ✓ |

### 14.2 Error conditions

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| E08 | Out of range index | 2 | `$.l.[100].get(type: INT)` | Read | Error or null (index beyond list) | ? |
| E09 | Negative index beyond list | 2 | `$.l.[-100].get(type: INT)` | Read | Error or null | ? |
| E10 | Non-existent map key | 2 | `$.m.zzz.get(type: INT)` | Read | Error or null | ? |
| E11 | Non-existent bin | 1 | `$.nonExistent > 0` | Filter | Error or record filtered | ? |
| E12 | Division by zero | 1 | `$.intBin / 0` | Read | Error | ? |
| E13 | Modulus by zero | 1 | `$.intBin % 0` | Read | Error | ? |
| E14 | Type mismatch in comparison | 1 | `$.intBin == 'hello'` | Filter | `false` (different types never equal) | ? |
| E15 | Empty expression | — | `` (empty string) | Read | Parse error | ✓ |
| E16 | Invalid syntax | — | `$.bin.>>>` | Read | Parse error | ✓ |

### 14.3 Quoted keys and special characters

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| E17 | Quoted key with special chars | 3 | `$.profile.'name'.get(type: STRING)` | Read | `"Alice"` | ✓ |
| E18 | Double-quoted key | 3 | `$.profile."name".get(type: STRING)` | Read | `"Alice"` | ✓ |
| E19 | Numeric string key (quoted) | 5 | `$.m."1".get(type: STRING)` | Read | Depends: string key "1" lookup | ? |

---

## 15. Complex Chaining / Transaction Scenario

These tests use Record 8 (transactions map). The goal is to demonstrate
realistic multi-step AEL usage patterns.

### 15.1 Basic transaction queries

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| TX01 | Total transaction count | 8 | `$.txns.{}.count()` | Read | `12` | ✓ |
| TX02 | Get first transaction (by key) | 8 | `$.txns.{0}.get(return: KEY_VALUE)` | Read | `{"1672531200000,txn01": [150, "Coffee subscription"]}` | ✓ |
| TX03 | Get last transaction (by key) | 8 | `$.txns.{-1}.get(return: KEY_VALUE)` | Read | `{"1701388800000,txn12": [9500, "Holiday gifts"]}` | ✓ |

### 15.2 Time-range filtering via key range

Since map keys are `"timestamp,txnId"` and maps are key-ordered, key range
queries naturally filter by time.

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| TX04 | Transactions in Q3 2023 (Jul-Sep) | 8 | `$.txns.{"1688169600000"-"1696118400000"}` | Read | 3 entries: txn07 (3500), txn08 (45), txn09 (12000) | ✓ |
| TX05 | Count transactions in Q3 | 8 | `$.txns.{"1688169600000"-"1696118400000"}.get(return: COUNT)` | Read | `3` | ✓ |
| TX06 | All from Jun onwards | 8 | `$.txns.{"1685577600000"-}` | Read | 7 entries: txn06 through txn12 | ✓ |
| TX07 | All before Apr | 8 | `$.txns.{-"1680307200000"}` | Read | 3 entries: txn01, txn02, txn03 | ✓ |
| TX08 | Empty range (no matches) | 8 | `$.txns.{"9999999999999"-}` | Read | Empty map `{}` | ✓ |
| TX09 | Count in range | 8 | `$.txns.{"1685577600000"-"1701388800000"}.get(return: COUNT)` | Read | `6` (txn06 through txn11) | ✓ |

### 15.3 Value-based ranking (top/bottom N)

Map values are lists `[amount, description]`. Rank ordering compares lists
element-by-element, so ranking is primarily by amount (the first element).

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| TX10 | Highest value transaction (rank -1) | 8 | `$.txns.{#-1}.get(return: KEY_VALUE)` | Read | `{"1693526400000,txn09": [12000, "Vacation"]}` | ✓ |
| TX11 | Lowest value transaction (rank 0) | 8 | `$.txns.{#0}.get(return: KEY_VALUE)` | Read | `{"1690848000000,txn08": [45, "Snacks"]}` | ✓ |
| TX12 | Top 3 by value (rank -3 to end) | 8 | `$.txns.{#-3:}` | Read | Top 3: txn02 (10000), txn09 (12000), txn12 (9500) — returned as ordered map | ✓ |
| TX13 | Bottom 3 by value (rank 0 to 3) | 8 | `$.txns.{#0:3}` | Read | Bottom 3: txn08 (45), txn05 (75), txn01 (150) | ✓ |
| TX14 | Top 5 by value | 8 | `$.txns.{#-5:}` | Read | 5 entries: txn04(500), txn07(3500), txn06(8000), txn12(9500), txn02(10000), txn09(12000) — wait, that's 6. Rank -5 to end = top 5 | ✓ |

### 15.4 Chained operations (time range + top N)

This is the key scenario: filter by time range, then rank within the
filtered subset to get top N by value.

**Challenge**: The AEL's plural elements are leaf-only, so you cannot
chain `{keyRange}` followed by `{#rankRange}` in a single path. This
requires `with...do` to capture intermediate results — but the AEL
currently cannot apply CDT path operations to variable references.

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| TX15 | Chain: time range → count | 8 | `with ('filtered' = $.txns.{"1685577600000"-"1701388800000"}.get(return: COUNT)) do (${filtered})` | Read | `6` | ✓ |
| TX16 | Chain: time range → top 3 | 8 | *(Requires applying rank range to variable — not expressible in current AEL)* | Read | Top 3 from Jun-Nov subset | ? |
| TX17 | Chain: time range → sum | 8 | *(Requires iteration/aggregation — not expressible in current AEL)* | Read | Sum of amounts in range | ? |

**Note on TX16 / TX17**: These require the AEL to support applying CDT
operations to expression results (not just bin paths). The equivalent
`Exp` API code would be:

```java
// TX16 equivalent using raw Exp API:
Exp.let(
    Exp.def("filtered",
        MapExp.getByKeyRange(MapReturnType.ORDERED_MAP,
            Exp.val("1685577600000"), Exp.val("1701388800000"),
            Exp.mapBin("txns"))),
    MapExp.getByRankRange(MapReturnType.ORDERED_MAP,
        Exp.val(-3), Exp.val(3),
        Exp.var("filtered", Exp.Type.MAP))
)
```

This is a gap in the AEL — variable references cannot be used as CDT
operation targets. Consider this a future enhancement.

### 15.5 Filter based on transaction data

Use the transaction map in `where` filter expressions.

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| TX18 | Has more than 10 transactions | 8 | `$.txns.{}.count() > 10` | Filter | `true` (12 > 10) | ✓ |
| TX19 | Has transactions in Q3 | 8 | `$.txns.{"1688169600000"-"1696118400000"}.get(return: COUNT) > 0` | Filter | `true` (3 > 0) | ✓ |
| TX20 | Highest transaction > 10000 | 8 | `$.txns.{#-1}.[0].get(type: INT) >= 10000` | Filter | `true` (txn09=12000) | ✓ |

---

## 16. Rank-Based Access Patterns (Record 9)

Record 9 `scores` rank order: `english(78) < math(85) < history(88) < science(92) < art(95)`.

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| R01 | Get key at rank 0 | 9 | `$.scores.{#0}.get(return: KEY)` | Read | `"english"` | ✓ |
| R02 | Get value at rank 0 | 9 | `$.scores.{#0}.get(type: INT)` | Read | `78` | ✓ |
| R03 | Get key at rank -1 (highest) | 9 | `$.scores.{#-1}.get(return: KEY)` | Read | `"art"` | ✓ |
| R04 | Get value at rank -1 | 9 | `$.scores.{#-1}.get(type: INT)` | Read | `95` | ✓ |
| R05 | Top 2 by rank | 9 | `$.scores.{#-2:}` | Read | `{"science": 92, "art": 95}` | ✓ |
| R06 | Bottom 2 by rank | 9 | `$.scores.{#0:2}` | Read | `{"english": 78, "math": 85}` | ✓ |
| R07 | All except top 2 | 9 | `$.scores.{!#-2:}` | Read | `{"english": 78, "math": 85, "history": 88}` | ✓ |
| R08 | Rank of specific key | 9 | `$.scores.math.get(return: RANK)` | Read | `1` (second lowest) | ✓ |
| R09 | Get key-value of top entry | 9 | `$.scores.{#-1}.get(return: KEY_VALUE)` | Read | `{"art": 95}` | ✓ |

---

## 17. Prepared Statements / Placeholders

| ID | Description | Record | AEL Expression | Params | Context | Expected | Status |
|----|-------------|--------|----------------|--------|---------|----------|--------|
| P01 | Single placeholder | 1 | `$.intBin > ?0` | `[40]` | Filter | `true` (42 > 40) | ✓ |
| P02 | Two placeholders | 1 | `$.intBin > ?0 and $.strBin == ?1` | `[40, "hello"]` | Filter | `true` | ✓ |
| P03 | Placeholder in arithmetic | 7 | `$.price + ?0` | `[50]` | Read | `150` | ✓ |
| P04 | Placeholder for string match | 7 | `$.status == ?0` | `["active"]` | Filter | `true` | ✓ |

---

## 18. Return Type Variations

These test different `get(return: ...)` values on the same data.

Using Record 2 with expression `$.m.{alpha,beta,gamma}`:

| ID | Description | Return Type | AEL Expression | Expected | Status |
|----|-------------|-------------|----------------|----------|--------|
| RT01 | Default (ORDERED_MAP) | — | `$.m.{alpha,beta,gamma}` | `{"alpha":10, "beta":20, "gamma":30}` | ✓ |
| RT02 | COUNT | COUNT | `$.m.{alpha,beta,gamma}.get(return: COUNT)` | `3` | ✓ |
| RT03 | KEY | KEY | `$.m.{alpha,beta,gamma}.get(return: KEY)` | `["alpha", "beta", "gamma"]` | ✓ |
| RT04 | VALUE | VALUE | `$.m.{alpha,beta,gamma}.get(return: VALUE)` | `[10, 20, 30]` | ✓ |
| RT05 | KEY_VALUE | KEY_VALUE | `$.m.{alpha,beta,gamma}.get(return: KEY_VALUE)` | `{"alpha":10, "beta":20, "gamma":30}` | ✓ |
| RT06 | INDEX | INDEX | `$.m.alpha.get(return: INDEX)` | `0` | ✓ |
| RT07 | RANK | RANK | `$.m.alpha.get(return: RANK)` | `0` (value 10 is lowest) | ✓ |
| RT08 | EXISTS (present) | EXISTS | `$.m.alpha.get(return: EXISTS)` | `true` | ✓ |
| RT09 | EXISTS (absent) | EXISTS | `$.m.zzz.get(return: EXISTS)` | `false` | ✓ |
| RT10 | NONE | NONE | `$.m.alpha.get(return: NONE)` | `null` | ✓ |

---

## 19. Mutation Path Functions

> These are in the grammar but have no visitor implementation (issue 2e).
> All tests are expected to fail.

| ID | Description | Record | AEL Expression | Context | Expected | Status |
|----|-------------|--------|----------------|---------|----------|--------|
| MUT01 | List sort | 2 | `$.l.[].sort()` | Write | `[5, 10, 20, 30, 40, 50, 60]` | ✗ 2e |
| MUT02 | List remove by value | 2 | `$.l.[=50].remove()` | Write | `[10, 40, 20, 30, 60, 5]` | ✗ 2e |
| MUT03 | List clear | 2 | `$.l.[].clear()` | Write | `[]` | ✗ 2e |
| MUT04 | Map remove by key | 2 | `$.m.alpha.remove()` | Write | Map without alpha | ✗ 2e |
| MUT05 | Map clear | 2 | `$.m.{}.clear()` | Write | `{}` | ✗ 2e |

---

## Summary

| Category | Test Count | Expected Pass | Expected Fail | Uncertain |
|----------|-----------|---------------|---------------|-----------|
| 1. Scalar Bin Access | 10 | 10 | 0 | 0 |
| 2. Type Inference & Casting | 10 | 10 | 0 | 0 |
| 2b. Type Derivation | 21 | 11 | 0 | 10 |
| 3. Map Access | 25 | 24 | 1 (M03) | 0 |
| 4. List Access | 20 | 20 | 0 | 0 |
| 5. Nested CDT | 13 | 13 | 0 | 0 |
| 6. Arithmetic | 12 | 12 | 0 | 0 |
| 7. Bitwise | 10 | 8 | 2 (B07, B08) | 0 |
| 8. Comparison | 16 | 15 | 0 | 1 (C14) |
| 9. Logical | 13 | 13 | 0 | 0 |
| 10. Control Structures | 17 | 15 | 1 (CS01s) | 1 (CS17) |
| 11. Metadata | 10 | 10 | 0 | 0 |
| 12. Path Functions | 12 | 10 | 2 (PF11, PF12) | 0 |
| 13. Special Values | 5 | 0 | 0 | 5 |
| 14. Edge Cases | 19 | 11 | 0 | 8 |
| 15. Transaction Scenario | 20 | 15 | 0 | 5 |
| 16. Rank-Based | 9 | 9 | 0 | 0 |
| 17. Prepared Statements | 4 | 4 | 0 | 0 |
| 18. Return Types | 10 | 10 | 0 | 0 |
| 19. Mutations | 5 | 0 | 5 | 0 |
| **Total** | **251** | **210** | **11** | **30** |
