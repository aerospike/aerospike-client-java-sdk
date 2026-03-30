# Changes required for DSL v2

This document tracks the features, changes, and additions planned for version 2 of
the Aerospike Expression Language. Each section below links to a dedicated page with
the full specification.

---

## 1. [Regex filtering: `=~` operator](dsl/regex-filtering.md)

The `=~` operator applies an ICU regex match, mapping to `Exp.regexCompare()`. Uses
`/pattern/flags` syntax with Perl-like conventions. Flags: `i` (case-insensitive),
`m` (multiline), `s` (dotall), `x` (comments), `w` (Unicode word boundaries).
Precedence is the same as other comparison operators. Result is always BOOLEAN.

## 2. [Advanced type inference](dsl/type-inference.md)

Comprehensive type inference system for bins, variables, and sub-expressions. Covers
inference from literals, operators, functions, `when`/`let` expressions, metadata,
CDT operations, and path parameters. Defines three intermediate states (`UNKNOWN`,
`SAME_TYPE`, `NUMERIC`) that must all resolve to concrete types before parsing
completes. Removes the v1 silent default-type fallback. Includes bidirectional
propagation, deferred validation, explicit type annotations, and error conditions.

## 3. [Path Expression support](dsl/path-expressions.md)

Syntax for navigating and manipulating nested CDT (Collection Data Type) elements
using path expressions. Covers `*[?(...)]` filter syntax, `@`/`@key`/`@index` loop
variables, `.modify()` and `.remove()` operations, `.select()` with `SelectFlags`,
regex filtering within paths, multi-level navigation, and the `*::fieldName` field
selection shorthand. Includes complete data examples, Exp equivalents, and grammar
proposals.

## 4. [Support sub-expressions for all parameters (except Regex)](dsl/sub-expressions.md)

Enables arbitrary expressions `(expr)` wherever literal constants currently appear in
path element parameters (keys, indexes, values, ranks, range bounds). Covers
fixed-parameter and list-parameter positions, disambiguation between singular and list
forms via type inference, type constraints per position, output type of path elements,
and grammar changes.

## 5. [Full syntax of all list / map functions](dsl/list-map-functions.md)

Complete coverage of `MapExp` and `ListExp` API surfaces. Documents read operations
(all covered), remove operations (via `.remove()`), and the new mutation functions
using verb-based write semantics: `setTo`, `insert`, `update`, `add`, `append`,
`appendItems`, `insertItems`, `putItems`, `clear`, `sort`. Includes optional `noFail`
and `order` modifiers, grammar changes, and a complete function reference.

## 6. [Support for HLL and Geo](dsl/hll-and-geo.md)

Geospatial support via `geoJson()` literals and `geoCompare(a, b)` for bidirectional
containment tests. HyperLogLog support via method-style functions on HLL bins:
`hllCount`, `hllDescribe`, `hllMayContain`, `hllUnion`, `hllUnionCount`,
`hllIntersectCount`, `hllSimilarity`, `hllInit`, `hllAdd`. Includes Exp equivalents
and grammar additions.

## 7. [Bit (BLOB) operations](dsl/bit-operations.md)

Bitwise operations on BLOB bins via `BitExp`, distinct from integer bitwise operators.
Read operations: `bitGet`, `bitCount`, `bitLscan`, `bitRscan`, `bitGetInt`. Modify
operations: `bitSet`, `bitOr`, `bitXor`, `bitAnd`, `bitNot`, `bitLshift`, `bitRshift`,
`bitAdd`, `bitSubtract`, `bitSetInt`, `bitResize`, `bitInsert`, `bitRemove`. All use
method-style with named parameters (`offset:`, `size:`, `value:`).

## 8. [Support for mapKeys and mapValues](dsl/map-keys-values.md)

*To be specified.* Syntax for accessing `mapKeys()` and `mapValues()` as collections
within the DSL.

## 9. [String library functions](dsl/string-functions.md)

Comprehensive string operations API (SERVER-97). P1 read/transform: `length`, `substr`,
`uppercase`/`lowercase`/`casefold`/`normalize`, `trim`, `indexOf`, `padStart`/`padEnd`.
P1 modify: `insert`, `overwrite`, `snip`, `replace`/`replaceAll`. P2: `toInt`/`toFloat`,
`regexReplace`, `startsWith`/`endsWith`, `split`, `repeat`. P3: character tests, byte
operations. Cross-type conversions: `toString`, `toBase64`, `fromBase64`. Standalone:
`concat`, `join`.

---

### Design principle: method-style for type-specific functions

The DSL follows a consistent pattern for function style across all sections:

| Category | Style | Why |
|---|---|---|
| Arithmetic (1-2 obvious args) | Standalone function | `abs($.val)`, `max(a, b, c)` — universally understood math notation |
| Varargs with no receiver | Standalone function | `concat(a, b, c)`, `min(a, b)`, `geoCompare(a, b)` |
| CDT operations | Method on path | `$.m.key.setTo(value)` — operates on specific bin/path |
| String operations | Method on path | `$.str.length()` — operates on string bin |
| Bit operations | Method on path | `$.blob.bitGet(offset:, size:)` — operates on blob bin |
| HLL operations | Method on path | `$.hbin.hllCount()` — operates on HLL bin |
| Path modifiers | Named path function | `$.m.key.get(return: VALUE, type: INT)` — optional, unordered params |
