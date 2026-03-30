# String library functions

The Aerospike server is adding a comprehensive string operations API (SERVER-97). String
functions in the DSL use the method-style pattern — the string bin or expression is the
path receiver, and parameters are named where they aid readability. This is consistent
with the CDT, Bit, and HLL function patterns.

All string operations support full Unicode. Character positions and lengths are measured
in Unicode code points, not bytes.

### String read/transform operations (P1)

**Length:**
```
$.name.length()                                    number of characters → INT
```

**Substrings:**
```
$.str.substr(from: 1, to: 3)                      "Aerospike" → "er"
$.str.substr(from: 3, to: -1)                      "Aerospike" → "ospik"
$.str.substr(from: 4, to: 1000)                    "Aerospike" → "spike"
```

`from` is inclusive, `to` is exclusive. Both support negative values (from end of string).
If the range is invalid (backwards), returns empty string.

**Case conversion:**
```
$.name.uppercase()                                 → STRING
$.name.lowercase()                                 → STRING
$.name.casefold()                                  Unicode case fold for comparison
$.name.normalize()                                 NFC normalization
```

**Trimming:**
```
$.str.trim()                                       remove Unicode whitespace from both ends
$.str.trimStart()                                  remove leading whitespace
$.str.trimEnd()                                    remove trailing whitespace
```

**Search:**
```
$.str.indexOf(needle: ':', occurrence: 1)          first ':' → INT position
$.str.indexOf(needle: ':', occurrence: 2)          second ':' → INT position
$.str.indexOf(needle: ':', occurrence: -1)         last ':' → INT position
```

Returns -1 if not found. `occurrence` of 0 returns PARAM_ERROR.

**Padding:**
```
$.str.padStart(length: 6, pad: '0')               "1234" → "001234"
$.str.padEnd(length: 6, pad: ' ')                  "abc" → "abc   "
```

`length` is the minimum length of the returned string. `pad` can be multi-character.

### String modify operations (P1)

**Insert and overwrite:**
```
$.str.insert(offset: 1, value: 'loh')             "aa" → "aloha"
$.str.overwrite(offset: 4, value: 'bleh')          "the best song" → "the bleh song"
```

**Snip (remove substring):**
```
$.str.snip(from: 6, to: 9)                        "faith no more" → "faith more"
$.str.snip(from: 5)                                "hello world" → "hello" (truncate)
```

`from` and `to` support negative values. If `from >= to`, returns original string unchanged.

**Replace:**
```
$.str.replace(find: ':', replace: '-')             first occurrence
$.str.replaceAll(find: ':', replace: '-')          all occurrences
```

Exp equivalents (example for concat, length, substr):
```java
StringExp.concat(Exp.stringBin("first"), Exp.val(" "), Exp.stringBin("last"))
StringExp.length(Exp.stringBin("name"))
StringExp.substr(Exp.stringBin("str"), Exp.val(1), Exp.val(3))
```

### String functions (P2)

**Type conversion:**
```
$.str.toInt()                                      parse numeric string → INT
$.str.toFloat()                                    parse numeric string → FLOAT
```

These are distinct from `asInt()` / `asFloat()` which perform numeric type casts
(float→int, int→float). The `toInt()` / `toFloat()` functions parse a string
representation of a number. Returns PARAM_ERROR if the string is not numeric.

**Regex replace:**
```
$.str.regexReplace(pattern: /\d+/, replace: '')
$.str.regexReplace(pattern: /(\w+),\s*(\w+)/, replace: '$2 $1')
```

Uses ICU regex syntax. The replacement string supports `$n` capture group references.

**Prefix/suffix tests:**
```
$.str.startsWith('prefix')                         → BOOLEAN
$.str.endsWith('.json')                            → BOOLEAN
```

**Split and repeat:**
```
$.str.split(',')                                   "a,c,v" → ["a","c","v"] (LIST)
$.str.repeat(3)                                    "abc" → "abcabcabc"
```

### String functions (P3)

**Character tests:**
```
$.str.isUpper()                                    → BOOLEAN
$.str.isLower()                                    → BOOLEAN
$.str.isNumeric()                                  → BOOLEAN
```

**Byte-level:**
```
$.str.bytesLength()                                length in bytes (not chars) → INT
$.str.toBlob()                                     string → BLOB
```

### Cross-type string conversions

These operate on non-string types and therefore use method-style on their respective
bin types:

```
$.intBin.toString()                                INT → STRING
$.floatBin.toString()                              FLOAT → STRING
$.blobBin.toString()                               BLOB → STRING (Unicode)
$.blobBin.toBase64()                               BLOB → STRING (Base64)
$.str.fromBase64()                                 Base64 STRING → BLOB
```

### Standalone string functions

Functions that take multiple string arguments with no natural receiver are standalone:

```
concat($.first, ' ', $.last)                       concatenate strings → STRING
concat($.a, $.b, $.c)                              varargs
```

`join` operates on a LIST and is a list path function:

```
$.listBin.join('-')                                ["a","b","c"] → "a-b-c" → STRING
```

### Usage examples

```
$.name.length() > 0 and $.name.length() <= 50         length bounds check
$.name.lowercase() == 'alice'                          case-insensitive comparison
$.email.indexOf(needle: '@', occurrence: 1) > 0        has @ sign
$.key.padStart(length: 10, pad: '0')                   normalize key width
$.desc.replaceAll(find: '  ', replace: ' ')            collapse double spaces
$.csv.split(',').count() > 3                           at least 4 fields
$.amount.toString().padStart(length: 8, pad: '0')      format number as padded string
$.name.startsWith('Dr.') or $.name.startsWith('Prof.') title check
```

### Complete string reference

**P1 — read/transform:**

| DSL | Parameters | Return type |
|---|---|---|
| `$.s.length()` | — | INT |
| `$.s.substr(from:, to:)` | INT, INT | STRING |
| `$.s.uppercase()` | — | STRING |
| `$.s.lowercase()` | — | STRING |
| `$.s.normalize()` | — | STRING |
| `$.s.casefold()` | — | STRING |
| `$.s.trim()` | — | STRING |
| `$.s.trimStart()` | — | STRING |
| `$.s.trimEnd()` | — | STRING |
| `$.s.indexOf(needle:, occurrence:)` | STRING, INT | INT |
| `$.s.padStart(length:, pad:)` | INT, STRING | STRING |
| `$.s.padEnd(length:, pad:)` | INT, STRING | STRING |

**P1 — modify:**

| DSL | Parameters | Return type |
|---|---|---|
| `$.s.insert(offset:, value:)` | INT, STRING | STRING |
| `$.s.overwrite(offset:, value:)` | INT, STRING | STRING |
| `$.s.snip(from: [, to:])` | INT [, INT] | STRING |
| `$.s.replace(find:, replace:)` | STRING, STRING | STRING |
| `$.s.replaceAll(find:, replace:)` | STRING, STRING | STRING |

**P2:**

| DSL | Parameters | Return type |
|---|---|---|
| `$.s.toInt()` | — | INT |
| `$.s.toFloat()` | — | FLOAT |
| `$.s.regexReplace(pattern:, replace:)` | REGEX, STRING | STRING |
| `$.s.startsWith(prefix)` | STRING | BOOLEAN |
| `$.s.endsWith(suffix)` | STRING | BOOLEAN |
| `$.s.split(separator)` | STRING | LIST |
| `$.s.repeat(count)` | INT | STRING |

**P3:**

| DSL | Parameters | Return type |
|---|---|---|
| `$.s.isUpper()` | — | BOOLEAN |
| `$.s.isLower()` | — | BOOLEAN |
| `$.s.isNumeric()` | — | BOOLEAN |
| `$.s.bytesLength()` | — | INT |
| `$.s.toBlob()` | — | BLOB |

**Cross-type:**

| DSL | Operates on | Return type |
|---|---|---|
| `$.i.toString()` | INT | STRING |
| `$.f.toString()` | FLOAT | STRING |
| `$.b.toString()` | BLOB | STRING |
| `$.b.toBase64()` | BLOB | STRING |
| `$.s.fromBase64()` | STRING | BLOB |
| `concat(s1, s2, ...)` | STRING (varargs) | STRING |
| `$.l.join(separator)` | LIST | STRING |

### Design principle: method-style for type-specific functions

The DSL follows a consistent pattern for function style:

| Category | Style | Why |
|---|---|---|
| Arithmetic (1-2 obvious args) | Standalone function | `abs($.val)`, `max(a, b, c)` — universally understood math notation |
| Varargs with no receiver | Standalone function | `concat(a, b, c)`, `min(a, b)`, `geoCompare(a, b)` |
| CDT operations | Method on path | `$.m.key.setTo(value)` — operates on specific bin/path |
| String operations | Method on path | `$.str.length()` — operates on string bin |
| Bit operations | Method on path | `$.blob.bitGet(offset:, size:)` — operates on blob bin |
| HLL operations | Method on path | `$.hbin.hllCount()` — operates on HLL bin |
| Path modifiers | Named path function | `$.m.key.get(return: VALUE, type: INT)` — optional, unordered params |

