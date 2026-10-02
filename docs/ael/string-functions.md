# String library functions

For Java SDK wiring (`BinBuilder`, `StringExp`, `FIND` vs occurrence, `substr` half-open ranges, `NO_FAIL` + nested `CTX`), see [String operations in the Java SDK](../string-operations.md).

The Aerospike server is adding a comprehensive string operations API [(SERVER-97)](https://aerospike.atlassian.net/browse/SERVER-97). String
functions in the AEL use the method-style pattern — the string bin or expression is the
path receiver, and parameters are named where they aid readability. This is consistent
with the CDT, Bit, and HLL function patterns.

All string operations support full Unicode. Character positions and lengths are measured
in Unicode code points, not bytes.

### String read/transform operations (P1)

**Length:**
```
$.name.strlen()                                    number of characters → INT
```

**Substrings:**

Optional `to` may be omitted — the slice runs from `from` through the **end** of the string.

```
$.str.substr(from: 1, to: 3)                      "Aerospike" → "er"
$.str.substr(from: 3, to: -1)                      "Aerospike" → "ospik"
$.str.substr(from: 4, to: 1000)                    "Aerospike" → "spike"
$.str.substr(from: 4)                             "Aerospike" → "spike"   (omit `to`: suffix from index 4)
$.str.substr(from: -3)                            "Aerospike" → "ike"     (omit `to`: last 3 codepoints)
```

`from` is inclusive, `to` is exclusive when present. Both support negative values (from end of string).
If the range is invalid (backwards), returns empty string.

**Case conversion:**
```
$.name.upper()                                     → STRING
$.name.lower()                                     → STRING
$.name.caseFold()                                  Unicode case fold for comparison
$.name.normalizeNFC()                              NFC normalization
```

**Trimming:**
```
$.str.trim()                                       remove Unicode whitespace from both ends
$.str.trimStart()                                  remove leading whitespace
$.str.trimEnd()                                    remove trailing whitespace
```

**Search (`find` / substring find):**

Optional `occurrence` may be omitted — it defaults to **1** (first match).

```
$.str.find(needle: ':')                            first ':' → INT position (default occurrence)
$.str.find(needle: ':', occurrence: 1)             first ':' → INT position
$.str.find(needle: ':', occurrence: 2)             second ':' → INT position
$.str.find(needle: ':', occurrence: -1)            last ':' → INT position
```

Returns -1 if not found. `occurrence` of 0 returns PARAM_ERROR.

The second parameter is the **1-based occurrence** (and negatives count from the last match), **not** a codepoint offset to start searching from. Older implementation drafts sometimes labeled a second `FIND` argument as “start”; the shipped server and AEL use **occurrence** semantics. The Java SDK exposes the same behavior as `find(…, occurrence)` / `StringExp.find`.

**Padding:**
```
$.str.padStart(length: 6, pad: '0')               "1234" → "001234"
$.str.padEnd(length: 6, pad: ' ')                  "abc" → "abc   "
```

`length` is the minimum length of the returned string. `pad` can be multi-character.

### String modify operations (P1)

**Splice and overwrite:**
```
$.str.splice(offset: 1, value: 'loh')             "aa" → "aloha"
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

Exp equivalents (optional arguments omitted where supported):
```java
Exp s = Exp.stringBin("str");
StringExp.concat(Exp.stringBin("first"), Exp.val(" "), Exp.stringBin("last"));
StringExp.strlen(s);
StringExp.substr(Exp.val(4), s);                    // suffix from index 4 (no end)
StringExp.substr(Exp.val(1), Exp.val(3), s);        // half-open [1, 3)
StringExp.find(Exp.val(":"), s);                     // first ":" (default occurrence)
StringExp.split(s);                                 // per-codepoint split (no separator)
```

### String functions (P2)

**Type conversion:**
```
$.str.toInt()                                      parse numeric string → INT
$.str.toFloat()                                    parse numeric string → FLOAT
```

In AEL, `toInt()` / `toFloat()` can parse string text or act as path read terminals
for numeric casts. Pin an unresolved receiver with `:STRING`, for example
`$.amount:STRING.toFloat()`, when the intent is to parse string text. Returns
PARAM_ERROR if the string is not numeric.

**Regex replace:**
```
$.str.regexReplace(pattern: /\d+/, replace: '')       first match
$.str.regexReplace(pattern: /\d+/g, replace: '')      all matches
$.str.regexReplace(pattern: /(\w+),\s*(\w+)/, replace: '$2 $1')
```

Uses ICU regex syntax. Regex replacement is first-match by default; add the
`g` flag to the regex literal for global replacement. Flags compose, for example
`/foo/ig` means global and case-insensitive. The replacement string supports
`$n` capture group references.

**Prefix/suffix tests:**
```
$.str.startsWith('prefix')                         → TRILEAN
$.str.endsWith('.json')                            → TRILEAN
```

**Split and repeat:**

Optional separator for `split` may be omitted — each **codepoint** becomes its own list element.

```
$.str.split()                                      per-codepoint → LIST of one-codepoint strings
$.str.split(',')                                   "a,c,v" → ["a","c","v"] (LIST)
$.str.repeat(3)                                    "abc" → "abcabcabc"
```

### String functions (P3)

**Character tests:**
```
$.str.isUpper()                                    → TRILEAN
$.str.isLower()                                    → TRILEAN
$.str.isNumeric()                                  → TRILEAN
```

**Byte-level and base64:**
```
$.str.bytesLength()                                length in bytes (not chars) → INT
$.str.toBlob()                                     string → BLOB
$.str.b64Decode()                                  Base64 STRING → BLOB
```

### Cross-type string conversions

These operate on non-string types and therefore use method-style on their respective
bin types. `toString()` on a `STRING` receiver is an identity conversion.

```
$.intBin.toString()                                INT → STRING
$.floatBin.toString()                              FLOAT → STRING
$.blobBin.toString()                               BLOB → STRING (Unicode)
$.boolBin.toString()                               BOOL → STRING
$.str.toString()                                   STRING → STRING
```

### String concatenation

Use the `+` operator to concatenate strings:

```
$.first + ' ' + $.last                             concatenate strings → STRING
$.a + $.b + $.c                                    concatenate multiple strings
```

`join` operates on a LIST and is a list path function:

```
$.listBin.join('-')                                ["a","b","c"] → "a-b-c" → STRING
```

### Usage examples

```
$.name.strlen() > 0 and $.name.strlen() <= 50          length bounds check
$.name.lower() == 'alice'                              case-insensitive comparison
$.email.find(needle: '@') > 0                          has @ sign (occurrence omitted → first)
$.key.padStart(length: 10, pad: '0')                   normalize key width
$.desc.replaceAll(find: '  ', replace: ' ')            collapse double spaces
$.csv.split(',').count() > 3                           at least 4 fields
$.tail.substr(from: 5)                                suffix from index 5 (`to` omitted)
$.amount.toString().padStart(length: 8, pad: '0')      format number as padded string
$.name.startsWith('Dr.') or $.name.startsWith('Prof.') title check
```

### Complete string reference

**P1 — read/transform:**

| AEL | Parameters | Return type |
|---|---|---|
| `$.s.strlen()` | — | INT |
| `$.s.substr(from: [, to:])` | INT [, INT] | STRING |
| `$.s.charAt(index:)` | INT | STRING |
| `$.s.upper()` | — | STRING |
| `$.s.lower()` | — | STRING |
| `$.s.normalizeNFC()` | — | STRING |
| `$.s.caseFold()` | — | STRING |
| `$.s.trim()` | — | STRING |
| `$.s.trimStart()` | — | STRING |
| `$.s.trimEnd()` | — | STRING |
| `$.s.find(needle: [, occurrence:])` | STRING [, INT] | INT |
| `$.s.contains(needle:)` | STRING | TRILEAN |
| `$.s.padStart(length:, pad:)` | INT, STRING | STRING |
| `$.s.padEnd(length:, pad:)` | INT, STRING | STRING |

**P1 — modify:**

| AEL | Parameters | Return type |
|---|---|---|
| `$.s.splice(offset:, value:)` | INT, STRING | STRING |
| `$.s.overwrite(offset:, value:)` | INT, STRING | STRING |
| `$.s.snip(from: [, to:])` | INT [, INT] | STRING |
| `$.s.replace(find:, replace:)` | STRING, STRING | STRING |
| `$.s.replaceAll(find:, replace:)` | STRING, STRING | STRING |

**P2:**

| AEL | Parameters | Return type |
|---|---|---|
| `$.s.toInt()` | — | INT |
| `$.s.toFloat()` | — | FLOAT |
| `$.s.regexReplace(pattern:, replace:)` | REGEX, STRING | STRING |
| `$.s.startsWith(prefix)` | STRING | TRILEAN |
| `$.s.endsWith(suffix)` | STRING | TRILEAN |
| `$.s.split([separator])` | [STRING] | LIST |
| `$.s.repeat(count)` | INT | STRING |

**P3:**

| AEL | Parameters | Return type |
|---|---|---|
| `$.s.isUpper()` | — | TRILEAN |
| `$.s.isLower()` | — | TRILEAN |
| `$.s.isNumeric()` | — | TRILEAN |
| `$.s.bytesLength()` | — | INT |
| `$.s.toBlob()` | — | BLOB |
| `$.s.b64Decode()` | — | BLOB |

**Cross-type:**

| AEL | Operates on | Return type |
|---|---|---|
| `$.i.toString()` | INT | STRING |
| `$.f.toString()` | FLOAT | STRING |
| `$.v.toString()` | BOOL | STRING |
| `$.s.toString()` | STRING | STRING |
| `$.b.toString()` | BLOB | STRING |
| `$.l.join(separator)` | LIST | STRING |

### Design principle: method-style for type-specific functions

The AEL follows a consistent pattern for function style:

| Category | Style | Why |
|---|---|---|
| Arithmetic (1-2 obvious args) | Standalone function | `abs($.val)`, `max(a, b, c)` — universally understood math notation |
| Varargs with no receiver | Standalone function | `min(a, b)`, `max(a, b)`, `geoCompare(a, b)` |
| CDT operations | Method on path | `$.m.key.setTo(value)` — operates on specific bin/path |
| String operations | Method on path | `$.str.strlen()` — operates on string bin |
| Bit operations | Method on path | `$.blob.bitGet(offset:, size:)` — operates on blob bin |
| HLL operations | Method on path | `$.hbin.hllCount()` — operates on HLL bin |
| Path modifiers | Named path function | `$.m.key.get(return: VALUE, type: INT)` — optional, unordered params |

