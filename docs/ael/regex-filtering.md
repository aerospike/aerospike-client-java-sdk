# Regex filtering: `=~` operator

The `=~` operator applies an ICU regex match. It maps to `Exp.regexCompare()`.
The server uses the [ICU Regular Expressions](https://unicode-org.github.io/icu/userguide/strings/regexp.html)
engine, which provides Perl-compatible regex syntax with full Unicode support.

The pattern uses `/pattern/flags` syntax, following the Perl/JavaScript/Ruby convention
that is the most widely recognised regex notation across programming languages.

**Syntax:**
```
expression =~ /regex_pattern/
expression =~ /regex_pattern/flags
```

The left hand expression must evaluate to a String type. The result is always BOOLEAN.

**Precedence:** `=~` has the same precedence as the other comparison operators (`==`,
`!=`, `>`, `<`, `in`) — it binds tighter than `and`/`or` but looser than arithmetic:

```
$.name =~ /^Alice/i and $.age > 21       →  ($.name =~ /^Alice/i) and ($.age > 21)
$.a + $.b =~ /^\d+$/                     →  PARSE ERROR: =~ requires STRING on LHS, not NUMERIC
```

**Flag letters:**

| Flag | ICU constant | Meaning |
|------|---|---|
| `i` | `UREGEX_CASE_INSENSITIVE` | Case-insensitive matching (full Unicode case folding) |
| `m` | `UREGEX_MULTILINE` | `^` and `$` match at line boundaries, not just start/end of string |
| `s` | `UREGEX_DOTALL` | `.` matches line terminators (by default `.` does not match `\n`) |
| `x` | `UREGEX_COMMENTS` | Free-format mode: unescaped whitespace is ignored, `#` starts a comment to end-of-line |
| `w` | `UREGEX_UWORD` | Unicode-aware word boundaries for `\b` (uses UAX #29 instead of simple `\w`/`\W` classification) |

Flags compose by concatenation: `/pattern/im` means case-insensitive + multiline.
No flags means defaults (case-sensitive, single-line `^`/`$`, `.` does not match `\n`).

> **Note — change from POSIX to ICU:** Earlier versions of Aerospike used POSIX regex
> with flags `EXTENDED`, `ICASE`, `NOSUB`, and `NEWLINE`. The ICU engine replaces these:
> - POSIX `EXTENDED` is gone — ICU always uses Perl-like syntax, which is a superset.
> - POSIX `NOSUB` is gone — it suppressed sub-match reporting, which was never relevant
>   for `Exp.regexCompare()` (boolean result only).
> - POSIX `NEWLINE` combined two behaviours that ICU separates: use `m` for multiline
>   `^`/`$` matching, and `s` for making `.` match newlines.
>
> ICU patterns support features not available in POSIX, including lookahead/lookbehind
> (`(?=...)`, `(?!...)`, `(?<=...)`, `(?<!...)`), non-capturing groups (`(?:...)`),
> named capture groups (`(?<name>...)`), Unicode property escapes (`\p{Letter}`,
> `\p{Script=Cyrillic}`), possessive quantifiers (`*+`, `++`, `?+`), and inline flag
> toggling (`(?i)`, `(?m:...)` etc.) within patterns.

**Examples:**
```
$.name =~ /^Alice/i                                name starts with Alice, case insensitive
$.store.book.*[?(@.title =~ /Lord.*/)]                title matches "Lord..."
$.store.book.*[?(@.author =~ /j\.r\.r\./i)]          case-insensitive match
$.store.stationery.*[?(@key =~ /pen.*/)]              keys starting with "pen"
$.store.book.*[?(@.title =~ /^the/im)]                case-insensitive, multiline
$.desc =~ /hello\s+world/x                            free-format: whitespace in pattern ignored
$.name =~ /\p{Script=Greek}/                           match Greek characters (ICU Unicode property)
$.text =~ /foo(?=bar)/                                 lookahead: "foo" only if followed by "bar"
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

Note: `=~` is a general AEL operator, not limited to path expressions. It works
anywhere you have a string expression on the left:
