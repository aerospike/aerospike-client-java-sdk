# Operators, comparisons, and precedence (AEL v2)

For a **one-page** summary with examples, see [cheat-sheet.md](cheat-sheet.md). This page is the full operator and precedence reference.

**See also:** [DECISIONS.md](DECISIONS.md), [literals-and-types.md](literals-and-types.md), [type-inference.md](type-inference.md).

---

## 1. Comparison operators

`==`, `!=`, `>`, `>=`, `<`, `<=`.

The left and right operands may be paths, literals, or compound expressions. A literal may appear on either side (`100 < $.age` is equivalent to `$.age > 100`).

---

## 2. Membership `in`

`expression in listExpression` — right-hand side must evaluate to a **list**. Same precedence as other comparisons.

```text
"gold" in $.allowedStatuses
$.name in ["Bob", "Mary"]
```

---

## 3. Regex match `=~`

Left-hand side must be **STRING**; result is **BOOLEAN**. Uses **ICU** regular-expression syntax (Perl-style features such as lookahead, Unicode properties, inline flags, etc., are available per ICU).

```text
expression =~ /regex_pattern/
expression =~ /regex_pattern/flags
```

**Flags** (compose by concatenation, e.g. `/pattern/im`):

| Flag | Meaning |
|------|---------|
| `i` | Case-insensitive (Unicode case folding) |
| `m` | `^` and `$` match line boundaries |
| `s` | Dot matches newlines |
| `x` | Free-spacing: unescaped whitespace ignored; `#` starts comment to end of line |
| `w` | Unicode-aware word boundaries for `\b` |

**Precedence:** same as other comparisons (§8).

---

## 4. Logical operators

| Form | Meaning |
|------|---------|
| `a and b` | Logical AND — **tighter** than `or` |
| `a or b` | Logical OR |
| `not(expr)` | Logical NOT |
| `exclusive(a, b, …)` | True if **exactly one** operand is true |

Boolean bins in a logical context are interpreted as boolean-typed without extra syntax.

---

## 5. Arithmetic

| Operator | Meaning | Notes |
|----------|---------|-------|
| `+`, `-` | Add, subtract | INT or FLOAT; types must match unless converted |
| `*`, `/`, `%` | Multiply, divide, modulus | `%` is integer |
| `**` | Power | **FLOAT** operands; **right-associative** |

Helper functions include `abs`, `ceil`, `floor`, `log`, `max`, `min` with the usual constraints (operand types must be consistent unless noted).

---

## 6. Casts on numeric paths

`asInt()` and `asFloat()` on path results convert between integer and float representations where the grammar allows those suffix forms.

---

## 7. Bitwise operators (integers)

| Operator | Meaning |
|----------|---------|
| `&` | Bitwise AND |
| `\|` | Bitwise OR |
| `^` | Bitwise XOR |
| `~` | Bitwise NOT (unary) |
| `<<`, `>>` | Left / arithmetic right shift |
| `>>>` | Logical right shift (zero-fill from left) |

`>>` preserves sign; `>>>` fills with zeros.

Integer helpers: `countOneBits`, `findBitLeft`, `findBitRight`.

> **Note:** integer bitwise **`&`** is unrelated to any **path** “filter chain” punctuator between selector segments; the latter is a separate lexical context — see [open-issues.md](open-issues.md).

---

## 8. Precedence (lowest → highest)

| Level | Operators | Associativity |
|-------|-----------|---------------|
| 1 | `or` | left |
| 2 | `and` | left |
| 3 | `==`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `=~` | left |
| 4 | `&`, `|`, `^` | left |
| 5 | `<<`, `>>`, `>>>` | left |
| 6 | `+`, `-` | left |
| 7 | `*`, `/`, `%` | left |
| 8 | `**` | right |
| 9 | unary `~`, `not()` | prefix |
| 10 | literals, calls, path expressions, `()` | — |

---

## 9. Path typing in comparisons

Comparing two bins without a literal on either side requires explicit typing on at least one side (a **`:T`** suffix on the path, e.g. **`$.a:INT`**, or **`@:T`** / **`@.field:T`** in a filter). Details: [type-inference.md](type-inference.md).
