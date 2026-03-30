# Bit (BLOB) operations

The Aerospike server supports bitwise operations on BLOB bins through `BitExp`. These
operate on arbitrary regions within byte arrays, specified by bit offset and bit size.

**These are distinct from the integer bitwise operators** (`&`, `|`, `^`, `~`, `<<`, `>>`)
which operate on whole 64-bit integers. The Bit operations target specific bit ranges
within BLOB data and cannot reuse the integer operator syntax because the parameter
signatures are fundamentally different.

Bit functions use the method-style pattern — the BLOB bin is the path receiver, and
parameters are named:

### Bit read operations

```
$.blob.bitGet(offset: 9, size: 5)                   extract bits → BLOB
$.blob.bitCount(offset: 0, size: 32)                count set bits → INT
$.blob.bitLscan(offset: 24, size: 8, value: true)   scan left for first 1-bit → INT
$.blob.bitRscan(offset: 32, size: 8, value: true)   scan right for first 1-bit → INT
$.blob.bitGetInt(offset: 8, size: 16)                extract as unsigned integer → INT
$.blob.bitGetInt(offset: 8, size: 16, signed: true)  extract as signed integer → INT
```

Exp equivalents:
```java
BitExp.get(Exp.val(9), Exp.val(5), Exp.blobBin("blob"))
BitExp.count(Exp.val(0), Exp.val(32), Exp.blobBin("blob"))
BitExp.lscan(Exp.val(24), Exp.val(8), Exp.val(true), Exp.blobBin("blob"))
BitExp.rscan(Exp.val(32), Exp.val(8), Exp.val(true), Exp.blobBin("blob"))
BitExp.getInt(Exp.val(8), Exp.val(16), false, Exp.blobBin("blob"))
BitExp.getInt(Exp.val(8), Exp.val(16), true, Exp.blobBin("blob"))
```

### Bit modify operations

Modify operations return the modified BLOB value.

**Bitwise logic on ranges:**
```
$.blob.bitSet(offset: 13, size: 3, value: 0xe0)     set bits in range
$.blob.bitOr(offset: 17, size: 6, value: 0xa8)      OR on range
$.blob.bitXor(offset: 17, size: 6, value: 0xac)     XOR on range
$.blob.bitAnd(offset: 23, size: 9, value: 0x3c80)   AND on range
$.blob.bitNot(offset: 25, size: 6)                   NOT on range
```

**Shifts on ranges:**
```
$.blob.bitLshift(offset: 32, size: 8, shift: 3)     left shift range
$.blob.bitRshift(offset: 0, size: 9, shift: 1)      right shift range
```

**Arithmetic on ranges:**
```
$.blob.bitAdd(offset: 24, size: 16, value: 128)                 unsigned add
$.blob.bitAdd(offset: 24, size: 16, value: 128, signed: true)   signed add
$.blob.bitSubtract(offset: 24, size: 16, value: 128)            unsigned subtract
$.blob.bitSetInt(offset: 1, size: 8, value: 127)                set integer value in range
```

**Byte-level operations:**
```
$.blob.bitResize(byteSize: 4)                        resize to 4 bytes
$.blob.bitInsert(byteOffset: 1, value: 0xffc7)       insert bytes at offset
$.blob.bitRemove(byteOffset: 2, byteSize: 3)         remove bytes at offset
```

Exp equivalents:
```java
BitExp.set(BitPolicy.Default, Exp.val(13), Exp.val(3), Exp.val(bytes), Exp.blobBin("blob"))
BitExp.or(BitPolicy.Default, Exp.val(17), Exp.val(6), Exp.val(bytes), Exp.blobBin("blob"))
BitExp.not(BitPolicy.Default, Exp.val(25), Exp.val(6), Exp.blobBin("blob"))
BitExp.lshift(BitPolicy.Default, Exp.val(32), Exp.val(8), Exp.val(3), Exp.blobBin("blob"))
BitExp.add(BitPolicy.Default, Exp.val(24), Exp.val(16), Exp.val(128), false, BitOverflowAction.FAIL, Exp.blobBin("blob"))
BitExp.resize(BitPolicy.Default, Exp.val(4), 0, Exp.blobBin("blob"))
BitExp.insert(BitPolicy.Default, Exp.val(1), Exp.val(bytes), Exp.blobBin("blob"))
BitExp.remove(BitPolicy.Default, Exp.val(2), Exp.val(3), Exp.blobBin("blob"))
```

### Usage examples

```
$.permissions.bitCount(offset: 0, size: 8) > 3          more than 3 permission bits set
$.header.bitGetInt(offset: 0, size: 16) == 0xCAFE       check magic number
$.data.bitGet(offset: 0, size: 8) == 0x01               check first byte
$.flags.bitLscan(offset: 0, size: 64, value: true) < 8  first set bit is in high byte
```

### Complete Bit reference

**Read operations:**

| AEL | Named parameters | Return type | Exp equivalent |
|---|---|---|---|
| `$.b.bitGet(offset:, size:)` | INT, INT | BLOB | `BitExp.get(offset, size, bin)` |
| `$.b.bitCount(offset:, size:)` | INT, INT | INT | `BitExp.count(offset, size, bin)` |
| `$.b.bitLscan(offset:, size:, value:)` | INT, INT, BOOL | INT | `BitExp.lscan(offset, size, value, bin)` |
| `$.b.bitRscan(offset:, size:, value:)` | INT, INT, BOOL | INT | `BitExp.rscan(offset, size, value, bin)` |
| `$.b.bitGetInt(offset:, size: [, signed:])` | INT, INT [, BOOL] | INT | `BitExp.getInt(offset, size, signed, bin)` |

**Modify operations:**

| AEL | Named parameters | Return type | Exp equivalent |
|---|---|---|---|
| `$.b.bitResize(byteSize:)` | INT | BLOB | `BitExp.resize(policy, byteSize, 0, bin)` |
| `$.b.bitInsert(byteOffset:, value:)` | INT, BLOB | BLOB | `BitExp.insert(policy, offset, value, bin)` |
| `$.b.bitRemove(byteOffset:, byteSize:)` | INT, INT | BLOB | `BitExp.remove(policy, offset, size, bin)` |
| `$.b.bitSet(offset:, size:, value:)` | INT, INT, BLOB | BLOB | `BitExp.set(policy, offset, size, value, bin)` |
| `$.b.bitOr(offset:, size:, value:)` | INT, INT, BLOB | BLOB | `BitExp.or(policy, offset, size, value, bin)` |
| `$.b.bitXor(offset:, size:, value:)` | INT, INT, BLOB | BLOB | `BitExp.xor(policy, offset, size, value, bin)` |
| `$.b.bitAnd(offset:, size:, value:)` | INT, INT, BLOB | BLOB | `BitExp.and(policy, offset, size, value, bin)` |
| `$.b.bitNot(offset:, size:)` | INT, INT | BLOB | `BitExp.not(policy, offset, size, bin)` |
| `$.b.bitLshift(offset:, size:, shift:)` | INT, INT, INT | BLOB | `BitExp.lshift(policy, offset, size, shift, bin)` |
| `$.b.bitRshift(offset:, size:, shift:)` | INT, INT, INT | BLOB | `BitExp.rshift(policy, offset, size, shift, bin)` |
| `$.b.bitAdd(offset:, size:, value: [, signed:])` | INT, INT, INT [, BOOL] | BLOB | `BitExp.add(policy, offset, size, value, signed, action, bin)` |
| `$.b.bitSubtract(offset:, size:, value: [, signed:])` | INT, INT, INT [, BOOL] | BLOB | `BitExp.subtract(policy, offset, size, value, signed, action, bin)` |
| `$.b.bitSetInt(offset:, size:, value:)` | INT, INT, INT | BLOB | `BitExp.setInt(policy, offset, size, value, bin)` |

### Design notes (Bit)

**Policies are implicit.** `BitPolicy.Default` is used for all modify operations. If
non-default policies are needed in future, an optional modifier can be added.

**`BitOverflowAction`** for `bitAdd`/`bitSubtract` defaults to `FAIL`. If needed, an
`overflow:` modifier could be added: `$.b.bitAdd(offset: 24, size: 16, value: 128, overflow: 'wrap')`.

**`signed:` defaults to `false`** for `bitGetInt`, `bitAdd`, and `bitSubtract`.

### Grammar additions (Bit)

```
pathFunction
    : ... existing ...
    | pathFunctionBitGet
    | pathFunctionBitCount
    | pathFunctionBitLscan
    | pathFunctionBitRscan
    | pathFunctionBitGetInt
    | pathFunctionBitResize
    | pathFunctionBitInsert
    | pathFunctionBitRemove
    | pathFunctionBitSet
    | pathFunctionBitOr
    | pathFunctionBitXor
    | pathFunctionBitAnd
    | pathFunctionBitNot
    | pathFunctionBitLshift
    | pathFunctionBitRshift
    | pathFunctionBitAdd
    | pathFunctionBitSubtract
    | pathFunctionBitSetInt
    ;

pathFunctionBitGet: 'bitGet' '(' 'offset' ':' expression ',' 'size' ':' expression ')';
pathFunctionBitCount: 'bitCount' '(' 'offset' ':' expression ',' 'size' ':' expression ')';
pathFunctionBitLscan: 'bitLscan' '(' 'offset' ':' expression ',' 'size' ':' expression ',' 'value' ':' expression ')';
pathFunctionBitRscan: 'bitRscan' '(' 'offset' ':' expression ',' 'size' ':' expression ',' 'value' ':' expression ')';
pathFunctionBitGetInt: 'bitGetInt' '(' 'offset' ':' expression ',' 'size' ':' expression (',' 'signed' ':' booleanOperand)? ')';
pathFunctionBitResize: 'bitResize' '(' 'byteSize' ':' expression ')';
pathFunctionBitInsert: 'bitInsert' '(' 'byteOffset' ':' expression ',' 'value' ':' expression ')';
pathFunctionBitRemove: 'bitRemove' '(' 'byteOffset' ':' expression ',' 'byteSize' ':' expression ')';
pathFunctionBitSet: 'bitSet' '(' 'offset' ':' expression ',' 'size' ':' expression ',' 'value' ':' expression ')';
pathFunctionBitOr: 'bitOr' '(' 'offset' ':' expression ',' 'size' ':' expression ',' 'value' ':' expression ')';
pathFunctionBitXor: 'bitXor' '(' 'offset' ':' expression ',' 'size' ':' expression ',' 'value' ':' expression ')';
pathFunctionBitAnd: 'bitAnd' '(' 'offset' ':' expression ',' 'size' ':' expression ',' 'value' ':' expression ')';
pathFunctionBitNot: 'bitNot' '(' 'offset' ':' expression ',' 'size' ':' expression ')';
pathFunctionBitLshift: 'bitLshift' '(' 'offset' ':' expression ',' 'size' ':' expression ',' 'shift' ':' expression ')';
pathFunctionBitRshift: 'bitRshift' '(' 'offset' ':' expression ',' 'size' ':' expression ',' 'shift' ':' expression ')';
pathFunctionBitAdd: 'bitAdd' '(' 'offset' ':' expression ',' 'size' ':' expression ',' 'value' ':' expression (',' 'signed' ':' booleanOperand)? ')';
pathFunctionBitSubtract: 'bitSubtract' '(' 'offset' ':' expression ',' 'size' ':' expression ',' 'value' ':' expression (',' 'signed' ':' booleanOperand)? ')';
pathFunctionBitSetInt: 'bitSetInt' '(' 'offset' ':' expression ',' 'size' ':' expression ',' 'value' ':' expression ')';
```

