# Support for HLL and Geo

### Geospatial operations

The Aerospike server supports GeoJSON data for spatial queries. The Java client provides
`Exp.geoCompare(left, right)` which returns true if the left GeoJSON expression is either
contained within or contains the right GeoJSON expression. This is a bidirectional
containment test — not a directional "A contains B" operation.

#### GeoJSON literal

A GeoJSON value is represented in the AEL using the `geoJson()` function wrapping a
JSON string:

```
geoJson('{"type":"Point","coordinates":[-122.0,37.5]}')
geoJson('{"type":"Polygon","coordinates":[[[-122.5,37.0],[-121.0,37.0],[-121.0,38.0],[-122.5,38.0],[-122.5,37.0]]]}')
geoJson('{"type":"AeroCircle","coordinates":[[-122.0,37.5],50000.0]}')
```

Exp equivalent:
```java
Exp.geo("{\"type\":\"Point\",\"coordinates\":[-122.0,37.5]}")
```

#### Spatial comparison: `geoCompare`

`geoCompare(a, b)` returns true if either argument spatially contains the other. Both
arguments must evaluate to GEO type.

```
geoCompare($.region, geoJson('{"type":"Point","coordinates":[-122.0,37.5]}'))
geoCompare($.point, $.region)
geoCompare(geoJson('{"type":"AeroCircle","coordinates":[[-122.0,37.5],50000.0]}'), $.loc)
```

Exp equivalent:
```java
Exp.geoCompare(Exp.geoBin("region"), Exp.geo("{\"type\":\"Point\",\"coordinates\":[-122.0,37.5]}"))
```

> **Naming note:** The name `geoCompare` matches the Aerospike Java client API directly.
> The operation is bidirectional — unlike PostGIS `ST_Contains` or MongoDB `$geoWithin`
> which are directional. `geoCompare` was chosen over alternatives like `geoContains`
> (which implies directionality) or `geoIntersects` (which implies arbitrary polygon overlap).

#### Complete geo reference

| AEL | Exp equivalent | Return type |
|---|---|---|
| `geoJson('...')` | `Exp.geo("...")` | GEO |
| `geoCompare(a, b)` | `Exp.geoCompare(a, b)` | BOOLEAN |
| `$.geoBin` | `Exp.geoBin("geoBin")` | GEO |
| `$.geoBin.get(type: GEO)` | (already works) | GEO |

#### Grammar additions (geo)

```
operand
    : ... existing ...
    | geoJsonLiteral
    | geoCompareFunction
    ;

geoJsonLiteral: 'geoJson' '(' QUOTED_STRING ')';

geoCompareFunction: 'geoCompare' '(' expression ',' expression ')';
```

### HyperLogLog (HLL) operations

HLL is a probabilistic data structure for estimating set cardinality. The Aerospike
server provides HLL bin operations through `HLLExp`. HLL functions in the AEL use the
method-style pattern — the HLL bin is the path receiver, and parameters are named where
they aid readability.

#### HLL read operations

```
$.hbin.hllCount()                                  estimated cardinality → INT
$.hbin.hllDescribe()                               [indexBitCount, minHashBitCount] → LIST
$.hbin.hllMayContain(['val1', 'val2'])             probabilistic membership → INT (1 or 0)
$.hbin.hllUnion($.otherHll)                        union of two HLLs → HLL
$.hbin.hllUnionCount($.otherHll)                   estimated count of union → INT
$.hbin.hllIntersectCount($.otherHll)               estimated count of intersection → INT
$.hbin.hllSimilarity($.otherHll)                   Jaccard similarity → FLOAT (0.0–1.0)
```

The second argument to `hllUnion`, `hllUnionCount`, `hllIntersectCount`, and
`hllSimilarity` is a LIST of HLL values. When comparing against a single HLL bin, the
AEL wraps it into a list internally. To compare against multiple HLLs, pass a list
expression:

```
$.hbin.hllUnionCount([$.hll1, $.hll2])             union count across 3 HLLs
$.hbin.hllSimilarity([$.hll1, $.hll2]) >= 0.75     similarity threshold
$.hbin.hllMayContain($.checkList)                  list from a bin
```

Exp equivalents:
```java
HLLExp.getCount(Exp.hllBin("hbin"))
HLLExp.describe(Exp.hllBin("hbin"))
HLLExp.mayContain(Exp.val(list), Exp.hllBin("hbin"))
HLLExp.getUnion(Exp.hllBin("otherHll"), Exp.hllBin("hbin"))
HLLExp.getUnionCount(Exp.hllBin("otherHll"), Exp.hllBin("hbin"))
HLLExp.getIntersectCount(Exp.hllBin("otherHll"), Exp.hllBin("hbin"))
HLLExp.getSimilarity(Exp.hllBin("otherHll"), Exp.hllBin("hbin"))
```

#### HLL modify operations

Modify operations return the modified HLL value.

```
$.hbin.hllInit(indexBits: 10)                      create/reset HLL
$.hbin.hllInit(indexBits: 10, minHashBits: 20)     with minhash
$.hbin.hllAdd(['val1', 'val2'])                    add values to HLL
$.hbin.hllAdd(['val1', 'val2'], indexBits: 10)     add values, create if needed
$.hbin.hllAdd(['val1', 'val2'], indexBits: 10, minHashBits: 20)
```

Exp equivalents:
```java
HLLExp.init(HLLPolicy.Default, Exp.val(10), Exp.hllBin("hbin"))
HLLExp.init(HLLPolicy.Default, Exp.val(10), Exp.val(20), Exp.hllBin("hbin"))
HLLExp.add(HLLPolicy.Default, Exp.val(list), Exp.hllBin("hbin"))
HLLExp.add(HLLPolicy.Default, Exp.val(list), Exp.val(10), Exp.hllBin("hbin"))
HLLExp.add(HLLPolicy.Default, Exp.val(list), Exp.val(10), Exp.val(20), Exp.hllBin("hbin"))
```

#### Usage examples

```
$.visitors.hllCount() > 1000000                    high-cardinality filter
$.visitors.hllMayContain(['user123']) == 1          probabilistic membership check
$.setA.hllSimilarity($.setB) >= 0.8                high similarity between sets
$.setA.hllIntersectCount($.setB) > 500             significant overlap
```

#### Complete HLL reference

| AEL | Parameters | Return type | Exp equivalent |
|---|---|---|---|
| `$.h.hllCount()` | — | INT | `HLLExp.getCount(bin)` |
| `$.h.hllDescribe()` | — | LIST | `HLLExp.describe(bin)` |
| `$.h.hllMayContain(list)` | LIST | INT (1/0) | `HLLExp.mayContain(list, bin)` |
| `$.h.hllUnion(list)` | LIST of HLLs | HLL | `HLLExp.getUnion(list, bin)` |
| `$.h.hllUnionCount(list)` | LIST of HLLs | INT | `HLLExp.getUnionCount(list, bin)` |
| `$.h.hllIntersectCount(list)` | LIST of HLLs | INT | `HLLExp.getIntersectCount(list, bin)` |
| `$.h.hllSimilarity(list)` | LIST of HLLs | FLOAT | `HLLExp.getSimilarity(list, bin)` |
| `$.h.hllInit(indexBits: [, minHashBits:])` | INT [, INT] | HLL | `HLLExp.init(policy, idxBits, [mhBits,] bin)` |
| `$.h.hllAdd(list [, indexBits: [, minHashBits:]])` | LIST [, INT [, INT]] | HLL | `HLLExp.add(policy, list, [idxBits, [mhBits,]] bin)` |

#### Grammar additions (HLL)

```
pathFunction
    : ... existing ...
    | pathFunctionHllCount
    | pathFunctionHllDescribe
    | pathFunctionHllMayContain
    | pathFunctionHllUnion
    | pathFunctionHllUnionCount
    | pathFunctionHllIntersectCount
    | pathFunctionHllSimilarity
    | pathFunctionHllInit
    | pathFunctionHllAdd
    ;

pathFunctionHllCount: 'hllCount' '()';
pathFunctionHllDescribe: 'hllDescribe' '()';
pathFunctionHllMayContain: 'hllMayContain' '(' expression ')';
pathFunctionHllUnion: 'hllUnion' '(' expression ')';
pathFunctionHllUnionCount: 'hllUnionCount' '(' expression ')';
pathFunctionHllIntersectCount: 'hllIntersectCount' '(' expression ')';
pathFunctionHllSimilarity: 'hllSimilarity' '(' expression ')';
pathFunctionHllInit: 'hllInit' '(' hllInitParams ')';
pathFunctionHllAdd: 'hllAdd' '(' expression hllAddParams? ')';

hllInitParams: 'indexBits' ':' expression (',' 'minHashBits' ':' expression)?;
hllAddParams: ',' 'indexBits' ':' expression (',' 'minHashBits' ':' expression)?;
```

