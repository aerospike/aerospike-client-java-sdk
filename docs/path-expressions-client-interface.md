# Path expressions — Java client interface

Aerospike server **8.1.1+** supports **path expressions**: iterate over children of nested CDT (list or map), optionally filter them, then **select**, **modify**, or **remove** matching elements in a single server-side operation.

This page describes how the **Aerospike Java Client SDK** (`com.aerospike.client.sdk`) relates to that feature: nested CDT navigation today, the **CTX** path model, and the operation/expression surface area described in the AEL path documentation.

For syntax, loop variables (`@`, `@key`, `@index`), and worked examples in AEL terms, see [AEL Path Expressions — Syntax Proposal](ael/path-expressions.md) (or the v2 mirror [path-operations](ael-v2/path-operations.md)).

---

## 1. Concepts

| Concept | Role |
|--------|------|
| **Path** | Ordered chain of **context** steps (`CTX[]`) from the bin root into nested list/map containers. |
| **Wildcard / iteration** | A step that visits **all** children at a level, or **all** children matching a boolean **filter expression**. |
| **Loop variables** | Inside filter/modify expressions, APIs such as `Exp.mapLoopVar(...)`, `Exp.stringLoopVar(MAP_KEY)`, etc., refer to the current child (see the AEL path doc). |
| **Select** | Read matched values (and optional flags controlling shape / error behavior). |
| **Modify** | Apply a modify expression to each match; use a remove-style expression to delete matches. |

---

## 2. CTX — building the path

Nested CDT operations are addressed with **`com.aerospike.client.sdk.cdt.CTX`**. A path is a `CTX[]` built from factories such as:

| Factory | Meaning |
|---------|---------|
| `CTX.mapKey(Value)` | Enter a map at a key. |
| `CTX.mapIndex(int)` / `CTX.mapRank(int)` / `CTX.mapValue(Value)` | Map selection by index, rank, or value. |
| `CTX.listIndex(int)` / `CTX.listRank(int)` / `CTX.listValue(Value)` | List selection by index, rank, or value. |
| `CTX.mapKeyCreate(Value, MapOrder)` / `CTX.listIndexCreate(int, ListOrder, boolean)` | Create-on-navigate variants. |

Utility methods **`CTX.toBytes` / `fromBytes` / `toBase64` / `fromBase64`** round-trip context arrays for persistence or debugging.

### Iteration steps (path expressions)

Path expressions require steps that mean “every child” or “every child matching a predicate”. The AEL documentation maps these to:

| Intended Java API | Meaning |
|-------------------|---------|
| `CTX.allChildren()` | Iterate over **all** children at this level. |
| `CTX.allChildrenWithFilter(Expression filter)` | Iterate over children for which `filter` is true. |

Those factories ship on **`com.aerospike.client.sdk.cdt.CTX`** in this SDK. For older checkouts or integration-test gating, see **`client/MIGRATION.md`** and **`SuiteCluster.java`**.

---

## 3. Operation and expression entry points (select / modify)

The AEL path doc standardizes these **Java** entry points (same semantics as the classic Aerospike Java client where those types exist):

| API | Purpose |
|-----|---------|
| `com.aerospike.client.sdk.cdt.CdtOperation.selectByPath(binName, flags, ctx…)` | Packed **`com.aerospike.client.sdk.Operation`** (CDT read): path + **`com.aerospike.client.sdk.cdt.SelectFlags`** (e.g. `SelectFlags.VALUE`). |
| `com.aerospike.client.sdk.cdt.CdtOperation.modifyByPath(binName, flags, modifyExp, ctx…)` | Packed **`Operation`**: modify/remove along a path; flags from **`com.aerospike.client.sdk.cdt.ModifyFlags`**; `modifyExp` is a compiled **`com.aerospike.client.sdk.exp.Expression`** (e.g. `Exp.build(Exp.removeResult())`). |
| `com.aerospike.client.sdk.exp.CdtExp.selectByPath(type, flags, bin, ctx…)` | **`Expression`**: read inside expression pipelines. |
| `com.aerospike.client.sdk.exp.CdtExp.modifyByPath(type, flags, modifyExp, bin, ctx…)` | **`Expression`**: nested modify inside an expression context. |

**Note:** The SDK **`com.aerospike.client.sdk.Operation`** type is what `CdtOperation.selectByPath` / `modifyByPath` return for wire encoding. That is different from the **`CdtGetOrRemoveBuilder.CdtOperation`** **enum** (map/list selector kind for fixed paths).

Use a **non-empty** path when you intend path semantics; see integration tests under `client/src/test/java/com/aerospike/client/sdk/` for server **8.1.1+** coverage.

---

## 4. Fluent Session API vs path expressions

The fluent builder flow is documented in [API Builder Reference](api-builder-reference.md).

- **`Session` → `.bin(name)` → `BinBuilder`** then **`onMapKey` / `onListIndex` / ranges …` → `CdtGetOrRemoveBuilder`** implements **fixed-path** CDT reads, writes, removes, counts, and related return types.
- **Path expressions** add **wildcard iteration and per-child filters**. On **`BinBuilder`** and **`QueryBinBuilder`**, use **`onEachChild()`** / **`onEachChild(Exp filter)`** (wired to **`CTX.allChildren()`** / **`CTX.allChildrenWithFilter`**) before terminals such as **`collectValues()`**, **`modifyBy(…)`**, **`removeMatches()`**, etc. You can also build **`CTX[]`** by hand and pass it to **`CdtOperation.selectByPath`** / **`modifyByPath`** or **`CdtExp`**.

---

## 5. Examples

### 5.1 Fixed nested path with the fluent Session API (no wildcards)

This reads the **value** at `store → stationery → pen` using only fixed map keys. It does **not** use path-expression iteration; it shows the usual SDK chain next to path-style data.

```java
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Session;

// session: connected Session; key: Key for the record
RecordStream rs = session.query(key)
    .bin("store")
        .onMapKey("stationery")
        .onMapKey("pen")
        .getValues()
    .execute();

// Consume the stream and read the projected bin (exact API depends on your RecordStream / row type)
```

Use [API Builder Reference](api-builder-reference.md) to continue the chain (`onListIndex`, ranges, `remove()`, and so on).

---

### 5.2 Round-trip a fixed `CTX` path (serialization)

The factories below are present on **`com.aerospike.client.sdk.cdt.CTX`** today. This is useful for debugging, storing a path, or matching wire context layout—**without** `allChildren` steps.

```java
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.cdt.CTX;

CTX[] path = new CTX[] {
    CTX.mapKey(Value.get("stationery")),
    CTX.mapKey(Value.get("pen"))
};

String base64 = CTX.toBase64(path);
CTX[] restored = CTX.fromBase64(base64);
```

---

### 5.3 Select all prices under `store → book → * → price`

AEL equivalent: `$.store.book.*.price` (see [ael/path-expressions.md §7.1](ael/path-expressions.md)).

Using **`com.aerospike.client.sdk.cdt`** types (packs a **`com.aerospike.client.sdk.Operation`** you can attach through the fluent batch APIs or send with the classic client):

```java
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.Operation;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.CdtOperation;
import com.aerospike.client.sdk.cdt.SelectFlags;

CTX ctx1 = CTX.mapKey(Value.get("book"));
CTX ctx2 = CTX.allChildren();
CTX ctx3 = CTX.mapKey(Value.get("price"));
Operation op = CdtOperation.selectByPath("store", SelectFlags.VALUE, ctx1, ctx2, ctx3);
// attach op to a session batch or call AerospikeClient.operate(...)
```

---

### 5.4 Select titles of cheap books (`price < 10`)

AEL equivalent: `$.store.book.*[?(@.price < 10.0)].*[?(@key == 'title')]` ([§7.2](ael/path-expressions.md)).

The SDK does not expose a **`Session`** entry point that takes a raw packed **`Operation`** from **`CdtOperation.selectByPath`**. For the same path semantics, use **`session.query(key)`** and **`QueryBinBuilder`** — **`onEachChild(Exp)`** records **`CTX.allChildrenWithFilter`**, and **`collectValues()`** emits the **`selectByPath`** read with **`SelectFlags.VALUE`**.

Assume the **`store`** bin matches the nested shape in §5.7 (`book` → list of maps with **`title`** and **`price`**). Server **8.1.1+**.

```java
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.cdt.MapReturnType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.LoopVarPart;
import com.aerospike.client.sdk.exp.MapExp;

// session: connected Session; key: Key for the record
session.query(key)
    .bin("store")
        .onMapKey("book")
        .onEachChild(
            Exp.lt(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                    Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(10.0)))
        .onEachChild(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title")))
        .collectValues()
    .execute();

// Consume the RecordStream / row API for the projected "store" bin (matched title values).
```

Pair with **`session.upsert(key).bin("store").setTo(...)`** as in §5.7 when you want write + read in one batch.

---

### 5.5 Remove every book where `inStock == false`

AEL equivalent: `$.store.book.*[?(@.inStock == false)].remove()` ([§6](ael/path-expressions.md)).

```java
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.Operation;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.CdtOperation;
import com.aerospike.client.sdk.cdt.MapReturnType;
import com.aerospike.client.sdk.cdt.ModifyFlags;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.exp.LoopVarPart;
import com.aerospike.client.sdk.exp.MapExp;

CTX ctx1 = CTX.mapKey(Value.get("book"));
CTX ctx2 = CTX.allChildrenWithFilter(
    Exp.eq(
        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.BOOL,
            Exp.val("inStock"), Exp.mapLoopVar(LoopVarPart.VALUE)),
        Exp.val(false)));
Expression removeExp = Exp.build(Exp.removeResult());
Operation op = CdtOperation.modifyByPath("store", ModifyFlags.DEFAULT, removeExp, ctx1, ctx2);
```

---

### 5.6 Use the result inside another `Expression` (`CdtExp`)

When you need the selected subtree as part of a larger filter or expression read, pass the same path and **`SelectFlags`** to **`CdtExp.selectByPath`**. With **`SelectFlags.VALUE`**, the result is a **list** of matched leaf values — use **`Exp.Type.LIST`** (or **`Exp.Type.MAP`** with **`SelectFlags.MATCHING_TREE`** on map bins); see **`CdtExp`** Javadoc.

```java
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.SelectFlags;
import com.aerospike.client.sdk.exp.CdtExp;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;

Expression titlesExp = Exp.build(CdtExp.selectByPath(
    Exp.Type.LIST,
    SelectFlags.VALUE,
    Exp.mapBin("store"),
    ctx1, ctx2, ctx3));
```

---

### 5.7 End-to-end: `session` write, then filtered path select

Server **8.1.1+**. This writes a nested `store` bin (`book` is a **list** of per-book maps), then in the **same batch** runs a **CDT path select** for titles of books with `price < 10`, using **`QueryBinBuilder.onEachChild(Exp)`** (server-side **`allChildrenWithFilter`**) twice—mirroring AEL `$.store.book.*[?(@.price < 10.0)].*[?(@key == 'title')]` ([§7.2](ael/path-expressions.md)).

```java
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.cdt.MapReturnType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.LoopVarPart;
import com.aerospike.client.sdk.exp.MapExp;

// session: connected Session
Key key = new Key("test", "demo", "path-filter-example");

Map<String, Object> book1 = new HashMap<>();
book1.put("title", "Cheap");
book1.put("price", 5.0);
Map<String, Object> book2 = new HashMap<>();
book2.put("title", "Pricey");
book2.put("price", 25.0);
List<Map<String, Object>> books = new ArrayList<>();
books.add(book1);
books.add(book2);
Map<String, Object> store = new HashMap<>();
store.put("book", books);

session.upsert(key)
    .bin("store").setTo(store)
    .query(key)
    .bin("store")
        .onMapKey("book")
        .onEachChild(
            Exp.lt(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                    Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(10.0)))
        .onEachChild(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title")))
        .collectValues()
    .execute();

// Read the projected "store" bin from the RecordStream (exact row API depends on your version).
```

The same path can be built imperatively with **`CTX`** + **`CdtOperation.selectByPath(..., SelectFlags.VALUE, …)`** as in §5.4 if you need a standalone **`Operation`** instead of the fluent query chain.

---

## 6. Server version and testing

- Path-expression behavior is gated on server **≥ 8.1.1** (see tests that call `Version.isGreaterOrEqual(8, 1, 1, 0)`).
- Cluster tests may be commented out or excluded from the default suite while CDT select/path APIs are still being wired; check **`client/MIGRATION.md`** and **`SuiteCluster.java`** for what runs in CI.

---

## 7. Related documentation

| Document | Content |
|----------|---------|
| [ael/path-expressions.md](ael/path-expressions.md) | AEL ↔ Java mapping, `CTX`, loop vars, examples. |
| [ael-v2/path-operations.md](ael-v2/path-operations.md) | v2 path syntax mirror. |
| [api-builder-reference.md](api-builder-reference.md) | `BinBuilder`, `CdtGetOrRemoveBuilder`, invertable vs non-invertable actions. |
| [client/MIGRATION.md](../client/MIGRATION.md) | Client upgrade notes; cluster-test / feature matrix. |
