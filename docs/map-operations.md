# Map CDT Operations

This document covers all map Collection Data Type (CDT) operations available in the Aerospike Java SDK.

## Table of Contents

- [Overview](#overview)
- [Map Structure Operations](#map-structure-operations)
  - [mapCreate](#mapcreate)
  - [mapSetPolicy](#mapsetpolicy)
  - [mapClear](#mapclear)
  - [mapSize](#mapsize)
- [Bulk Write Operations](#bulk-write-operations)
  - [mapUpsertItems](#mapupsertitems)
  - [mapInsertItems](#mapinsertitems)
  - [mapUpdateItems](#mapupdateitems)
- [Navigating Into a Map](#navigating-into-a-map)
  - [By Key](#by-key)
  - [By Index](#by-index)
  - [By Rank](#by-rank)
  - [By Value](#by-value)
  - [By Key Range](#by-key-range)
  - [By Value Range](#by-value-range)
  - [By Index Range](#by-index-range)
  - [By Rank Range](#by-rank-range)
  - [By Key List](#by-key-list)
  - [By Value List](#by-value-list)
  - [By Key Relative Index Range](#by-key-relative-index-range)
  - [By Value Relative Rank Range](#by-value-relative-rank-range)
- [Single-Entry Write Operations](#single-entry-write-operations)
  - [setTo](#setto)
  - [insert](#insert)
  - [update](#update)
  - [upsert](#upsert)
  - [add (increment)](#add-increment)
  - [MapEntryWriteOptions](#mapentrywriteoptions)
- [Read Operations](#read-operations)
  - [getValues](#getvalues)
  - [getKeys](#getkeys)
  - [getKeysAndValues](#getkeysandvalues)
  - [getIndexes / getReverseIndexes](#getindexes--getreverseindexes)
  - [getRanks / getReverseRanks](#getranks--getreverseranks)
  - [count](#count)
  - [exists](#exists)
  - [getAsMap / getAsOrderedMap](#getasmap--getasorderedmap)
- [Remove Operations](#remove-operations)
  - [remove](#remove)
  - [removeAllOthers](#removeallothers)
- [Inverted Operations](#inverted-operations)
- [Nested Map Operations](#nested-map-operations)
- [MapOrder Reference](#maporder-reference)
- [MapBulkWriteOptions Reference](#mapbulkwriteoptions-reference)

---

## Overview

Map operations follow a builder pattern. You start from a `bin(...)` call, then either perform a structural operation directly (like `mapCreate`, `mapSize`) or navigate into the map with an `onMap*` selector, then apply a terminal action (read, write, or remove).

**Default map ordering:** When a write operation creates a new map (because it doesn't already exist), the map defaults to `MapOrder.KEY_ORDERED`. This can be overridden per-operation via `MapEntryWriteOptions` or `MapBulkWriteOptions`, or by passing a `MapOrder` to `onMapKey`.

```java
// Structural: create a map, get its size
session.update(key)
    .bin("myMap").mapCreate(MapOrder.KEY_ORDERED)
    .bin("myMap").mapSize()
    .execute();

// Navigate + action: read a value by key
session.select(key)
    .bin("myMap").onMapKey("name").getValues()
    .execute();

// Navigate + write: insert a new entry
session.update(key)
    .bin("myMap").onMapKey("name").insert("Alice")
    .execute();
```

---

## Map Structure Operations

These operate on the map bin itself without navigating to a specific element.

### mapCreate

Create a map with a specified ordering. If no CDT context is present, this creates (or sets the order of) the top-level bin map. With context, it creates a nested map at that path.

```java
// Create a key-ordered map
session.update(key)
    .bin("myMap").mapCreate(MapOrder.KEY_ORDERED)
    .execute();

// Create with a persisted index for faster lookups (top-level ordered maps only)
session.update(key)
    .bin("myMap").mapCreate(MapOrder.KEY_ORDERED, true)
    .execute();
```

### mapSetPolicy

Set the ordering policy for an existing map. If the map does not exist at the top level, it will be created. However, `mapSetPolicy` **cannot create nested maps** -- use `mapCreate` for that.

```java
// Change map ordering to key-value ordered
session.update(key)
    .bin("myMap").mapSetPolicy(MapOrder.KEY_VALUE_ORDERED)
    .execute();

// Set policy with a persisted index
session.update(key)
    .bin("myMap").mapSetPolicy(MapOrder.KEY_ORDERED, true)
    .execute();
```

### mapClear

Remove all entries from a map without deleting the bin.

```java
session.update(key)
    .bin("myMap").mapClear()
    .execute();
```

### mapSize

Return the number of entries in the map.

```java
long size = session.select(key)
    .bin("myMap").mapSize()
    .execute()
    .asLong("myMap");
```

---

## Bulk Write Operations

Insert, update, or upsert multiple map entries in a single server operation. All bulk methods accept an optional `Consumer<MapBulkWriteOptions>` for controlling map ordering, persist-index, failure handling, and partial writes.

### mapUpsertItems

Create or update multiple map entries unconditionally.

```java
Map<String, Object> items = Map.of("a", 1, "b", 2, "c", 3);

// Simple upsert
session.update(key)
    .bin("myMap").mapUpsertItems(items)
    .execute();

// With options
session.update(key)
    .bin("myMap").mapUpsertItems(items, opt -> opt
        .mapOrder(MapOrder.KEY_VALUE_ORDERED)
        .persistIndex())
    .execute();
```

### mapInsertItems

Insert multiple entries. Fails if any key already exists, unless `allowFailures()` is set. Use `allowPartial()` to commit successful entries even when some fail.

```java
Map<String, Object> items = Map.of("x", 10, "y", 20);

// Strict insert -- fails if any key exists
session.update(key)
    .bin("myMap").mapInsertItems(items)
    .execute();

// Allow partial success: insert what we can, skip existing keys
session.update(key)
    .bin("myMap").mapInsertItems(items, opt -> opt
        .allowFailures()
        .allowPartial())
    .execute();
```

### mapUpdateItems

Update multiple existing entries. Fails if any key does not exist, unless `allowFailures()` is set.

```java
Map<String, Object> updates = Map.of("a", 100, "b", 200);

session.update(key)
    .bin("myMap").mapUpdateItems(updates)
    .execute();

// Silently skip keys that don't exist
session.update(key)
    .bin("myMap").mapUpdateItems(updates, opt -> opt
        .allowFailures()
        .allowPartial())
    .execute();
```

---

## Navigating Into a Map

Before performing a read, write, or remove on specific map elements, you navigate into the map using an `onMap*` selector. The selector determines which element(s) are targeted and what terminal operations are available.

### By Key

Select a single map entry by its key. This is the only selector that supports write operations (`setTo`, `insert`, `update`, `upsert`, `add`).

```java
// String key
session.select(key)
    .bin("myMap").onMapKey("name").getValues()
    .execute();

// Long key
session.select(key)
    .bin("myMap").onMapKey(42L).getValues()
    .execute();

// byte[] key
session.select(key)
    .bin("myMap").onMapKey(new byte[]{1,2,3}).getValues()
    .execute();

// With explicit map creation order (used only if map doesn't exist)
session.update(key)
    .bin("myMap").onMapKey("name", MapOrder.KEY_VALUE_ORDERED).insert("Alice")
    .execute();
```

### By Index

Select a map entry by its positional index (0-based). Negative indexes count from the end.

```java
// First entry
session.select(key)
    .bin("myMap").onMapIndex(0).getValues()
    .execute();

// Last entry
session.select(key)
    .bin("myMap").onMapIndex(-1).getValues()
    .execute();
```

### By Rank

Select a map entry by value rank (0 = smallest value). Negative ranks count from the end.

```java
// Entry with the smallest value
session.select(key)
    .bin("scores").onMapRank(0).getValues()
    .execute();

// Entry with the largest value
session.select(key)
    .bin("scores").onMapRank(-1).getValues()
    .execute();
```

### By Value

Select all map entries that have a specific value. Supports inverted operations.

```java
session.select(key)
    .bin("myMap").onMapValue("active").getKeys()
    .execute();
```

### By Key Range

Select map entries whose keys fall within a range (start inclusive, end exclusive). Supports `SpecialValue.INFINITY` and `SpecialValue.NULL` for unbounded ranges.

```java
// Keys from "a" (inclusive) to "m" (exclusive)
session.select(key)
    .bin("myMap").onMapKeyRange("a", "m").getValues()
    .execute();

// All keys from "m" onwards
session.select(key)
    .bin("myMap").onMapKeyRange("m", SpecialValue.INFINITY).getValues()
    .execute();

// All keys up to "m"
session.select(key)
    .bin("myMap").onMapKeyRange(SpecialValue.NULL, "m").getValues()
    .execute();
```

### By Value Range

Select map entries whose values fall within a range.

```java
// Values from 10 (inclusive) to 100 (exclusive)
session.select(key)
    .bin("scores").onMapValueRange(10L, 100L).getKeys()
    .execute();
```

### By Index Range

Select map entries by a range of positional indexes.

```java
// 5 entries starting at index 0
session.select(key)
    .bin("myMap").onMapIndexRange(0, 5).getValues()
    .execute();

// All entries from index 3 to the end
session.select(key)
    .bin("myMap").onMapIndexRange(3).getValues()
    .execute();
```

### By Rank Range

Select map entries by a range of value ranks.

```java
// 3 entries with the lowest values
session.select(key)
    .bin("scores").onMapRankRange(0, 3).getValues()
    .execute();

// All entries from rank 5 to the end
session.select(key)
    .bin("scores").onMapRankRange(5).getValues()
    .execute();
```

### By Key List

Select map entries matching a list of specific keys. Supports inverted operations.

```java
List<String> keys = List.of("a", "b", "c");

session.select(key)
    .bin("myMap").onMapKeyList(keys).getValues()
    .execute();
```

### By Value List

Select map entries matching a list of specific values. Supports inverted operations.

```java
List<Object> values = List.of("active", "pending");

session.select(key)
    .bin("myMap").onMapValueList(values).getKeys()
    .execute();
```

### By Key Relative Index Range

Select map entries starting at the key nearest to a given key, offset by a relative index. Useful for pagination or "find items near this key."

```java
// All entries at or after key "m"
session.select(key)
    .bin("myMap").onMapKeyRelativeIndexRange("m", 0).getValues()
    .execute();

// 3 entries starting at key "m" (or the next key if "m" doesn't exist)
session.select(key)
    .bin("myMap").onMapKeyRelativeIndexRange("m", 0, 3).getValues()
    .execute();
```

### By Value Relative Rank Range

Select map entries starting at the value nearest to a given value, offset by a relative rank.

```java
// All entries with values at or above 50
session.select(key)
    .bin("scores").onMapValueRelativeRankRange(50L, 0).getValues()
    .execute();

// Top 3 entries relative to value 50
session.select(key)
    .bin("scores").onMapValueRelativeRankRange(50L, 0, 3).getValues()
    .execute();
```

---

## Single-Entry Write Operations

These are available after navigating with `onMapKey(...)`. Each operation has a no-arg form and an overload accepting `Consumer<MapEntryWriteOptions>` for fine-grained control. All value types are supported: `long`, `String`, `byte[]`, `boolean`, `double`, `List<?>`, `Map<?,?>`, and `RecordMapper<U>`.

When no options are specified, write operations that may create a new map will default to `MapOrder.KEY_ORDERED`.

### setTo

Set the value unconditionally (upsert semantics, no write-flag control).

```java
session.update(key)
    .bin("myMap").onMapKey("name").setTo("Alice")
    .execute();
```

### insert

Insert a value only if the key does **not** already exist. Throws an exception by default if the key exists.

```java
// Strict insert
session.update(key)
    .bin("myMap").onMapKey("name").insert("Alice")
    .execute();

// Insert, but silently skip if key exists
session.update(key)
    .bin("myMap").onMapKey("name").insert("Alice", opt -> opt.allowFailures())
    .execute();
```

### update

Update a value only if the key **already exists**. Throws an exception by default if the key does not exist.

```java
// Strict update
session.update(key)
    .bin("myMap").onMapKey("name").update("Bob")
    .execute();

// Update, but silently skip if key doesn't exist
session.update(key)
    .bin("myMap").onMapKey("name").update("Bob", opt -> opt.allowFailures())
    .execute();
```

### upsert

Create or update a map entry unconditionally. Functionally equivalent to `setTo`, but supports `MapEntryWriteOptions` for controlling map ordering and persist-index on creation.

```java
session.update(key)
    .bin("myMap").onMapKey("name").upsert("Alice")
    .execute();

// Upsert with specific map creation order
session.update(key)
    .bin("myMap").onMapKey("name").upsert("Alice", opt -> opt
        .mapOrder(MapOrder.KEY_VALUE_ORDERED))
    .execute();
```

### add (increment)

Atomically increment a numeric map value. If the key does not exist, it is created with an initial value of 0 and then incremented. Only available for `long` and `double` types.

```java
// Increment a counter by 1
session.update(key)
    .bin("counters").onMapKey("views").add(1L)
    .execute();

// Increment a floating-point score
session.update(key)
    .bin("scores").onMapKey("player1").add(2.5)
    .execute();

// Increment with options
session.update(key)
    .bin("counters").onMapKey("views").add(1L, opt -> opt.allowFailures())
    .execute();
```

### MapEntryWriteOptions

All single-entry write operations (except `setTo`) accept an optional `Consumer<MapEntryWriteOptions>`:

| Method | Description |
|--------|-------------|
| `mapOrder(MapOrder)` | Map ordering to use if the map does not already exist. Defaults to `KEY_ORDERED`. |
| `persistIndex()` | Persist the map index for faster lookups. Top-level ordered maps only. |
| `allowFailures()` | Silently ignore failures instead of throwing exceptions (maps to `NO_FAIL`). |

```java
session.update(key)
    .bin("myMap").onMapKey("k1").insert("v1", opt -> opt
        .mapOrder(MapOrder.KEY_VALUE_ORDERED)
        .persistIndex()
        .allowFailures())
    .execute();
```

### Using RecordMapper

Write operations also support mapping Java objects to map values via `RecordMapper`:

```java
session.update(key)
    .bin("myMap").onMapKey("user1").upsert(myUserObj, userMapper)
    .execute();

session.update(key)
    .bin("myMap").onMapKey("user1").insert(myUserObj, userMapper, opt -> opt.allowFailures())
    .execute();
```

---

## Read Operations

After navigating with any `onMap*` selector, the following read operations are available.

### getValues

Return the values of the selected map entries.

```java
session.select(key)
    .bin("myMap").onMapKeyRange("a", "z").getValues()
    .execute();
```

### getKeys

Return the keys of the selected map entries.

```java
session.select(key)
    .bin("myMap").onMapValue("active").getKeys()
    .execute();
```

### getKeysAndValues

Return both keys and values of the selected map entries.

```java
session.select(key)
    .bin("myMap").onMapIndexRange(0, 10).getKeysAndValues()
    .execute();
```

### getIndexes / getReverseIndexes

Return the positional indexes (or reverse indexes) of the selected map entries.

```java
session.select(key)
    .bin("myMap").onMapValue("important").getIndexes()
    .execute();
```

### getRanks / getReverseRanks

Return the value ranks (or reverse ranks) of the selected map entries.

```java
session.select(key)
    .bin("myMap").onMapKeyList(List.of("a", "b")).getRanks()
    .execute();
```

### count

Return the number of selected map entries.

```java
long count = session.select(key)
    .bin("myMap").onMapKeyRange("a", "m").count()
    .execute()
    .asLong("myMap");
```

### exists

Return whether the selected element(s) exist.

```java
session.select(key)
    .bin("myMap").onMapKey("name").exists()
    .execute();
```

### getAsMap / getAsOrderedMap

> **@Deprecated** -- These will be replaced by `AerospikeMap` which intrinsically supports ordering.

Return selected map entries as a Java `Map` (unordered) or ordered `Map`.

```java
session.select(key)
    .bin("myMap").onMapKeyRange("a", "m").getAsMap()
    .execute();

session.select(key)
    .bin("myMap").onMapKeyRange("a", "m").getAsOrderedMap()
    .execute();
```

---

## Remove Operations

### remove

Remove the selected map entries and return the count of removed items.

```java
// Remove a single entry by key
session.update(key)
    .bin("myMap").onMapKey("oldEntry").remove()
    .execute();

// Remove entries in a key range
session.update(key)
    .bin("myMap").onMapKeyRange("temp_", SpecialValue.INFINITY).remove()
    .execute();

// Remove entry by index
session.update(key)
    .bin("myMap").onMapIndex(0).remove()
    .execute();
```

### removeAllOthers

Remove all entries **except** those selected (inverted remove). Only available on range and list selectors.

```java
// Keep only keys "a" through "m", remove everything else
session.update(key)
    .bin("myMap").onMapKeyRange("a", "m").removeAllOthers()
    .execute();

// Keep only specific keys
session.update(key)
    .bin("myMap").onMapKeyList(List.of("keep1", "keep2")).removeAllOthers()
    .execute();
```

---

## Inverted Operations

Range and list selectors support inverted read operations, which return everything **except** the selected entries. These are available via `getAllOther*` methods.

```java
// Get all values EXCEPT those in the key range "a" to "m"
session.select(key)
    .bin("myMap").onMapKeyRange("a", "m").getAllOtherValues()
    .execute();

// Count all entries EXCEPT those with specific keys
session.select(key)
    .bin("myMap").onMapKeyList(List.of("x", "y")).countAllOthers()
    .execute();
```

Available inverted operations:

| Method | Returns |
|--------|---------|
| `getAllOtherValues()` | Values of non-selected entries |
| `getAllOtherKeys()` | Keys of non-selected entries |
| `getAllOtherKeysAndValues()` | Keys and values of non-selected entries |
| `getAllOtherIndexes()` | Indexes of non-selected entries |
| `getAllOtherReverseIndexes()` | Reverse indexes of non-selected entries |
| `getAllOtherRanks()` | Ranks of non-selected entries |
| `getAllOtherReverseRanks()` | Reverse ranks of non-selected entries |
| `countAllOthers()` | Count of non-selected entries |

---

## Nested Map Operations

Map operations support nested CDT paths. Chain multiple `onMap*` or `onListIndex` selectors to navigate into deeply nested structures.

```java
// Read a value from a nested map: myMap -> "users" -> "alice" -> getValues
session.select(key)
    .bin("myMap").onMapKey("users").onMapKey("alice").getValues()
    .execute();

// Write to a nested map entry
session.update(key)
    .bin("myMap").onMapKey("users").onMapKey("alice").upsert("admin")
    .execute();

// Create a nested map with a specific order, then write to it
session.update(key)
    .bin("myMap").onMapKey("stats").mapCreate(MapOrder.KEY_ORDERED)
    .bin("myMap").onMapKey("stats").onMapKey("count").upsert(0L)
    .execute();

// Navigate through a list into a map
session.select(key)
    .bin("items").onListIndex(0).onMapKey("name").getValues()
    .execute();
```

When writing to nested maps, the `mapOrder` option on `onMapKey` or via `MapEntryWriteOptions` only applies if the map does not already exist:

```java
// The KEY_VALUE_ORDERED order is used only if the nested map under "config" doesn't exist yet
session.update(key)
    .bin("myMap").onMapKey("config", MapOrder.KEY_VALUE_ORDERED).onMapKey("timeout").insert(30L)
    .execute();
```

---

## MapOrder Reference

| Value | Description |
|-------|-------------|
| `MapOrder.UNORDERED` | Map is not ordered. Fastest writes, unordered iteration. |
| `MapOrder.KEY_ORDERED` | Map is ordered by key. Default. |
| `MapOrder.KEY_VALUE_ORDERED` | Map is ordered by key, then by value. |

---

## MapBulkWriteOptions Reference

Used with bulk operations (`mapUpsertItems`, `mapInsertItems`, `mapUpdateItems`).

| Method | Description |
|--------|-------------|
| `mapOrder(MapOrder)` | Map ordering to use if the map does not already exist. Defaults to `KEY_ORDERED`. |
| `persistIndex()` | Persist the map index for faster lookups. Top-level ordered maps only. |
| `allowFailures()` | Silently ignore failures instead of throwing exceptions (maps to `NO_FAIL`). |
| `allowPartial()` | Allow other valid entries to be committed even if some are denied due to write flag constraints (maps to `PARTIAL`). Only meaningful for bulk operations. |

```java
session.update(key)
    .bin("myMap").mapInsertItems(items, opt -> opt
        .mapOrder(MapOrder.KEY_VALUE_ORDERED)
        .persistIndex()
        .allowFailures()
        .allowPartial())
    .execute();
```
