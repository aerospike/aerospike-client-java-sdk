# List CDT Operations

This document covers all list Collection Data Type (CDT) operations available in the Aerospike Java Fluent Client.

## Table of Contents

- [Overview](#overview)
- [List Structure Operations](#list-structure-operations)
  - [listCreate](#listcreate)
  - [listSetOrder](#listsetorder)
  - [listClear](#listclear)
  - [listSize](#listsize)
  - [listSort](#listsort)
- [Append and Add Operations](#append-and-add-operations)
  - [listAppend (unordered)](#listappend-unordered)
  - [listAdd (ordered)](#listadd-ordered)
  - [ListEntryWriteOptions](#listentrywriteoptions)
- [Bulk Write Operations](#bulk-write-operations)
  - [listAppendItems](#listappenditems)
  - [listAddItems](#listadditems)
  - [ListBulkWriteOptions](#listbulkwriteoptions)
- [Index-Based Write Operations](#index-based-write-operations)
  - [listInsert](#listinsert)
  - [listInsertItems](#listinsertitems)
  - [listSet](#listset)
  - [listIncrement](#listincrement)
- [Index-Based Read Operations](#index-based-read-operations)
  - [listGet](#listget)
  - [listGetRange](#listgetrange)
- [Index-Based Remove Operations](#index-based-remove-operations)
  - [listRemove](#listremove)
  - [listRemoveRange](#listremoverange)
  - [listPop / listPopRange](#listpop--listpoprange)
  - [listTrim](#listtrim)
- [Navigating Into a List](#navigating-into-a-list)
  - [By Index](#by-index)
  - [By Rank](#by-rank)
  - [By Value](#by-value)
  - [By Index Range](#by-index-range)
  - [By Rank Range](#by-rank-range)
  - [By Value Range](#by-value-range)
  - [By Value List](#by-value-list)
  - [By Value Relative Rank Range](#by-value-relative-rank-range)
- [Terminal Actions After Navigation](#terminal-actions-after-navigation)
  - [getValues](#getvalues)
  - [getIndexes / getReverseIndexes](#getindexes--getreverseindexes)
  - [getRanks / getReverseRanks](#getranks--getreverseranks)
  - [count](#count)
  - [exists](#exists)
  - [remove](#remove)
- [Write After Navigation](#write-after-navigation)
  - [setTo (by index)](#setto-by-index)
  - [insert (before index)](#insert-before-index)
  - [add (increment at index)](#add-increment-at-index)
- [Inverted Operations](#inverted-operations)
- [Nested List Operations](#nested-list-operations)

---

## Overview

List operations follow the same fluent builder pattern as map operations. You start from a `bin(...)` call, then either:
- Perform a **structural** or **index-based** operation directly (like `listCreate`, `listAppend`, `listGet(3)`), or
- **Navigate** into the list with an `onList*` selector, then apply a terminal action (read, write, or remove).

```java
// Structural: create an ordered list, append a value
session.update(key)
    .bin("scores").listCreate(ListOrder.ORDERED)
    .bin("scores").listAdd(95)
    .execute();

// Index-based: get the first element
session.select(key)
    .bin("scores").listGet(0)
    .execute();

// Navigate + action: remove all items with value 42
session.update(key)
    .bin("scores").onListValue(42).remove()
    .execute();
```

---

## List Structure Operations

### listCreate

Create a list at the current context level with the specified ordering. Defaults to `pad = true` (nil-fill on out-of-bounds writes) and `persistIndex = false`.

```java
// Common case -- just specify the order
session.update(key)
    .bin("scores").listCreate(ListOrder.ORDERED)
    .execute();

// Disable padding (fail on out-of-bounds index writes)
session.update(key)
    .bin("slots").listCreate(ListOrder.ORDERED, opt -> opt.noPad())
    .execute();

// Persist the index for faster lookups
session.update(key)
    .bin("logs").listCreate(ListOrder.ORDERED, opt -> opt.persistIndex())
    .execute();

// Both: no padding and persist index
session.update(key)
    .bin("data").listCreate(ListOrder.UNORDERED, opt -> opt.noPad().persistIndex())
    .execute();
```

### listSetOrder

Change the ordering of an existing list.

```java
session.update(key)
    .bin("items").listSetOrder(ListOrder.ORDERED)
    .execute();

// With persistent index
session.update(key)
    .bin("items").listSetOrder(ListOrder.ORDERED, true)
    .execute();
```

### listClear

Remove all items from the list without deleting the bin.

```java
session.update(key)
    .bin("scores").listClear()
    .execute();
```

### listSize

Return the number of items in the list.

```java
session.select(key)
    .bin("scores").listSize()
    .execute();
```

### listSort

Sort the list in place.

```java
// Default sort
session.update(key)
    .bin("scores").listSort()
    .execute();

// Sort and drop duplicates
session.update(key)
    .bin("scores").listSort(ListSortFlags.DROP_DUPLICATES)
    .execute();
```

---

## Append and Add Operations

### listAppend (unordered)

Append an item to the end of an unordered list. Supports all primitive types plus `List` and `Map`.

```java
session.update(key)
    .bin("tags").listAppend("important")
    .bin("scores").listAppend(95)
    .bin("ratios").listAppend(3.14)
    .execute();
```

With options:

```java
// Append only if the value is unique
session.update(key)
    .bin("tags").listAppend("important", opt -> opt.addUnique())
    .execute();

// Append unique, silently ignoring duplicates
session.update(key)
    .bin("tags").listAppend("important", opt -> opt.addUnique().allowFailures())
    .execute();
```

### listAdd (ordered)

Add an item to the appropriate position in an ordered list (the server determines placement).

```java
session.update(key)
    .bin("sortedScores").listAdd(88)
    .execute();

// Add unique to ordered list
session.update(key)
    .bin("sortedScores").listAdd(88, opt -> opt.addUnique().allowFailures())
    .execute();
```

### ListEntryWriteOptions

Options available for single-item list write operations:

| Method | Flag | Description |
|--------|------|-------------|
| `addUnique()` | `ADD_UNIQUE` | Reject duplicate values |
| `insertBounded()` | `INSERT_BOUNDED` | Fail if index is out of bounds |
| `allowFailures()` | `NO_FAIL` | Silently ignore failures instead of throwing |

```java
session.update(key)
    .bin("uniqueItems").listAppend("value", opt -> opt
        .addUnique()
        .allowFailures())
    .execute();
```

---

## Bulk Write Operations

### listAppendItems

Append multiple items to an unordered list.

```java
session.update(key)
    .bin("tags").listAppendItems(Arrays.asList("a", "b", "c"))
    .execute();

// With options
session.update(key)
    .bin("tags").listAppendItems(Arrays.asList("a", "b", "c"), opt -> opt
        .addUnique()
        .allowFailures()
        .allowPartial())
    .execute();
```

### listAddItems

Add multiple items to an ordered list.

```java
session.update(key)
    .bin("sortedScores").listAddItems(Arrays.asList(88, 92, 75))
    .execute();
```

### listInsertItems

Insert multiple items at a specific index.

```java
session.update(key)
    .bin("items").listInsertItems(2, Arrays.asList("x", "y", "z"))
    .execute();

// With options
session.update(key)
    .bin("items").listInsertItems(2, Arrays.asList("x", "y", "z"), opt -> opt
        .addUnique()
        .allowFailures()
        .allowPartial())
    .execute();
```

### ListBulkWriteOptions

Extends `ListEntryWriteOptions` with one additional option:

| Method | Flag | Description |
|--------|------|-------------|
| `allowPartial()` | `PARTIAL` | Commit valid items even when some are rejected |

---

## Index-Based Write Operations

### listInsert

Insert a value at a specific index, shifting subsequent elements right.

```java
session.update(key)
    .bin("items").listInsert(0, "first")   // Insert at beginning
    .bin("items").listInsert(2, 42)        // Insert at index 2
    .execute();
```

### listSet

Replace the value at a specific index.

```java
session.update(key)
    .bin("items").listSet(0, "updated")
    .bin("scores").listSet(3, 100)
    .execute();
```

### listIncrement

Increment the numeric value at a specific index.

```java
// Increment by 1
session.update(key)
    .bin("counters").listIncrement(0)
    .execute();

// Increment by a specific amount
session.update(key)
    .bin("counters").listIncrement(0, 5)
    .bin("ratios").listIncrement(1, 0.5)
    .execute();
```

---

## Index-Based Read Operations

### listGet

Get the value at a specific index.

```java
session.select(key)
    .bin("items").listGet(0)       // First element
    .bin("items").listGet(-1)      // Last element
    .execute();
```

### listGetRange

Get a range of values by index.

```java
// Get 3 items starting at index 0
session.select(key)
    .bin("items").listGetRange(0, 3)
    .execute();

// Get all items from index 5 to the end
session.select(key)
    .bin("items").listGetRange(5)
    .execute();
```

---

## Index-Based Remove Operations

### listRemove

Remove and discard the item at a specific index.

```java
session.update(key)
    .bin("items").listRemove(0)
    .execute();
```

### listRemoveRange

Remove and discard a range of items by index.

```java
// Remove 3 items starting at index 2
session.update(key)
    .bin("items").listRemoveRange(2, 3)
    .execute();

// Remove all items from index 5 to the end
session.update(key)
    .bin("items").listRemoveRange(5)
    .execute();
```

### listPop / listPopRange

Remove and **return** items (unlike `listRemove` which discards).

```java
// Pop single item
session.update(key)
    .bin("queue").listPop(0)
    .execute();

// Pop 3 items from the beginning
session.update(key)
    .bin("queue").listPopRange(0, 3)
    .execute();

// Pop all from index 5 onward
session.update(key)
    .bin("queue").listPopRange(5)
    .execute();
```

### listTrim

Keep only a range of items, removing everything else.

```java
// Keep only 10 items starting at index 0
session.update(key)
    .bin("recent").listTrim(0, 10)
    .execute();
```

---

## Navigating Into a List

These selectors navigate into a list to target specific elements. After selecting, you can chain a terminal action (read, write, or remove).

### By Index

Select a single element by its index. Returns a non-invertable context (single element).

```java
session.update(key)
    .bin("items").onListIndex(0).getValues()
    .bin("items").onListIndex(-1).remove()
    .execute();

// With list ordering context (creates ordered sub-list if absent)
session.update(key)
    .bin("nested").onListIndex(2, ListOrder.ORDERED, true).listAdd(42)
    .execute();
```

### By Rank

Select a single element by its rank (sort order position). Returns a non-invertable context.

```java
// Get the smallest value (rank 0)
session.select(key)
    .bin("scores").onListRank(0).getValues()
    .execute();

// Get the largest value (rank -1)
session.select(key)
    .bin("scores").onListRank(-1).getValues()
    .execute();
```

### By Value

Select all elements matching a specific value. Returns an invertable context (may match multiple elements).

```java
session.update(key)
    .bin("tags").onListValue("obsolete").remove()
    .execute();

session.select(key)
    .bin("scores").onListValue(100).count()
    .execute();
```

### By Index Range

Select elements by a range of indexes. Returns an invertable action builder.

```java
// Get 5 items starting at index 2
session.select(key)
    .bin("items").onListIndexRange(2, 5).getValues()
    .execute();

// Get all items from index 3 to the end
session.select(key)
    .bin("items").onListIndexRange(3).getValues()
    .execute();
```

### By Rank Range

Select elements by a range of ranks. Returns an invertable action builder.

```java
// Get the 3 smallest items
session.select(key)
    .bin("scores").onListRankRange(0, 3).getValues()
    .execute();

// Get all items from rank 5 onward
session.select(key)
    .bin("scores").onListRankRange(5).getValues()
    .execute();
```

### By Value Range

Select elements within a value range `[startIncl, endExcl)`. Returns an invertable action builder.

```java
// Get all scores between 80 (inclusive) and 100 (exclusive)
session.select(key)
    .bin("scores").onListValueRange(80, 100).getValues()
    .execute();

// Get all values from "a" to end
session.select(key)
    .bin("tags").onListValueRange("a", SpecialValue.INFINITY).getValues()
    .execute();

// Get all values from beginning to "m"
session.select(key)
    .bin("tags").onListValueRange(SpecialValue.WILDCARD, "m").getValues()
    .execute();
```

### By Value List

Select all elements whose values are in the provided list. Returns an invertable context.

```java
session.select(key)
    .bin("scores").onListValueList(Arrays.asList(90, 95, 100)).getValues()
    .execute();

// Remove specific values
session.update(key)
    .bin("tags").onListValueList(Arrays.asList("old", "deprecated")).remove()
    .execute();
```

### By Value Relative Rank Range

Select elements starting from the rank of a given value, with an optional count.

```java
// Get all values with rank >= rank of 80 (i.e., all values >= 80)
session.select(key)
    .bin("scores").onListValueRelativeRankRange(80, 0).getValues()
    .execute();

// Get 3 values starting from the rank of 80
session.select(key)
    .bin("scores").onListValueRelativeRankRange(80, 0, 3).getValues()
    .execute();

// Get values with rank < rank of 80 (negative rank offset)
session.select(key)
    .bin("scores").onListValueRelativeRankRange(80, -2, 2).getValues()
    .execute();
```

---

## Terminal Actions After Navigation

After navigating with an `onList*` selector, these terminal actions are available:

### getValues

Return the value(s) selected by the navigation.

```java
session.select(key)
    .bin("scores").onListRank(0).getValues()    // Single value (smallest)
    .bin("scores").onListRankRange(0, 3).getValues()  // Multiple values
    .execute();
```

### getIndexes / getReverseIndexes

Return the index(es) of the selected elements. Not available after `onListIndex` (index is already known).

```java
session.select(key)
    .bin("scores").onListValue(100).getIndexes()
    .bin("scores").onListRankRange(0, 3).getReverseIndexes()
    .execute();
```

### getRanks / getReverseRanks

Return the rank(s) of the selected elements. Not available after `onListRank` (rank is already known).

```java
session.select(key)
    .bin("scores").onListValue(95).getRanks()
    .bin("scores").onListIndexRange(0, 5).getReverseRanks()
    .execute();
```

### count

Return the number of elements matching the selection.

```java
session.select(key)
    .bin("scores").onListValue(100).count()
    .bin("tags").onListValueRange("a", "m").count()
    .execute();
```

### exists

Return a boolean indicating whether any elements match the selection.

```java
session.select(key)
    .bin("scores").onListValue(100).exists()
    .execute();
```

### remove

Remove the selected elements from the list.

```java
session.update(key)
    .bin("scores").onListValue(0).remove()
    .bin("tags").onListValueList(Arrays.asList("old", "stale")).remove()
    .execute();
```

---

## Write After Navigation

After navigating to a specific list index with `onListIndex(n)`, you can perform writes:

### setTo (by index)

Replace the value at the navigated index.

```java
session.update(key)
    .bin("items").onListIndex(0).setTo("updated")
    .execute();
```

### insert (before index)

Insert a value **before** the navigated index position, shifting subsequent elements right. This is different from map `insert()` which uses CREATE_ONLY semantics.

```java
// Insert "new" before index 2
session.update(key)
    .bin("items").onListIndex(2).insert("new")
    .execute();
```

### add (increment at index)

Increment the numeric value at the navigated index.

```java
session.update(key)
    .bin("counters").onListIndex(0).add(5)
    .execute();
```

> **Note:** `upsert()` and `update()` are not available after `onListIndex()` -- these are map-only concepts.

---

## Inverted Operations

Invertable selectors (ranges, value lists, value matches) support "all others" operations that act on every element **except** those matching the selection:

```java
// Remove all scores outside the 80-100 range
session.update(key)
    .bin("scores").onListValueRange(80, 100).removeAllOthers()
    .execute();

// Count items NOT matching the value list
session.select(key)
    .bin("tags").onListValueList(Arrays.asList("keep", "important")).countAllOthers()
    .execute();

// Get all values except those at ranks 0-2
session.select(key)
    .bin("scores").onListRankRange(0, 3).getAllOtherValues()
    .execute();
```

Available inverted methods:
- `getAllOtherValues()` / `countAllOthers()` / `removeAllOthers()`
- `getAllOtherIndexes()` / `getAllOtherReverseIndexes()`
- `getAllOtherRanks()` / `getAllOtherReverseRanks()`

---

## Nested List Operations

Lists can be nested inside maps or other lists. Use context chaining to navigate:

```java
// Map containing lists: get first score from "math" subject
session.select(key)
    .bin("grades").onMapKey("math").onListIndex(0).getValues()
    .execute();

// List of lists: get item at [2][0]
session.select(key)
    .bin("matrix").onListIndex(2).onListIndex(0).getValues()
    .execute();

// Navigate into a nested ordered list, add a value
session.update(key)
    .bin("data").onMapKey("scores").listAdd(99)
    .execute();

// Map -> List -> range remove
session.update(key)
    .bin("data").onMapKey("history").onListValueRange(0, 50).remove()
    .execute();
```
