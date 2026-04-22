# Durable delete: explicit APIs vs implicit (SC) behavior

This document describes how the Java fluent client decides when **durable delete** (`INFO2_DURABLE_DELETE`) is sent on the wire, which controls are **explicit** (user or policy), and which paths **implicitly** enable it—typically for **strongly consistent (SC)** namespaces where non-durable expunge deletes are rejected or unsafe.

It is aimed at maintainers and advanced users. Implementation details live in `OperationSpecExecutor`, `BatchAttr`, `ChainableNoBinsBuilder`, and background builders.

---

## Terminology

| Term | Meaning |
|------|---------|
| **Durable delete** | Enterprise delete semantics: leave a tombstone so deleted records do not reappear after certain failure modes. Carried as `INFO2_DURABLE_DELETE` on applicable commands. |
| **`OperationSpec.durablyDelete`** | Per-operation `Boolean`: `true` / `false` / `null` (unset = no spec-level override). |
| **`Behavior` / `Settings.useDurableDelete`** | Session-wide defaults from the behavior matrix (including YAML). Feeds `BatchAttr.setDelete` / `setWrite` and `WriteCommand` unless overridden for that call. |
| **`scMode`** | Derived from the cluster partition map for the key’s namespace (`Partitions.scMode`). When true, the namespace is treated as strongly consistent for client policy purposes. |

---

## Explicit user (or policy) control

These are the knobs that express **intention** before any SC-specific overrides are applied.

### Per-delete fluent API (`ChainableNoBinsBuilder`)

Used for **`session.delete(key|keys)`** and for **`.delete(key)`** legs chained after **`session.upsert` / `update` / …`** (the builder switches to `ChainableNoBinsBuilder` for that entry).

| Method | Effect on `OperationSpec` |
|--------|---------------------------|
| **`durablyDelete(boolean)`** | Sets `durablyDelete` to `true` or `false`. |
| **`withDurableDelete()`** | Sets `durablyDelete` to `true` (and updates parent builder state). |
| **`withoutDurableDelete()`** | Sets `durablyDelete` to `false` (and updates parent builder state). |

If none of these are called, the spec’s durable flag stays **`null`** (no explicit per-operation choice).

### Behavior matrix / YAML

**`useDurableDelete(boolean)`** on the appropriate **write** behavior selectors sets the default **`Settings.useDurableDelete`** for matching operations. That value is the baseline until the executor applies an explicit **`OperationSpec`** override (no client-side “always on for SC” default on batch delete or operate `deleteRecord()`—those are opt-in due to server cost).

### Inherited `withDurableDelete()` / `withoutDurableDelete()` on other builders

**`AbstractSessionOperationBuilder`** exposes these for all builders; they set a **parent `durableDelete` field**. **`ChainableNoBinsBuilder.prepareSpecs()`** copies that field onto **`OperationSpec`** for **`DELETE`** specs when the spec did not already set durable delete. **`ChainableOperationBuilder.prepareSpecs()`** does the same for write specs whose operation list includes **`Operation.Type.DELETE`** (e.g. **`deleteRecord()`**), so **`withDurableDelete()`** before **`deleteRecord()`** reaches the wire on SC when you opt in.

**`BackgroundOperationBuilder`** (set **`delete`**) and **`BackgroundUdfBuilder`** pass the parent **`durableDelete`** field into **`BackgroundQueryCommand`** as an optional override when non-null, so **`Settings`** from the behavior matrix is not cloned (no implicit SC default).

---

## Implicit (client-default) durable delete

**Batch `BatchDelete`**, **foreground operate `deleteRecord()`**, and **background task delete / UDF** do **not** turn on durable delete based on **`scMode` alone**—callers must opt in (**`durablyDelete(true)`**, **`withDurableDelete()`**, **`withoutDurableDelete()`**, or behavior **`useDurableDelete`**) because of **server-side cost** of durable deletes.

---

## Quick matrix: who wins? (batch `DELETE` and `OperationSpec`)

| Spec `durablyDelete` | `BatchAttr` merge after `setDelete(settings)` |
|----------------------|-----------------------------------------------|
| `null` | No spec override: wire flag follows **`settings.getUseDurableDelete()`** only. |
| `true` | **`INFO2_DURABLE_DELETE`** is OR’d in. |
| `false` | Durable bit is cleared on the batch attrs for that delete. |

For **operate** / **`deleteRecord()`** and **single-key `WriteCommand` deletes**, **`spec.getDurablyDelete()`** is passed as a **`Boolean` override** into **`OperateWriteCommand` / `WriteCommand`** when non-null (including after **`ChainableOperationBuilder`** copies **`withDurableDelete()`** into the spec in **`prepareSpecs()`**), avoiding a **`Settings`** copy.

On **SC** namespaces that disallow non-durable deletes, callers must set durable delete explicitly (or via **behavior** / YAML) or the server may return **`FAIL_FORBIDDEN` (22)**.

---

## 1. If we make durable delete the default (everywhere)

Making durable delete **always on by default** (including AP namespaces and all write shapes) would be a **behavioral and compatibility** change. A practical checklist:

1. **Behavior matrix defaults**  
   - Set default **`useDurableDelete`** to **`true`** for all write/delete selectors where it is safe and meaningful (Enterprise-only semantics on server).  
   - Document that non-Enterprise clusters may reject or ignore the flag.

2. **`OperationSpecExecutor`**  
   - Revisit **`executeSingleKeyDelete`**: today **`scMode && spec == null`** forces durable; if global default is **`true`**, simplify branches and ensure **`durablyDelete(false)`** still clears the bit.  
   - Revisit **batch `DELETE`** SC branch: avoid double-applying or conflicting with defaults.  
   - **`executeSingleKeyWrite` / batch `BatchWrite`**: align embedded-delete behavior with the new default (today SC-only).

3. **Background jobs**  
   - **`BackgroundOperationBuilder`** / **`BackgroundUdfBuilder`**: today SC gates durable delete; decide whether AP should also default to durable and whether UDF should remain “always durable on SC only” or broader.

4. **Fluent API surface**  
   - **`withoutDurableDelete()`** / **`durablyDelete(false)`** must remain first-class so callers can opt out where the server allows (e.g. AP or SC with expunge policy).  
   - Consider deprecating redundant **`withDurableDelete()`** calls in docs/examples if defaults flip.

5. **Tests and examples**  
   - Many tests today branch on **`args.scMode`** for deletes; with a global default, trim or rewrite assumptions.  
   - Add regression tests for **explicit opt-out** on AP and SC (where server permits).

6. **Documentation**  
   - Update this doc, **`api-builder-reference`**, and any “getting started” material that mentions deletes.  
   - Call out Enterprise requirement and interaction with **`strong-consistency-allow-expunge`** server settings.

7. **Wire / server compatibility**  
   - Confirm behavior on mixed Enterprise/non-Enterprise test clusters; consider feature detection if the client ever needs to avoid sending the bit on unsupported servers.

---

## 2. If we want the user in control for **all** APIs (no implicit SC defaults)

Goal: **only** explicit **`durablyDelete` / `withDurableDelete` / `withoutDurableDelete`** and **behavior `useDurableDelete`** determine the wire flag—**no** automatic durable delete based solely on **`scMode`**.

Checklist:

1. **`OperationSpecExecutor.executeSingleKeyDelete`**  
   - Remove the **`else if (scMode) { settings.withUseDurableDelete(true); }`** branch (or gate it behind a new explicit session/behavior flag, e.g. **`autoDurableDeleteOnSc`** defaulting to **`false`**).

2. **Batch `case DELETE`**  
   - Remove the SC block that ORs **`INFO2_DURABLE_DELETE`** when **`spec.getDurablyDelete()`** is not **`false`**, **or** tie it to the same explicit opt-in flag.

3. **`executeSingleKeyWrite` and batch `BatchWrite`**  
   - Remove **`scMode && operationsContainRecordDelete(ops)`** auto-durable, **or** require an explicit spec field / builder method for “this operate pipeline may delete the record.”

4. **Background builders**  
   - **`BackgroundOperationBuilder`**: remove or guard **`opType == DELETE && scMode`** durable forcing.  
   - **`BackgroundUdfBuilder`**: remove or guard **`scMode`** durable forcing for all UDFs.

5. **New or existing explicit API**  
   - If SC still **requires** durable deletes for correctness unless the server allows expunge, surface that as **documented requirement** + optional **`session` / `Behavior`** knob (“**auto SC durable delete**”) default **`false`**, so “user in control” is the default but teams can opt in once per session.

6. **Tests**  
   - Today many tests use **`if (args.scMode) withDurableDelete()`** helpers; with no implicit default, **every** test that deletes on SC must set durable explicitly **or** enable the new opt-in flag in fixtures.  
   - Add tests proving **`withoutDurableDelete()`** is honored on SC when the server policy allows non-durable deletes.

7. **Documentation**  
   - State clearly that on SC namespaces without expunge, **users must** set durable delete (or enable opt-in automation). Link to Aerospike SC / expunge docs.

---

## Related code (for maintainers)

| Area | File / symbol |
|------|----------------|
| Single-key delete, operate write, batch assembly | `OperationSpecExecutor` (`executeSingleKeyDelete`, `executeSingleKeyWrite`, batch switch on `OpType`) |
| Merge spec into batch attrs | `mergeOperationSpecDurableDeleteIntoBatchAttr` |
| Embedded delete detection | `operationsContainRecordDelete` |
| Delete fluent overrides | `ChainableNoBinsBuilder` (`durablyDelete`, `withDurableDelete`, `withoutDurableDelete`) |
| Background delete / UDF | `BackgroundOperationBuilder.execute`, `BackgroundUdfBuilder.execute` |
| Operate / point delete override | `WriteCommand` / `OperateWriteCommand` `durableDeleteOverride` parameter |
| Policy defaults | `Behavior` matrix / YAML loaders |

---

## See also

- Server: strong consistency, expunge, and delete semantics in [Aerospike documentation](https://aerospike.com/docs/database/learn/architecture/clustering/consistency-modes).
- Client wire details: `docs/wire-protocol.md` (if present in this repo).
