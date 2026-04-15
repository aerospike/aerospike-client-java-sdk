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

**`useDurableDelete(boolean)`** on the appropriate **write** behavior selectors sets the default **`Settings.useDurableDelete`** for matching operations. That value is the baseline for deletes and writes until the executor applies spec-level or SC logic.

### Inherited `withDurableDelete()` / `withoutDurableDelete()` on other builders

**`AbstractSessionOperationBuilder`** exposes these for all builders, but they only toggle a **parent `durableDelete` field** that is **not** wired into `OperationSpec` for bin/operate flows. For **delete chains**, **`ChainableNoBinsBuilder`** overrides them so they **also** update **`OperationSpec`**—that is the supported way to pair ergonomics with the wire flag for **`session.delete`**.

---

## Implicit (client-default) durable delete

The client **turns durable delete on without the user calling** `durablyDelete` / `withDurableDelete` in these situations. Motivation is usually **SC + server policy** (e.g. `FAIL_FORBIDDEN` on non-durable expunge deletes) or **embedded record deletes** in an operate pipeline.

| User-facing entry | When durable delete is applied implicitly |
|-------------------|------------------------------------------|
| **`session.delete(...).execute()`** (single-key path in `OperationSpecExecutor`) | Namespace **`scMode`** and spec **`durablyDelete` is `null`** → settings force **`useDurableDelete(true)`**. If the user sets **`durablyDelete(false)`** (`withoutDurableDelete()`), that **opts out**. |
| **Heterogeneous batch: `BatchDelete`** | After `BatchAttr.setDelete(settings)` and merging **`OperationSpec.getDurablyDelete()`**, if **`scMode`** and spec is **not** explicitly **`false`**, **`INFO2_DURABLE_DELETE`** is OR’d into the batch write attrs. |
| **`session.upsert` / … with `deleteRecord()`** (operate list contains `Operation.Type.DELETE`) | Single-key **`executeSingleKeyWrite`** and batch **`BatchWrite`**: if **`scMode`** and the op list contains a record-level delete → **`Settings.withUseDurableDelete(true)`**. There is **no** separate fluent **`durablyDelete`** on the upsert spec for this path today. |
| **`session.backgroundTask().delete(...).execute()`** | **`OpType.DELETE`** and namespace **`scMode`** → **`Settings.withUseDurableDelete(true)`**. |
| **`session.backgroundTask().executeUdf(...).execute()`** | Namespace **`scMode`** → **`Settings.withUseDurableDelete(true)`** for the background UDF command (broad: UDF may remove records). |

**Foreground `session`-style UDF** on a single key is a separate code path and does **not** currently mirror the background UDF SC durable-default.

---

## Quick matrix: who wins?

| Spec `durablyDelete` | SC namespace | Typical outcome for **plain `session.delete`** |
|----------------------|--------------|-----------------------------------------------|
| `null` | no | Behavior / `Settings` defaults only. |
| `null` | yes | Client forces **durable** (unless behavior already set and not overridden—implementation forces via `Settings` for single-key delete). |
| `true` | either | **Durable** (explicit). |
| `false` | either | **Not** durable: you asked for a normal delete. The client does **not** add the extra “SC batch” durable-delete flag on top of that. |

**Plain words for the `false` row:** If you call **`withoutDurableDelete()`** or **`durablyDelete(false)`**, the delete is a **normal** delete as far as the spec is concerned. On **batch** deletes in an SC namespace, the client normally ORs in durable delete for safety; when you set the spec to **`false`**, it **skips** that extra step so your choice is respected (the server may still reject the delete if your namespace forbids non-durable deletes).

For **embedded `deleteRecord()`** on SC, the client currently **forces** durable behavior for that write without a per-spec `false` knob on the fluent upsert builder.

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
| Settings copy with override | `Settings.withUseDurableDelete` |
| Policy defaults | `Behavior` matrix / YAML loaders |

---

## See also

- Server: strong consistency, expunge, and delete semantics in [Aerospike documentation](https://aerospike.com/docs/database/learn/architecture/clustering/consistency-modes).
- Client wire details: `docs/wire-protocol.md` (if present in this repo).
