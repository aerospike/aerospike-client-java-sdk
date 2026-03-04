# Exception Hierarchy

All exceptions in the fluent client extend `AerospikeException`, which itself extends `RuntimeException`. The hierarchy is designed so you can catch at exactly the level of specificity you need — catch `BinException` to handle all bin-level errors, or catch `BinNotFoundException` to handle just that one case.

## Tree

```
AerospikeException (RuntimeException)
│
│   // Record-level errors
├── RecordNotFoundException                     KEY_NOT_FOUND_ERROR (2)
├── RecordExistsException                       KEY_EXISTS_ERROR (5)
├── GenerationException                         GENERATION_ERROR (3)
├── FilteredException                           FILTERED_OUT (27)
├── RecordTooBigException                       RECORD_TOO_BIG (13)
│
│   // Bin-level errors
├── BinException                                BIN_NAME_TOO_LONG (21)
│   ├── BinExistsException                      BIN_EXISTS_ERROR (6)
│   ├── BinNotFoundException                    BIN_NOT_FOUND (17)
│   ├── BinTypeException                        BIN_TYPE_ERROR (12)
│   └── BinOpInvalidException                   OP_NOT_APPLICABLE (26)
│
│   // CDT element-level errors
├── ElementException
│   ├── ElementNotFoundException                ELEMENT_NOT_FOUND (23)
│   └── ElementExistsException                  ELEMENT_EXISTS (24)
│
│   // Transaction / MRT errors
├── TransactionException                        TXN_ALREADY_ABORTED (-19)
│   │                                           TXN_ALREADY_COMMITTED (-18)
│   │                                           MRT_BLOCKED (120)
│   │                                           MRT_VERSION_MISMATCH (121)
│   │                                           MRT_EXPIRED (122)
│   │                                           MRT_TOO_MANY_WRITES (123)
│   │                                           MRT_COMMITTED (124)
│   │                                           MRT_ABORTED (125)
│   │                                           MRT_ALREADY_LOCKED (126)
│   │                                           MRT_MONITOR_EXISTS (127)
│   └── Commit                                  TXN_FAILED (-17)
│
│   // Security errors
├── SecurityException                           ILLEGAL_STATE (56)
│   │                                           USER_ALREADY_EXISTS (61)
│   │                                           FORBIDDEN_PASSWORD (64)
│   │                                           SECURITY_NOT_ENABLED (52)
│   │                                           SECURITY_NOT_SUPPORTED (51)
│   │                                           SECURITY_SCHEME_NOT_SUPPORTED (53)
│   │                                           EXPIRED_SESSION (66)
│   │                                           INVALID_ROLE (70)
│   │                                           ROLE_ALREADY_EXISTS (71)
│   │                                           INVALID_PRIVILEGE (72)
│   │                                           INVALID_WHITELIST (73)
│   ├── AuthenticationException                 INVALID_USER (60)
│   │                                           INVALID_PASSWORD (62)
│   │                                           INVALID_CREDENTIAL (65)
│   │                                           EXPIRED_PASSWORD (63)
│   │                                           NOT_AUTHENTICATED (80)
│   └── AuthorizationException                  ROLE_VIOLATION (81)
│                                               NOT_WHITELISTED (82)
│
│   // Quota errors
├── QuotaException                              QUOTA_EXCEEDED (83)
│                                               QUOTAS_NOT_ENABLED (74)
│                                               INVALID_QUOTA (75)
│
│   // Capacity / resource exhaustion errors
├── CapacityException                           SERVER_MEM_ERROR (8)
│   │                                           DEVICE_OVERLOAD (18)
│   │                                           NO_MORE_CONNECTIONS (-7)
│   │                                           ASYNC_QUEUE_FULL (-9)
│   │                                           BATCH_QUEUES_FULL (152)
│   │                                           BATCH_MAX_REQUESTS_EXCEEDED (151)
│   ├── KeyBusyException                        KEY_BUSY (14)
│   └── Connection                              SERVER_NOT_AVAILABLE (-8)
│
│   // Secondary index errors
├── IndexException                              INDEX_ALREADY_EXISTS (200)
│                                               INDEX_NOTFOUND (201)
│                                               INDEX_OOM (202)
│                                               INDEX_NOTREADABLE (203)
│                                               INDEX_GENERIC (204)
│                                               INDEX_NAME_MAXLEN (205)
│                                               INDEX_MAXCOUNT (206)
│
│   // Query / scan errors
├── QueryException                              QUERY_ABORTED (210)
│                                               QUERY_QUEUEFULL (211)
│                                               QUERY_TIMEOUT (212)
│                                               QUERY_GENERIC (213)
│                                               SCAN_ABORT (15)
│
│   // Batch errors
├── BatchException                              BATCH_FAILED (-16)
│                                               BATCH_DISABLED (150)
│
│   // UDF errors
├── UdfException                                UDF_BAD_RESPONSE (100)
│
│   // Client infrastructure errors
├── Timeout                                     TIMEOUT (9)
├── InvalidNode                                 INVALID_NODE_ERROR (-3)
├── Serialize                                   SERIALIZE_ERROR (-10)
├── Parse                                       PARSE_ERROR (-2)
├── InvalidNamespace                            INVALID_NAMESPACE (20)
├── QueryTerminated                             QUERY_TERMINATED (-5)
├── Backoff                                     MAX_ERROR_RATE (-12)
│
│   // Codes with no dedicated subclass (plain AerospikeException)
│   CLIENT_ERROR (-1)                           Generic client error
│   NO_RESPONSE (-15)                           No response received from server
│   MAX_RETRIES_EXCEEDED (-11)                  Max retries limit reached
│   SCAN_TERMINATED (-4)                        Scan was terminated by user
│   OK (0)                                      Operation was successful
│   SERVER_ERROR (1)                            Unknown server failure
│   PARAMETER_ERROR (4)                         Bad parameter(s) in operation call
│   CLUSTER_KEY_MISMATCH (7)                    Expected cluster was not received
│   ALWAYS_FORBIDDEN (10)                       Operation not allowed in current configuration
│   PARTITION_UNAVAILABLE (11)                  Partition is unavailable
│   UNSUPPORTED_FEATURE (16)                    Unsupported server feature
│   KEY_MISMATCH (19)                           Key type mismatch
│   FAIL_FORBIDDEN (22)                         Operation not allowed at this time
│   ENTERPRISE_ONLY (25)                        Enterprise feature on Community server
│   LOST_CONFLICT (28)                          Write lost conflict to XDR
│   XDR_KEY_BUSY (32)                           Write blocked by XDR shipping
│   QUERY_END (50)                              No more records left for query
│   INVALID_COMMAND (54)                        Invalid administration command
│   INVALID_FIELD (55)                          Invalid administration field
```

## Catching Exceptions

The hierarchy lets you catch at the right level of granularity.

### Catch a specific error

```java
try {
    session.insert(key).bin("name").setTo("Alice").execute();
} catch (AerospikeException.RecordExistsException e) {
    // Record already exists — handle the duplicate
}
```

### Catch a category of errors

```java
try {
    session.update(key).bin("data").setTo(largePayload).execute();
} catch (AerospikeException.BinException e) {
    // Any bin-level problem: name too long, wrong type, not found, already exists, etc.
    System.err.println("Bin error (code " + e.getResultCode() + "): " + e.getBaseMessage());
}
```

### Catch security errors broadly, with special handling for auth

```java
try {
    session.query(key).execute();
} catch (AerospikeException.AuthenticationException e) {
    // Bad credentials or session expired — re-authenticate
    reconnect();
} catch (AerospikeException.AuthorizationException e) {
    // Valid user, but lacks permissions
    throw new AccessDeniedException(e);
} catch (AerospikeException.SecurityException e) {
    // Any other security error
    log.error("Security error: " + e.getBaseMessage());
}
```

### Catch CDT element errors

```java
try {
    session.update(key)
        .bin("map").onMapKey("newKey").insert("value")
        .execute();
} catch (AerospikeException.ElementExistsException e) {
    // Key already exists in the map (CREATE_ONLY mode)
} catch (AerospikeException.ElementException e) {
    // Any CDT element error
}
```

### Catch capacity/resource exhaustion

```java
try {
    session.upsert(key).bin("data").setTo(payload).execute();
} catch (AerospikeException.KeyBusyException e) {
    // Hot key — too many concurrent operations on this record
    retryWithBackoff();
} catch (AerospikeException.Connection e) {
    // Can't reach the server
    failover();
} catch (AerospikeException.CapacityException e) {
    // Server memory, device overload, connection pool exhausted, etc.
    applyBackpressure();
}
```

### Transaction errors

```java
try {
    session.doInTransaction(txn -> {
        txn.update(accounts.id("a1")).bin("balance").add(-100).execute();
        txn.update(accounts.id("a2")).bin("balance").add(100).execute();
    });
} catch (AerospikeException.Commit e) {
    // Commit specifically failed — inspect verify/roll records
    System.err.println("Commit error: " + e.error);
} catch (AerospikeException.TransactionException e) {
    // Any MRT error: blocked, expired, version mismatch, etc.
}
```

## Creating Exceptions from Result Codes

When you have a result code (e.g., from a `RecordResult` in a batch stream), use the factory method to get the correct exception subclass:

```java
AerospikeException ex = AerospikeException.resultCodeToException(
    resultCode, message, inDoubt
);
```

This returns the most specific subclass for the given code. For example, `resultCodeToException(2, null, false)` returns a `RecordNotFoundException`. Unrecognized codes return a plain `AerospikeException`.

## Result Code Reference

| Code | Constant | Description | Exception Class |
|------|----------|-------------|-----------------|
| -19 | `TXN_ALREADY_ABORTED` | Transaction was already aborted | `TransactionException` |
| -18 | `TXN_ALREADY_COMMITTED` | Transaction was already committed | `TransactionException` |
| -17 | `TXN_FAILED` | Transaction commit failed | `Commit` |
| -16 | `BATCH_FAILED` | One or more keys failed in a batch | `BatchException` |
| -15 | `NO_RESPONSE` | No response received from server | `AerospikeException` |
| -12 | `MAX_ERROR_RATE` | Max error rate exceeded | `Backoff` |
| -11 | `MAX_RETRIES_EXCEEDED` | Max retries limit reached | `AerospikeException` |
| -10 | `SERIALIZE_ERROR` | Client serialization error | `Serialize` |
| -9 | `ASYNC_QUEUE_FULL` | Async delay queue is full | `CapacityException` |
| -8 | `SERVER_NOT_AVAILABLE` | Connection to server failed | `Connection` |
| -7 | `NO_MORE_CONNECTIONS` | No more available connections | `CapacityException` |
| -5 | `QUERY_TERMINATED` | Query was terminated by user | `QueryTerminated` |
| -4 | `SCAN_TERMINATED` | Scan was terminated by user | `AerospikeException` |
| -3 | `INVALID_NODE_ERROR` | Chosen node is not active | `InvalidNode` |
| -2 | `PARSE_ERROR` | Client parse error | `Parse` |
| -1 | `CLIENT_ERROR` | Generic client error | `AerospikeException` |
| 0 | `OK` | Operation was successful | *(not thrown)* |
| 1 | `SERVER_ERROR` | Unknown server failure | `AerospikeException` |
| 2 | `KEY_NOT_FOUND_ERROR` | Record does not exist | `RecordNotFoundException` |
| 3 | `GENERATION_ERROR` | Generation mismatch | `GenerationException` |
| 4 | `PARAMETER_ERROR` | Bad parameter(s) in operation call | `AerospikeException` |
| 5 | `KEY_EXISTS_ERROR` | Record already exists | `RecordExistsException` |
| 6 | `BIN_EXISTS_ERROR` | Bin already exists (create-only) | `BinExistsException` |
| 7 | `CLUSTER_KEY_MISMATCH` | Expected cluster was not received | `AerospikeException` |
| 8 | `SERVER_MEM_ERROR` | Server out of memory | `CapacityException` |
| 9 | `TIMEOUT` | Client or server timeout | `Timeout` |
| 10 | `ALWAYS_FORBIDDEN` | Operation not allowed in current configuration | `AerospikeException` |
| 11 | `PARTITION_UNAVAILABLE` | Partition is unavailable | `AerospikeException` |
| 12 | `BIN_TYPE_ERROR` | Incompatible bin type | `BinTypeException` |
| 13 | `RECORD_TOO_BIG` | Record exceeds size limit | `RecordTooBigException` |
| 14 | `KEY_BUSY` | Hot key contention | `KeyBusyException` |
| 15 | `SCAN_ABORT` | Scan aborted by server | `QueryException` |
| 16 | `UNSUPPORTED_FEATURE` | Unsupported server feature | `AerospikeException` |
| 17 | `BIN_NOT_FOUND` | Bin not found (update-only) | `BinNotFoundException` |
| 18 | `DEVICE_OVERLOAD` | Storage device overloaded | `CapacityException` |
| 19 | `KEY_MISMATCH` | Key type mismatch | `AerospikeException` |
| 20 | `INVALID_NAMESPACE` | Namespace not found | `InvalidNamespace` |
| 21 | `BIN_NAME_TOO_LONG` | Bin name exceeds 15 chars or max bins exceeded | `BinException` |
| 22 | `FAIL_FORBIDDEN` | Operation not allowed at this time | `AerospikeException` |
| 23 | `ELEMENT_NOT_FOUND` | CDT element not found (update-only) | `ElementNotFoundException` |
| 24 | `ELEMENT_EXISTS` | CDT element already exists (create-only) | `ElementExistsException` |
| 25 | `ENTERPRISE_ONLY` | Enterprise feature on Community server | `AerospikeException` |
| 26 | `OP_NOT_APPLICABLE` | Operation not applicable to bin value | `BinOpInvalidException` |
| 27 | `FILTERED_OUT` | Filter expression evaluated to false | `FilteredException` |
| 28 | `LOST_CONFLICT` | Write lost conflict to XDR | `AerospikeException` |
| 32 | `XDR_KEY_BUSY` | Write blocked until XDR finishes shipping | `AerospikeException` |
| 50 | `QUERY_END` | No more records left for query | `AerospikeException` |
| 51 | `SECURITY_NOT_SUPPORTED` | Security not supported | `SecurityException` |
| 52 | `SECURITY_NOT_ENABLED` | Security not enabled | `SecurityException` |
| 53 | `SECURITY_SCHEME_NOT_SUPPORTED` | Security scheme not supported | `SecurityException` |
| 54 | `INVALID_COMMAND` | Invalid administration command | `AerospikeException` |
| 55 | `INVALID_FIELD` | Invalid administration field | `AerospikeException` |
| 56 | `ILLEGAL_STATE` | Security protocol violation | `SecurityException` |
| 60 | `INVALID_USER` | Invalid user name | `AuthenticationException` |
| 61 | `USER_ALREADY_EXISTS` | User already exists | `SecurityException` |
| 62 | `INVALID_PASSWORD` | Invalid password | `AuthenticationException` |
| 63 | `EXPIRED_PASSWORD` | Password has expired | `AuthenticationException` |
| 64 | `FORBIDDEN_PASSWORD` | Password not allowed (e.g., recently used) | `SecurityException` |
| 65 | `INVALID_CREDENTIAL` | Invalid security credential | `AuthenticationException` |
| 66 | `EXPIRED_SESSION` | Login session expired | `SecurityException` |
| 70 | `INVALID_ROLE` | Invalid role name | `SecurityException` |
| 71 | `ROLE_ALREADY_EXISTS` | Role already exists | `SecurityException` |
| 72 | `INVALID_PRIVILEGE` | Invalid privilege | `SecurityException` |
| 73 | `INVALID_WHITELIST` | Invalid IP whitelist | `SecurityException` |
| 74 | `QUOTAS_NOT_ENABLED` | Quotas not enabled on server | `QuotaException` |
| 75 | `INVALID_QUOTA` | Invalid quota value | `QuotaException` |
| 80 | `NOT_AUTHENTICATED` | Not authenticated | `AuthenticationException` |
| 81 | `ROLE_VIOLATION` | Insufficient role privileges | `AuthorizationException` |
| 82 | `NOT_WHITELISTED` | IP not whitelisted | `AuthorizationException` |
| 83 | `QUOTA_EXCEEDED` | Quota exceeded | `QuotaException` |
| 100 | `UDF_BAD_RESPONSE` | UDF returned an error | `UdfException` |
| 120 | `MRT_BLOCKED` | Record blocked by another transaction | `TransactionException` |
| 121 | `MRT_VERSION_MISMATCH` | Transaction version mismatch on commit | `TransactionException` |
| 122 | `MRT_EXPIRED` | Transaction deadline reached | `TransactionException` |
| 123 | `MRT_TOO_MANY_WRITES` | Transaction write limit (4096) exceeded | `TransactionException` |
| 124 | `MRT_COMMITTED` | Transaction already committed | `TransactionException` |
| 125 | `MRT_ABORTED` | Transaction already aborted | `TransactionException` |
| 126 | `MRT_ALREADY_LOCKED` | Record locked by prior update in this transaction | `TransactionException` |
| 127 | `MRT_MONITOR_EXISTS` | Transaction already started (unsafe thread sharing) | `TransactionException` |
| 150 | `BATCH_DISABLED` | Batch functionality disabled | `BatchException` |
| 151 | `BATCH_MAX_REQUESTS_EXCEEDED` | Batch max requests exceeded | `CapacityException` |
| 152 | `BATCH_QUEUES_FULL` | All batch queues full | `CapacityException` |
| 200 | `INDEX_ALREADY_EXISTS` | Secondary index already exists | `IndexException` |
| 201 | `INDEX_NOTFOUND` | Secondary index not found | `IndexException` |
| 202 | `INDEX_OOM` | Secondary index out of memory | `IndexException` |
| 203 | `INDEX_NOTREADABLE` | Secondary index not available | `IndexException` |
| 204 | `INDEX_GENERIC` | Generic secondary index error | `IndexException` |
| 205 | `INDEX_NAME_MAXLEN` | Index name exceeds max length | `IndexException` |
| 206 | `INDEX_MAXCOUNT` | Maximum number of indices exceeded | `IndexException` |
| 210 | `QUERY_ABORTED` | Query aborted | `QueryException` |
| 211 | `QUERY_QUEUEFULL` | Query queue full | `QueryException` |
| 212 | `QUERY_TIMEOUT` | Query timed out on server | `QueryException` |
| 213 | `QUERY_GENERIC` | Generic query error | `QueryException` |
| *other* | — | Unrecognized code | `AerospikeException` |
