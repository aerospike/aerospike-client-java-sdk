# AerospikeException Hierarchy

All exceptions are inner classes of `AerospikeException` and carry the original `resultCode` for backward compatibility.

```
AerospikeException
├── RecordNotFoundException         Record does not exist (read, touch, update, replace)
├── RecordExistsException           Record already exists on a create-only (insert) operation
├── GenerationException             Generation mismatch — optimistic locking conflict
├── FilteredException               Operation skipped because filter expression was false
├── RecordTooBigException           Record exceeds the server's size limit
│
├── BinException                    Bin-level error (parent for bin sub-types)
│   ├── BinExistsException          Bin already exists on a create-only bin operation
│   ├── BinNotFoundException        Bin not found on an update-only bin operation
│   ├── BinTypeException            Operation incompatible with the bin's data type
│   └── BinOpInvalidException       Operation cannot be applied to the bin's current value
│
├── ElementException                CDT element-level error (parent for element sub-types)
│   ├── ElementNotFoundException    Map key / list element not found in update-only mode
│   └── ElementExistsException      Map key / list element already exists in create-only mode
│
├── TransactionException            Multi-record transaction (MRT) error
│   └── Commit                      Transaction commit failed (includes verify/roll details)
│
├── SecurityException               Security-related error (parent for auth sub-types)
│   ├── AuthenticationException     Invalid user, password, credential, or expired session
│   └── AuthorizationException      User lacks the required role or is not whitelisted
│
├── QuotaException                  Quota exceeded or quota configuration error
│
├── CapacityException               Server or client resource exhaustion
│   └── KeyBusyException            Hot key — too many concurrent operations on one record
│
├── IndexException                  Secondary index error (not found, already exists, OOM, etc.)
├── QueryException                  Server-side query/scan error (aborted, timed out, queue full)
├── BatchException                  Batch operation error (partial failure or batch disabled)
├── UdfException                    User-defined function returned an error
│
├── Timeout                         Operation timed out (client or server initiated)
├── Connection                      Cannot connect to the server
├── InvalidNode                     Target node is not active
├── Serialize                       Client serialization error
├── Parse                           Client cannot parse server response
├── InvalidNamespace                Namespace not found in the cluster
├── QueryTerminated                 Query terminated by the client
└── Backoff                         Node in backoff mode due to excessive errors
```

## Catching Exceptions

Catch specific exceptions for programmatic handling:

```java
try {
    session.insert(set.id(1)).bin("name").setTo("Bob").execute();
}
catch (AerospikeException.RecordExistsException e) {
    // Record already exists — update instead, or skip
}
catch (AerospikeException.RecordTooBigException e) {
    // Payload too large — reduce data or split across records
}
```

Use parent classes for broader catches:

```java
catch (AerospikeException.BinException e) {
    // Any bin-level error (type mismatch, not found, already exists, etc.)
}
catch (AerospikeException.TransactionException e) {
    // Any MRT error (blocked, expired, version conflict, commit failure, etc.)
}
catch (AerospikeException.CapacityException e) {
    // Server or client at capacity — back off and retry
}
```

The `resultCode` field is always available for fine-grained inspection:

```java
catch (AerospikeException.TransactionException e) {
    if (e.getResultCode() == ResultCode.MRT_EXPIRED) {
        // Transaction deadline exceeded
    }
}
```

## ResultCode Mapping

| Exception | ResultCode(s) |
|---|---|
| `RecordNotFoundException` | `KEY_NOT_FOUND_ERROR` (2) |
| `RecordExistsException` | `KEY_EXISTS_ERROR` (5) |
| `GenerationException` | `GENERATION_ERROR` (3) |
| `FilteredException` | `FILTERED_OUT` (27) |
| `RecordTooBigException` | `RECORD_TOO_BIG` (13) |
| `BinException` | `BIN_NAME_TOO_LONG` (21) |
| `BinExistsException` | `BIN_EXISTS_ERROR` (6) |
| `BinNotFoundException` | `BIN_NOT_FOUND` (17) |
| `BinTypeException` | `BIN_TYPE_ERROR` (12) |
| `BinOpInvalidException` | `OP_NOT_APPLICABLE` (26) |
| `ElementNotFoundException` | `ELEMENT_NOT_FOUND` (23) |
| `ElementExistsException` | `ELEMENT_EXISTS` (24) |
| `TransactionException` | `TXN_ALREADY_ABORTED` (-19), `TXN_ALREADY_COMMITTED` (-18), `MRT_BLOCKED` (120), `MRT_VERSION_MISMATCH` (121), `MRT_EXPIRED` (122), `MRT_TOO_MANY_WRITES` (123), `MRT_COMMITTED` (124), `MRT_ABORTED` (125), `MRT_ALREADY_LOCKED` (126), `MRT_MONITOR_EXISTS` (127) |
| `Commit` | `TXN_FAILED` (-17) |
| `AuthenticationException` | `INVALID_USER` (60), `INVALID_PASSWORD` (62), `INVALID_CREDENTIAL` (65), `EXPIRED_PASSWORD` (63), `NOT_AUTHENTICATED` (80) |
| `AuthorizationException` | `ROLE_VIOLATION` (81), `NOT_WHITELISTED` (82) |
| `SecurityException` | `ILLEGAL_STATE` (56), `USER_ALREADY_EXISTS` (61), `FORBIDDEN_PASSWORD` (64), `SECURITY_NOT_ENABLED` (52), `SECURITY_NOT_SUPPORTED` (51), `SECURITY_SCHEME_NOT_SUPPORTED` (53), `EXPIRED_SESSION` (66), `INVALID_ROLE` (70), `ROLE_ALREADY_EXISTS` (71), `INVALID_PRIVILEGE` (72), `INVALID_WHITELIST` (73) |
| `QuotaException` | `QUOTA_EXCEEDED` (83), `QUOTAS_NOT_ENABLED` (74), `INVALID_QUOTA` (75) |
| `KeyBusyException` | `KEY_BUSY` (14) |
| `CapacityException` | `SERVER_MEM_ERROR` (8), `DEVICE_OVERLOAD` (18), `NO_MORE_CONNECTIONS` (-7), `ASYNC_QUEUE_FULL` (-9), `BATCH_QUEUES_FULL` (152), `BATCH_MAX_REQUESTS_EXCEEDED` (151) |
| `IndexException` | `INDEX_ALREADY_EXISTS` (200), `INDEX_NOTFOUND` (201), `INDEX_OOM` (202), `INDEX_NOTREADABLE` (203), `INDEX_GENERIC` (204), `INDEX_NAME_MAXLEN` (205), `INDEX_MAXCOUNT` (206) |
| `QueryException` | `QUERY_ABORTED` (210), `QUERY_QUEUEFULL` (211), `QUERY_TIMEOUT` (212), `QUERY_GENERIC` (213), `SCAN_ABORT` (15) |
| `BatchException` | `BATCH_FAILED` (-16), `BATCH_DISABLED` (150) |
| `UdfException` | `UDF_BAD_RESPONSE` (100) |

All other result codes map to the base `AerospikeException`.
