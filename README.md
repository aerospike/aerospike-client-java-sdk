# Aerospike New Client API

> [!NOTE]
> This is a developer preview, not yet ready for production use, but with future releases planned. We encourage feedback from the Aerospike 
> developer community through GitHub issues.

A higher-level API for Aerospike that provides type-safe, fluent interfaces for working with Aerospike databases.

## Overview

This project provides a modern, type-safe API for Aerospike operations, including:

- **DataSet**: A fluent API for creating and managing Aerospike keys
- **InfoParser & InfoCommands**: High-level info command execution and parsing
- **TransactionalSession**: Transactional operations with automatic retry logic
- **AEL (Aerospike Expression Language)**: A type-safe query builder with compile-time checking
- **Type Safety**: Compile-time validation to prevent runtime errors
- **Fluent APIs**: Chainable method calls for better readability

## DataSet Class

The `DataSet` class represents a collection of records within an Aerospike namespace. It provides a fluent API for creating Aerospike keys with various identifier types.

### Key Features

- **Multiple Key Types**: Support for String, Integer, Long, and byte array keys
- **Batch Operations**: Create multiple keys at once
- **Type Safety**: Compile-time validation of key types
- **Fluent API**: Chainable method calls for better readability

### Basic Usage

```java
// Create a dataset
DataSet users = DataSet.of("test", "users");

// Create single keys
Key userKey = users.id("user123");
Key userId = users.id(12345L);

// Create multiple keys
List<Key> userKeys = users.ids("user1", "user2", "user3");
List<Key> userIds = users.ids(1, 2, 3, 4, 5);
```

### Supported Key Types

#### String Keys
```java
Key key = users.id("user123");
List<Key> keys = users.ids("user1", "user2", "user3");
```

#### Numeric Keys
```java
// Integer keys
Key key = users.id(123);
List<Key> keys = users.ids(1, 2, 3, 4, 5);

// Long keys
Key key = users.id(12345L);
List<Key> keys = users.ids(1L, 2L, 3L, 4L, 5L);
```

#### Byte Array Keys
```java
byte[] id = {1, 2, 3, 4};
Key key = users.id(id);

// Using subset of byte array
Key key = users.id(id, 1, 2); // Uses bytes at offset 1, length 2
```

#### Object Keys
```java
// Automatically determines the appropriate key type
Key key1 = users.idForObject("user123");     // String key
Key key2 = users.idForObject(123);           // Integer key
Key key3 = users.idForObject(12345L);        // Long key
Key key4 = users.idForObject(new byte[]{1,2,3}); // Byte array key
```

### API Reference

#### Static Methods

- `DataSet.of(String namespace, String set)` - Creates a new DataSet instance

#### Instance Methods

##### Key Creation
- `id(String id)` - Creates a key with String identifier
- `id(int id)` - Creates a key with Integer identifier  
- `id(long id)` - Creates a key with Long identifier
- `id(byte[] id)` - Creates a key with byte array identifier
- `id(byte[] id, int offset, int length)` - Creates a key with subset of byte array
- `idForObject(Object object)` - Creates a key from any supported object type

##### Batch Key Creation
- `ids(String... ids)` - Creates multiple String keys
- `ids(int... ids)` - Creates multiple Integer keys
- `ids(long... ids)` - Creates multiple Long keys
- `ids(byte[]... ids)` - Creates multiple byte array keys
- `ids(List<? extends Object> ids)` - Creates keys from a list of objects

##### Accessors
- `getNamespace()` - Returns the namespace name
- `getSet()` - Returns the set name

### Error Handling

The `idForObject()` method throws `IllegalArgumentException` if the object type is not supported. Supported types are:
- String
- Integer, Long, Byte, Short (converted to Long)
- byte[] arrays

### Examples

#### User Management System
```java
DataSet users = DataSet.of("app", "users");

// Create keys for different user types
Key adminKey = users.id("admin");
Key guestKey = users.id("guest");
Key numericKey = users.id(1001);

// Batch operations
List<Key> userKeys = users.ids("user1", "user2", "user3", "user4");
List<Key> userIds = users.ids(1001, 1002, 1003, 1004);
```

#### Session Management
```java
DataSet sessions = DataSet.of("app", "sessions");

// Create session keys
Key sessionKey = sessions.id("session_abc123");
Key sessionId = sessions.id(987654321L);

// Batch session operations
List<Key> sessionKeys = sessions.ids("session1", "session2", "session3");
```

## InfoParser Class

The `InfoParser` class provides functionality to parse Aerospike info command responses and convert them into structured Java objects.

### Key Features

- **Multiple Response Types**: Handles both single-item and multiple-item info responses
- **Node Aggregation**: Merges data from multiple nodes in a cluster
- **Type Safety**: Converts raw strings to strongly-typed Java objects
- **Error Handling**: Graceful handling of parsing errors and node failures

### Response Types

#### Single-Item Responses
Key-value pairs separated by semicolons:
```
key1=value1;key2=value2;key3=value3
```

#### Multiple-Item Responses
Multiple records separated by semicolons, each containing key-value pairs separated by colons:
```
key1=value1:key2=value2;key3=value3:key4=value4
```

### Basic Usage

```java
InfoParser parser = new InfoParser();

// Parse single item info
Optional<NamespaceDetail> namespace = parser.getInfoForSingleItem(session, NamespaceDetail.class, "namespace/test");

// Parse multiple items info
List<Sindex> indexes = parser.getInfoForMultipleItems(session, Sindex.class, "sindex-list");

// Get per-node results
Map<Node, Optional<NamespaceDetail>> perNode = parser.getInfoForSingleItemPerNode(session, NamespaceDetail.class, "namespace/test");
```

### API Reference

#### Single Item Methods
- `getInfoForSingleItem(Session, Class<T>, String)` - Gets and merges single item from all nodes
- `getInfoForSingleItemPerNode(Session, Class<T>, String)` - Gets single item from each node separately

#### Multiple Items Methods
- `getInfoForMultipleItems(Session, Class<T>, String)` - Gets and merges multiple items from all nodes
- `getInfoForMultipleItemsPerNode(Session, Class<T>, String)` - Gets multiple items from each node separately

#### Utility Methods
- `mergeCommaSeparatedLists(Session, String)` - Merges comma-separated lists from all nodes

## InfoCommands Class

The `InfoCommands` class provides high-level methods to execute common Aerospike info commands.

### Key Features

- **Common Commands**: Encapsulates frequently used info commands
- **Dual Modes**: Supports both aggregated and per-node results
- **Type Safety**: Returns strongly-typed objects instead of raw strings
- **Convenience Methods**: Overloaded methods for different parameter types

### Supported Commands

- **Build Information**: Get build details from all nodes
- **Namespace Details**: Get detailed information about specific namespaces
- **Set Information**: Get information about all sets
- **Secondary Index Information**: Get details about secondary indexes

### Basic Usage

```java
InfoCommands commands = new InfoCommands(session);

// Get all namespaces
Set<String> namespaces = commands.namespaces();

// Get namespace details
Optional<NamespaceDetail> nsDetail = commands.namespaceDetails("test");

// Get all secondary indexes
List<Sindex> indexes = commands.secondaryIndexes();

// Get per-node results
Map<Node, List<SetDetail>> setsPerNode = commands.setsPerNode();
```

### API Reference

#### Namespace Operations
- `namespaces()` - Get all namespace names
- `namespaceDetails(String)` - Get detailed namespace information (merged)
- `namespaceDetailsPerNode(String)` - Get namespace details per node

#### Set Operations
- `sets()` - Get all set information (merged)
- `setsPerNode()` - Get set information per node

#### Secondary Index Operations
- `secondaryIndexes()` - Get all secondary indexes (merged)
- `secondaryIndexesPerNode()` - Get secondary indexes per node
- `secondaryIndexDetails(String, String)` - Get specific index details (merged)
- `secondaryIndexDetailsPerNode(String, String)` - Get index details per node
- `secondaryIndexDetails(Sindex)` - Get index details using Sindex object
- `secondaryIndexDetailsPerNode(Sindex)` - Get index details per node using Sindex object

#### System Information
- `build()` - Get build information from all nodes

## TransactionalSession Class

The `TransactionalSession` class extends the base Session to provide transactional operations with automatic retry logic.

### Key Features

- **Automatic Retry**: Retries on transient failures (MRT_BLOCKED, MRT_VERSION_MISMATCH, TXN_FAILED)
- **Nested Transactions**: Supports nested transaction calls without creating multiple contexts
- **Resource Cleanup**: Automatically aborts transactions on exceptions
- **Dual Operations**: Supports both void and value-returning operations

### Retryable Errors

The following Aerospike result codes trigger automatic retry:
- `MRT_BLOCKED` - Multi-record transaction blocked
- `MRT_VERSION_MISMATCH` - Multi-record transaction version mismatch
- `TXN_FAILED` - Transaction failed

### Basic Usage

```java
TransactionalSession session = new TransactionalSession(cluster, behavior);

// Void transaction
session.doInTransaction(txSession -> {
    // Perform operations within transaction
    txSession.put(key, bin);
});

// Value-returning transaction
String result = session.doInTransaction(txSession -> {
    Record record = txSession.get(key);
    return record.getString("value");
});
```

### API Reference

#### Transaction Methods
- `doInTransaction(Transactional<T>)` - Execute a value-returning transaction
- `doInTransaction(TransactionalVoid)` - Execute a void transaction
- `getCurrentTransaction()` - Get the current transaction object

### Error Handling

- **Retryable Errors**: Automatically retried with exponential backoff
- **Non-Retryable Errors**: Thrown immediately without retry
- **Resource Cleanup**: Transactions are automatically aborted on any exception

## AEL (Aerospike Expression Language)

The project also includes a comprehensive AEL for building type-safe queries with compile-time checking. See the AEL examples in the `com.aerospike.ael` package for more information.

## Building and Running

```bash
# Compile the project
mvn compile

# Run tests
mvn test

# Generate Javadoc
mvn javadoc:javadoc
```

## Dependencies

- Java 21+  For improved virtual thread performance Java 24+ is highly recommended.
- Maven 3.9.5+
