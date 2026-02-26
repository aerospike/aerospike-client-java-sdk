/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.fluent;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.aerospike.client.fluent.command.QueryCommand;
import com.aerospike.client.fluent.query.RecordStreamImpl;
import com.aerospike.client.fluent.query.SingleItemRecordStream;

public class RecordStream implements Iterator<RecordResult>, Closeable {
    private final RecordStreamImpl impl;

    // ========================================
    // Unconsumed error stream detection (disabled)
    // ========================================
    // At millions of TPS, Cleaner registration per-stream is too costly.
    // This implements lazy registration: only streams that actually contain
    // errors register with the Cleaner. If such a stream is GC'd without
    // being iterated, a warning is logged -- similar to how CompletableFuture
    // reports unhandled exceptions.
    //
    // To enable:
    //   1. Uncomment the block below.
    //   2. Have builders call rs.markContainsErrors() when embedding error
    //      results into a stream under IN_STREAM disposition.
    //   3. Uncomment the markConsumed() call in hasNext().
    //
    // private static final java.lang.ref.Cleaner CLEANER = java.lang.ref.Cleaner.create();
    //
    // /**
    //  * Shared state between the RecordStream and its Cleaner action.
    //  * MUST be a separate object -- the cleaning action must never capture
    //  * a reference to the RecordStream itself, or it can never become
    //  * phantom-reachable and the cleaner will never fire.
    //  */
    // private static class CleanerState implements Runnable {
    //     volatile boolean consumed;
    //     final String creationSite;
    //
    //     CleanerState(String creationSite) {
    //         this.creationSite = creationSite;
    //     }
    //
    //     @Override
    //     public void run() {
    //         if (!consumed) {
    //             Log.warn("RecordStream with errors was garbage-collected without being " +
    //                 "consumed. Errors may have been silently lost. Created at: " + creationSite);
    //         }
    //     }
    // }
    //
    // private CleanerState cleanerState;
    //
    // /**
    //  * Mark this stream as containing error results. On the first call,
    //  * registers with the Cleaner so that a warning is logged if the stream
    //  * is GC'd without ever being iterated.
    //  */
    // public void markContainsErrors() {
    //     if (cleanerState == null) {
    //         StackTraceElement caller = Thread.currentThread().getStackTrace()[2];
    //         cleanerState = new CleanerState(caller.toString());
    //         CLEANER.register(this, cleanerState);
    //     }
    // }
    //
    // private void markConsumed() {
    //     if (cleanerState != null) {
    //         cleanerState.consumed = true;
    //     }
    // }

    /**
     * Creates an empty RecordStream with no implementation.
     * This constructor is typically used internally or for creating an empty stream.
     */
    public RecordStream() {impl = null;}

    /**
     * Creates a RecordStream containing a single RecordResult.
     *
     * @param rec the single record result to include in the stream
     */
    public RecordStream(RecordResult rec) {
        impl = new SingleItemRecordStream(rec);
    }

    /**
     * Creates a RecordStream containing a single record from a key and record pair.
     *
     * @param key the key of the record
     * @param record the record data
     */
    public RecordStream(Key key, Record record) {
        RecordResult rec = new RecordResult(key, record, 0); // Single item, index = 0
        impl = new SingleItemRecordStream(rec);
    }
    /**
     * Creates a RecordStream from a list of RecordResult objects.
     * This is typically used for batch query results.
     *
     * @param records the list of results
     * @param limit the maximum number of records to include (0 or negative means no limit)
     * @param respondAllKeys if false, null records are filtered out
     */
    public RecordStream(List<RecordResult> records, long limit) {
        AsyncRecordStream asyncStream = new AsyncRecordStream(Math.max(100, records.size()));

        // Filter and limit records
        int count = 0;
        for (RecordResult record : records) {
            if (limit <= 0 || count < limit) {
                asyncStream.publish(record);
                count++;
            }
        }
        asyncStream.complete();
        impl = asyncStream;
    }
    /**
     * Creates a RecordStream from an AsyncRecordStream.
     * This constructor is used internally to wrap asynchronous record streams.
     *
     * @param asyncStream the asynchronous record stream to wrap
     */
    public RecordStream(AsyncRecordStream asyncStream) {
        impl = asyncStream;
    }

    /**
     * Creates a RecordStream for index/scan queries with server-side chunking.
     *
     * <p>This constructor is used for queries that stream results from the server in chunks.
     * For client-side sorting and pagination, use {@link #asNavigatableStream()} on the
     * returned stream.</p>
     */
    public RecordStream(AsyncRecordStream stream, QueryCommand cmd, long limit, int recordQueueSize) {
        if (limit <= 0) {
            limit = Long.MAX_VALUE;
        }

        impl = new ChunkedRecordStream(stream, cmd, limit, recordQueueSize);
    }

    /**
     * Checks if there are more chunks available from the server.
     *
     * <p>This method is used for server-side streaming pagination (chunks).
     * Returns true if more data chunks are available from the server.</p>
     *
     * <p><b>Note:</b> This is distinct from client-side pagination (pages) provided by
     * {@link NavigatableRecordStream}. Use {@link #asNavigatableStream()} for
     * client-side sorting and bi-directional pagination.</p>
     *
     * @return true if more chunks are available, false otherwise
     */
    public boolean hasMoreChunks() {
        return impl == null ? false : impl.hasMoreChunks();
    }

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext() {
        // markConsumed();  // uncomment to enable unconsumed-error-stream detection
        return impl == null ? false : impl.hasNext();
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     */
    @Override
    public RecordResult next() {
        return impl == null ? null : impl.next();
    }

    /**
     * Convert the elements in this RecordStream into a Java Stream class. Note that this loses
     * pagination information, all the records are accessible through the Stream.
     * @return
     */
    public Stream<RecordResult> stream() {
        Stream<RecordResult> records = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                        this, Spliterator.NONNULL | Spliterator.IMMUTABLE), false);
        records.onClose(() -> {
            this.close();
        });
        return records;
    }

    /**
     * Filter the stream to return only failed operations. A failed operation is one where
     * the result code is not {@link ResultCode#OK}.
     * <p>
     * This method consumes the current stream and returns a new RecordStream containing
     * only the records with non-OK result codes. Useful for error handling and debugging.
     * <p>
     * Example usage:
     * <pre>
     * RecordStream results = session.update(keys).bin("name").setTo("value").execute();
     * RecordStream failures = results.failures();
     * failures.forEach(failure -> {
     *     System.err.println("Failed for key: " + failure.key() +
     *                        ", reason: " + failure.message());
     * });
     * </pre>
     *
     * @return A new RecordStream containing only records with resultCode != OK
     */
    public RecordStream failures() {
        List<RecordResult> failedRecords = new ArrayList<>();

        while (this.hasNext()) {
            RecordResult result = this.next();
            if (result.resultCode() != ResultCode.OK) {
                failedRecords.add(result);
            }
        }

        // Return new RecordStream with filtered results
        return new RecordStream(failedRecords, 0L);
    }

    /**
     * Return the records from the current page as a list of entities.
     * @param <T>
     * @param mapper
     * @return
     */
    public <T> List<T> toObjectList(RecordMapper<T> mapper) {
        // TODO: What should happen if there is an exception in the stream of records? At the moment it is just thrown
        // to the detriment of the other recods
        List<T> result = new ArrayList<>();
        while (hasNext()) {
            RecordResult keyRecord = next();
            Record rec = keyRecord.recordOrThrow();
            result.add(mapper.fromMap(rec.bins, keyRecord.key(), rec.generation));
        }
        return result;
    }

    /**
     * Converts this RecordStream into a NavigatableRecordStream for in-memory sorting and pagination.
     *
     * <p>This method reads all records from the current stream into memory and returns a
     * NavigatableRecordStream that provides builder-style APIs for sorting and pagination.
     * This is useful when you need to sort results after fetching them from the database,
     * or when you want to paginate through results in a different way than the original query.</p>
     *
     * <p>Example usage:</p>
     * <pre>
     * RecordStream results = session.query(customerDataSet).execute();
     * NavigatableRecordStream navigatable = results.asNavigatableStream()
     *     .pageSize(20)
     *     .sortBy(List.of(
     *         SortProperties.ascending("name"),
     *         SortProperties.descending("age")
     *     ));
     *
     * // Iterate through pages
     * while (navigatable.hasMorePages()) {
     *     while (navigatable.hasNext()) {
     *         RecordResult record = navigatable.next();
     *         // Process record
     *     }
     * }
     * </pre>
     *
     * <p><b>Warning:</b> This method loads all records into memory. For large result sets,
     * consider using the limit parameter in {@link #asNavigatableStream(long)} to avoid
     * excessive memory usage.</p>
     *
     * @return a NavigatableRecordStream containing all records from this stream
     */
    public NavigatableRecordStream asNavigatableStream() {
        return new NavigatableRecordStream(this);
    }

    /**
     * Converts this RecordStream into a NavigatableRecordStream with a record limit.
     *
     * <p>This method reads records from the current stream into memory up to the specified
     * limit and returns a NavigatableRecordStream that provides builder-style APIs for
     * sorting and pagination.</p>
     *
     * <p>Example usage:</p>
     * <pre>
     * RecordStream results = session.query(customerDataSet).execute();
     * NavigatableRecordStream navigatable = results.asNavigatableStream(1000)
     *     .pageSize(20)
     *     .sortBy(SortProperties.descending("age"));
     * </pre>
     *
     * @param limit the maximum number of records to load into memory (0 or negative means no limit)
     * @return a NavigatableRecordStream containing up to limit records from this stream
     */
    public NavigatableRecordStream asNavigatableStream(long limit) {
        return new NavigatableRecordStream(this, limit);
    }

    /**
     * Performs the given action for each element of the stream until all elements
     * have been processed or the action throws an exception.
     *
     * @param consumer the action to be performed for each element
     */
    public void forEach(Consumer<RecordResult> consumer) {
        while (hasNext()) {
            consumer.accept(next());
        }
    }

    /**
     * Searches the stream for a record with the specified key and returns it if found.
     * Note that searching through the stream consumes elements, which may not be replayable
     * if the stream is not generated from a {@code Key} or {@code List<Key>}.
     *
     * @param key the key to search for
     * @return an Optional containing the record if found, or empty if not found or if the result code is not OK
     * @throws com.aerospike.client.AerospikeException if the record exists but the result code is not OK
     */
    public Optional<Record> get(Key key) {
        while (hasNext()) {
            RecordResult kr = next();
            if (kr.key().equals(key)) {
                return Optional.of(kr.recordOrThrow());
            }
        }
        return Optional.empty();
    }

    /**
     * Find a particular key in the stream and return the data associated with that key, or {@code Optional.empty}
     * if the key doesn't exist. Note that if the stream is not generated from a {@code Key} or {@code List<Key>}
     * then finding the key will consume elements in the stream which may not be able to be replayed.
     * @param <T> - The type of the object to be returned.
     * @param key - The key of the record
     * @param mapper - The mapper to use to convert the record to the class
     * @return An optional containing the data or empty. If the result code is not OK, an exception will be thrown
     */
    public <T> Optional<T> get(Key key, RecordMapper<T> mapper) {
        while (hasNext()) {
            RecordResult thisRecord = next();
            if (thisRecord.key().equals(key)) {
                Record rec = thisRecord.recordOrThrow();
                return Optional.of(mapper.fromMap(rec.bins, thisRecord.key(), rec.generation));
            }
        }
        return Optional.empty();
    }

    /**
     * Convenience method to get the first record out of the record stream. If the record is not present, {@code null}
     * will be returned. If an exception has occurred, the exception will be thrown.
     * <p/>
     * @return
     */
    public Record getFirstRecord() {
        return hasNext() ? next().recordOrThrow() : null;
    }
    /**
     * Get the first element from the stream. If this element failed for any reason, an exception is thrown.
     * @return the first element in the stream
     */
    public Optional<RecordResult> getFirst() {
        return this.getFirst(true);
    }

    /**
     * Get the first element from the stream. If this element failed for any reason and "throwException" is true,
     * an appropriate exception is thrown.
     * @param throwException - If this is true and the resultCode != OK, an exception is thrown. If this is false,
     * no exception is thrown, but the resultCode() in the response must be consulted to see if the call was successful or not.
     * @return the first element in the stream
     */
    public Optional<RecordResult> getFirst(boolean throwException) {
        if (hasNext()) {
            if (throwException) {
                return Optional.of(next().orThrow());
            }
            else {
            return Optional.of(next());
            }
        }
        return Optional.empty();
    }

    /**
     * Get the first element from the stream. If this element failed for any reason, an exception is thrown.
     * @return the first element in the stream
     */
    public <T> Optional<T> getFirst(RecordMapper<T> mapper) {
        if (hasNext()) {
            RecordResult item = next();
            Record rec = item.recordOrThrow();
            return Optional.of(mapper.fromMap(rec.bins, item.key(), item.recordOrThrow().generation));
        }
        return Optional.empty();
    }

    /**
     * Gets the first element from the stream and converts it to a Boolean value.
     * If the stream is empty, returns an empty Optional.
     *
     * @return an Optional containing the first element as a Boolean, or empty if the stream is empty
     */
    public Optional<Boolean> getFirstBoolean() {
        if (hasNext()) {
            return Optional.of(next().asBoolean());
        }
        return Optional.empty();
    }
    
    /**
     * Gets the first element from the stream and extract the UDF Result (object) from this. 
     * If the stream is empty, returns an empty Optional.
     *
     * @return an Optional containing the result from the UDF invocation of the first result or empty if the stream is empty
     */
    public Optional<Object> getFirstUdfResult() {
        if (hasNext()) {
            return Optional.of(next().udfResultOrThrow());
        }
        return Optional.empty();
    }

    /**
     * Gets the first element from the stream and extract the UDF Result (object) from this. This must return a map, and that map
     * will be converted into an object using the passed mapper.
     * If the stream is empty, returns an empty Optional.
     *
     * @return an Optional containing the result from the UDF invocation of the first result or empty if the stream is empty
     * @throws AerospikeException with ResultCode = OP_NOT_APPLICABLE if the return value of the UDF is not a map, or other
     * AerospikeException subclasses based on the ResultCode from the server if other things went wrong.
     */
    public <T> Optional<T> getFirstUdfResult(RecordMapper<T> mapper) {
        if (hasNext()) {
            return Optional.of(next().udfResultAs(mapper));
        }
        return Optional.empty();
    }

    public static class ObjectWithMetadata<T> {
        private final int generation;
        private final int expiration;
        private final T object;
        public ObjectWithMetadata(T object, Record rec) {
            this.object = object;
            this.generation = rec.generation;
            this.expiration = rec.expiration;
        }

        public T get() {
            return object;
        }

        public int getExpiration() {
            return expiration;
        }

        public int getGeneration() {
            return generation;
        }
    }
    /**
     * Gets the first element from the stream, maps it to an object using the provided mapper,
     * and returns it along with its metadata (generation and expiration).
     *
     * <p>This method is useful when you need both the mapped object and its record metadata
     * (generation and expiration) from the first record in the stream.</p>
     *
     * @param <T> the type of the object to be returned
     * @param mapper the mapper to use to convert the record to the class
     * @return an Optional containing an ObjectWithMetadata with the mapped object and its metadata,
     *         or empty if the stream is empty
     * @throws com.aerospike.client.AerospikeException if the result code is not OK
     */
    public <T> Optional<ObjectWithMetadata<T>> getFirstWithMetadata(RecordMapper<T> mapper) {
        if (hasNext()) {
            RecordResult item = next();
            Record rec = item.recordOrThrow();
            T object = mapper.fromMap(rec.bins, item.key(), rec.generation);
            return Optional.of(new ObjectWithMetadata<T>(object, rec));
        }
        return Optional.empty();
    }


    /**
     * Closes this stream, releasing any underlying resources.
     * After closing, the stream should not be used further.
     */
    @Override
    public void close() {
        if (impl != null) {
            impl.close();
        }
    }
}
