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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.aerospike.client.fluent.query.RecordComparator;
import com.aerospike.client.fluent.query.ResettablePagination;
import com.aerospike.client.fluent.query.SortDir;
import com.aerospike.client.fluent.query.SortProperties;

/**
 * A navigatable record stream that loads all records into memory and provides
 * sorting and pagination capabilities.
 * 
 * <p>This class is useful when you need to sort and paginate through results
 * after they have been fetched from the database. It provides a builder-style
 * API for configuring sort order and page size.</p>
 * 
 * <h2>Sorting Behavior</h2>
 * 
 * <p>Each call to any {@code sortBy()} method replaces the entire sort criteria.
 * This keeps the API simple and predictable.</p>
 * 
 * <h3>Single-Column Sorting</h3>
 * <pre>{@code
 * navigatable.sortBy("name");                           // Sort by name (ascending)
 * navigatable.sortBy("age", SortDir.SORT_DESC);        // Sort by age (descending)
 * navigatable.sortBy("name", SortDir.SORT_ASC, false); // Case-insensitive
 * }</pre>
 * 
 * <h3>Multi-Column Sorting</h3>
 * <p>For multi-column sorting, use the method that accepts a list with static factory methods:</p>
 * <pre>{@code
 * navigatable.sortBy(List.of(
 *     SortProperties.ascending("name"),      // Primary sort
 *     SortProperties.descending("age")       // Secondary sort
 * ));
 * }</pre>
 * 
 * <h3>Dynamic Re-sorting</h3>
 * <p>Change sort order without re-querying the database:</p>
 * <pre>{@code
 * navigatable.sortBy("age", SortDir.SORT_DESC);  // Sort by age
 * // ... process some pages ...
 * navigatable.sortBy("name");  // Re-sort by name (replaces age sort)
 * navigatable.reset();  // Reset to beginning with new sort
 * }</pre>
 * 
 * <h2>Example Usage</h2>
 * <pre>{@code
 * RecordStream results = session.query(customerDataSet).execute();
 * NavigatableRecordStream navigatable = results.asNavigatableStream()
 *     .pageSize(20)
 *     .sortBy("age", SortDir.SORT_DESC);
 * 
 * // Iterate through pages
 * while (navigatable.hasMorePages()) {
 *     while (navigatable.hasNext()) {
 *         RecordResult record = navigatable.next();
 *         // Process record
 *     }
 * }
 * 
 * // Change sort without re-querying database
 * navigatable.sortBy("name");  // Replaces previous sort
 * navigatable.reset();
 * // Process pages with new sort order
 * }</pre>
 */
public class NavigatableRecordStream implements ResettablePagination, Closeable {
    private final RecordResult[] records;
    private int pageSize = 0;
    private int numPages = 1;
    private int currentPage = -1;
    private int index = 0;
    private List<SortProperties> sortInfo = null;
    
    /**
     * Creates a NavigatableRecordStream from an existing RecordStream.
     * 
     * <p>This constructor reads all records from the source stream into memory.
     * Once created, the original stream is closed.</p>
     * 
     * @param source the source RecordStream to read from
     */
    public NavigatableRecordStream(RecordStream source) {
        this(source, 0);
    }
    
    /**
     * Creates a NavigatableRecordStream from an existing RecordStream with a limit.
     * 
     * <p>This constructor reads records from the source stream into memory up to
     * the specified limit. Once the limit is reached or the stream is exhausted,
     * the original stream is closed.</p>
     * 
     * @param source the source RecordStream to read from
     * @param limit the maximum number of records to read (0 or negative means no limit)
     */
    public NavigatableRecordStream(RecordStream source, long limit) {
        List<RecordResult> recordList = new ArrayList<>();
        
        try {
            int count = 0;
            while (source.hasNext() && (limit <= 0 || count < limit)) {
                recordList.add(source.next());
                count++;
            }
        } finally {
            source.close();
        }
        
        this.records = recordList.toArray(new RecordResult[0]);
        recalculatePages();
    }
    
    /**
     * Sets the page size for pagination.
     * 
     * <p>This method configures how many records are returned per page.
     * If set to 0 or not called, all records are considered a single page.</p>
     * 
     * @param pageSize the number of records per page (must be > 0, or 0 for no pagination)
     * @return this NavigatableRecordStream for method chaining
     * @throws IllegalArgumentException if pageSize is negative
     */
    public NavigatableRecordStream pageSize(int pageSize) {
        if (pageSize < 0) {
            throw new IllegalArgumentException("Page size must be >= 0, not " + pageSize);
        }
        this.pageSize = pageSize;
        recalculatePages();
        resetIteration();
        return this;
    }
    
    /**
     * Sorts the records by a single field in ascending order with case sensitivity.
     * 
     * <p>This method <b>replaces</b> any existing sort criteria. For multi-column
     * sorting, use {@link #sortBy(List)}.</p>
     * 
     * @param field the field name to sort by
     * @return this NavigatableRecordStream for method chaining
     */
    public NavigatableRecordStream sortBy(String field) {
        return sortBy(field, SortDir.SORT_ASC, true);
    }
    
    /**
     * Sorts the records by a single field in ascending order with specified case sensitivity.
     * 
     * <p>This method <b>replaces</b> any existing sort criteria.</p>
     * 
     * @param field the field name to sort by
     * @param caseInsensitive true for case-insensitive sorting, false for case-sensitive
     * @return this NavigatableRecordStream for method chaining
     */
    public NavigatableRecordStream sortBy(String field, boolean caseInsensitive) {
        return sortBy(field, SortDir.SORT_ASC, caseInsensitive);
    }
    
    /**
     * Sorts the records by a single field with specified direction.
     * 
     * <p>This method <b>replaces</b> any existing sort criteria.</p>
     * 
     * @param field the field name to sort by
     * @param sortDir the sort direction (ascending or descending)
     * @return this NavigatableRecordStream for method chaining
     */
    public NavigatableRecordStream sortBy(String field, SortDir sortDir) {
        return sortBy(field, sortDir, true);
    }
    
    /**
     * Sorts the records by a single field with specified direction and case sensitivity.
     * 
     * <p>This method <b>replaces</b> any existing sort criteria. For multi-column
     * sorting, use {@link #sortBy(List)}.</p>
     * 
     * <p>Example:</p>
     * <pre>{@code
     * navigatable.sortBy("age", SortDir.SORT_DESC, true);
     * }</pre>
     * 
     * @param field the field name to sort by
     * @param sortDir the sort direction (ascending or descending)
     * @param caseSensitive true for case-sensitive sorting, false for case-insensitive
     * @return this NavigatableRecordStream for method chaining
     */
    public NavigatableRecordStream sortBy(String field, SortDir sortDir, boolean caseSensitive) {
        if (sortDir == null) {
            sortDir = SortDir.SORT_ASC;
        }
        this.sortInfo = List.of(new SortProperties(field, sortDir, caseSensitive));
        applySort();
        return this;
    }
    
    /**
     * Sorts the records by multiple sort properties.
     * 
     * <p>This method <b>replaces</b> any existing sort criteria with the new list.
     * The records are immediately re-sorted and iteration is reset. This is the
     * recommended way to sort by multiple columns.</p>
     * 
     * <p>Example:</p>
     * <pre>{@code
     * navigatable.sortBy(List.of(
     *     new SortProperties("lastName", SortDir.SORT_ASC, true),   // Primary sort
     *     new SortProperties("firstName", SortDir.SORT_ASC, true),  // Secondary sort
     *     new SortProperties("age", SortDir.SORT_DESC, true)        // Tertiary sort
     * ));
     * }</pre>
     * 
     * @param sortPropertyList the list of sort properties to apply
     * @return this NavigatableRecordStream for method chaining
     */
    public NavigatableRecordStream sortBy(List<SortProperties> sortPropertyList) {
        this.sortInfo = sortPropertyList;
        applySort();
        return this;
    }
    
    /**
     * Sorts the records by multiple sort properties.
     * 
     * <p>This method <b>replaces</b> any existing sort criteria with the new list.
     * The records are immediately re-sorted and iteration is reset. This is the
     * recommended way to sort by multiple columns.</p>
     * 
     * <p>Example:</p>
     * <pre>{@code
     * navigatable.sortBy(
     *     SortProperties.ascendingIgnoreCase("lastName"),    // Primary sort
     *     SortProperties.descendingIgnoreCase("firstName"),  // Secondary sort
     *     SortProperties.descending("age")                   // Tertiary sort
     * );
     * }</pre>
     * 
     * @param sortPropertyList the list of sort properties to apply
     * @return this NavigatableRecordStream for method chaining
     */
    public NavigatableRecordStream sortBy(SortProperties ... sortPropertyList) {
        this.sortInfo = Arrays.asList(sortPropertyList);
        applySort();
        return this;
    }
    
    /**
     * Sorts the records by a single sort property.
     * 
     * <p>This method <b>replaces</b> any existing sort criteria with the new property.
     * The records are immediately re-sorted and iteration is reset.</p>
     * 
     * @param sortProperty the sort property to apply
     * @return this NavigatableRecordStream for method chaining
     */
    public NavigatableRecordStream sortBy(SortProperties sortProperty) {
        this.sortInfo = List.of(sortProperty);
        applySort();
        return this;
    }
    
    /**
     * Applies the current sort criteria to the records.
     */
    private void applySort() {
        if (sortInfo != null && !sortInfo.isEmpty()) {
            Arrays.sort(this.records, new RecordComparator(sortInfo));
        }
        resetIteration();
    }
    
    /**
     * Recalculates the number of pages based on current page size.
     */
    private void recalculatePages() {
        this.numPages = (pageSize > 0) ? ((records.length + pageSize - 1) / pageSize) : 1;
    }
    
    /**
     * Resets iteration to the beginning.
     */
    private void resetIteration() {
        this.currentPage = -1;
        this.index = 0;
    }
    
    /**
     * Checks if there are more pages available.
     * 
     * <p>Calling this method advances to the next page if there are more pages.
     * This is typically used in a loop to iterate through all pages.</p>
     * 
     * @return true if there are more pages, false otherwise
     */
    public boolean hasMorePages() {
        if (currentPage == -1) {
            currentPage = 0;
            return true;
        } else {
            return (++currentPage) < numPages;
        }
    }
    
    /**
     * Checks if there are more records on the current page.
     * 
     * @return true if there are more records, false otherwise
     */
    public boolean hasNext() {
        if (index >= records.length) {
            return false;
        }
        long pageOfIndex = pageSize == 0 ? 0 : (index / pageSize);
        // If the current page is -1, they are not using pagination.
        return currentPage == -1 || pageOfIndex == currentPage;
    }
    
    /**
     * Returns the next record in the stream.
     * 
     * @return the next RecordResult, or null if no more records
     */
    public RecordResult next() {
        if (index >= 0 && index < records.length) {
            return records[index++];
        }
        return null;
    }
    
    /**
     * Returns the current page number (1-based).
     * 
     * @return the current page number, or 0 if pagination hasn't started
     */
    @Override
    public int currentPage() {
        return currentPage + 1;
    }
    
    /**
     * Returns the maximum number of pages.
     * 
     * @return the total number of pages
     */
    @Override
    public int maxPages() {
        return numPages;
    }
    
    /**
     * Sets the current page to the specified page number.
     * 
     * <p>This allows you to jump to any page in the result set.
     * Page numbers are 1-based.</p>
     * 
     * @param newPage the page number to jump to (1-based, must be between 1 and maxPages())
     * @throws IllegalArgumentException if newPage is out of range
     */
    @Override
    public void setPageTo(int newPage) {
        if (newPage < 1 || newPage > numPages) {
            throw new IllegalArgumentException(String.format(
                    "setPageTo must take page number in the range of 1 to %,d, not %,d", 
                    numPages, newPage));
        }
        currentPage = newPage - 1;
        index = currentPage * pageSize;
    }
    
    /**
     * Converts the records on the current page to a Java Stream.
     * 
     * <p>This method creates a stream of the records on the current page in the
     * current sort order. It does not advance pages.</p>
     * 
     * @return a Stream of RecordResult for the current page
     */
    public Stream<RecordResult> stream() {
        // Save current position
        int savedIndex = this.index;
        int savedPage = this.currentPage;
        
        // Create a custom iterator for the current page
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                new java.util.Iterator<RecordResult>() {
                    private int streamIndex = savedIndex;
                    
                    @Override
                    public boolean hasNext() {
                        if (streamIndex >= records.length) {
                            return false;
                        }
                        long pageOfIndex = pageSize == 0 ? 0 : (streamIndex / pageSize);
                        return savedPage == -1 || pageOfIndex == savedPage;
                    }
                    
                    @Override
                    public RecordResult next() {
                        if (streamIndex >= 0 && streamIndex < records.length) {
                            return records[streamIndex++];
                        }
                        return null;
                    }
                }, 
                Spliterator.NONNULL | Spliterator.IMMUTABLE
            ), 
            false
        );
    }
    
    /**
     * Iterates through all remaining records on the current page.
     * 
     * @param consumer the consumer to accept each record
     */
    public void forEach(Consumer<RecordResult> consumer) {
        while (hasNext()) {
            consumer.accept(next());
        }
    }
    
    /**
     * Converts the records on the current page to a list of objects using the specified mapper.
     * 
     * @param <T> the type of objects to create
     * @param mapper the mapper to convert records to objects
     * @return a list of mapped objects
     */
    public <T> List<T> toObjectList(RecordMapper<T> mapper) {
        List<T> result = new ArrayList<>();
        while (hasNext()) {
            RecordResult keyRecord = next();
            Record rec = keyRecord.recordOrThrow();
            result.add(mapper.fromMap(rec.bins, keyRecord.key(), rec.generation));
        }
        return result;
    }
    
    /**
     * Gets the first record from the current page.
     * 
     * <p>If the record failed for any reason, an exception is thrown.</p>
     * 
     * @return an Optional containing the first record, or empty if no records
     * @throws AerospikeException if the first record has an error
     */
    public Optional<RecordResult> getFirst() throws AerospikeException {
        return getFirst(true);
    }
    
    /**
     * Gets the first record from the current page.
     * 
     * @param throwException if true and the record has an error, throws an exception
     * @return an Optional containing the first record, or empty if no records
     * @throws AeroException if throwException is true and the first record has an error
     */
    public Optional<RecordResult> getFirst(boolean throwException) {
        if (hasNext()) {
            RecordResult result = next();
            if (throwException) {
                return Optional.of(result.orThrow());
            } else {
                return Optional.of(result);
            }
        }
        return Optional.empty();
    }
    
    /**
     * Gets the first record from the current page and maps it to an object.
     * 
     * <p>If the record failed for any reason, an exception is thrown.</p>
     * 
     * @param <T> the type of object to create
     * @param mapper the mapper to convert the record to an object
     * @return an Optional containing the mapped object, or empty if no records
     */
    public <T> Optional<T> getFirst(RecordMapper<T> mapper) {
        if (hasNext()) {
            RecordResult item = next();
            Record rec = item.recordOrThrow();
            return Optional.of(mapper.fromMap(rec.bins, item.key(), rec.generation));
        }
        return Optional.empty();
    }
    
    /**
     * Gets the total number of records in this stream.
     * 
     * @return the total number of records
     */
    public int size() {
        return records.length;
    }
    
    /**
     * Resets the stream to the beginning, allowing re-iteration of all records.
     * 
     * @return this NavigatableRecordStream for method chaining
     */
    public NavigatableRecordStream reset() {
        resetIteration();
        return this;
    }
    
    @Override
    public void close() {
        // Nothing to close - all records are in memory
    }
}

