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
package com.aerospike.client.sdk;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.sdk.exp.Expression;

/**
 * Internal class representing a single operation specification in a chainable batch operation.
 * Each OperationSpec holds all the information needed to execute one logical operation
 * on one or more keys, including the operation type, bins to modify, filters, and policies.
 *
 * <p>This class is used internally by {@link ChainableOperationBuilder},
 * {@link ChainableNoBinsBuilder}, and {@link ChainableQueryBuilder} to build up
 * a list of heterogeneous operations that will be executed as a single batch.</p>
 */
public class OperationSpec {
    /** The keys that this operation applies to */
    private final List<Key> keys;

    /** The type of operation (UPSERT, UPDATE, INSERT, REPLACE, DELETE, TOUCH, EXISTS or null for query) */
    private final OpType opType;

    /** The list of operations to perform on the bins (empty for DELETE, TOUCH, EXISTS) */
    private final List<Operation> operations;

    /** Optional filter expression for conditional operations */
    private Expression whereClause = null;

    /** Generation check value (0 means no generation check) */
    private int generation = 0;

    /** Expiration in seconds. NOT_EXPLICITLY_SET means not set, 0 = server default, -1 = never expire, -2 = no change */
    private long expirationInSeconds = AbstractOperationBuilder.NOT_EXPLICITLY_SET;

    /** Whether to fail if a record is filtered out by the where clause */
    private boolean failOnFilteredOut = false;

    /** Whether to include results for keys that don't exist or are filtered out */
    private boolean includeMissingKeys = false;

    /** For DELETE operations: whether to use durable delete */
    private Boolean durableDelete = null;

    /** For QUERY operations: specific bins to read (null means all bins) */
    private String[] projectedBins = null;

    /** For UDF operations: the package name containing the UDF */
    private String udfPackageName = null;

    /** For UDF operations: the function name to execute */
    private String udfFunctionName = null;

    /** For UDF operations: the arguments to pass to the UDF */
    private Value[] udfArguments = null;

    /**
     * Create a write operation spec (upsert, update, insert, replace, delete, touch).
     */
    OperationSpec(List<Key> keys, OpType opType) {
        this.keys = keys;
        this.opType = opType;
        this.operations = new ArrayList<>();
    }

    /**
     * Create a query (read) operation spec.
     */
    OperationSpec(List<Key> keys) {
        this.keys = keys;
        this.opType = null; // Query operations don't have an OpType
        this.operations = new ArrayList<>();
    }

    /**
     * Returns true if this is a query/read operation.
     */
    boolean isQuery() {
        return opType == null;
    }

    /**
     * Returns true if this operation type can have bin operations.
     */
    boolean canHaveBinOperations() {
        return opType != null &&
               opType != OpType.DELETE &&
               opType != OpType.TOUCH &&
               opType != OpType.EXISTS;
    }

    /**
     * Optional filter expression for this operation, or {@code null} if none.
     *
     * @return the compiled where clause, or {@code null}
     */
    public Expression getWhereClause() {
        return whereClause;
    }

    /**
     * Sets the filter expression for this operation.
     *
     * @param whereClause compiled expression, or {@code null} to clear
     */
    public void setWhereClause(Expression whereClause) {
        this.whereClause = whereClause;
    }

    /**
     * Expected record generation for optimistic locking; {@code 0} means no check.
     *
     * @return generation value to match on the server
     */
    public int getGeneration() {
        return generation;
    }

    /**
     * Sets the generation check for this spec.
     *
     * @param generation expected generation, or {@code 0} to disable
     */
    public void setGeneration(int generation) {
        this.generation = generation;
    }

    /**
     * TTL for this operation in seconds, or {@link AbstractOperationBuilder#NOT_EXPLICITLY_SET} if unset.
     *
     * @return expiration seconds (including sentinel values such as never-expire / no-change)
     */
    public long getExpirationInSeconds() {
        return expirationInSeconds;
    }

    /**
     * Sets TTL for this operation in seconds.
     *
     * @param expirationInSeconds seconds until expiry, or Aerospike TTL sentinels ({@code 0}, {@code -1}, {@code -2})
     */
    public void setExpirationInSeconds(long expirationInSeconds) {
        this.expirationInSeconds = expirationInSeconds;
    }

    /**
     * Check if expiration was explicitly set on this operation.
     * @return true if expiration was explicitly set, false if using default
     */
    public boolean hasExplicitExpiration() {
        return expirationInSeconds != AbstractOperationBuilder.NOT_EXPLICITLY_SET;
    }

    /**
     * Whether a filtered-out record should surface as {@code FILTERED_OUT} in results instead of being omitted.
     *
     * @return {@code true} if fail-on-filtered-out is enabled
     */
    public boolean isFailOnFilteredOut() {
        return failOnFilteredOut;
    }

    /**
     * @param failOnFilteredOut {@code true} to fail or report when the where clause filters out a key
     */
    public void setFailOnFilteredOut(boolean failOnFilteredOut) {
        this.failOnFilteredOut = failOnFilteredOut;
    }

    /**
     * Whether missing keys (or similar) should still produce a stream entry.
     * For {@link OpType#UPDATE} and {@link OpType#REPLACE_IF_EXISTS} this always behaves as {@code true}
     * so callers see {@link com.aerospike.client.sdk.ResultCode#KEY_NOT_FOUND_ERROR} when the record does not exist.
     *
     * @return effective include-missing-keys flag
     */
    public boolean isIncludeMissingKeys() {
        // If UPDATE or REPLACE_IF_EXISTS is specified we must include missing keys too, as these
        // records SHOULD throw an exception if the record doesn't exist.
        return includeMissingKeys || opType == OpType.REPLACE_IF_EXISTS || opType == OpType.UPDATE;
    }

    /**
     * @param includeMissingKeys {@code true} to emit results for keys with no record when applicable
     */
    public void setIncludeMissingKeys(boolean includeMissingKeys) {
        this.includeMissingKeys = includeMissingKeys;
    }

    /**
     * For delete operations: {@code true}/{@code false} selects durable vs normal delete, {@code null} if unset.
     *
     * @return durable-delete preference, or {@code null}
     */
    public Boolean getDurableDelete() {
        return durableDelete;
    }

    /**
     * @param durableDelete durable delete flag, or {@code null} to leave server/default behavior
     */
    public void setDurableDelete(Boolean durableDelete) {
        this.durableDelete = durableDelete;
    }

    /**
     * For query specs: bin names to project, or {@code null} for all bins.
     *
     * @return projected bin names, or {@code null}
     */
    public String[] getProjectedBins() {
        return projectedBins;
    }

    /**
     * @param projectedBins bin names to read, or {@code null} for all bins
     */
    public void setProjectedBins(String[] projectedBins) {
        this.projectedBins = projectedBins;
    }

    /**
     * Keys this spec applies to (immutable list reference; not a copy).
     *
     * @return non-null key list
     */
    public List<Key> getKeys() {
        return keys;
    }

    /**
     * Write/read operation kind, or {@code null} for a query-only spec.
     *
     * @return operation type, or {@code null} when {@link #isQuery()}
     */
    public OpType getOpType() {
        return opType;
    }

    /**
     * Bin-level operations accumulated for this spec (empty for delete, touch, exists, query).
     *
     * @return mutable list of Aerospike {@link Operation}s
     */
    public List<Operation> getOperations() {
        return operations;
    }

    /**
     * Returns true if this is a UDF operation.
     */
    public boolean isUdf() {
        return opType == OpType.UDF;
    }

    /**
     * UDF Lua package name when {@link #isUdf()}, otherwise may be {@code null}.
     *
     * @return registered UDF package name
     */
    public String getUdfPackageName() {
        return udfPackageName;
    }

    /**
     * @param udfPackageName Lua package containing the function
     */
    public void setUdfPackageName(String udfPackageName) {
        this.udfPackageName = udfPackageName;
    }

    /**
     * UDF function name when {@link #isUdf()}.
     *
     * @return function name within the package
     */
    public String getUdfFunctionName() {
        return udfFunctionName;
    }

    /**
     * @param udfFunctionName function to invoke
     */
    public void setUdfFunctionName(String udfFunctionName) {
        this.udfFunctionName = udfFunctionName;
    }

    /**
     * Arguments passed to the UDF, or {@code null} if none.
     *
     * @return UDF argument values
     */
    public Value[] getUdfArguments() {
        return udfArguments;
    }

    /**
     * @param udfArguments arguments for the UDF call, or {@code null}
     */
    public void setUdfArguments(Value[] udfArguments) {
        this.udfArguments = udfArguments;
    }
}
