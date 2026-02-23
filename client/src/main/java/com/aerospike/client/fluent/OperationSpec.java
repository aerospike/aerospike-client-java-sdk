package com.aerospike.client.fluent;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.fluent.exp.Expression;

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
    private boolean respondAllKeys = false;

    /** For DELETE operations: whether to use durable delete */
    private Boolean durablyDelete = null;

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

    public Expression getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(Expression whereClause) {
        this.whereClause = whereClause;
    }

    public int getGeneration() {
        return generation;
    }

    public void setGeneration(int generation) {
        this.generation = generation;
    }

    public long getExpirationInSeconds() {
        return expirationInSeconds;
    }

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

    public boolean isFailOnFilteredOut() {
        return failOnFilteredOut;
    }

    public void setFailOnFilteredOut(boolean failOnFilteredOut) {
        this.failOnFilteredOut = failOnFilteredOut;
    }

    public boolean isRespondAllKeys() {
        // If UPDATE or REPLACE_IF_EXISTS is specified we must respond all keys too, as these
        // records SHOULD throw an exception if the record doesn't exist.
        return respondAllKeys || opType == OpType.REPLACE_IF_EXISTS || opType == OpType.UPDATE;
    }

    public void setRespondAllKeys(boolean respondAllKeys) {
        this.respondAllKeys = respondAllKeys;
    }

    public Boolean getDurablyDelete() {
        return durablyDelete;
    }

    public void setDurablyDelete(Boolean durablyDelete) {
        this.durablyDelete = durablyDelete;
    }

    public String[] getProjectedBins() {
        return projectedBins;
    }

    public void setProjectedBins(String[] projectedBins) {
        this.projectedBins = projectedBins;
    }

    public List<Key> getKeys() {
        return keys;
    }

    public OpType getOpType() {
        return opType;
    }

    public List<Operation> getOperations() {
        return operations;
    }

    /**
     * Returns true if this is a UDF operation.
     */
    public boolean isUdf() {
        return opType == OpType.UDF;
    }

    public String getUdfPackageName() {
        return udfPackageName;
    }

    public void setUdfPackageName(String udfPackageName) {
        this.udfPackageName = udfPackageName;
    }

    public String getUdfFunctionName() {
        return udfFunctionName;
    }

    public void setUdfFunctionName(String udfFunctionName) {
        this.udfFunctionName = udfFunctionName;
    }

    public Value[] getUdfArguments() {
        return udfArguments;
    }

    public void setUdfArguments(Value[] udfArguments) {
        this.udfArguments = udfArguments;
    }
}
