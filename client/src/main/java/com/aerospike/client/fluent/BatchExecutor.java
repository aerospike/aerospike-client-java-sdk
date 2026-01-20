package com.aerospike.client.fluent;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.fluent.command.BatchDelete;
import com.aerospike.client.fluent.command.BatchRead;
import com.aerospike.client.fluent.command.BatchRecord;
import com.aerospike.client.fluent.command.BatchWrite;
import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.policy.BatchDeletePolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchReadPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.GenerationPolicy;

/**
 * Executor for heterogeneous batch operations.
 * This class converts OperationSpec objects to the appropriate BatchRecord types
 * and executes them as a single batch operation.
 * 
 * <p>The executor handles mixed operation types including:
 * <ul>
 *   <li>Write operations (upsert, update, insert, replace) - converted to BatchWrite</li>
 *   <li>Delete operations - converted to BatchDelete</li>
 *   <li>Touch operations - converted to BatchWrite with Operation.touch()</li>
 *   <li>Exists operations - converted to BatchRead with no bins</li>
 *   <li>Query/read operations - converted to BatchRead with specified bins</li>
 * </ul>
 * </p>
 */
class BatchExecutor {
    
    /**
     * Execute a batch of heterogeneous operations.
     * 
     * @param session the session to use for execution
     * @param specs the list of operation specifications
     * @param defaultWhereClause optional default filter for operations without explicit where clause
     * @param txn optional transaction to use
     * @return RecordStream containing the results of all operations
     */
    public static RecordStream execute(Session session, List<OperationSpec> specs, 
                                        Expression defaultWhereClause, Txn txn) {
        if (specs.isEmpty()) {
            return new RecordStream();
        }
        
        // Get the namespace from the first key
        String namespace = specs.get(0).keys.get(0).namespace;
        
        // Get settings for batch operations
        Settings settings = session.getBehavior()
                .getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, session.isNamespaceSC(namespace));
        
        // Create batch policy (using deprecated method - same as rest of codebase)
        BatchPolicy batchPolicy = settings.asBatchPolicy();
        batchPolicy.txn = txn;
        
        // Set failOnFilteredOut on batch policy if ANY spec has it enabled
        boolean anyFailOnFilteredOut = specs.stream().anyMatch(s -> s.isFailOnFilteredOut());
        batchPolicy.failOnFilteredOut = anyFailOnFilteredOut;
        
        // Build list of BatchRecord objects
        List<BatchRecord> batchRecords = new ArrayList<>();
        
        for (OperationSpec spec : specs) {
            // Determine which filter to use - per-operation or default
            Expression filterToUse = spec.getWhereClause() != null ? spec.getWhereClause() : defaultWhereClause;
            
            // Create BatchRecord(s) for each key in this spec
            for (Key key : spec.getKeys()) {
                BatchRecord batchRecord = createBatchRecord(spec, key, filterToUse, settings);
                batchRecords.add(batchRecord);
            }
        }
        
        // Execute the batch
        session.getClient().operate(batchPolicy, batchRecords);
        
        // Convert results to RecordStream
        return buildRecordStream(batchRecords, specs, settings);
    }
    
    /**
     * Create the appropriate BatchRecord for an operation spec and key.
     */
    private static BatchRecord createBatchRecord(OperationSpec spec, Key key, 
                                                  Expression filterExp, Settings settings) {
        if (spec.isQuery()) {
            // Query (read) operation
            return createBatchRead(spec, key, filterExp);
        }
        
        switch (spec.getOpType()) {
        case DELETE:
            return createBatchDelete(spec, key, filterExp, settings);
        case TOUCH:
            return createBatchTouch(spec, key, filterExp, settings);
        case EXISTS:
            return createBatchExists(spec, key, filterExp);
        case UPSERT:
        case UPDATE:
        case INSERT:
        case REPLACE:
            return createBatchWrite(spec, key, filterExp, settings);
        default:
            throw new IllegalStateException("Unknown operation type: " + spec.getOpType());
        }
    }
    
    /**
     * Create BatchWrite for write operations (upsert, update, insert, replace).
     */
    private static BatchWrite createBatchWrite(OperationSpec spec, Key key, 
                                               Expression filterExp, Settings settings) {
        BatchWritePolicy policy = new BatchWritePolicy();
        policy.sendKey = settings.getSendKey();
        policy.recordExistsAction = AbstractSessionOperationBuilder.recordExistsActionFromOpType(spec.getOpType());
        policy.filterExp = filterExp;
        
        if (spec.getExpirationInSeconds() != 0) {
            policy.expiration = (int) spec.getExpirationInSeconds();
        }
        
        if (spec.getGeneration() > 0) {
            policy.generation = spec.getGeneration();
            policy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        }
        
        // Convert operations list to array
        Operation[] operations = spec.operations.toArray(new Operation[0]);
        
        return new BatchWrite(policy, key, operations);
    }
    
    /**
     * Create BatchDelete for delete operations.
     */
    private static BatchDelete createBatchDelete(OperationSpec spec, Key key,
                                                  Expression filterExp, Settings settings) {
        BatchDeletePolicy policy = new BatchDeletePolicy();
        policy.sendKey = settings.getSendKey();
        policy.filterExp = filterExp;
        
        if (spec.getDurablyDelete() != null) {
            policy.durableDelete = spec.getDurablyDelete();
        } else {
            policy.durableDelete = settings.getUseDurableDelete();
        }
        
        if (spec.getGeneration() > 0) {
            policy.generation = spec.getGeneration();
            policy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        }
        
        return new BatchDelete(policy, key);
    }
    
    /**
     * Create BatchWrite with touch operation.
     */
    private static BatchWrite createBatchTouch(OperationSpec spec, Key key,
                                               Expression filterExp, Settings settings) {

        // TODO: BN: How to set the "sendKey" and "filterExp" settings?
        return new BatchWrite(key, new Operation[] { Operation.touch() }, OpType.TOUCH, spec.getGeneration(), (int)spec.getExpirationInSeconds());
    }
    
    /**
     * Create BatchRead for exists check (metadata only, no bins).
     */
    private static BatchRead createBatchExists(OperationSpec spec, Key key, Expression filterExp) {
        BatchReadPolicy policy = new BatchReadPolicy();
        policy.filterExp = filterExp;
        
        // Exists check: read no bins, just check if record exists
        return new BatchRead(policy, key, false);
    }
    
    /**
     * Create BatchRead for query operations.
     */
    private static BatchRead createBatchRead(OperationSpec spec, Key key, Expression filterExp) {
        BatchReadPolicy policy = new BatchReadPolicy();
        policy.filterExp = filterExp;
        
        if (spec.getProjectedBins() != null && spec.getProjectedBins().length > 0) {
            // Read specific bins
            return new BatchRead(policy, key, spec.getProjectedBins());
        } else {
            // Read all bins
            return new BatchRead(policy, key, true);
        }
    }
    
    /**
     * Build RecordStream from batch results, respecting respondAllKeys and failOnFilteredOut flags.
     */
    private static RecordStream buildRecordStream(List<BatchRecord> batchRecords, 
                                                   List<OperationSpec> specs,
                                                   Settings settings) {
        List<RecordResult> results = new ArrayList<>();
        
        int recordIndex = 0;
        
        for (OperationSpec spec : specs) {
            for (int keyIndex = 0; keyIndex < spec.getKeys().size(); keyIndex++) {
                BatchRecord br = batchRecords.get(recordIndex);
                
                // Determine if we should include this result
                boolean includeResult = shouldIncludeResult(br.resultCode, spec);
                
                if (includeResult) {
                    RecordResult result;
                    if (settings.getStackTraceOnException() && br.resultCode != ResultCode.OK) {
                        result = new RecordResult(
                            br, 
                            AerospikeException.resultCodeToException(br.resultCode, null, br.inDoubt), 
                            recordIndex);
                    } else {
                        result = new RecordResult(br, recordIndex);
                    }
                    results.add(result);
                }
                
                recordIndex++;
            }
        }
        
        return new RecordStream(results, 0);
    }
    
    /**
     * Determine if a result should be included based on result code and operation flags.
     */
    private static boolean shouldIncludeResult(int resultCode, OperationSpec spec) {
        switch (resultCode) {
        case ResultCode.OK:
            return true;
        case ResultCode.KEY_NOT_FOUND_ERROR:
            return spec.isRespondAllKeys();
        case ResultCode.FILTERED_OUT:
            return spec.isFailOnFilteredOut() || spec.isRespondAllKeys();
        default:
            return true;  // Include errors in the stream
        }
    }
}
