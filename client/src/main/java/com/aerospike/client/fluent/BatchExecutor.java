package com.aerospike.client.fluent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.aerospike.client.fluent.command.Batch;
import com.aerospike.client.fluent.command.BatchAttr;
import com.aerospike.client.fluent.command.BatchCommand;
import com.aerospike.client.fluent.command.BatchDelete;
import com.aerospike.client.fluent.command.BatchNode;
import com.aerospike.client.fluent.command.BatchNodeList;
import com.aerospike.client.fluent.command.BatchRead;
import com.aerospike.client.fluent.command.BatchReadCommand;
import com.aerospike.client.fluent.command.BatchRecord;
import com.aerospike.client.fluent.command.BatchSingle;
import com.aerospike.client.fluent.command.BatchStatus;
import com.aerospike.client.fluent.command.BatchWrite;
import com.aerospike.client.fluent.command.BatchWriteCommand;
import com.aerospike.client.fluent.command.IBatchCommand;
import com.aerospike.client.fluent.command.OperateArgs;
import com.aerospike.client.fluent.command.ReadAttr;
import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.command.TxnMonitor;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.tend.Partitions;

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
        String namespace = specs.get(0).getKeys().get(0).namespace;
        
        // Get settings for batch operations
        Settings settings = session.getBehavior()
                .getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, session.isNamespaceSC(namespace));
        
        // Build list of BatchRecord objects
        List<BatchRecord> batchRecords = new ArrayList<>();
        
        // Set failOnFilteredOut on batch policy if ANY spec has it enabled
        boolean anyFailOnFilteredOut = specs.stream().anyMatch(s -> s.isFailOnFilteredOut());
        boolean anyRespondAllKeys = specs.stream().anyMatch(s -> s.isRespondAllKeys());

        
        // TODO: BN: Is there a better way to do this?
        // Seem to need this for OperateArgs, this isn't efficient
        List<Operation> ops = new ArrayList<>();
        
        for (OperationSpec spec : specs) {
            // Determine which filter to use - per-operation or default
            Expression filterToUse = spec.getWhereClause() != null ? spec.getWhereClause() : defaultWhereClause;

            ops.addAll(spec.getOperations());
            // Create BatchRecord(s) for each key in this spec
            for (Key key : spec.getKeys()) {
                BatchRecord batchRecord = createBatchRecord(spec, key, filterToUse, settings);
                batchRecords.add(batchRecord);
            }
        }
        
        return executeBatchSync(session, 
                settings, 
                batchRecords, 
                new OperateArgs(ops.toArray(new Operation[0])),
                txn,
                namespace,
                anyFailOnFilteredOut,
                defaultWhereClause,
                anyRespondAllKeys);
    }
    
    private static Partitions getPartitions(Cluster cluster, String namespace) {
        HashMap<String, Partitions> partitionMap = cluster.getPartitionMap();
        Partitions partitions = partitionMap.get(namespace);

        if (partitions == null) {
            throw new AerospikeException.InvalidNamespace(namespace, partitionMap.size());
        }
        return partitions;
    }

    // TODO: BN - I think it makes sense to merge this into `execute()` but I don't want to limit 
    // reusability if I do that.
    protected static RecordStream executeBatchSync(Session session, Settings settings, 
            List<BatchRecord> batchRecords, OperateArgs args, Txn txnToUse, String namespace,
            boolean anyFailOnFilteredOut, Expression filterExp, boolean respondAllKeys) {
        
        Cluster cluster = session.getCluster();
        Partitions partitions = getPartitions(cluster, namespace);

        // TODO: BN: Each BatchRecord will have it's where clause in built.
//        final Expression filterExp = processWhereClause(namespace, session);

        if (txnToUse != null) {
            if (args.hasWrite) {
                TxnMonitor.addKeys(txnToUse, cluster, partitions, settings, keys);
            }
            else {
                txnToUse.prepareRead(namespace);
            }
        }

        BatchCommand parent;

        if (args.hasWrite) {
//            for (Key key : keys) {
//                batchRecords.add(new BatchWrite(key, operations, opType, generation, (int)expirationInSecondsForAll));
//            }

            parent = new BatchWriteCommand(cluster, partitions, txnToUse, namespace,
                batchRecords, filterExp, respondAllKeys, settings);
       }
        else {
//            for (Key key : keys) {
//                batchRecords.add(new BatchRead(key, operations));
//            }

            ReadAttr attr = new ReadAttr(partitions, settings);

            parent = new BatchReadCommand(cluster, partitions, txnToUse, namespace,
                    batchRecords, filterExp, respondAllKeys, settings, attr);
       }

        BatchStatus status = new BatchStatus(true);
        List<BatchNode> bns = BatchNodeList.generate(cluster, partitions, settings.getReplicaOrder(), batchRecords,
            status);

        IBatchCommand[] commands = new IBatchCommand[bns.size()];
        int count = 0;

        for (BatchNode bn : bns) {
            if (bn.offsetsSize == 1) {
                int i = bn.offsets[0];
                BatchRecord rec = batchRecords.get(i);

                if (args.hasWrite) {
                    BatchWrite bw = (BatchWrite)rec;
                    BatchAttr attr = new BatchAttr();

                    attr.setWrite((BatchWriteCommand)parent, bw);
                    attr.adjustWrite(bw.ops);
                    attr.setOpSize(bw.ops);

                    commands[count++] = new BatchSingle.OperateRecordSync(cluster, parent, bw.ops, attr, rec, status,
                            bn.node);
                }
                else {
                    BatchRead br = (BatchRead)rec;
                    BatchAttr attr = new BatchAttr();

                    attr.setRead((BatchReadCommand)parent);
                    attr.adjustRead(br.ops);
                    attr.setOpSize(br.ops);

                    commands[count++] = new BatchSingle.ReadRecord(cluster, (BatchReadCommand)parent, br, status, bn.node);
                }
            }
            else {
                commands[count++] = new Batch.OperateListSync(cluster, parent, bn, batchRecords, status);
            }
        }

        BatchExecutor.execute(cluster, commands, status);

        // Convert BatchRecord to RecordResult with proper filtering and stack trace handling
        AsyncRecordStream recordStream = new AsyncRecordStream(batchRecords.size());
        try {
            for (int i = 0; i < batchRecords.size(); i++) {
                BatchRecord br = batchRecords.get(i);
                if (shouldIncludeResult(br.resultCode)) {
                    recordStream.publish(createRecordResultFromBatchRecord(br, settings, i));
                }
            }
            return new RecordStream(recordStream);
        }
        finally {
            recordStream.complete();
        }
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
        // TODO BN: How to pass the filter expression?
//        policy.filterExp = filterExp;
        
        // Convert operations list to array
        Operation[] operations = spec.getOperations().toArray(new Operation[0]);
        
        return new BatchWrite(key, operations, spec.getOpType(), spec.getGeneration(), (int)spec.getExpirationInSeconds());
    }
    
    /**
     * Create BatchDelete for delete operations.
     */
    private static BatchDelete createBatchDelete(OperationSpec spec, Key key,
                                                  Expression filterExp, Settings settings) {
        // TODO: BN: What about durable delete, filter expression?
        return new BatchDelete(key, spec.getGeneration());
    }
    
    /**
     * Create BatchWrite with touch operation.
     */
    private static BatchWrite createBatchTouch(OperationSpec spec, Key key,
                                               Expression filterExp, Settings settings) {

        // TODO: BN: How to set the "filterExp" settings?
        return new BatchWrite(key, new Operation[] { Operation.touch() }, OpType.TOUCH, spec.getGeneration(), (int)spec.getExpirationInSeconds());
    }
    
    /**
     * Create BatchRead for exists check (metadata only, no bins).
     */
    private static BatchRead createBatchExists(OperationSpec spec, Key key, Expression filterExp) {
        // TODO: BN: Filter expressions?
        // Exists check: read no bins, just check if record exists
        return new BatchRead(key, false);
    }
    
    /**
     * Create BatchRead for query operations.
     */
    private static BatchRead createBatchRead(OperationSpec spec, Key key, Expression filterExp) {
        // TODO: BN: Filter expressions?
        if (spec.getProjectedBins() != null && spec.getProjectedBins().length > 0) {
            // Read specific bins
            return new BatchRead(key, spec.getProjectedBins());
        } else {
            // Read all bins
            return new BatchRead(key, true);
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
