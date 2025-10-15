package com.aerospike.client.fluent.query;

import java.util.List;

import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Log;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;

class BatchKeyQueryBuilderImpl extends QueryImpl {
    private final List<Key> keyList;
    public BatchKeyQueryBuilderImpl(QueryBuilder builder, Session session, List<Key> keyList) {
        super(builder, session);
        this.keyList = keyList;
    }
	
    @Override
    public boolean allowsSecondaryIndexQuery() {
        return false;
    }

    @Override
    public RecordStream execute() {
        // Query default: async unless in transaction
        if (getQueryBuilder().getTxnToUse() != null) {
            return executeSync();
        } else {
            return executeAsync();
        }
    }
    
    @Override
    public RecordStream executeSync() {
        return executeInternal();
    }
    
    @Override
    public RecordStream executeAsync() {
        if (getQueryBuilder().getTxnToUse() != null && Log.warnEnabled()) {
            Log.warn(
                "executeAsync() called within a transaction. " +
                "Async operations may still be in flight when commit() is called, " +
                "which could lead to inconsistent state. " +
                "Consider using executeSync() or execute() for transactional safety."
            );
        }
        // For batch operations, async and sync are effectively the same
        // since we need to wait for the batch to complete anyway
        return executeInternal();
    }
    
    public RecordStream executeInternal() {
    	return null;
    	/*
        if (keyList.size() == 0) {
            return new RecordStream();
        }
        Expression whereExp = null;
        if (getQueryBuilder().getDsl() != null) {
            ParseResult parseResult = getQueryBuilder().getDsl().process(this.keyList.get(0).namespace, getSession());
            whereExp = Exp.build(parseResult.getExp());
        }
        
        long limit = getQueryBuilder().getLimit();
        List<BatchRecord> batchRecords = new ArrayList<>();
        List<BatchRecord> batchRecordsForServer = hasPartitionFilter() ? new ArrayList<>() : batchRecords;
        
        for (Key thisKey : keyList) {
            // If there is no "where" clause and the limit has been exceeded, exit the loop
            if (whereExp == null && limit > 0 && batchRecords.size() >= limit) {
                break;
            }
            if (hasPartitionFilter() && !getQueryBuilder().isKeyInPartitionRange(thisKey)) {
                // We know this one will fail
                if (!getQueryBuilder().isRespondAllKeys()) {
                    // Filter it out
                    continue;
                }
                else {
                    // Need to include a record but do not send it to the server
                    batchRecords.add(new BatchRecord(thisKey, false));
                }
            }
            else {
                BatchRecord thisBatchRecord;
                if (getQueryBuilder().getWithNoBins()) {
                    thisBatchRecord = new BatchRead(thisKey, false);
                }
                else if (getQueryBuilder().getBinNames() != null) {
                    thisBatchRecord = new BatchRead(thisKey, getQueryBuilder().getBinNames());
                }
                else {
                    thisBatchRecord = new BatchRead(thisKey, true);
                }
                batchRecordsForServer.add(thisBatchRecord);
            }
        }

        BatchPolicy policy = getSession().getBehavior().getMutablePolicy(CommandType.BATCH_READ);
        policy.filterExp = whereExp;
        policy.setTxn(this.getQueryBuilder().getTxnToUse());
        policy.failOnFilteredOut = this.getQueryBuilder().isFailOnFilteredOut();
        
        try {
            getSession().getClient().operate(policy, batchRecordsForServer);
            if (!getQueryBuilder().isRespondAllKeys()) {
                // Remove any items which have been filtered out.
                batchRecordsForServer.removeIf(br -> (br.resultCode == ResultCode.OK && br.record == null) 
                        || (br.resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
                        || (br.resultCode == ResultCode.FILTERED_OUT && !getQueryBuilder().isFailOnFilteredOut()));
            }
            if (hasPartitionFilter()) {
                // Add the server results into any that were filtered out earlier
                batchRecords.addAll(batchRecordsForServer);
            }
            
            // TODO: ResultsInKeyOrder?
            return new RecordStream(batchRecords,
                    limit,
                    getQueryBuilder().getPageSize(),
                    getQueryBuilder().getSortInfo());
        }
        catch (AerospikeException ae) {
            if (Log.warnEnabled() && ae.getResultCode() == ResultCode.UNSUPPORTED_FEATURE) {
                if (this.getQueryBuilder().getTxnToUse() != null) {
                    Set<String> namespaces = keyList.stream().map(key->key.namespace).collect(Collectors.toSet());
                    namespaces.forEach(namespace -> {
                        if (!getSession().isNamespaceSC(namespace)) {
                            Log.warn(String.format("Namespace '%s' is involved in transaction, but it is not an SC namespace. "
                                    + "This will throw an Unsupported Server Feature exception.", namespace));
                        }

                    });
                }
            }
            throw ae;
        }
    */
    }
}