package com.aerospike.client.fluent.query;

import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Log;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;

class SingleKeyQueryBuilderImpl extends QueryImpl {
    private final Key key;
    public SingleKeyQueryBuilderImpl(QueryBuilder builder, Session session, Key key) {
        super(builder, session);
        this.key = key;
    }

    @Override
    public boolean allowsSecondaryIndexQuery() {
        return false;
    }
    // No need to implement limit on single read
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
        // Single key reads are fast; async and sync are effectively the same
        return executeInternal();
    }

    private RecordStream executeInternal() {
    	return null;
    	/*
        Policy policy = getSession().getBehavior().getMutablePolicy(CommandType.READ_SC);
        policy.txn = this.getQueryBuilder().getTxnToUse();
        policy.failOnFilteredOut = this.getQueryBuilder().isFailOnFilteredOut();
        if (!getQueryBuilder().isKeyInPartitionRange(key)) {
            if (this.getQueryBuilder().respondAllKeys) {
                return new RecordStream(key, null, true);
            }
            return new RecordStream();
        }
        try {
            if (getQueryBuilder().getWithNoBins()) {
                return new RecordStream(key, getSession().getClient().getHeader(policy, key), this.getQueryBuilder().respondAllKeys);
            }
            else {
                return new RecordStream(key, 
                        getSession().getClient().get(policy, key, getQueryBuilder().getBinNames()),
                        this.getQueryBuilder().respondAllKeys
                    );
            }
        }
        catch (AerospikeException ae) {
            if (Log.warnEnabled() && ae.getResultCode() == ResultCode.UNSUPPORTED_FEATURE) {
                if (this.getQueryBuilder().getTxnToUse() != null && !getSession().isNamespaceSC(key.namespace)) {
                    Log.warn(String.format("Namespace '%s' is involved in transaction, but it is not an SC namespace. "
                            + "This will throw an Unsupported Server Feature Exception.", key.namespace));
                }
            }
            throw ae;
        }*/
    }
}