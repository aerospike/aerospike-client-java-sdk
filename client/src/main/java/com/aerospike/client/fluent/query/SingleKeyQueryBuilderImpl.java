package com.aerospike.client.fluent.query;

import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;

class SingleKeyQueryBuilderImpl extends QueryImpl {
    private final Key key;
    public SingleKeyQueryBuilderImpl(QueryBuilder builder, Session session, Key key) {
        super(builder, session);
        this.key = key;
    }

    // No need to implement limit on single read
    @Override
    public RecordStream execute() {
    	return null;
    	/*
        Policy policy = getSession().getBehavior().getMutablePolicy(CommandType.READ_SC);
        policy.txn = this.getQueryBuilder().getTxnToUse();
        if (!getQueryBuilder().isKeyInPartitionRange(key)) {
            return new RecordStream();
        }
        try {
            if (getQueryBuilder().getWithNoBins()) {
                return new RecordStream(key, getSession().getClient().getHeader(policy, key));
            }
            else {
                return new RecordStream(key, getSession().getClient().get(policy, key, getQueryBuilder().getBinNames()));
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