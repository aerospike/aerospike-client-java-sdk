package com.aerospike.client.fluent.query;

import java.util.HashMap;

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Log;
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.RecordResult;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.command.ReadAttr;
import com.aerospike.client.fluent.command.ReadCommand;
import com.aerospike.client.fluent.command.ReadExecutor;
import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.tend.Partitions;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.dsl.ParseResult;

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
    	Session session = getSession();
    	Cluster cluster = session.getCluster();
    	QueryBuilder qb = getQueryBuilder();
        boolean failOnFilteredOut = qb.isFailOnFilteredOut();

        Txn txn = qb.getTxnToUse();

        if (txn != null) {
			txn.prepareRead(key.namespace);
		}

        Expression filterExp = null;
        WhereClauseProcessor dsl = qb.getDsl();

        if (dsl != null) {
            ParseResult parseResult = dsl.process(key.namespace, session);
            filterExp = Exp.build(parseResult.getExp());
        }

        // Must copy hashmap reference for copy on write semantics to work.
		HashMap<String,Partitions> partitionMap = cluster.getPartitionMap();
		Partitions partitions = partitionMap.get(key.namespace);

		if (partitions == null) {
			throw new AerospikeException.InvalidNamespace(key.namespace, partitionMap.size());
		}

		Settings policy = session.getBehavior().getSettings(OpKind.READ, OpShape.POINT, partitions.scMode);
		ReadAttr attr = new ReadAttr(partitions, policy);

		try {
			ReadCommand cmd = new ReadCommand(cluster, partitions, txn, key, qb.getBinNames(),
				qb.getWithNoBins(), filterExp, failOnFilteredOut, policy, attr);

        	ReadExecutor exec = new ReadExecutor(cluster, cmd);
        	exec.execute();

        	Record record = exec.getRecord();
            if (record != null || qb.isRespondAllKeys()) {
	        	return new RecordStream(key, record);
			}
			return new RecordStream();
        }
        catch (AerospikeException ae) {
            if (Log.warnEnabled() && ae.getResultCode() == ResultCode.UNSUPPORTED_FEATURE) {
                if (this.getQueryBuilder().getTxnToUse() != null && !getSession().isNamespaceSC(key.namespace)) {
                    Log.warn(String.format("Namespace '%s' is involved in transaction, but it is not an SC namespace. "
                            + "This will throw an Unsupported Server Feature Exception.", key.namespace));
                }
            }
            if (qb.shouldIncludeResult(0)) {
                return new RecordStream(new RecordResult(key, ae, 0));
            }
            return new RecordStream();
        }
    }
}