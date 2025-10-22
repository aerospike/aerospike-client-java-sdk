package com.aerospike.client.fluent.query;

import java.util.HashMap;

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Log;
import com.aerospike.client.fluent.Partitions;
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.Txn;
import com.aerospike.client.fluent.command.ReadCommand;
import com.aerospike.client.fluent.command.SyncReadExecutor;
import com.aerospike.client.fluent.dsl.ParseResult;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Behavior.CommandType;
import com.aerospike.client.fluent.policy.SettableAvailabilityModeReadPolicy;
import com.aerospike.client.fluent.policy.SettableConsistencyModeReadPolicy;

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
    	System.out.println("IN executeInternal");
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

        try {
    		ReadCommand cmd;

        	if (partitions.scMode) {
    			SettableConsistencyModeReadPolicy policy = session.getBehavior().getSettablePolicy(
    				CommandType.READ_SC);
    			cmd = new ReadCommand(cluster, partitions, txn, key, qb.getBinNames(),
    				qb.getWithNoBins(), filterExp, failOnFilteredOut, policy);
    		}
    		else {
    			SettableAvailabilityModeReadPolicy policy = session.getBehavior().getSettablePolicy(
    				CommandType.READ_AP);
    			cmd = new ReadCommand(cluster, partitions, txn, key, qb.getBinNames(),
    				qb.getWithNoBins(), filterExp, failOnFilteredOut, policy);
    		}

        	SyncReadExecutor exec = new SyncReadExecutor(cluster, cmd);
        	exec.execute();

        	Record record = exec.getRecord();
        	return new RecordStream(key, record, true);
        }
        catch (AerospikeException ae) {
            if (Log.warnEnabled() && ae.getResultCode() == ResultCode.UNSUPPORTED_FEATURE) {
                if (this.getQueryBuilder().getTxnToUse() != null && !getSession().isNamespaceSC(key.namespace)) {
                    Log.warn(String.format("Namespace '%s' is involved in transaction, but it is not an SC namespace. "
                            + "This will throw an Unsupported Server Feature Exception.", key.namespace));
                }
            }
            throw ae;
        }
    }
}