package com.aerospike.client.fluent.query;

import com.aerospike.client.fluent.AsyncRecordStream;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Log;
import com.aerospike.client.fluent.Node;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.command.PartitionFilter;
import com.aerospike.client.fluent.command.PartitionTracker;
import com.aerospike.client.fluent.command.QueryCommand;
import com.aerospike.client.fluent.command.QueryExecutor;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Behavior.Mode;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.dsl.ParseResult;

class IndexQueryBuilderImpl extends QueryImpl {
    private final DataSet dataSet;
    public IndexQueryBuilderImpl(QueryBuilder builder, Session session, DataSet dataSet) {
        super(builder, session);
        this.dataSet = dataSet;
    }

    @Override
    public boolean allowsSecondaryIndexQuery() {
        return true;
    }
    @Override
    public RecordStream execute() {
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
        // Index queries stream results; async and sync behave similarly
        return executeInternal();
    }

    private RecordStream executeInternal() {
        Session session = getSession();
        Cluster cluster = session.getCluster();
        QueryBuilder qb = getQueryBuilder();
        Settings policy = session.getBehavior().getSettings(OpKind.READ, OpShape.QUERY, Mode.ANY);
        WhereClauseProcessor dsl = getQueryBuilder().getDsl();
        Filter filter = null;
        Expression filterExp = null;

        if (dsl != null) {
        	ParseResult pr = dsl.process(dataSet.getNamespace(), getSession());
        	filter = pr.getFilter();
        	filterExp = pr.getExpression();
        }

		QueryCommand cmd = new QueryCommand(cluster, dataSet, filter, filterExp, policy, qb);

		Node[] nodes = cluster.validateNodes();

		PartitionFilter pf = PartitionFilter.range(
                getQueryBuilder().getStartPartition(),
                getQueryBuilder().getEndPartition() - getQueryBuilder().getStartPartition());

		PartitionTracker tracker = new PartitionTracker(cmd, nodes, pf);
        AsyncRecordStream stream = new AsyncRecordStream(policy.getRecordQueueSize());
        QueryExecutor exec = new QueryExecutor(cluster, cmd, nodes.length, tracker, stream);

        cluster.startVirtualThread(() -> {
    		try {
    	        exec.execute();
    		}
    		catch (Throwable e) {
    			exec.stopThreads(e);
    		}
        });

        return new RecordStream(stream);
    }
}