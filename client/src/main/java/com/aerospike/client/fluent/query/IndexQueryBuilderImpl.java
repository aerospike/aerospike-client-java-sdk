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
import com.aerospike.client.fluent.dsl.ParseResult;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Behavior.Mode;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.Settings;

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
        Expression filterExp = getFilterExp();

        // TODO Sort?
        //List<SortProperties> sortInfo = getQueryBuilder().getSortInfo();

		QueryCommand cmd = new QueryCommand(cluster, dataSet, filterExp, policy, qb);

		Node[] nodes = cluster.validateNodes();

		PartitionFilter filter = PartitionFilter.range(
                getQueryBuilder().getStartPartition(),
                getQueryBuilder().getEndPartition() - getQueryBuilder().getStartPartition());

		PartitionTracker tracker = new PartitionTracker(cmd, nodes, filter);
        AsyncRecordStream stream = new AsyncRecordStream(5000);
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

    private Expression getFilterExp() {
        WhereClauseProcessor dsl = getQueryBuilder().getDsl();

        if (dsl != null) {
            // Apply filter expression clause.
            ParseResult parseResult = dsl.process(dataSet.getNamespace(), getSession());
            return Exp.build(parseResult.getExp());
        }
        else {
            return null;
        }
    }
}