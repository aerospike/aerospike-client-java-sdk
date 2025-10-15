package com.aerospike.client.fluent.query;

import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Log;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;

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
		return null;
	/*
        QueryPolicy queryPolicy = getSession().getBehavior().getMutablePolicy(CommandType.QUERY);
        if (this.getQueryBuilder().getWithNoBins()) {
            queryPolicy.includeBinData = false;
        }

        long pageSize = getQueryBuilder().getPageSize();
        long limit = getQueryBuilder().getLimit();
        List<SortProperties> sortInfo = getQueryBuilder().getSortInfo();

        Statement stmt = new Statement();
        stmt.setNamespace(dataSet.getNamespace());
        stmt.setSetName(dataSet.getSet());
        stmt.setBinNames(getQueryBuilder().getBinNames());

        if (getQueryBuilder().dslString != null) {
            ParseResult parseResult = this.getParseResultFromWhereClause(getQueryBuilder().dslString, this.dataSet.getNamespace(), true);
            queryPolicy.filterExp = parseResult.getExp() == null ? null : Exp.build(parseResult.getExp());
            stmt.setFilter(parseResult.getFilter());
        }

        if (pageSize > 0) {
            stmt.setMaxRecords(pageSize);
        }
        else if (limit > 0 && pageSize == 0) {
            stmt.setMaxRecords(limit);
        }

        // No need to set transactions, they're not supported by queries
        // queryPolicy.txn = this.getQueryBuilder().getTxnToUse();

        PartitionFilter filter = PartitionFilter.range(
                getQueryBuilder().getStartPartition(),
                getQueryBuilder().getEndPartition() - getQueryBuilder().getStartPartition());

        RecordSet queryResults = getSession().getClient().queryPartitions(queryPolicy, stmt, filter);
        return new RecordStream(getSession(), queryPolicy, stmt, filter, queryResults, limit, sortInfo);
        */
    }
}