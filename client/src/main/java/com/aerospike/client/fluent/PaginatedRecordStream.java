package com.aerospike.client.fluent;

import com.aerospike.client.fluent.policy.QueryPolicy;
import com.aerospike.client.fluent.query.PartitionFilter;
import com.aerospike.client.fluent.query.RecordSet;
import com.aerospike.client.fluent.query.RecordStreamImpl;
import com.aerospike.client.fluent.query.Statement;

public class PaginatedRecordStream implements RecordStreamImpl {
    private final QueryPolicy queryPolicy;
    private final Statement statement;
    private final PartitionFilter filter;
    private RecordSet recordSet;
    private final long limit;
    private final Session session;
    private long recordCount = 0;

    public PaginatedRecordStream(Session session, QueryPolicy queryPolicy, Statement statement,
            PartitionFilter filter, RecordSet recordSet, long limit) {

        this.queryPolicy = queryPolicy;
        this.statement = statement;
        this.filter = filter;
        this.recordSet = recordSet;
        this.limit = limit;
        this.session = session;
    }

    @Override
    public boolean hasMorePages() {
        if (limit > 0 && recordCount >= limit) {
            return false;
        }
        return !filter.isDone();
    }

    @Override
    public boolean hasNext() {
    	/*
        if (limit > 0 && recordCount >= limit) {
            return false;
        }
        boolean result = recordSet.next();
        if (!result) {
            // Move onto the next page
            recordSet = session.getClient().queryPartitions(queryPolicy, statement, filter);
        }
        else {
            recordCount++;
        }
        return result;
        */
    	return false;
    }

    @Override
    public RecordResult next() {
        return new RecordResult(recordSet.getKeyRecord());
    }

    @Override
    public void close() {
        if (this.recordSet != null) {
            this.recordSet.close();
        }
    }
}
