package com.aerospike.client.fluent;

import com.aerospike.client.fluent.command.PartitionFilter;
import com.aerospike.client.fluent.command.RecordSet;
import com.aerospike.client.fluent.command.Statement;
import com.aerospike.client.fluent.policy.QueryPolicy;
import com.aerospike.client.fluent.query.RecordStreamImpl;

/**
 * A RecordStreamImpl that supports server-side streaming with chunking.
 * 
 * <p>This implementation fetches records in chunks from the server, allowing for
 * efficient processing of large result sets without loading all data into memory.
 * The chunks are fetched on-demand as the client iterates through the results.</p>
 * 
 * <p>This is distinct from client-side pagination provided by {@link NavigatableRecordStream},
 * which loads all data into memory and provides bi-directional navigation and sorting.</p>
 * 
 * <p><b>Key characteristics:</b></p>
 * <ul>
 *   <li>Forward-only iteration</li>
 *   <li>No sorting capability</li>
 *   <li>Suitable for billions of records</li>
 *   <li>Low memory footprint</li>
 * </ul>
 */
public class ChunkedRecordStream implements RecordStreamImpl {
    private final QueryPolicy queryPolicy;
    private final Statement statement;
    private final PartitionFilter filter;
    private RecordSet recordSet;
    private final long limit;
    private final Session session;
    private long recordCount = 0;
    
    public ChunkedRecordStream(Session session, QueryPolicy queryPolicy, Statement statement,
            PartitionFilter filter, RecordSet recordSet, long limit) {

        this.queryPolicy = queryPolicy;
        this.statement = statement;
        this.filter = filter;
        this.recordSet = recordSet;
        this.limit = limit;
        this.session = session;
    }
    
    @Override
    public boolean hasMoreChunks() {
        if (limit > 0 && recordCount >= limit) {
            return false;
        }
        return !filter.isDone();
    }
    
    @Override
    public boolean hasNext() {
        if (limit > 0 && recordCount >= limit) {
            return false;
        }
        boolean result = recordSet.next();
        if (!result) {
            // Move onto the next chunk
            // TODO: BN
            //recordSet = session.getClient().queryPartitions(queryPolicy, statement, filter);
        }
        else {
            recordCount++;
        }
        return result;
    }
    
    @Override
    public RecordResult next() {
        return new RecordResult(recordSet.getKeyRecord(), -1); // Query operation, index = -1
    }

    @Override
    public void close() {
        if (this.recordSet != null) {
            this.recordSet.close();
        }
    }
}

