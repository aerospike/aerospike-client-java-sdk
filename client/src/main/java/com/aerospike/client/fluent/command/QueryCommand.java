/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.fluent.command;

import com.aerospike.client.fluent.AsyncRecordStream;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Node;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.QueryDuration;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.query.Filter;
import com.aerospike.client.fluent.query.QueryBuilder;
import com.aerospike.client.fluent.query.QueryHint;

public final class QueryCommand extends Command {
	final String set;
	final Filter filter;
    final PartitionFilter pf;
	final QueryDuration expectedDuration;
	final long maxRecords;
	final String[] binNames;
	final int maxConcurrentNodes;
	final int recordsPerSecond;
	final int readTouchTtlPercent;
	final boolean withNoBins;

	public QueryCommand(
		Cluster cluster, DataSet set, Filter filter, Expression filterExp, Settings policy, QueryBuilder qb
	) {
		super(cluster, set.getNamespace(), null, filterExp, policy.getReplicaOrder(), policy);
		this.set = set.getSet();
		this.filter = applyHintToFilter(filter, qb.getQueryHint());

		this.pf = PartitionFilter.range(qb.getStartPartition(),
	        qb.getEndPartition() - qb.getStartPartition());

		this.recordsPerSecond = qb.getRecordsPerSecond();
		this.expectedDuration = qb.getEffectiveQueryDuration();
		this.binNames = qb.getBinNames();
		this.maxConcurrentNodes = policy.getMaxConcurrentNodes();
		this.readTouchTtlPercent = policy.getResetTtlOnReadAtPercent();
		this.withNoBins = qb.getWithNoBins();

		if (qb.getChunkSize() > 0) {
			this.maxRecords = qb.getChunkSize();
		}
	    else if (qb.getChunkSize() == 0 && qb.getLimit() > 0) {
			this.maxRecords = qb.getLimit();
		}
	    else {
	    	this.maxRecords = 0;
	    }
	}

    public void execute(AsyncRecordStream stream) {
		Node[] nodes = cluster.validateNodes();

		PartitionTracker tracker = new PartitionTracker(this, nodes, pf);
        QueryExecutor exec = new QueryExecutor(cluster, this, nodes.length, tracker, stream);

        cluster.startVirtualThread(() -> {
    		try {
    	        exec.execute();
    		}
    		catch (Throwable e) {
    			exec.stopThreads(e);
    		}
        });
    }

    public boolean isDone() {
    	return pf.isDone();
    }

    private static Filter applyHintToFilter(Filter filter, QueryHint.Result hint) {
        if (hint == null || filter == null) {
            return filter;
        }
        String hintIndex = hint.getIndexName();
        String hintBin = hint.getBinName();
        if (hintIndex == null && hintBin == null) {
            return filter;
        }
        if (hintIndex != null) {
            return Filter.withOverrides(filter, null, hintIndex);
        }
        return Filter.withOverrides(filter, hintBin, null);
    }
}
