/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.command.PartitionTracker.NodePartitions;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.QueryDuration;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.query.QueryBuilder;

public class QueryForeground extends Command {
	final String set;
	final long taskId;
	final long maxRecords;
	final PartitionTracker tracker;
	final NodePartitions nodePartitions;
	final String[] binNames;
	final int recordsPerSecond;
	final int readTouchTtlPercent;
	final boolean withNoBins;
	final QueryDuration expectedDuration;

	public QueryForeground(
		Cluster cluster, DataSet set, Expression filterExp, Settings policy, QueryBuilder qb, long taskId,
		PartitionTracker tracker, NodePartitions nodePartitions
	) {
		super(cluster, set.getNamespace(), null, filterExp, policy.getReplicaOrder(), policy);
		this.set = set.getSet();
		this.taskId = taskId;
 		this.tracker = tracker;
		this.nodePartitions = nodePartitions;
		this.binNames = qb.getBinNames();
		this.recordsPerSecond = 0;
		this.readTouchTtlPercent = 0;
		this.withNoBins = qb.getWithNoBins();
		this.expectedDuration = QueryDuration.LONG;

		if (qb.getPageSize() > 0) {
			this.maxRecords = qb.getPageSize();
		}
	    else if (qb.getPageSize() == 0 && qb.getLimit() > 0) {
			this.maxRecords = qb.getLimit();
		}
	    else {
	    	this.maxRecords = 0;
	    }
	}
}
