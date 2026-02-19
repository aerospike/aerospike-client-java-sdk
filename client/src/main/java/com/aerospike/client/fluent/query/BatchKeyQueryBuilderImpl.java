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
package com.aerospike.client.fluent.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.aerospike.client.fluent.AbstractFilterableBuilder;
import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Log;
import com.aerospike.client.fluent.Operation;
import com.aerospike.client.fluent.RecordResult;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.command.Batch;
import com.aerospike.client.fluent.command.BatchAttr;
import com.aerospike.client.fluent.command.BatchCommand;
import com.aerospike.client.fluent.command.BatchExecutor;
import com.aerospike.client.fluent.command.BatchNode;
import com.aerospike.client.fluent.command.BatchNodes;
import com.aerospike.client.fluent.command.BatchRead;
import com.aerospike.client.fluent.command.BatchRecord;
import com.aerospike.client.fluent.command.BatchSingle;
import com.aerospike.client.fluent.command.BatchStatus;
import com.aerospike.client.fluent.command.IBatchCommand;
import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.tend.Partitions;
import com.aerospike.dsl.ParseResult;

class BatchKeyQueryBuilderImpl extends QueryImpl {
    private final List<Key> keyList;
    public BatchKeyQueryBuilderImpl(QueryBuilder builder, Session session, List<Key> keyList) {
        super(builder, session);
        this.keyList = keyList;
    }

    @Override
    public boolean allowsSecondaryIndexQuery() {
        return false;
    }

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
        // For batch operations, async and sync are effectively the same
        // since we need to wait for the batch to complete anyway
        return executeInternal();
    }

    public RecordStream executeInternal() {
    	if (keyList.size() == 0) {
            return new RecordStream();
        }

    	Session session = getSession();
    	Cluster cluster = session.getCluster();

    	// Assume all keys have the same namespace.
        String namespace = keyList.get(0).namespace;
		HashMap<String,Partitions> partitionMap = cluster.getPartitionMap();
		Partitions partitions = partitionMap.get(namespace);

		if (partitions == null) {
			throw new AerospikeException.InvalidNamespace(namespace, partitionMap.size());
		}

		Settings settings = session.getBehavior().getSettings(OpKind.READ, OpShape.BATCH,
			partitions.scMode);

		BatchAttr attr = new BatchAttr();
        attr.setRead(settings, partitions.scMode);

    	QueryBuilder qb = getQueryBuilder();

        Txn txn = qb.getTxnToUse();

        if (txn != null) {
			txn.prepareReadKeys(keyList);
		}

        Expression where = null;
        WhereClauseProcessor dsl = qb.getDsl();

        if (dsl != null) {
            ParseResult parseResult = dsl.process(keyList.get(0).namespace, session);
            where = Exp.build(parseResult.getExp());
        }

        long limit = getQueryBuilder().getLimit();
        List<BatchRecord> records = new ArrayList<>(keyList.size());
        List<BatchRecord> recordsForServer = hasPartitionFilter() ? new ArrayList<>() : records;
        int ttl = settings.getResetTtlOnReadAtPercent();

        for (Key thisKey : keyList) {
			if (where == null && limit > 0 && records.size() >= limit) {
                break;
            }

            if (hasPartitionFilter() && !getQueryBuilder().isKeyInPartitionRange(thisKey)) {
                // We know this one will fail
                if (!getQueryBuilder().isRespondAllKeys()) {
                    // Filter it out
                    continue;
                }
                else {
                    // Need to include a record but do not send it to the server
                    records.add(new BatchRecord(thisKey, false));
                }
            }
            else {
                BatchRecord rec;
                List<Operation> ops = getQueryBuilder().getOperations();

                if (ops != null && !ops.isEmpty()) {
                    // Use operations constructor for CDT reads, selectFrom, etc.
                    rec = new BatchRead(thisKey, null, attr, ttl, ops);
                }
                else if (getQueryBuilder().getBinNames() != null) {
                    rec = new BatchRead(thisKey, null, attr, ttl, getQueryBuilder().getBinNames());
                }
                else if (getQueryBuilder().getWithNoBins()) {
                    rec = new BatchRead(thisKey, null, attr, ttl, false);
                }
                else {
                    rec = new BatchRead(thisKey, null, attr, ttl, true);
                }
                recordsForServer.add(rec);
            }
        }

		BatchCommand parent = new BatchCommand(cluster, partitions, txn, namespace,
			recordsForServer, where, qb.isRespondAllKeys(), attr.linearize, settings);

    	BatchStatus status = new BatchStatus();
		List<BatchNode> bns = BatchNodes.generate(cluster, parent, recordsForServer, status);

		IBatchCommand[] commands = new IBatchCommand[bns.size()];
    	int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				BatchRecord record = recordsForServer.get(i);
				BatchRead br = (BatchRead)record;

				commands[count++] = new BatchSingle.ReadRecordSync(cluster, parent, br, status, bn.node);
			}
			else {
				commands[count++] = new Batch.OperateListSync(cluster, parent, bn, recordsForServer, status);
			}
		}

		try {
			BatchExecutor.execute(cluster, commands, status);

            if (!qb.isRespondAllKeys()) {
                // Remove any items which have been filtered out.
                recordsForServer.removeIf(br -> (br.resultCode == ResultCode.OK && br.record == null)
                        || (br.resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
                        || (br.resultCode == ResultCode.FILTERED_OUT && !getQueryBuilder().isFailOnFilteredOut()));
            }

            if (hasPartitionFilter()) {
                // Add the server results into any that were filtered out earlier
                records.addAll(recordsForServer);
            }
            // Convert BatchRecord to RecordResult
            List<RecordResult> results = new ArrayList<>();

            for (int i = 0; i < records.size(); i++) {
                BatchRecord br = records.get(i);

                if (getQueryBuilder().shouldIncludeResult(br.resultCode)) {
                    results.add(AbstractFilterableBuilder.createRecordResultFromBatchRecord(br, settings, i));
                }
            }
            return new RecordStream(results, limit);
        }
        catch (AerospikeException ae) {
            if (Log.warnEnabled() && ae.getResultCode() == ResultCode.UNSUPPORTED_FEATURE) {
                if (this.getQueryBuilder().getTxnToUse() != null) {
                    Set<String> namespaces = keyList.stream().map(key->key.namespace).collect(Collectors.toSet());
                    namespaces.forEach(ns -> {
                        if (!getSession().isNamespaceSC(ns)) {
                            Log.warn(String.format("Namespace '%s' is involved in transaction, but it is not an SC namespace. "
                                    + "This will throw an Unsupported Server Feature exception.", ns));
                        }
                    });
                }
            }
            throw ae;
        }
    }
}
