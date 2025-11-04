package com.aerospike.client.fluent.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Log;
import com.aerospike.client.fluent.Partitions;
import com.aerospike.client.fluent.RecordResult;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.Txn;
import com.aerospike.client.fluent.command.Batch;
import com.aerospike.client.fluent.command.BatchExecutor;
import com.aerospike.client.fluent.command.BatchNode;
import com.aerospike.client.fluent.command.BatchNodeList;
import com.aerospike.client.fluent.command.BatchRead;
import com.aerospike.client.fluent.command.BatchReadCommand;
import com.aerospike.client.fluent.command.BatchRecord;
import com.aerospike.client.fluent.command.BatchSingle;
import com.aerospike.client.fluent.command.BatchStatus;
import com.aerospike.client.fluent.command.IBatchCommand;
import com.aerospike.client.fluent.dsl.ParseResult;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.ReadModeSC;
import com.aerospike.client.fluent.policy.Replica;
import com.aerospike.client.fluent.policy.Settings;

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
    	QueryBuilder qb = getQueryBuilder();

        Txn txn = qb.getTxnToUse();

        if (txn != null) {
			txn.prepareReadKeys(keyList);
		}

        Expression filterExp = null;
        WhereClauseProcessor dsl = qb.getDsl();

        if (dsl != null) {
            ParseResult parseResult = dsl.process(keyList.get(0).namespace, session);
            filterExp = Exp.build(parseResult.getExp());
        }

        long limit = getQueryBuilder().getLimit();
        List<BatchRecord> batchRecords = new ArrayList<BatchRecord>(keyList.size());
        List<BatchRecord> batchRecordsForServer = hasPartitionFilter() ? new ArrayList<>() : batchRecords;

        for (Key thisKey : keyList) {
            // If there is no "where" clause and the limit has been exceeded, exit the loop
            if (filterExp == null && limit > 0 && batchRecords.size() >= limit) {
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
                    batchRecords.add(new BatchRecord(thisKey, false));
                }
            }
            else {
                BatchRecord thisBatchRecord;
                if (getQueryBuilder().getWithNoBins()) {
                    thisBatchRecord = new BatchRead(thisKey, false);
                }
                else if (getQueryBuilder().getBinNames() != null) {
                    thisBatchRecord = new BatchRead(thisKey, getQueryBuilder().getBinNames());
                }
                else {
                    thisBatchRecord = new BatchRead(thisKey, true);
                }
                batchRecordsForServer.add(thisBatchRecord);
            }
        }

        // Assume all keys have the same namespace.
        String namespace = keyList.get(0).namespace;
		HashMap<String,Partitions> partitionMap = cluster.getPartitionMap();
		Partitions partitions = partitionMap.get(namespace);

		if (partitions == null) {
			throw new AerospikeException.InvalidNamespace(namespace, partitionMap.size());
		}

		Settings policy = session.getBehavior().getSettings(OpKind.READ, OpShape.BATCH, partitions.scMode);
		BatchReadCommand parent;
		Replica replica;

        if (partitions.scMode) {
            ReadModeSC mode = policy.getReadModeSC();
            boolean linearize;

            switch (mode) {
            case SESSION:
            	replica = Replica.MASTER;
            	linearize = false;
                break;

            case LINEARIZE:
                replica = policy.getReplicaOrder();

                if (replica == Replica.PREFER_RACK) {
                    replica = Replica.SEQUENCE;
                }
            	linearize = true;
                break;

            default:
                replica = policy.getReplicaOrder();
            	linearize = false;
                break;
            }

            parent = new BatchReadCommand(cluster, partitions, qb.getTxnToUse(), namespace,
            	batchRecordsForServer, filterExp, replica, mode, qb.isRespondAllKeys(),
            	linearize, policy);
        }
        else {
            replica = policy.getReplicaOrder();
            parent = new BatchReadCommand(cluster, partitions, qb.getTxnToUse(), namespace,
                batchRecordsForServer, filterExp, replica, policy.getReadModeAP(),
                qb.isRespondAllKeys(), policy);
       }

    	BatchStatus status = new BatchStatus(true);
		List<BatchNode> bns = BatchNodeList.generate(cluster, partitions, replica,
			batchRecordsForServer, status);

		IBatchCommand[] commands = new IBatchCommand[bns.size()];
    	int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				BatchRecord record = batchRecordsForServer.get(i);

				BatchRead br = (BatchRead)record;
				commands[count++] = new BatchSingle.ReadRecord(cluster, parent, br, status, bn.node);
			}
			else {
				commands[count++] = new Batch.OperateListSync(cluster, parent, bn, batchRecordsForServer, status);
			}
		}

		try {
			BatchExecutor.execute(cluster, commands, status);

            if (!qb.isRespondAllKeys()) {
                // Remove any items which have been filtered out.
                batchRecordsForServer.removeIf(br -> (br.resultCode == ResultCode.OK && br.record == null)
                        || (br.resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
                        || (br.resultCode == ResultCode.FILTERED_OUT && !getQueryBuilder().isFailOnFilteredOut()));
            }

            if (hasPartitionFilter()) {
                // Add the server results into any that were filtered out earlier
                batchRecords.addAll(batchRecordsForServer);
            }
            // Convert BatchRecord to RecordResult
            List<RecordResult> results = new ArrayList<>();
            Settings settings = getSession().getBehavior()
                    .getSettings(OpKind.READ, OpShape.BATCH, getSession().isNamespaceSC(keyList.get(0).namespace));
            for (int i = 0; i < batchRecords.size(); i++) {
                BatchRecord br = batchRecords.get(i);
                if (getQueryBuilder().shouldIncludeResult(br.resultCode)) {
                    results.add(getQueryBuilder().createRecordResultFromBatchRecord(br, settings, i));
                }
            }
            // TODO: ResultsInKeyOrder?
            return new RecordStream(results,
                    limit,
                    getQueryBuilder().getPageSize(),
                    getQueryBuilder().getSortInfo(),
                    true);
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
