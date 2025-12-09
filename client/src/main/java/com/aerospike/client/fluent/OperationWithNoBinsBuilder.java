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
package com.aerospike.client.fluent;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.aerospike.client.fluent.command.Batch;
import com.aerospike.client.fluent.command.BatchAttr;
import com.aerospike.client.fluent.command.BatchCommand;
import com.aerospike.client.fluent.command.BatchDelete;
import com.aerospike.client.fluent.command.BatchExecutor;
import com.aerospike.client.fluent.command.BatchNode;
import com.aerospike.client.fluent.command.BatchNodeList;
import com.aerospike.client.fluent.command.BatchRead;
import com.aerospike.client.fluent.command.BatchReadCommand;
import com.aerospike.client.fluent.command.BatchRecord;
import com.aerospike.client.fluent.command.BatchSingle;
import com.aerospike.client.fluent.command.BatchStatus;
import com.aerospike.client.fluent.command.BatchWrite;
import com.aerospike.client.fluent.command.BatchWriteCommand;
import com.aerospike.client.fluent.command.DeleteExecutor;
import com.aerospike.client.fluent.command.ExistsExecutor;
import com.aerospike.client.fluent.command.IBatchCommand;
import com.aerospike.client.fluent.command.ReadAttr;
import com.aerospike.client.fluent.command.ReadCommand;
import com.aerospike.client.fluent.command.TouchExecutor;
import com.aerospike.client.fluent.command.TxnMonitor;
import com.aerospike.client.fluent.command.WriteCommand;
import com.aerospike.client.fluent.dsl.BooleanExpression;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.BatchWritePolicy;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.query.PreparedDsl;
import com.aerospike.client.fluent.query.WhereClauseProcessor;

public class OperationWithNoBinsBuilder extends AbstractSessionOperationBuilder<OperationWithNoBinsBuilder> implements FilterableOperation<OperationWithNoBinsBuilder> {
    private final List<Key> keys;
    private final Key key;
    protected long expirationInSecondsForAll = 0;
    protected Boolean durablyDelete = null;  // null means use behavior default

    public OperationWithNoBinsBuilder(Session session, Key key, OpType type) {
        super(session, type);
        this.keys = null;
        this.key = key;
    }

    public OperationWithNoBinsBuilder(Session session, List<Key> keys, OpType type) {
        super(session, type);
        if (keys.size() == 1) {
            this.keys = null;
            this.key = keys.get(0);
        }
        else {
            this.key = null;
            this.keys = keys;
        }
    }

    /**
     * Set the expiration for all records in this operation relative to the current time.
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @param duration The duration after which all records should expire
     * @return This builder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     */
    public OperationWithNoBinsBuilder expireAllRecordsAfter(Duration duration) {
        if (!isMultiKey()) {
            throw new IllegalStateException("expireAllRecordsAfter() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = duration.getSeconds();
        return this;
    }

    /**
     * Set the expiration for all records in this operation relative to the current time.
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @param seconds The number of seconds after which all records should expire
     * @return This builder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     */
    public OperationWithNoBinsBuilder expireAllRecordsAfterSeconds(long seconds) {
        if (!isMultiKey()) {
            throw new IllegalStateException("expireAllRecordsAfterSeconds() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = seconds;
        return this;
    }

    /**
     * Set the expiration for all records in this operation to an absolute date/time.
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @param dateTime The date/time at which all records should expire
     * @return This builder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     * @throws IllegalArgumentException if the date is in the past
     */
    public OperationWithNoBinsBuilder expireAllRecordsAt(LocalDateTime dateTime) {
        if (!isMultiKey()) {
            throw new IllegalStateException("expireAllRecordsAt() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = getExpirationInSecondsAndCheckValue(dateTime);
        return this;
    }

    /**
     * Set the expiration for all records in this operation to an absolute date/time.
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @param date The date at which all records should expire
     * @return This builder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     * @throws IllegalArgumentException if the date is in the past
     */
    public OperationWithNoBinsBuilder expireAllRecordsAt(Date date) {
        if (!isMultiKey()) {
            throw new IllegalStateException("expireAllRecordsAt() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = getExpirationInSecondsAndCheckValue(date);
        return this;
    }

    /**
     * Set all records to never expire (TTL = -1).
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @return This builder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     */
    public OperationWithNoBinsBuilder neverExpireAllRecords() {
        if (!isMultiKey()) {
            throw new IllegalStateException("neverExpireAllRecords() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = OperationBuilder.TTL_NEVER_EXPIRE;
        return this;
    }

    /**
     * Do not change the expiration of any records (TTL = -2).
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @return This builder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     */
    public OperationWithNoBinsBuilder withNoChangeInExpirationForAllRecords() {
        if (!isMultiKey()) {
            throw new IllegalStateException("withNoChangeInExpirationForAllRecords() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = OperationBuilder.TTL_NO_CHANGE;
        return this;
    }

    /**
     * Use the server's default expiration for all records (TTL = 0).
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @return This builder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     */
    public OperationWithNoBinsBuilder expiryFromServerDefaultForAllRecords() {
        if (!isMultiKey()) {
            throw new IllegalStateException("expiryFromServerDefaultForAllRecords() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = OperationBuilder.TTL_SERVER_DEFAULT;
        return this;
    }

    @Override
    public OperationWithNoBinsBuilder where(String dsl, Object ... params) {
        setWhereClause(createWhereClauseProcessor(false, dsl, params));
        return this;
    }

    @Override
    public OperationWithNoBinsBuilder where(BooleanExpression dsl) {
        setWhereClause(WhereClauseProcessor.from(dsl));
        return this;
    }

    @Override
    public OperationWithNoBinsBuilder where(PreparedDsl dsl, Object ... params) {
        setWhereClause(WhereClauseProcessor.from(false, dsl, params));
        return this;
    }

    @Override
    public OperationWithNoBinsBuilder where(Exp exp) {
        setWhereClause(WhereClauseProcessor.from(exp));
        return this;
    }

    @Override
    public OperationWithNoBinsBuilder failOnFilteredOut() {
        this.failOnFilteredOut = true;
        return this;
    }

    @Override
    public OperationWithNoBinsBuilder respondAllKeys() {
        this.respondAllKeys = true;
        return this;
    }

    private boolean isMultiKey() {
        return keys != null && keys.size() > 1;
    }

    @Override
    protected int getExpirationAsInt() {
        long effectiveExpiration = (expirationInSeconds != 0) ? expirationInSeconds : expirationInSecondsForAll;
        return super.getExpirationAsInt(effectiveExpiration);
    }

    public List<Boolean> execute() {
        if (key != null) {
            return executeSingleKey();
        }
        else {
        	return executeBatch();
        }
    }

    private List<Boolean> executeSingleKey() {
        boolean result;

        switch (opType) {
        case EXISTS:
        	result = exists(key);
            break;
        case TOUCH:
        	result = touch(key);
            break;
        case DELETE:
            result = delete(key);
            break;
        default:
            throw new IllegalStateException("received an action of " + opType + " which should be handled elsewhere");
        }

        return List.of(result);
    }

    private boolean exists(Key key) {
        Cluster cluster = session.getCluster();
        Partitions partitions = getPartitions(cluster, key.namespace);
        Settings policy = session.getBehavior().getSettings(OpKind.READ, OpShape.POINT, partitions.scMode);
		ReadAttr attr = new ReadAttr(partitions, policy);
        Expression filterExp = processWhereClause(key.namespace, session);

		ReadCommand cmd = new ReadCommand(cluster, partitions, txnToUse, key, null, true, filterExp,
			failOnFilteredOut, policy, attr);

		if (txnToUse != null) {
			txnToUse.prepareRead(key.namespace);
		}

        ExistsExecutor exec = new ExistsExecutor(cluster, cmd);
        exec.execute();
        return exec.exists();
    }

    private boolean touch(Key key) {
        Cluster cluster = session.getCluster();
        Partitions partitions = getPartitions(cluster, key.namespace);
        Settings policy = session.getBehavior().getSettings(OpKind.READ, OpShape.POINT, partitions.scMode);
        Expression filterExp = processWhereClause(key.namespace, session);
        int ttl = getExpirationAsInt();

        WriteCommand cmd = new WriteCommand(cluster, partitions, txnToUse, key, OpType.TOUCH,
            generation, ttl, filterExp, failOnFilteredOut, policy);

        if (txnToUse != null) {
        	TxnMonitor.addKey(txnToUse, cluster, partitions, policy, key);
        }

        TouchExecutor exec = new TouchExecutor(cluster, cmd);
        exec.execute();
        return exec.touched();
    }

    private boolean delete(Key key) {
        Cluster cluster = session.getCluster();
        Partitions partitions = getPartitions(cluster, key.namespace);
        Settings policy = session.getBehavior().getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, partitions.scMode);
        Expression filterExp = processWhereClause(key.namespace, session);
        int ttl = getExpirationAsInt();

        WriteCommand cmd = new WriteCommand(cluster, partitions, txnToUse, key, OpType.DELETE,
        	generation, ttl, filterExp, failOnFilteredOut, policy);

        if (txnToUse != null) {
        	TxnMonitor.addKey(txnToUse, cluster, partitions, policy, key);
        }

        DeleteExecutor exec = new DeleteExecutor(cluster, cmd);
        exec.execute();
        return exec.existed();
    }

    private List<Boolean> executeBatch() {
        switch (opType) {
        case EXISTS:
        	return existsBatch();

        case TOUCH:
        	return touchBatch();

        case DELETE:
        	return deleteBatch();

        default:
            throw new IllegalStateException("received an action of " + opType + " which should be handled elsewhere");
        }
    }

    private List<Boolean> existsBatch() {
    	Cluster cluster = session.getCluster();
        String namespace = keys.get(0).namespace;
        Partitions partitions = getPartitions(cluster, namespace);
		Settings policy = session.getBehavior().getSettings(OpKind.READ, OpShape.BATCH, partitions.scMode);
        Expression filterExp = processWhereClause(namespace, session);

        List<BatchRecord> batchRecords = new ArrayList<>(keys.size());

        for (Key key : keys) {
            batchRecords.add(new BatchRead(key, false));
        }

		if (txnToUse != null) {
			txnToUse.prepareReadKeys(keys);
		}

		ReadAttr attr = new ReadAttr(partitions, policy);

		BatchReadCommand parent = new BatchReadCommand(cluster, partitions, txnToUse, namespace,
			batchRecords, filterExp, respondAllKeys, policy, attr);

    	BatchStatus status = new BatchStatus(true);
		List<BatchNode> bns = BatchNodeList.generate(cluster, partitions, parent.replica,
			batchRecords, status);

		IBatchCommand[] commands = new IBatchCommand[bns.size()];
    	int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				BatchRecord record = batchRecords.get(i);

				BatchRead br = (BatchRead)record;
				commands[count++] = new BatchSingle.Exists(cluster, parent, br, status, bn.node);
			}
			else {
				commands[count++] = new Batch.OperateListSync(cluster, parent, bn, batchRecords, status);
			}
		}

		BatchExecutor.execute(cluster, commands, status);

		ArrayList<Boolean> list = new ArrayList<>(batchRecords.size());

		for (BatchRecord br : batchRecords) {
			list.add(br.resultCode == ResultCode.OK);
		}

		return list;
    }

    private List<Boolean> touchBatch() {
    	Cluster cluster = session.getCluster();
        String namespace = keys.get(0).namespace;
        Partitions partitions = getPartitions(cluster, namespace);
		Settings policy = session.getBehavior().getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, partitions.scMode);
        Expression filterExp = processWhereClause(namespace, session);

		if (txnToUse != null) {
        	TxnMonitor.addKeys(txnToUse, cluster, partitions, policy, keys);
		}

    	// TODO: Move policies directly into BatchWriteCommand (like is done in BatchReadCommand).
        BatchWritePolicy bwp = new BatchWritePolicy();
        bwp.generation = generation;
        bwp.expiration = getExpirationAsInt();
        bwp.opType = OpType.TOUCH;
        bwp.sendKey = policy.getSendKey();
        bwp.durableDelete = policy.getUseDurableDelete();

        Operation[] ops = new Operation[] {Operation.touch()};

        List<BatchRecord> batchRecords = new ArrayList<>(keys.size());

        for (Key key : keys) {
            batchRecords.add(new BatchWrite(bwp, key, ops));
        }

        BatchCommand parent = new BatchCommand(cluster, partitions, txnToUse, namespace,
        	batchRecords, filterExp, policy.getReplicaOrder(), respondAllKeys, policy);

        BatchStatus status = new BatchStatus(true);
        List<BatchNode> bns = BatchNodeList.generate(cluster, partitions, policy.getReplicaOrder(),
        	batchRecords, status);

        IBatchCommand[] commands = new IBatchCommand[bns.size()];
        int count = 0;

        for (BatchNode bn : bns) {
            if (bn.offsetsSize == 1) {
                int i = bn.offsets[0];
                BatchRecord record = batchRecords.get(i);

                BatchWrite bw = (BatchWrite) record;
                BatchAttr battr = new BatchAttr();

                battr.setWrite(bw.policy);
                battr.adjustWrite(bw.ops);
                battr.setOpSize(bw.ops);

                commands[count++] = new BatchSingle.OperateRecordSync(cluster, parent, bw.ops, battr, record, status,
                        bn.node);
            }
            else {
                commands[count++] = new Batch.OperateListSync(cluster, parent, bn, batchRecords, status);
            }
        }

		BatchExecutor.execute(cluster, commands, status);

        List<Boolean> booleanArray = new ArrayList<>();

        for (BatchRecord br : batchRecords) {
        	if (br.resultCode == ResultCode.OK) {
				booleanArray.add(true);
        	}
        	else if (br.resultCode == ResultCode.FILTERED_OUT && failOnFilteredOut) {
                throw new RuntimeException("Record was filtered out by filter expression");
            }
        	else {
				booleanArray.add(false);
        	}
        }
        return booleanArray;
    }

    private List<Boolean> deleteBatch() {
    	Cluster cluster = session.getCluster();
        String namespace = keys.get(0).namespace;
        Partitions partitions = getPartitions(cluster, namespace);
		Settings policy = session.getBehavior().getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, partitions.scMode);
        Expression filterExp = processWhereClause(namespace, session);

		if (txnToUse != null) {
        	TxnMonitor.addKeys(txnToUse, cluster, partitions, policy, keys);
		}

        List<BatchRecord> batchRecords = new ArrayList<>(keys.size());

        for (Key key : keys) {
        	// TODO: Tim: When generation is used, it's highly likely to change between keys,
        	// but the api only specifies generation once for the entire batch.
            batchRecords.add(new BatchDelete(key, generation));
        }

        BatchWriteCommand parent = new BatchWriteCommand(cluster, partitions, txnToUse, namespace,
        	batchRecords, filterExp, respondAllKeys, policy);

        BatchStatus status = new BatchStatus(true);
        List<BatchNode> bns = BatchNodeList.generate(cluster, partitions, policy.getReplicaOrder(),
        	batchRecords, status);

        IBatchCommand[] commands = new IBatchCommand[bns.size()];
        int count = 0;

        for (BatchNode bn : bns) {
            if (bn.offsetsSize == 1) {
                int i = bn.offsets[0];
                BatchDelete bd = (BatchDelete)batchRecords.get(i);

                commands[count++] = new BatchSingle.Delete(cluster, parent, bd, status, bn.node);
            }
            else {
                commands[count++] = new Batch.OperateListSync(cluster, parent, bn, batchRecords, status);
            }
        }

		BatchExecutor.execute(cluster, commands, status);

        List<Boolean> booleanArray = new ArrayList<>();

        for (BatchRecord br : batchRecords) {
        	if (br.resultCode == ResultCode.OK || br.resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
				booleanArray.add(true);
        	}
        	else if (br.resultCode == ResultCode.FILTERED_OUT && failOnFilteredOut) {
                throw new RuntimeException("Record was filtered out by filter expression");
            }
        	else {
				booleanArray.add(false);
        	}
        }
        return booleanArray;
    }

    private Partitions getPartitions(Cluster cluster, String namespace) {
        HashMap<String, Partitions> partitionMap = cluster.getPartitionMap();
        Partitions partitions = partitionMap.get(namespace);

        if (partitions == null) {
            throw new AerospikeException.InvalidNamespace(namespace, partitionMap.size());
        }
        return partitions;
    }
}
