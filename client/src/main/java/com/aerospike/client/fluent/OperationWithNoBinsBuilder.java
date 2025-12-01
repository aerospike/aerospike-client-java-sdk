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
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.aerospike.client.fluent.command.DeleteExecutor;
import com.aerospike.client.fluent.command.ExistsExecutor;
import com.aerospike.client.fluent.command.ReadAttr;
import com.aerospike.client.fluent.command.ReadCommand;
import com.aerospike.client.fluent.command.TouchExecutor;
import com.aerospike.client.fluent.command.TxnMonitor;
import com.aerospike.client.fluent.command.WriteCommand;
import com.aerospike.client.fluent.dsl.BooleanExpression;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
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
            // Single key operation
            return executeSingleKey();
        }
        else {
        	// TODO Batch exists/touch/delete.
            //return batchExecute(wp);
        	return null;
            // Multi-key (batch) operation
        	/*
            WritePolicy wp = session.getBehavior()
                    .getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, session.isNamespaceSC(getAnyKey().namespace))
                    .asWritePolicy();
            return batchExecute(wp);
            */
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

	/* TODO Batch
    private List<Boolean> batchExecute() {
        String namespace = getAnyKey().namespace;

        switch (opType) {
        case EXISTS: {
            BatchPolicy batchPolicy = session.getBehavior()
                    .getSettings(OpKind.READ, OpShape.BATCH, session.isNamespaceSC(namespace))
                    .asBatchPolicy();
            batchPolicy = applyBatchPolicySettings(batchPolicy, namespace);

            boolean[] results = session.getClient().exists(batchPolicy, keys.toArray(new Key[0]));
            return toList(results);
        }

        case TOUCH: {
            BatchPolicy batchPolicy = session.getBehavior()
                    .getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, session.isNamespaceSC(namespace))
                    .asBatchPolicy();
            batchPolicy = applyBatchPolicySettings(batchPolicy, namespace);

            BatchWritePolicy batchWritePolicy = new BatchWritePolicy();
            batchWritePolicy.sendKey = batchPolicy.sendKey;

            if (expirationInSecondsForAll != 0) {
                batchWritePolicy.expiration = (int) expirationInSecondsForAll;
            }
            applyGenerationPolicy(batchWritePolicy);

            BatchResults results = session.getClient().operate(batchPolicy, batchWritePolicy, keys.toArray(Key[]::new), Operation.touch());
            return processBatchResults(results);
        }

        case DELETE: {
            BatchPolicy batchPolicy = session.getBehavior()
                    .getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, session.isNamespaceSC(namespace))
                    .asBatchPolicy();
            batchPolicy = applyBatchPolicySettings(batchPolicy, namespace);

            BatchDeletePolicy batchDeletePolicy = new BatchDeletePolicy();
            batchDeletePolicy.sendKey = batchPolicy.sendKey;

            applyGenerationPolicy(batchDeletePolicy);

            if (durablyDelete != null) {
                batchDeletePolicy.durableDelete = durablyDelete;
            }

            BatchResults results = session.getClient().delete(batchPolicy, batchDeletePolicy, keys.toArray(Key[]::new));
            return processBatchResults(results);
        }

        default:
            throw new IllegalStateException("received an action of " + opType + " which should be handled elsewhere");
        }
    }

    private List<Boolean> processBatchResults(BatchResults results) {
        List<Boolean> booleanArray = new ArrayList<>();
        for (BatchRecord record : results.records) {
            if (failOnFilteredOut && record.resultCode == ResultCode.FILTERED_OUT) {
                throw new RuntimeException("Record was filtered out by filter expression");
            }
            booleanArray.add(record.resultCode == ResultCode.OK);
        }
        return booleanArray;
    }

    private void applyGenerationPolicy(Object policy) {
        if (generation > 0) {
            if (policy instanceof BatchWritePolicy) {
                BatchWritePolicy bwp = (BatchWritePolicy) policy;
                bwp.generationPolicy = com.aerospike.client.policy.GenerationPolicy.EXPECT_GEN_EQUAL;
                bwp.generation = generation;
            } else if (policy instanceof BatchDeletePolicy) {
                BatchDeletePolicy bdp = (BatchDeletePolicy) policy;
                bdp.generationPolicy = com.aerospike.client.policy.GenerationPolicy.EXPECT_GEN_EQUAL;
                bdp.generation = generation;
            }
        }
    }
    */

    /**
     * Apply filter expression and respondAllKeys to a batch policy.
     * Creates a new BatchPolicy if modifications are needed.
     */
    /*
    private BatchPolicy applyBatchPolicySettings(BatchPolicy batchPolicy, String namespace) {
        // Apply filter expression if set
        Expression filterExp = processWhereClause(namespace, session);
        if (filterExp != null) {
            batchPolicy = new BatchPolicy(batchPolicy);
            batchPolicy.filterExp = filterExp;
        }

        // Apply respondAllKeys flag
        if (respondAllKeys) {
            if (batchPolicy == null || filterExp == null) {
                batchPolicy = new BatchPolicy(batchPolicy);
            }
            batchPolicy.respondAllKeys = true;
        }

        return batchPolicy;
    }

    private List<Boolean> toList(boolean[] booleanArray) {
        List<Boolean> results = new ArrayList<>();
        for (int i = 0; i < booleanArray.length; i++) {
            results.add(booleanArray[i]);
        }
        return results;
    }
    private Key getAnyKey() {
        if (key != null) {
            return key;
        }
        else {
            return keys.get(0);
        }
    }

    */

    private Partitions getPartitions(Cluster cluster, String namespace) {
        HashMap<String, Partitions> partitionMap = cluster.getPartitionMap();
        Partitions partitions = partitionMap.get(namespace);

        if (partitions == null) {
            throw new AerospikeException.InvalidNamespace(namespace, partitionMap.size());
        }
        return partitions;
    }
}
