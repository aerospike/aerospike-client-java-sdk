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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.aerospike.client.fluent.command.DeleteExecutor;
import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.command.TxnMonitor;
import com.aerospike.client.fluent.command.WriteCommand;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.Settings;

public class OperationWithNoBinsBuilder {
    private final List<Key> keys;
    private final Key key;
    private final OpType opType;
    private final Session session;

    public OperationWithNoBinsBuilder(Session session, Key key, OpType type) {
        this.keys = null;
        this.key = key;
        this.opType = type;
        this.session = session;
    }

    public OperationWithNoBinsBuilder(Session session, List<Key> keys, OpType type) {
        if (keys.size() == 1) {
            this.keys = null;
            this.key = keys.get(0);
        }
        else {
            this.key = null;
            this.keys = keys;
        }
        this.opType = type;
        this.session = session;
    }
    private Key getAnyKey() {
        if (key != null) {
            return key;
        }
        else {
            return keys.get(0);
        }
    }
    private List<Boolean> toList(boolean[] booleanArray) {
        List<Boolean> results = new ArrayList<>();
        for (int i = 0; i < booleanArray.length; i++) {
            results.add(booleanArray[i]);
        }
        return results;
    }
/*
    private List<Boolean> batchExecute(WritePolicy wp) {
        // TODO: Exceptions!!

        switch (opType) {
        case EXISTS: {
            BatchPolicy batchPolicy = session.getBehavior()
                    .getSettings(OpKind.READ, OpShape.BATCH, session.isNamespaceSC(getAnyKey().namespace))
                    .asBatchPolicy();
            boolean[] results = session.getClient().exists(batchPolicy, keys.toArray(new Key[0]));
            return toList(results);
        }

        case TOUCH: {
            BatchPolicy batchPolicy = session.getBehavior()
                    .getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, session.isNamespaceSC(keys.get(0).namespace))
                    .asBatchPolicy();
            BatchWritePolicy batchWritePolicy = new BatchWritePolicy();
            batchWritePolicy.sendKey = batchPolicy.sendKey;

            BatchResults results = session.getClient().operate(batchPolicy, batchWritePolicy, keys.toArray(Key[]::new), Operation.touch());
            List<Boolean> booleanArray = new ArrayList<>();
            for (BatchRecord record : results.records) {
                booleanArray.add(record.resultCode == ResultCode.OK);
            }
            return booleanArray;
        }


        case DELETE: {
            BatchPolicy batchPolicy = session.getBehavior()
                    .getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, session.isNamespaceSC(getAnyKey().namespace))
                    .asBatchPolicy();
            BatchDeletePolicy batchDeletePolicy = new BatchDeletePolicy();
            batchDeletePolicy.sendKey = batchPolicy.sendKey;
            BatchResults results = session.getClient().delete(batchPolicy, batchDeletePolicy, keys.toArray(Key[]::new));
            List<Boolean> booleanArray = new ArrayList<>();
            for (BatchRecord record : results.records) {
                booleanArray.add(record.resultCode == ResultCode.OK);
            }
            return booleanArray;
        }
        default:
            throw new IllegalStateException("received an action of " + opType + " which should be handled elsewhere");
        }
    }
    */

    public List<Boolean> execute() {
        if (key != null) {
            boolean result;

            switch (opType) {
            case EXISTS:
            	// TODO: Do exists
                //result = session.getClient().exists(wp, key);
            	result = false;
                break;
            case TOUCH:
            	// TODO: Do touch
                //result = session.getClient().touched(wp, key);
            	result = false;
                break;
            case DELETE:
                result = delete(key);
                break;
            default:
                throw new IllegalStateException("received an action of " + opType + " which should be handled elsewhere");
            }

            return List.of(result);
        }
        else {
        	// TODO Batch exists/touch/delete.
            //return batchExecute(wp);
        	return null;
        }
    }

    private boolean delete(Key key) {
        Cluster cluster = session.getCluster();
        Partitions partitions = getPartitions(cluster, key.namespace);
        Settings policy = session.getBehavior().getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, partitions.scMode);

        // TODO: Where is the txn instance?
        Txn txn = null;

        // TODO: Where is the gen instance?
        int gen = 0;

        // TODO: Where is the ttl instance?
        int ttl = 0;

        // TODO: Where is the filter expression?
        Expression filterExp = null;

        // TODO: Where is failOnFilteredOut?
        boolean failOnFilteredOut = false;

        WriteCommand cmd = new WriteCommand(cluster, partitions, txn, key, OpType.DELETE,
        	gen, ttl, filterExp, failOnFilteredOut, policy);

        if (txn != null) {
        	TxnMonitor.addKeys(txn, cluster, partitions, policy, keys);
        }

        DeleteExecutor exec = new DeleteExecutor(cluster, cmd);
        exec.execute();
        return exec.existed();
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
