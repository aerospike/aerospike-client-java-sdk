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
package com.aerospike.client.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.command.BatchCommand;
import com.aerospike.client.sdk.command.BatchNode;
import com.aerospike.client.sdk.command.BatchNodes;
import com.aerospike.client.sdk.command.BatchRecord;
import com.aerospike.client.sdk.command.Info;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.policy.Behavior.Selectors;
import com.aerospike.client.sdk.policy.ReadModeSC;
import com.aerospike.client.sdk.policy.Replica;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.tend.Partitions;

/**
 * allowSingleNodeBatchReads against a live, stable cluster. Runs only when a node owns every
 * partition of the test namespace, which is true when the replication factor equals the
 * cluster size (a single node, 2-node RF 2, 3-node RF 3, ...). The topology-specific cases
 * are covered without a server in {@code BatchNodesSingleNodeTest}.
 */
public class SingleNodeBatchReadIntegrationTest extends ClusterTest {
    private static final String BIN = "v";
    private static final int KEY_COUNT = 64;
    private static final String PROXY_STAT = "from_proxy_batch_sub_read";

    @AfterEach
    void restoreRegistry() {
        Behavior.restoreBehaviorRegistry();
    }

    @Test
    public void readOnlyBatchGoesToOneNodeWithoutProxies() {
        Partitions partitions = cluster.getPartitions(args.namespace);
        Assumptions.assumeFalse(partitions.getFullOwners().isEmpty(),
            "No node owns every partition of " + args.namespace +
            ": the replication factor is below the cluster size");

        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("single-node-batch-it", builder -> {
            builder.on(Selectors.reads().batch(), ops -> {
                ops.replicaOrder(Replica.MASTER_PROLES);
                ops.allowSingleNodeBatchReads(true);
            });
            // SESSION would restrict an SC namespace to the master.
            builder.on(Selectors.reads().batch().cp(), ops -> ops.consistency(ReadModeSC.ALLOW_REPLICA));
        });
        Session singleNode = cluster.createSession(behavior);

        List<Key> keys = new ArrayList<Key>(KEY_COUNT);

        for (int i = 0; i < KEY_COUNT; i++) {
            Key key = args.set.id("snbatch_" + i);
            session.upsert(key).bin(BIN).setTo(i).execute();
            keys.add(key);
        }

        // The client sends this batch to one node, rotating across the full owners.
        ResolvedSettings settings = behavior.getSettings(OpKind.READ, OpShape.BATCH,
            partitions.scMode ? Mode.CP : Mode.AP);
        Set<Node> targets = new HashSet<Node>();
        int rounds = 2 * partitions.getFullOwners().size();

        for (int round = 0; round < rounds; round++) {
            List<BatchRecord> records = new ArrayList<BatchRecord>(KEY_COUNT);

            for (Key key : keys) {
                records.add(new BatchRecord(key, false));
            }

            BatchCommand cmd = new BatchCommand(cluster, null, null, args.namespace, records, null, false,
                false, false, false, settings);
            List<BatchNode> batchNodes = BatchNodes.generate(cluster, cmd, records, new FailOnErrorStatus());
            assertEquals(1, batchNodes.size(), "read-only batch sent to one node");
            assertEquals(KEY_COUNT, batchNodes.get(0).offsetsSize);
            targets.add(batchNodes.get(0).node);
        }
        assertEquals(partitions.getFullOwners(), targets, "target rotates across every full owner");

        long proxiesBefore = proxyBatchSubReads();

        for (int round = 0; round < rounds; round++) {
            int found = 0;

            try (RecordStream rs = singleNode.query(keys).execute()) {
                while (rs.hasNext()) {
                    Record rec = rs.next().recordOrThrow();
                    assertTrue(rec.getLong(BIN) >= 0 && rec.getLong(BIN) < KEY_COUNT);
                    found++;
                }
            }
            assertEquals(KEY_COUNT, found, "every key read in round " + round);
        }

        long proxiesAfter = proxyBatchSubReads();

        if (proxiesBefore >= 0 && proxiesAfter >= 0) {
            assertEquals(proxiesBefore, proxiesAfter, PROXY_STAT + "_* must not change on a stable cluster");
        }
    }

    /**
     * Return the sum of the namespace's from_proxy_batch_sub_read_* statistics across all
     * nodes, or -1 if the server doesn't report them.
     */
    private static long proxyBatchSubReads() {
        long total = 0;
        boolean seen = false;

        for (Node node : cluster.getNodes()) {
            String stats = Info.request(node, "namespace/" + args.namespace);

            if (stats == null) {
                continue;
            }

            for (String pair : stats.split(";")) {
                int eq = pair.indexOf('=');

                if (eq > 0 && pair.substring(0, eq).contains(PROXY_STAT)) {
                    total += Long.parseLong(pair.substring(eq + 1).trim());
                    seen = true;
                }
            }
        }
        return seen ? total : -1;
    }

    private static final class FailOnErrorStatus implements BatchNodes.IBatchStatus {
        @Override
        public void batchKeyError(Key key, int index, AerospikeException ae, boolean inDoubt, boolean hasWrite) {
            throw ae;
        }

        @Override
        public void batchKeyError(AerospikeException ae) {
            throw ae;
        }
    }
}
