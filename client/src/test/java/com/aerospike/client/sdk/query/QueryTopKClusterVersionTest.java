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
package com.aerospike.client.sdk.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.SingleNodeBatchFixture;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.util.Version;

/**
 * A Top-K query needs server version 8.2.1 on every node, and fails before it is sent when any node
 * is older. See {@link QueryBuilder#topK(int)}.
 */
class QueryTopKClusterVersionTest {
    private static final String NOT_SUPPORTED = "not supported by all nodes in the cluster";

    private static final DataSet DATA_SET = DataSet.of("test", "topk_cluster_version");

    @ParameterizedTest
    @CsvSource({
        "8.1.3.0,    false",
        "8.2.0.0,    false",
        "8.2.0.3,    false",
        "8.2.1.0,    true",
        "8.2.1.0-46, true",
        "8.2.2.0,    true",
        "8.3.0.0,    true",
        "9.0.0.0,    true"
    })
    void supportsTopKWhenTheOnlyNodeIsAtLeast821(String version, boolean expected) {
        Cluster cluster = clusterOf(version);

        assertEquals(expected, cluster.supportsTopK());
    }

    @ParameterizedTest
    @CsvSource({
        "8.2.1.0, 8.1.3.0, false",
        "8.1.3.0, 8.2.1.0, false",
        "8.2.2.0, 8.2.0.3, false",
        "8.2.1.0, 8.2.2.0, true"
    })
    void supportsTopKOnlyWhenEveryNodeIsAtLeast821(String first, String second, boolean expected) {
        Cluster cluster = clusterOf(first, second);

        assertEquals(expected, cluster.supportsTopK());
    }

    @ParameterizedTest
    @CsvSource({
        "8.2.1.0, 8.1.3.0",
        "8.2.2.0, 8.2.0.3",
        "8.1.3.0, 8.1.3.0"
    })
    void topKQueryFailsBeforeSendingWhenAnyNodeIsOlder(String first, String second) {
        Cluster cluster = clusterOf(first, second);
        Session session = cluster.createSession(Behavior.DEFAULT);
        QueryBuilder query = session.query(DATA_SET)
            .orderBy("n", BinDataType.INTEGER, Order.ASC)
            .topK(5);

        AerospikeException ae = assertThrows(AerospikeException.class, query::execute);

        assertEquals(QueryBuilder.TOP_K_CLIENT_ERROR, ae.getResultCode());
        assertTrue(ae.getMessage().contains(NOT_SUPPORTED), ae.getMessage());
    }

    @Test
    void failureNamesTheLowestServerVersion() {
        Cluster cluster = clusterOf("8.2.2.0", "8.1.3.0", "8.2.1.0");
        Session session = cluster.createSession(Behavior.DEFAULT);
        QueryBuilder query = session.query(DATA_SET)
            .orderBy("n", BinDataType.INTEGER, Order.ASC)
            .topK(5);

        AerospikeException ae = assertThrows(AerospikeException.class, query::execute);

        assertTrue(ae.getMessage().contains("8.1.3.0"), ae.getMessage());
    }

    @Test
    void topKQueryPassesTheClusterCheckWhenEveryNodeSupportsIt() {
        Cluster cluster = clusterOf("8.2.1.0", "8.2.2.0");
        Session session = cluster.createSession(Behavior.DEFAULT);
        QueryBuilder query = session.query(DATA_SET)
            .orderBy("n", BinDataType.INTEGER, Order.ASC)
            .topK(5);

        assertNotRejectedForServerVersion(query);
    }

    @Test
    void queryWithoutTopKIsNotAffectedByAnOlderNode() {
        Cluster cluster = clusterOf("8.2.1.0", "8.1.3.0");
        Session session = cluster.createSession(Behavior.DEFAULT);
        QueryBuilder query = new QueryBuilder(session, DATA_SET);

        assertNotRejectedForServerVersion(query);
    }

    /**
     * The query is not run against a server: the nodes are never connected, so it can fail for other
     * reasons. It must not fail because of the server version.
     */
    private static void assertNotRejectedForServerVersion(QueryBuilder query) {
        try (RecordStream stream = query.execute()) {
            assertNotNull(stream);
        }
        catch (AerospikeException ae) {
            assertFalse(ae.getMessage().contains(NOT_SUPPORTED), ae.getMessage());
        }
    }

    /** A cluster with one never-connected node for each server version, in the given order. */
    private static Cluster clusterOf(String... versions) {
        Cluster cluster = SingleNodeBatchFixture.cluster();
        Node[] nodes = new Node[versions.length];

        for (int i = 0; i < versions.length; i++) {
            nodes[i] = SingleNodeBatchFixture.node(cluster, "n" + i, new Version(versions[i]));
        }
        cluster.setNodes(nodes);
        return cluster;
    }
}
