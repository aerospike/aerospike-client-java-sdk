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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.util.Version;

class QueryPlanApiTest extends ClusterTest {

    private final DataSet dataSet = DataSet.of(args.namespace, "planapitest");

    @Test
    void supportsQuerySelectionVersionGate() {
        Version saved = cluster.getVersion();
        try {
            cluster.setVersion(Version.SERVER_VERSION_8_1_2);
            assertFalse(cluster.supportsQuerySelection());
            cluster.setVersion(Version.SERVER_VERSION_8_1_3);
            assertTrue(cluster.supportsQuerySelection());
        } finally {
            cluster.setVersion(saved);
        }
    }

    @Test
    void planRequiresWhereClause() {
        AerospikeException ex = assertThrows(AerospikeException.class, () ->
            session.query(dataSet).plan());
        assertTrue(ex.getMessage().contains("where"));
    }

    @Test
    void planRequiresSupportsQuerySelection() {
        Version saved = cluster.getVersion();
        try {
            cluster.setVersion(Version.SERVER_VERSION_8_1_2);
            AerospikeException ex = assertThrows(AerospikeException.class, () ->
                session.query(dataSet).where("$.age > 30").plan());
            assertTrue(ex.getMessage().contains("query selection"));
        } finally {
            cluster.setVersion(saved);
        }
    }
}
