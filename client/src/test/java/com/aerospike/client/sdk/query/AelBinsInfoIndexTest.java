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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AelMaterializer;
import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.util.Version;

import static com.aerospike.client.sdk.query.QueryPlannerSupport.dropIndexQuietly;

/**
 * Validates server AEL {@code bins_info} for {@code .exists()} / {@code .type()} terms
 * when creating expression secondary indexes (Aug-11 server fixes).
 */
public class AelBinsInfoIndexTest extends ClusterTest {
    private static final String SET_NAME = "ael_bins_info";
    private static final String KEY_PREFIX = "abi";
    private static final String MAP_BIN = "map_bin";
    private static final String MAP_KEY = "target_key";
    private static final String EXISTS_INDEX = "ael_exists_only_idx";
    private static final String TYPE_INDEX = "ael_type_only_idx";

    private static DataSet dataSet;
    private static boolean serverSupportsExistsOnlyIndex;
    private static boolean serverSupportsTypedExistsIndex;

    @BeforeAll
    public static void prepare() {
        Assumptions.assumeTrue(cluster.supportsAel(), "cluster does not report AEL support");
        Version serverVersion = cluster.getRandomNode().getVersion();
        Assumptions.assumeTrue(serverVersion.isGreaterOrEqual(8, 1, 0, 0),
            "expression secondary index tests require server 8.1.0+");

        dataSet = DataSet.of(args.namespace, SET_NAME);
        session.delete(dataSet.ids(KEY_PREFIX + "1")).execute();
        session.delete(dataSet.ids(KEY_PREFIX + "2")).execute();

        Map<String, Object> map = new HashMap<>();
        map.put(MAP_KEY, "indexed-value");
        session.upsert(dataSet.ids(KEY_PREFIX + "1"))
            .bin(MAP_BIN).setTo(map)
            .execute();

        serverSupportsExistsOnlyIndex = probeCreateIndex(
            EXISTS_INDEX + "_probe",
            "$." + MAP_BIN + "." + MAP_KEY + ".exists()");
        serverSupportsTypedExistsIndex = probeCreateIndex(
            TYPE_INDEX + "_probe",
            "$." + MAP_BIN + "." + MAP_KEY + ":STRING.exists()");

        dropIndexQuietly(dataSet, EXISTS_INDEX + "_probe");
        dropIndexQuietly(dataSet, TYPE_INDEX + "_probe");
    }

    @AfterAll
    public static void tearDown() {
        if (dataSet == null) {
            return;
        }
        session.delete(dataSet.ids(KEY_PREFIX + "1")).execute();
        session.delete(dataSet.ids(KEY_PREFIX + "2")).execute();
        dropIndexQuietly(dataSet, EXISTS_INDEX);
        dropIndexQuietly(dataSet, TYPE_INDEX);
    }

    @Test
    public void createMapKeysIndexFromExistsOnlyAel() {
        Assumptions.assumeTrue(serverSupportsExistsOnlyIndex,
            "server rejects expression index whose only bin dependency is .exists()");

        createIndexQuietly(EXISTS_INDEX,
            "$." + MAP_BIN + "." + MAP_KEY + ".exists()");

        try (RecordStream rs = session.query(dataSet)
            .where("$." + MAP_BIN + "." + MAP_KEY + ".exists() == true")
            .execute()) {
            assertTrue(rs.hasNext());
        }
    }

    @Test
    public void createMapKeysIndexFromTypedExistsAel() {
        Assumptions.assumeTrue(serverSupportsTypedExistsIndex,
            "server rejects typed .exists() expression index");

        createIndexQuietly(TYPE_INDEX,
            "$." + MAP_BIN + "." + MAP_KEY + ":STRING.exists()");

        try (RecordStream rs = session.query(dataSet)
            .where("$." + MAP_BIN + "." + MAP_KEY + ":STRING.exists() == true")
            .execute()) {
            assertTrue(rs.hasNext());
        }
    }

    @Test
    public void existsOnlyIndexRefreshesWhenIndexedBinWritten() {
        Assumptions.assumeTrue(serverSupportsExistsOnlyIndex,
            "server rejects expression index whose only bin dependency is .exists()");

        createIndexQuietly(EXISTS_INDEX,
            "$." + MAP_BIN + "." + MAP_KEY + ".exists()");

        Key key = dataSet.id(KEY_PREFIX + "2");
        session.delete(key).execute();

        Map<String, Object> map = new HashMap<>();
        map.put(MAP_KEY, "fresh");
        session.upsert(key)
            .bin(MAP_BIN).setTo(map)
            .execute();

        try (RecordStream rs = session.query(dataSet)
            .where("$." + MAP_BIN + "." + MAP_KEY + ".exists() == true")
            .execute()) {
            int matched = 0;
            while (rs.hasNext()) {
                rs.next();
                matched++;
            }
            assertTrue(matched >= 2,
                "expected both records after index refresh, got " + matched);
        }
    }

    private static boolean probeCreateIndex(String indexName, String ael) {
        try {
            Expression exp = AelMaterializer.expressionFromString(cluster, ael);
            session.createIndex(dataSet, indexName, IndexType.STRING, IndexCollectionType.MAPKEYS, exp)
                .waitTillComplete();
            return true;
        }
        catch (AerospikeException ex) {
            return false;
        }
    }

    private static void createIndexQuietly(String indexName, String ael) {
        Expression exp = AelMaterializer.expressionFromString(cluster, ael);
        try {
            session.createIndex(dataSet, indexName, IndexType.STRING, IndexCollectionType.MAPKEYS, exp)
                .waitTillComplete();
        }
        catch (AerospikeException ex) {
            if (ex.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ex;
            }
        }
    }
}
