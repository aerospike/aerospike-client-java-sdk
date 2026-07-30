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

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.Session;

/** Shared {@code qselint} fixture for server query-selection integration tests. */
final class QuerySelectionIntegSupport {
    static final String SET_NAME = "qselint";
    static final String AGE_INDEX = "qsel_age_idx";
    static final String SCORE_INDEX = "qsel_score_idx";
    static final String KEY_PREFIX = "qselkey";
    static final String AGE_BIN = "age";
    static final String SCORE_BIN = "score";
    static final String COUNTRY_BIN = "country";
    static final int RECORD_COUNT = 50;

    private QuerySelectionIntegSupport() {
    }

    static void assumeQuerySelectionEnabled() {
        assumeTrue(ClusterTest.cluster.supportsQuerySelection(),
            "server does not support query selection");
    }

    static DataSet prepareQselint(Session session, String namespace) {
        DataSet dataSet = DataSet.of(namespace, SET_NAME);

        for (int i = 1; i <= RECORD_COUNT; i++) {
            session.delete(dataSet.ids(KEY_PREFIX + i));
        }

        createIndexQuietly(session, dataSet, AGE_INDEX, AGE_BIN, IndexType.INTEGER,
            IndexCollectionType.DEFAULT);
        createIndexQuietly(session, dataSet, SCORE_INDEX, SCORE_BIN, IndexType.INTEGER,
            IndexCollectionType.DEFAULT);

        for (int i = 1; i <= RECORD_COUNT; i++) {
            String country = (i % 2 == 0) ? "US" : "CA";
            session.upsert(dataSet.ids(KEY_PREFIX + i))
                .bins(AGE_BIN, SCORE_BIN, COUNTRY_BIN)
                .values(i, i, country)
                .execute();
        }

        return dataSet;
    }

    static void destroyQselint(Session session, DataSet dataSet) {
        if (dataSet == null) {
            return;
        }

        for (int i = 1; i <= RECORD_COUNT; i++) {
            session.delete(dataSet.ids(KEY_PREFIX + i));
        }
        session.delete(dataSet.ids(KEY_PREFIX + "missing"));
        dropIndexQuietly(session, dataSet, AGE_INDEX);
        dropIndexQuietly(session, dataSet, SCORE_INDEX);
    }

    static void upsertRow(Session session, DataSet dataSet, int keyNum, int age, int score, String country) {
        session.upsert(dataSet.ids(KEY_PREFIX + keyNum))
            .bins(AGE_BIN, SCORE_BIN, COUNTRY_BIN)
            .values(age, score, country)
            .execute();
    }

    static List<Integer> collectAges(RecordStream rs, String ageBin) {
        try {
            List<Integer> ages = new ArrayList<>();
            while (rs.hasNext()) {
                ages.add(rs.next().recordOrThrow().getInt(ageBin));
            }
            ages.sort(Integer::compareTo);
            return ages;
        }
        finally {
            rs.close();
        }
    }

    static int countRecords(RecordStream rs) {
        try {
            int count = 0;
            while (rs.hasNext()) {
                rs.next().recordOrThrow();
                count++;
            }
            return count;
        }
        finally {
            rs.close();
        }
    }

    private static void createIndexQuietly(
        Session session,
        DataSet dataSet,
        String indexName,
        String binName,
        IndexType indexType,
        IndexCollectionType collectionType
    ) {
        try {
            session.createIndex(dataSet, indexName, binName, indexType, collectionType)
                .waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }
    }

    private static void dropIndexQuietly(Session session, DataSet dataSet, String indexName) {
        try {
            session.dropIndex(dataSet, indexName);
        }
        catch (AerospikeException ignored) {
        }
    }
}
