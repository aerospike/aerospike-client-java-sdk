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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.ErrorStrategy;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.util.Version;

public class QueryExpressionCollectionIndexFilterTest extends ClusterTest {
    private static final String SET_NAME = "query_exp_coll_filter";
    private static final String INDEX_NAME = "query_exp_coll_filter_idx";
    private static final String MISSING_INDEX = "query_exp_coll_filter_missing_idx";
    private static final String LICENSES_BIN = "licenses";
    private static final String STATUS_BIN = "status";
    private static final String MATCH_LICENSE = "7XYZ789";
    private static final DataSet DATA_SET = DataSet.of(args.namespace, SET_NAME);
    private static final Expression LICENSES_EXP = Exp.build(Exp.listBin(LICENSES_BIN));

    @BeforeAll
    static void prepare() {
        Version version = session.getCluster().getRandomNode().getVersion();
        assumeTrue(version.isGreaterOrEqual(8, 1, 0, 0),
            "expression secondary indexes require server 8.1+");

        deleteFixtureRecords();
        dropFixtureIndexIfExistsAndWait(INDEX_NAME);
        session.createIndex(DATA_SET, INDEX_NAME, IndexType.STRING, IndexCollectionType.LIST, LICENSES_EXP)
            .waitTillComplete();
        seedFixtureRecords();
    }

    @AfterAll
    static void destroy() {
        deleteFixtureRecords();
        dropFixtureIndexIfExistsAndWait(INDEX_NAME);
    }

    @Test
    void containsByIndexReturnsRowsFromExpressionListIndex() {
        assertStatuses(
            session.query(DATA_SET)
                .filter(Filter.containsByIndex(INDEX_NAME, IndexCollectionType.LIST, MATCH_LICENSE))
                .execute(),
            "active", "inactive");
    }

    @Test
    void containsExpressionReturnsRowsFromExpressionListIndex() {
        assertStatuses(
            session.query(DATA_SET)
                .filter(Filter.contains(LICENSES_EXP, IndexCollectionType.LIST, MATCH_LICENSE))
                .execute(),
            "active", "inactive");
    }

    @Test
    void explicitFilterWithProgrammaticResidualReturnsActiveMatch() {
        assertStatuses(
            session.query(DATA_SET)
                .filter(Filter.containsByIndex(INDEX_NAME, IndexCollectionType.LIST, MATCH_LICENSE))
                .where(Exp.eq(Exp.stringBin(STATUS_BIN), Exp.val("active")))
                .execute(),
            "active");
    }

    @Test
    void explicitFilterWithTextualResidualReturnsActiveMatchWhenAelIsSupported() {
        assumeTrue(session.getCluster().supportsAel(), "textual residual where requires server 8.2+");

        assertStatuses(
            session.query(DATA_SET)
                .filter(Filter.containsByIndex(INDEX_NAME, IndexCollectionType.LIST, MATCH_LICENSE))
                .where("$.status == 'active'")
                .execute(),
            "active");
    }

    @Test
    void missingIndexFailureSurfacesWhileConsumingStream() {
        AerospikeException ae = assertThrows(AerospikeException.class, () ->
            drain(session.query(DATA_SET)
                .filter(Filter.containsByIndex(MISSING_INDEX, IndexCollectionType.LIST, MATCH_LICENSE))
                .execute()));

        assertEquals(ResultCode.INDEX_NOTFOUND, ae.getResultCode());
    }

    @Test
    void asyncExecutionPreservesExplicitFilter() {
        assertStatuses(
            session.query(DATA_SET)
                .filter(Filter.containsByIndex(INDEX_NAME, IndexCollectionType.LIST, MATCH_LICENSE))
                .executeAsync(ErrorStrategy.IN_STREAM),
            "active", "inactive");
    }

    @Test
    void chunkedExecutionPreservesExplicitFilter() {
        assertStatusesChunked(
            session.query(DATA_SET)
                .filter(Filter.containsByIndex(INDEX_NAME, IndexCollectionType.LIST, MATCH_LICENSE))
                .chunkSize(1)
                .execute(),
            "active", "inactive");
    }

    private static void seedFixtureRecords() {
        session.upsert(DATA_SET.ids("vehicle-1"))
            .bin(LICENSES_BIN).setTo(List.of(MATCH_LICENSE, "ABC123"))
            .bin(STATUS_BIN).setTo("active")
            .execute();
        session.upsert(DATA_SET.ids("vehicle-2"))
            .bin(LICENSES_BIN).setTo(List.of(MATCH_LICENSE, "XYZ456"))
            .bin(STATUS_BIN).setTo("inactive")
            .execute();
        session.upsert(DATA_SET.ids("vehicle-3"))
            .bin(LICENSES_BIN).setTo(List.of("OTHER123"))
            .bin(STATUS_BIN).setTo("active")
            .execute();
    }

    private static void deleteFixtureRecords() {
        session.delete(DATA_SET.ids("vehicle-1")).execute();
        session.delete(DATA_SET.ids("vehicle-2")).execute();
        session.delete(DATA_SET.ids("vehicle-3")).execute();
    }

    private static void dropFixtureIndexIfExistsAndWait(String indexName) {
        try {
            session.dropIndex(DATA_SET, indexName).waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_NOTFOUND) {
                throw ae;
            }
        }
    }

    private static void assertStatuses(RecordStream rs, String... expected) {
        List<String> actual = drain(rs);
        List<String> expectedList = new ArrayList<>(List.of(expected));
        Collections.sort(expectedList);
        assertEquals(expectedList, actual);
    }

    private static void assertStatusesChunked(RecordStream rs, String... expected) {
        List<String> actual = new ArrayList<>();
        try (rs) {
            while (rs.hasMoreChunks()) {
                while (rs.hasNext()) {
                    actual.add(rs.next().recordOrThrow().getString(STATUS_BIN));
                }
            }
        }
        Collections.sort(actual);

        List<String> expectedList = new ArrayList<>(List.of(expected));
        Collections.sort(expectedList);
        assertEquals(expectedList, actual);
    }

    private static List<String> drain(RecordStream rs) {
        try (rs) {
            List<String> statuses = new ArrayList<>();
            while (rs.hasNext()) {
                statuses.add(rs.next().recordOrThrow().getString(STATUS_BIN));
            }
            Collections.sort(statuses);
            return statuses;
        }
    }
}
