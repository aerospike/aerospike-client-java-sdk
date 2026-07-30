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

import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.AGE_BIN;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.COUNTRY_BIN;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.KEY_PREFIX;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.RECORD_COUNT;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.collectAges;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.countRecords;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.prepareQselint;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.upsertRow;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QuerySelection;

/**
 * Range and data-shape coverage for server-led query selection.
 */
class QuerySelectionDataShapesIntegrationTest extends ClusterTest {
    private static DataSet dataSet;

    @BeforeAll
    static void prepare() {
        QuerySelectionIntegSupport.assumeQuerySelectionEnabled();
        dataSet = prepareQselint(session, args.namespace);
    }

    @AfterAll
    static void destroy() {
        QuerySelectionIntegSupport.destroyQselint(session, dataSet);
    }

    /**
     * delete a few keys so the indexed range has gaps in stored ages.
     */
    @Test
    void executeRangeAcrossSparseAgesReturnsExistingSubset() {
        session.delete(dataSet.ids(KEY_PREFIX + 15));
        session.delete(dataSet.ids(KEY_PREFIX + 16));
        session.delete(dataSet.ids(KEY_PREFIX + 17));

        try {
            List<Integer> ages = collectAges(session.query(dataSet)
                .readingOnlyBins(AGE_BIN)
                .where("$.age >= 14 and $.age <= 18")
                .execute(), AGE_BIN);

            assertEquals(List.of(14, 18), ages);
        }
        finally {
            upsertRow(session, dataSet, 15, 15, 15, "CA");
            upsertRow(session, dataSet, 16, 16, 16, "US");
            upsertRow(session, dataSet, 17, 17, 17, "CA");
        }
    }

    /** full index span over the fixture. */
    @Test
    void executeFullAgeSpanReturnsAllRecords() {
        List<Integer> ages = collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where("$.age >= 1 and $.age <= 50")
            .execute(), AGE_BIN);

        assertEquals(RECORD_COUNT, ages.size());
        assertEquals(1, ages.get(0));
        assertEquals(RECORD_COUNT, ages.get(ages.size() - 1));
    }

    /** degenerate point range. */
    @Test
    void executePointRangeReturnsSingleAge() {
        List<Integer> ages = collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where("$.age >= 25 and $.age <= 25")
            .execute(), AGE_BIN);

        assertEquals(List.of(25), ages);
    }

    /**
     * B.4 — range above dataset max: SI plan, zero matching rows.
     */
    @Test
    void executeRangeAboveDatasetMaxReturnsEmptyStream() {
        String where = "$.age >= 51 and $.age <= 60";
        QueryPlan plan = QueryPlannerSupport.plan(dataSet, where);

        int count = countRecords(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .execute());

        assertAll("rangeAboveMax",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(0, count));
    }

    /**
     * records without {@code age} bin do not appear in an age filter result set.
     */
    @Test
    void executeRangeSkipsRecordsMissingAgeBin() {
        session.upsert(dataSet.ids(KEY_PREFIX + "missing"))
            .bin(COUNTRY_BIN).setTo("US")
            .execute();

        try {
            List<Integer> ages = collectAges(session.query(dataSet)
                .readingOnlyBins(AGE_BIN)
                .where("$.age >= 14 and $.age <= 18")
                .execute(), AGE_BIN);

            assertEquals(List.of(14, 15, 16, 17, 18), ages);
        }
        finally {
            session.delete(dataSet.ids(KEY_PREFIX + "missing"));
        }
    }

    /**
     * PI filter on a minority country value after skewing the fixture.
     */
    @Test
    void executeMinorityCountryPredicateReturnsSkewedCount() {
        upsertRow(session, dataSet, 1, 1, 1, "MX");
        upsertRow(session, dataSet, 3, 3, 3, "MX");
        upsertRow(session, dataSet, 5, 5, 5, "MX");

        try {
            QueryPlan plan = QueryPlannerSupport.plan(dataSet, "$.country == 'MX'");

            int count = countRecords(session.query(dataSet)
                .readingOnlyBins(COUNTRY_BIN)
                .where("$.country == 'MX'")
                .execute());

            assertAll("minorityCountry",
                () -> assertEquals(QuerySelection.PRIMARY_INDEX, plan.getSelection()),
                () -> assertEquals(3, count));
        }
        finally {
            upsertRow(session, dataSet, 1, 1, 1, "CA");
            upsertRow(session, dataSet, 3, 3, 3, "CA");
            upsertRow(session, dataSet, 5, 5, 5, "CA");
        }
    }
}
