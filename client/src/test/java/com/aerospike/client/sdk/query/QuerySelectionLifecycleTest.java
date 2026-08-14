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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.NavigatableRecordStream;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;

/**
 * What an application sees when the index catalog changes underneath it, and when a matching result
 * set is drained in pieces rather than all at once.
 *
 * <p>The PRD's operational promise is that adding or dropping a secondary index is an optimisation
 * decision, never a correctness one: the same predicate must return the same rows before and after,
 * differing only in how the server got them. These tests assert exactly that — identical row sets
 * across a catalog change — plus the pagination invariant that motivates pinning a plan for the life
 * of a query, namely that chunked and paged reads neither drop nor duplicate a record.</p>
 *
 * <p>This class owns a private set and private index names. Every other query-selection class shares
 * the {@code qselint} fixture, and a test that drops an index mid-run would otherwise corrupt it.
 * {@link #resetIndexBaseline()} restores the intended catalog before each test so a failure part-way
 * through one test cannot cascade into the next.</p>
 */
class QuerySelectionLifecycleTest extends ClusterTest {
    private static final String setName = "qsellife";
    private static final String keyPrefix = "qsellifekey";
    private static final String ageBin = "age";
    private static final String countryBin = "country";
    private static final String ageIndex = "qsellife_age_idx";
    private static final String countryIndex = "qsellife_country_idx";
    private static final int recordCount = 20;

    /** Matches ages 5..9, so it is a strict subset of the set and spans several chunks/pages. */
    private static final String ageRangeWhere = "$.age >= 5 and $.age <= 9";
    private static final List<Integer> ageRangeRows = List.of(5, 6, 7, 8, 9);

    private static DataSet dataSet;

    @BeforeAll
    static void prepare() {
        QuerySelectionIntegSupport.assumeQuerySelectionEnabled();

        dataSet = DataSet.of(args.namespace, setName);

        for (int i = 1; i <= recordCount; i++) {
            session.upsert(dataSet.ids(keyPrefix + i))
                .bins(ageBin, countryBin)
                .values(i, (i % 2 == 0) ? "US" : "CA")
                .execute();
        }
    }

    @AfterAll
    static void destroy() {
        if (dataSet == null) {
            return;
        }
        for (int i = 1; i <= recordCount; i++) {
            session.delete(dataSet.ids(keyPrefix + i)).execute();
        }
        dropIndexQuietly(ageIndex);
        dropIndexQuietly(countryIndex);
    }

    /** Baseline for every test: age indexed, country not. */
    @BeforeEach
    void resetIndexBaseline() {
        createIndexQuietly(ageIndex, ageBin, IndexType.INTEGER);
        dropIndexQuietly(countryIndex);
    }

    // -------------------------------------------------------------------------- catalog changes

    /**
     * Dropping the only index that can serve the predicate must degrade to a primary-index scan and
     * return the identical row set, not an error and not a short result.
     *
     * <p>Row-set equality alone would also hold if the drop silently did nothing, so the
     * {@code requireIndex()} probe is what makes this test mean something: it forbids a
     * primary-index plan, so its failure after the drop proves no index remained and the read above
     * genuinely fell back to a scan.</p>
     */
    @Test
    void executeAfterOnlyIndexDroppedReturnsSameRows() {
        List<Integer> withIndex = agesWhere(ageRangeWhere);

        dropIndexQuietly(ageIndex);
        List<Integer> withoutIndex = agesWhere(ageRangeWhere);
        AerospikeException noIndexLeft = assertThrows(AerospikeException.class,
            () -> requireIndexAges(ageRangeWhere));

        assertAll("indexDropped",
            () -> assertEquals(ageRangeRows, withIndex),
            () -> assertEquals(withIndex, withoutIndex),
            () -> assertEquals(ResultCode.INDEX_NOTFOUND, noIndexLeft.getResultCode(),
                "the drop must be visible to the planner"));
    }

    /**
     * The reverse direction: an operator adds an index for a predicate that was scanning. Results
     * must be unchanged, since the new index only changes how the rows are found.
     *
     * <p>The {@code requireIndex()} probe brackets the change — rejected before the index exists,
     * satisfied after — so the equality assertion cannot pass by the creation being a no-op.</p>
     */
    @Test
    void executeAfterIndexCreatedReturnsSameRows() {
        String where = "$.country == 'US'";

        List<Integer> beforeIndex = agesWhere(where);
        AerospikeException noIndexYet = assertThrows(AerospikeException.class,
            () -> requireIndexAges(where));

        createIndexQuietly(countryIndex, countryBin, IndexType.STRING);

        List<Integer> afterIndex = agesWhere(where);
        List<Integer> viaNewIndex = requireIndexAges(where);

        assertAll("indexCreated",
            () -> assertEquals(10, beforeIndex.size(), "half the fixture has country US"),
            () -> assertEquals(ResultCode.INDEX_NOTFOUND, noIndexYet.getResultCode()),
            () -> assertEquals(beforeIndex, afterIndex),
            () -> assertEquals(beforeIndex, viaNewIndex,
                "the new index must return what the scan returned"));
    }

    // ------------------------------------------------------------------------- pagination stability

    /**
     * A query pinned to a plan must finish on that plan. An index appearing between chunks could
     * plausibly re-plan mid-stream and re-scan or skip a partition; the drained set must still be
     * exactly the matching rows, with no duplicates.
     */
    @Test
    void executeChunkedWhileSecondIndexCreatedReturnsStableSet() {
        List<Integer> ages = new ArrayList<>();
        int chunks = 0;
        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(ageBin)
            .where(ageRangeWhere)
            .chunkSize(2)
            .execute();

        try {
            while (rs.hasMoreChunks()) {
                chunks++;
                while (rs.hasNext()) {
                    ages.add(rs.next().recordOrThrow().getInt(ageBin));
                }
                if (chunks == 1) {
                    createIndexQuietly(countryIndex, countryBin, IndexType.STRING);
                }
            }
        }
        finally {
            rs.close();
        }

        List<Integer> sorted = new ArrayList<>(ages);
        sorted.sort(Integer::compareTo);
        int observedChunks = chunks;

        assertAll("chunkedDuringIndexCreate",
            () -> assertEquals(ageRangeRows, sorted),
            () -> assertEquals(ageRangeRows.size(), ages.size(), "no record returned twice"),
            () -> assertTrue(observedChunks > 1,
                "the index must be created mid-stream for this test to mean anything, but the "
                    + "query completed in " + observedChunks + " chunk(s)"));
    }

    /**
     * Chunking and partition restriction must compose: draining complementary partition ranges in
     * chunks yields each matching record exactly once between them.
     */
    @Test
    void executeChunkedAcrossPartitionRangesReturnsCompleteSet() {
        List<Integer> lower = chunkedAges(ageRangeWhere, 0, 2048);
        List<Integer> upper = chunkedAges(ageRangeWhere, 2048, 4096);

        List<Integer> union = new ArrayList<>(lower);
        union.addAll(upper);
        union.sort(Integer::compareTo);

        assertAll("chunkedPartitionRanges",
            () -> assertEquals(ageRangeRows, union),
            () -> assertTrue(Collections.disjoint(lower, upper),
                "a record must not appear in both partition ranges"));
    }

    /**
     * Client-side pagination over an AEL result set: every page together yields the complete set,
     * and the page boundaries fall where {@code pageSize} says they should.
     */
    @Test
    void navigatableStreamWithWherePaginatesCompleteSet() {
        NavigatableRecordStream nav = session.query(dataSet)
            .readingOnlyBins(ageBin)
            .where(ageRangeWhere)
            .execute()
            .asNavigatableStream()
            .pageSize(2)
            .sortBy(ageBin);

        List<Integer> ages = new ArrayList<>();
        List<Integer> pageSizes = new ArrayList<>();

        while (nav.hasMorePages()) {
            int onThisPage = 0;
            while (nav.hasNext()) {
                ages.add(nav.next().recordOrThrow().getInt(ageBin));
                onThisPage++;
            }
            pageSizes.add(onThisPage);
        }

        assertAll("navigatablePagination",
            () -> assertEquals(ageRangeRows, ages, "pages in order yield the sorted set"),
            () -> assertEquals(List.of(2, 2, 1), pageSizes, "five rows at pageSize 2"));
    }

    // ------------------------------------------------------------------------------------ helpers

    private static List<Integer> agesWhere(String where) {
        return collectAges(session.query(dataSet)
            .readingOnlyBins(ageBin)
            .where(where)
            .execute());
    }

    /** Same query, but a primary-index plan is forbidden, so it fails unless an index can serve it. */
    private static List<Integer> requireIndexAges(String where) {
        return collectAges(session.query(dataSet)
            .readingOnlyBins(ageBin)
            .where(where)
            .withHint(hint -> hint.requireIndex())
            .execute());
    }

    private static List<Integer> chunkedAges(String where, int startIncl, int endExcl) {
        List<Integer> ages = new ArrayList<>();
        RecordStream rs = session.query(dataSet)
            .onPartitionRange(startIncl, endExcl)
            .readingOnlyBins(ageBin)
            .where(where)
            .chunkSize(2)
            .execute();

        try {
            while (rs.hasMoreChunks()) {
                while (rs.hasNext()) {
                    ages.add(rs.next().recordOrThrow().getInt(ageBin));
                }
            }
        }
        finally {
            rs.close();
        }
        return ages;
    }

    private static List<Integer> collectAges(RecordStream rs) {
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

    private static void createIndexQuietly(String indexName, String binName, IndexType indexType) {
        try {
            session.createIndex(dataSet, indexName, binName, indexType, IndexCollectionType.DEFAULT)
                .waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }
    }

    /**
     * Waits for the drop rather than only dispatching it. This class drops and recreates indexes far
     * more often than the rest of the suite, and leaving catalog work in flight risks a later
     * same-named create racing a pending drop.
     */
    private static void dropIndexQuietly(String indexName) {
        try {
            session.dropIndex(dataSet, indexName).waitTillComplete();
        }
        catch (AerospikeException ignored) {
        }
    }
}
