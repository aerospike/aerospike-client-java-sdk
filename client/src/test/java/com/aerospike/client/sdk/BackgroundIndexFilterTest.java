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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.query.Filter;
import com.aerospike.client.sdk.query.IndexCollectionType;
import com.aerospike.client.sdk.query.IndexType;
import com.aerospike.client.sdk.query.PreparedAel;
import com.aerospike.client.sdk.task.ExecuteTask;

public class BackgroundIndexFilterTest extends ClusterTest {
    private static final String SET_NAME = "bg_index_filter";
    private static final String INDEX_NAME = "bg_index_filter_age_idx";
    private static final String MISSING_INDEX = "bg_index_filter_missing_idx";
    private static final String AGE_BIN = "age";
    private static final String PASS_BIN = "update_pass";
    private static final String MARKER_BIN = "marker";
    private static final DataSet DATA_SET = DataSet.of(args.namespace, SET_NAME);
    private static final Filter AGE_FILTER = Filter.rangeByIndex(INDEX_NAME, 30, 65);

    private static final String INSIDE_PASS = "inside-pass";
    private static final String INSIDE_FAIL = "inside-fail";
    private static final String OUTSIDE_PASS = "outside-pass";
    private static final String OUTSIDE_FAIL = "outside-fail";

    @BeforeAll
    static void prepareIndex() {
        deleteFixtureRecords();
        dropFixtureIndexIfExistsAndWait(INDEX_NAME);
        dropFixtureIndexIfExistsAndWait(MISSING_INDEX);
        session.createIndex(DATA_SET, INDEX_NAME, AGE_BIN, IndexType.INTEGER, IndexCollectionType.DEFAULT)
            .waitTillComplete();
    }

    @BeforeEach
    void reseedFixture() {
        deleteFixtureRecords();
        seed(INSIDE_PASS, 40, 1);
        seed(INSIDE_FAIL, 45, 10);
        seed(OUTSIDE_PASS, 80, 1);
        seed(OUTSIDE_FAIL, 85, 10);
    }

    @AfterAll
    static void destroyIndex() {
        deleteFixtureRecords();
        dropFixtureIndexIfExistsAndWait(INDEX_NAME);
        dropFixtureIndexIfExistsAndWait(MISSING_INDEX);
    }

    @Test
    void updateFilterOnlyAffectsIndexCandidates() {
        ExecuteTask task = session.backgroundTask()
            .update(DATA_SET)
            .filter(AGE_FILTER)
            .bin(MARKER_BIN).setTo("filter-only")
            .execute();

        task.waitTillComplete();

        assertMarker(INSIDE_PASS, "filter-only");
        assertMarker(INSIDE_FAIL, "filter-only");
        assertMarker(OUTSIDE_PASS, "original");
        assertMarker(OUTSIDE_FAIL, "original");
    }

    @Test
    void updateFilterAndProgrammaticWhereAffectIntersectionOnly() {
        ExecuteTask task = session.backgroundTask()
            .update(DATA_SET)
            .filter(AGE_FILTER)
            .where(Exp.lt(Exp.intBin(PASS_BIN), Exp.val(5)))
            .bin(MARKER_BIN).setTo("intersection")
            .execute();

        task.waitTillComplete();

        assertMarker(INSIDE_PASS, "intersection");
        assertMarker(INSIDE_FAIL, "original");
        assertMarker(OUTSIDE_PASS, "original");
        assertMarker(OUTSIDE_FAIL, "original");
    }

    @Test
    void deleteFilterOnlyRemovesIndexCandidates() {
        ExecuteTask task = deleteBuilder()
            .filter(AGE_FILTER)
            .execute();

        task.waitTillComplete();

        assertMissing(INSIDE_PASS);
        assertMissing(INSIDE_FAIL);
        assertPresent(OUTSIDE_PASS);
        assertPresent(OUTSIDE_FAIL);
    }

    @Test
    void deleteFilterAndProgrammaticWhereRemoveIntersectionOnly() {
        ExecuteTask task = deleteBuilder()
            .filter(AGE_FILTER)
            .where(Exp.lt(Exp.intBin(PASS_BIN), Exp.val(5)))
            .execute();

        task.waitTillComplete();

        assertMissing(INSIDE_PASS);
        assertPresent(INSIDE_FAIL);
        assertPresent(OUTSIDE_PASS);
        assertPresent(OUTSIDE_FAIL);
    }

    @Test
    void touchFilterAndProgrammaticWhereAffectIntersectionOnly() {
        assumeTrue(args.hasTtl, "Server does not support TTL");

        ExecuteTask task = session.backgroundTask()
            .touch(DATA_SET)
            .filter(AGE_FILTER)
            .where(Exp.lt(Exp.intBin(PASS_BIN), Exp.val(5)))
            .expireRecordAfter(Duration.ofSeconds(60))
            .execute();

        task.waitTillComplete();

        assertExpiring(INSIDE_PASS);
        assertNeverExpires(INSIDE_FAIL);
        assertNeverExpires(OUTSIDE_PASS);
        assertNeverExpires(OUTSIDE_FAIL);
    }

    @Test
    void missingNamedIndexFailsAtExecute() {
        AerospikeException ae = assertThrows(AerospikeException.class, () ->
            session.backgroundTask()
                .update(DATA_SET)
                .filter(Filter.rangeByIndex(MISSING_INDEX, 30, 65))
                .bin(MARKER_BIN).setTo("missing-index")
                .execute());

        assertEquals(ResultCode.INDEX_NOTFOUND, ae.getResultCode());
    }

    @Test
    void updateFilterAndTextualWhereAffectIntersectionOnlyWhenAelIsSupported() {
        assumeSupportsAel();

        ExecuteTask task = session.backgroundTask()
            .update(DATA_SET)
            .filter(AGE_FILTER)
            .where("$.update_pass < 5")
            .bin(MARKER_BIN).setTo("textual")
            .execute();

        task.waitTillComplete();

        assertMarker(INSIDE_PASS, "textual");
        assertMarker(INSIDE_FAIL, "original");
        assertMarker(OUTSIDE_PASS, "original");
        assertMarker(OUTSIDE_FAIL, "original");
    }

    @Test
    void updateFilterAndPreparedWhereAffectIntersectionOnlyWhenAelIsSupported() {
        assumeSupportsAel();
        PreparedAel prepared = PreparedAel.prepare("$.update_pass < ?0");

        ExecuteTask task = session.backgroundTask()
            .update(DATA_SET)
            .filter(AGE_FILTER)
            .where(prepared, 5)
            .bin(MARKER_BIN).setTo("prepared")
            .execute();

        task.waitTillComplete();

        assertMarker(INSIDE_PASS, "prepared");
        assertMarker(INSIDE_FAIL, "original");
        assertMarker(OUTSIDE_PASS, "original");
        assertMarker(OUTSIDE_FAIL, "original");
    }

    private static BackgroundOperationBuilder deleteBuilder() {
        BackgroundOperationBuilder builder = session.backgroundTask().delete(DATA_SET);
        if (args.scMode) {
            builder = builder.defaultWithDurableDelete();
        }
        return builder;
    }

    private static void seed(String id, int age, int updatePass) {
        session.upsert(DATA_SET.id(id))
            .neverExpire()
            .bin(AGE_BIN).setTo(age)
            .bin(PASS_BIN).setTo(updatePass)
            .bin(MARKER_BIN).setTo("original")
            .execute();
    }

    private static void deleteFixtureRecords() {
        List<Key> keys = Arrays.asList(
            DATA_SET.id(INSIDE_PASS),
            DATA_SET.id(INSIDE_FAIL),
            DATA_SET.id(OUTSIDE_PASS),
            DATA_SET.id(OUTSIDE_FAIL));
        ChainableNoBinsBuilder delete = session.delete(keys);
        if (args.scMode) {
            delete = delete.withDurableDelete();
        }
        delete.execute();
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

    private static void assertMarker(String id, String expected) {
        assertEquals(expected, record(id).getString(MARKER_BIN), id);
    }

    private static void assertPresent(String id) {
        assertTrue(exists(id), "expected record to exist: " + id);
    }

    private static void assertMissing(String id) {
        assertFalse(exists(id), "expected record to be deleted: " + id);
    }

    private static void assertExpiring(String id) {
        assertTrue(record(id).getTimeToLive() > 0, "expected record to have TTL: " + id);
    }

    private static void assertNeverExpires(String id) {
        assertEquals(-1, record(id).getTimeToLive(), "expected record to remain never-expire: " + id);
    }

    private static boolean exists(String id) {
        try (RecordStream rs = session.query(DATA_SET.id(id)).execute()) {
            return rs.hasNext();
        }
    }

    private static Record record(String id) {
        try (RecordStream rs = session.query(DATA_SET.id(id)).execute()) {
            assertTrue(rs.hasNext(), "expected record to exist: " + id);
            return rs.next().recordOrThrow();
        }
    }
}
