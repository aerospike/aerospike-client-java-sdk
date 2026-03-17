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
package com.aerospike.client.fluent.query;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.*;
import com.aerospike.client.fluent.info.classes.IndexType;
import com.aerospike.client.fluent.policy.QueryDuration;

/**
 * Integration tests for {@link QueryHint} applied through the {@link QueryBuilder} fluent API.
 *
 * <p>These tests validate that hint values are correctly stored on the builder and that
 * the effective query duration respects hint precedence. Full server-side validation
 * of index selection is not possible in a unit test, so we focus on the client-side
 * plumbing.</p>
 */
public class QueryHintBuilderTest extends ClusterTest {
    private static final String indexName = "hintTestIndex";
    private static final String binName = "hintbin";
    private static final String keyPrefix = "hintkey";
    private static final int size = 5;

    private static DataSet dataSet;

    @BeforeAll
    public static void prepare() {
        dataSet = DataSet.of(args.namespace, "hinttest");

        for (int i = 1; i <= size; i++) {
            session.upsert(dataSet.ids(keyPrefix + i))
                .bins(binName)
                .values(i * 10)
                .execute();
        }

        try {
            session.createIndex(dataSet, indexName, binName, IndexType.INTEGER, IndexCollectionType.DEFAULT)
                .waitTillComplete();
        } catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }
    }

    @AfterAll
    public static void destroy() {
        try {
            session.dropIndex(dataSet, indexName);
        } catch (AerospikeException ignored) {}

        for (int i = 1; i <= size; i++) {
            session.delete(dataSet.ids(keyPrefix + i));
        }
    }

    // -- QueryBuilder stores hint correctly -----------------------------------

    @Test
    public void hintIsNullByDefault() {
        QueryBuilder qb = new QueryBuilder(session, dataSet);
        assertNull(qb.getQueryHint());
    }

    @Test
    public void hintForIndexStoredOnBuilder() {
        QueryBuilder qb = new QueryBuilder(session, dataSet);
        qb.where("$." + binName + " > 0")
          .withHint(hint -> hint.forIndex("my_idx"));

        QueryHint.Result hint = qb.getQueryHint();
        assertNotNull(hint);
        assertEquals("my_idx", hint.getIndexName());
        assertNull(hint.getBinName());
        assertNull(hint.getQueryDuration());
    }

    @Test
    public void hintForBinStoredOnBuilder() {
        QueryBuilder qb = new QueryBuilder(session, dataSet);
        qb.where("$." + binName + " > 0")
          .withHint(hint -> hint.forBin("age"));

        QueryHint.Result hint = qb.getQueryHint();
        assertNotNull(hint);
        assertNull(hint.getIndexName());
        assertEquals("age", hint.getBinName());
    }

    @Test
    public void hintWithAllOptions() {
        QueryBuilder qb = new QueryBuilder(session, dataSet);
        qb.where("$." + binName + " > 0")
          .withHint(hint -> hint.forIndex("idx").queryDuration(QueryDuration.SHORT));

        QueryHint.Result hint = qb.getQueryHint();
        assertEquals("idx", hint.getIndexName());
        assertEquals(QueryDuration.SHORT, hint.getQueryDuration());
    }

    // -- effective query duration ------------------------------------------------

    @Test
    public void effectiveDurationDefaultsToLong() {
        QueryBuilder qb = new QueryBuilder(session, dataSet);
        assertEquals(QueryDuration.LONG, qb.getEffectiveQueryDuration());
    }

    @Test
    public void effectiveDurationFromHint() {
        QueryBuilder qb = new QueryBuilder(session, dataSet);
        qb.where("$." + binName + " > 0")
          .withHint(hint -> hint.queryDuration(QueryDuration.SHORT));
        assertEquals(QueryDuration.SHORT, qb.getEffectiveQueryDuration());
    }

    @Test
    public void hintWithoutDurationDefaultsToLong() {
        QueryBuilder qb = new QueryBuilder(session, dataSet);
        qb.where("$." + binName + " > 0")
          .withHint(hint -> hint.forIndex("idx"));

        assertEquals(QueryDuration.LONG, qb.getEffectiveQueryDuration());
    }

    // -- double call throws ---------------------------------------------------

    @Test
    public void doubleWithHintThrows() {
        QueryBuilder qb = new QueryBuilder(session, dataSet);
        qb.where("$." + binName + " > 0")
          .withHint(hint -> hint.forIndex("idx"));

        assertThrows(IllegalArgumentException.class, () ->
            qb.withHint(hint -> hint.forBin("age"))
        );
    }

    // -- query executes with hint (smoke test) --------------------------------

    @Test
    public void queryWithIndexHintExecutes() {
        RecordStream rs = session.query(dataSet)
            .where("$." + binName + " >= 10 and $." + binName + " <= 50")
            .withHint(hint -> hint.forIndex(indexName).queryDuration(QueryDuration.SHORT))
            .execute();

        int count = 0;
        while (rs.hasNext()) {
            rs.next().recordOrThrow();
            count++;
        }
        assertEquals(size, count);
    }

    @Test
    public void queryWithBinHintExecutes() {
        RecordStream rs = session.query(dataSet)
            .where("$." + binName + " >= 10 and $." + binName + " <= 50")
            .withHint(hint -> hint.forBin(binName))
            .execute();

        int count = 0;
        while (rs.hasNext()) {
            rs.next().recordOrThrow();
            count++;
        }
        assertEquals(size, count);
    }

    @Test
    public void queryWithDurationOnlyHintExecutes() {
        RecordStream rs = session.query(dataSet)
            .where("$." + binName + " >= 10 and $." + binName + " <= 50")
            .withHint(hint -> hint.queryDuration(QueryDuration.SHORT))
            .execute();

        int count = 0;
        while (rs.hasNext()) {
            rs.next().recordOrThrow();
            count++;
        }
        assertEquals(size, count);
    }
}
