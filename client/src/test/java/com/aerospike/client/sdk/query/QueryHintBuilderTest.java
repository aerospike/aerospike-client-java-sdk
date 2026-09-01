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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.policy.QueryDuration;

/**
 * {@link QueryBuilder} hint plumbing: default state, effective duration/scan policy, and
 * single {@code withHint} enforcement.
 *
 * <p>Hint field capture is covered by {@link QueryHintTest}. {@code withHint} stores the same
 * {@link QueryHint.Result} — no separate getter round-trip tests here.</p>
 */
public class QueryHintBuilderTest extends ClusterTest {
    private static final String indexName = "hintTestIndex";
    private static final String binName = "hintbin";
    private static final String keyPrefix = "hintkey";
    private static final int size = 5;

    private static DataSet dataSet;

    @BeforeAll
    static void prepare() {
        dataSet = DataSet.of(args.namespace, "hinttest");

        for (int i = 1; i <= size; i++) {
            session.upsert(dataSet.ids(keyPrefix + i))
                .bins(binName)
                .values(i * 10)
                .execute();
        }

        try {
            session.createIndex(dataSet, indexName, binName, IndexType.INTEGER,
                IndexCollectionType.DEFAULT)
                .waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }
    }

    @AfterAll
    static void destroy() {
        try {
            session.dropIndex(dataSet, indexName);
        }
        catch (AerospikeException ignored) {
        }

        for (int i = 1; i <= size; i++) {
            session.delete(dataSet.ids(keyPrefix + i));
        }
    }

    // -- QueryBuilder stores hint correctly -----------------------------------

    @Test
    void hintIsNullByDefault() {
        QueryBuilder qb = new QueryBuilder(session, dataSet);
        assertNull(qb.getQueryHint());
    }

    // -- effective query duration ------------------------------------------------

    @Test
    void effectiveDurationDefaultsToLong() {
        QueryBuilder qb = new QueryBuilder(session, dataSet);
        assertEquals(QueryDuration.LONG, qb.getEffectiveQueryDuration());
    }

    @Test
    void effectiveDurationFromHint() {
        QueryBuilder qb = new QueryBuilder(session, dataSet);
        qb.where("$." + binName + " > 0")
            .withHint(hint -> hint.queryDuration(QueryDuration.SHORT)
            .allowScansWithWhere()
            );
        assertEquals(QueryDuration.SHORT, qb.getEffectiveQueryDuration());
        assertEquals(true, qb.getEffectiveAllowScansWithWhere());
    }

    @Test
    void hintWithoutDurationDefaultsToLong() {
        QueryBuilder qb = new QueryBuilder(session, dataSet);
        qb.where("$." + binName + " > 0")
            .withHint(hint -> hint.forIndex("idx"));

        assertEquals(QueryDuration.LONG, qb.getEffectiveQueryDuration());
    }

    // -- double call throws ---------------------------------------------------

    @Test
    void doubleWithHintThrows() {
        QueryBuilder qb = new QueryBuilder(session, dataSet);
        qb.where("$." + binName + " > 0")
            .withHint(hint -> hint.forIndex("idx"));

        assertThrows(IllegalArgumentException.class, () -> qb.withHint(hint -> hint.forBin("age")));
    }
}
