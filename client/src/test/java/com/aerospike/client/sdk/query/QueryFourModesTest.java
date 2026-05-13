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
 * Unless required by applicable law or agreed in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.sdk.query;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.exp.Exp;

/**
 * Illustrates four legacy-style query shapes on the fluent client (compare to classic
 * {@code Statement} + {@code QueryPolicy}):
 *
 * <ol>
 *   <li><b>Where clause only</b> — secondary index drives the query (legacy: {@code setFilter}
 *       only). Here: AEL on the indexed bin only so the client can emit {@code INDEX_RANGE}
 *       without a separate packed row filter when the predicate fits the index slice.</li>
 *   <li><b>Where clause + filter expression</b> — SI slice plus residual row predicate (legacy:
 *       {@code setFilter} + {@code policy.filterExp}). Here: AEL combining indexed bin and
 *       another bin.</li>
 *   <li><b>Filter expression / primary-style</b> — no SI slice; scan partitions and evaluate
 *       packed expression only (legacy: no {@code setFilter}, {@code policy.filterExp} only).
 *       Fluent: {@code where(Exp)} (and {@code where(Expression)}) disable SI selection in
 *       {@link com.aerospike.client.sdk.query.WhereClauseProcessor}.</li>
 *   <li><b>AEL with format parameters</b> — {@code where(String, Object...)} binds values via
 *       {@link String#format(String, Object[])} (see {@link QueryBuilder#where(String, Object...)}).
 *       You can also build the string with {@link PreparedAel#formValue(Object...)} and pass it
 *       to {@code where(String)}.</li>
 * </ol>
 */
public class QueryFourModesTest extends ClusterTest {

    private static final String setSuffix = "q4mode";
    private static final String indexName = "q4modeidx";
    private static final String idxBin = "idxbin";
    private static final String bin2 = "bin2";
    private static final String keyPrefix = "q4";
    private static final int size = 40;

    private static DataSet dataSet;

    @BeforeAll
    public static void prepare() {
        String setName = args.set.getSet() + setSuffix;
        dataSet = DataSet.of(args.namespace, setName);

        for (int i = 1; i <= size; i++) {
            session.delete(dataSet.ids(keyPrefix + i));
        }

        try {
            session.createIndex(dataSet, indexName, idxBin, IndexType.INTEGER, IndexCollectionType.DEFAULT)
                .waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }

        for (int i = 1; i <= size; i++) {
            session.upsert(dataSet.ids(keyPrefix + i))
                .bins(idxBin, bin2)
                .values(i, i * 10)
                .execute();
        }
    }

    @AfterAll
    public static void destroy() {
        for (int i = 1; i <= size; i++) {
            session.delete(dataSet.ids(keyPrefix + i));
        }
        session.dropIndex(dataSet, indexName);
    }

    /**
     * Legacy (1): secondary-index predicate only — AEL confined to the indexed bin.
     */
    @Test
    public void mode1_whereClause_secondaryIndexOnly() {
        String ael = "$." + idxBin + " >= 10 and $." + idxBin + " <= 12";

        RecordStream rs = session.query(dataSet)
            .where(ael)
            .execute();

        try {
            assertEquals(3, count(rs));
        }
        finally {
            rs.close();
        }
    }

    /**
     * Legacy (2): SI slice + additional row filter — AEL on indexed bin and on {@code bin2}.
     */
    @Test
    public void mode2_whereClause_plusFilterExpression_residual() {
        // idxbin in [10, 25] and bin2 == 220  =>  i == 22
        String ael = "$." + idxBin + " >= 10 and $." + idxBin + " <= 25 and $." + bin2 + " == 220";

        RecordStream rs = session.query(dataSet)
            .where(ael)
            .execute();

        try {
            assertEquals(1, count(rs));
        }
        finally {
            rs.close();
        }
    }

    /**
     * Legacy (3): primary / scan path with packed expression only — {@code where(Exp)} (no SI).
     */
    @Test
    public void mode3_filterExpression_primaryIndexStyle() {
        Exp filter = Exp.and(
            Exp.ge(Exp.intBin(idxBin), Exp.val(5)),
            Exp.le(Exp.intBin(idxBin), Exp.val(8)));

        RecordStream rs = session.query(dataSet)
            .where(filter)
            .execute();

        try {
            assertEquals(4, count(rs));
        }
        finally {
            rs.close();
        }
    }

    /**
     * Legacy (4): AEL with bound parameters — {@code where(String, Object...)} (not the packed
     * {@code Exp} API). Same DSL as (1)/(2), different way to inject values than a single literal.
     */
    @Test
    public void mode4_aelWithFormatParameters() {
        RecordStream rs = session.query(dataSet)
            .where("$." + idxBin + " == %d", 23)
            .execute();

        try {
            assertEquals(1, count(rs));
        }
        finally {
            rs.close();
        }
    }

    private static int count(RecordStream rs) {
        int n = 0;
        while (rs.hasNext()) {
            rs.next();
            n++;
        }
        return n;
    }
}
