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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.ael.Ael;
import com.aerospike.client.sdk.ael.BooleanExpression;
import com.aerospike.client.sdk.ael.LocalVariableExpression;
import com.aerospike.client.sdk.ael.LongExpression;
import com.aerospike.client.sdk.command.Buffer;

/**
 * Integration tests for the fluent {@link Ael} builder API ({@code where(BooleanExpression)},
 * {@code selectFrom(BooleanExpression)}). Complements string-AEL tests such as
 * {@link QueryStringTest} and {@link AelMaterializerWhereTest}.
 */
public class AelBuilderIntegrationTest extends ClusterTest {

    private static final String SET = "aelbuilder";
    private static final String INT_INDEX = "aelbuilder_int";
    private static final String STR_INDEX = "aelbuilder_str";
    private static final String BLOB_INDEX = "aelbuilder_blob";

    private static final String IVAL = "ival";
    private static final String SVAL = "sval";
    private static final String BVAL = "bval";
    private static final String DVAL = "dval";
    private static final String BLOB = "blob";
    private static final String OFFSET = "offset";

    private static final int SIZE = 10;

    private static DataSet dataSet;

    @BeforeAll
    public static void prepare() {
        assumeSupportsAel();

        dataSet = DataSet.of(args.namespace, SET);

        for (int i = 1; i <= SIZE; i++) {
            session.delete(dataSet.ids(i));
        }

        createIndexIfAbsent(INT_INDEX, IVAL, IndexType.INTEGER);
        createIndexIfAbsent(STR_INDEX, SVAL, IndexType.STRING);
        createIndexIfAbsent(BLOB_INDEX, BLOB, IndexType.BLOB);

        for (int i = 1; i <= SIZE; i++) {
            byte[] blob = new byte[8];
            Buffer.longToBytes(60_000L + i, blob, 0);

            session.upsert(dataSet.ids(i))
                .bins(IVAL, SVAL, BVAL, DVAL, BLOB, OFFSET)
                .values(i, "s" + i, i % 2 == 0, i * 1.5d, blob, 2)
                .execute();
        }
    }

    @AfterAll
    public static void destroy() {
        if (dataSet == null) {
            return;
        }

        for (int i = 1; i <= SIZE; i++) {
            session.delete(dataSet.ids(i));
        }
        session.dropIndex(dataSet, INT_INDEX);
        session.dropIndex(dataSet, STR_INDEX);
        session.dropIndex(dataSet, BLOB_INDEX);
    }

    @Test
    public void queryLongRangeFluent() {
        LongExpression ival = Ael.longBin(IVAL);

        BooleanExpression filter = ival.gte(Ael.val(3))
            .and(ival.lte(Ael.val(7)));

        assertEquals(5, count(session.query(dataSet).where(filter).execute()));
    }

    @Test
    public void queryStringEqFluent() {
        BooleanExpression filter = Ael.stringBin(SVAL).eq("s5");

        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(SVAL)
            .where(filter)
            .execute();

        try {
            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals("s5", rec.getString(SVAL));
            assertFalse(rs.hasNext());
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void queryBooleanAndNeFluent() {
        BooleanExpression active = Ael.booleanBin(BVAL).eq(true);
        BooleanExpression notEight = Ael.longBin(IVAL).ne(8);

        assertEquals(4, count(session.query(dataSet).where(active.and(notEight)).execute()));
    }

    @Test
    public void queryDoubleGteFluent() {
        BooleanExpression filter = Ael.doubleBin(DVAL).gte(6.0d);

        assertEquals(7, count(session.query(dataSet).where(filter).execute()));
    }

    @Test
    public void queryBlobEqFluent() {
        byte[] target = new byte[8];
        Buffer.longToBytes(60_003L, target, 0);

        BooleanExpression filter = Ael.blobBin(BLOB).eq(target);

        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(BLOB)
            .where(filter)
            .execute();

        try {
            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertTrue(Arrays.equals(target, rec.getBytes(BLOB)));
            assertFalse(rs.hasNext());
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void queryLogicalOrAndBinCompareFluent() {
        BooleanExpression high = Ael.longBin(IVAL).gte(Ael.val(9));
        BooleanExpression named = Ael.stringBin(SVAL).eq(Ael.val("s2"));
        BooleanExpression aboveOffset = Ael.longBin(IVAL).gt(Ael.longBin(OFFSET));

        assertEquals(3, count(session.query(dataSet).where(high.or(named)).execute()));
        assertEquals(8, count(session.query(dataSet).where(aboveOffset).execute()));
    }

    @Test
    public void queryArithmeticInFilterFluent() {
        BooleanExpression filter = Ael.longBin(IVAL)
            .add(1)
            .gte(6);

        assertEquals(6, count(session.query(dataSet).where(filter).execute()));
    }

    @Test
    public void queryIfElseIfThresholdFluent() {
        LongExpression bucket = Ael.ifLong(
            Ael.if_(Ael.longBin(IVAL).lt(Ael.val(3)), Ael.val(1L))
                .elseIf(Ael.longBin(IVAL).lt(Ael.val(7)), Ael.val(2L))
                .else_(Ael.val(3L)));

        BooleanExpression filter = bucket.eq(2);

        assertEquals(4, count(session.query(dataSet).where(filter).execute()));
    }

    @Test
    public void selectFromArithmeticFluent() {
        Key key = dataSet.id(4);

        RecordStream rs = session.query(key)
            .bin("sum").selectFrom(Ael.longBin(IVAL).add(10))
            .execute();

        try {
            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals(14L, rec.getLong("sum"));
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void selectFromIfFluent() {
        Key key = dataSet.id(8);

        LongExpression result = Ael.ifLong(
            Ael.if_(Ael.longBin(IVAL).gt(Ael.val(5)), Ael.val(100L))
                .else_(Ael.val(0L)));

        RecordStream rs = session.query(key)
            .bin("bucket").selectFrom(result)
            .execute();

        try {
            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals(100L, rec.getLong("bucket"));
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void selectFromLocalVariableFluent() {
        Key key = dataSet.id(5);

        LocalVariableExpression doubledPlusOne = Ael.define("doubled")
            .as(Ael.longBin(IVAL).mul(Ael.val(2)))
            .thenReturn(Ael.varLong("doubled").add(Ael.val(1)));

        RecordStream rs = session.query(key)
            .bin("computed").selectFrom(Ael.localVarLong(doubledPlusOne))
            .execute();

        try {
            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals(11L, rec.getLong("computed"));
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void selectFromTypeConversionFluent() {
        Key key = dataSet.id(6);

        RecordStream rs = session.query(key)
            .bin("trunc").selectFrom(Ael.toInt(Ael.doubleBin(DVAL)))
            .bin("widened").selectFrom(Ael.longBin(IVAL).toFloat())
            .execute();

        try {
            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals(9L, rec.getLong("trunc"));
            assertEquals(6.0d, rec.getDouble("widened"), 0.001d);
        }
        finally {
            rs.close();
        }
    }

    private static void createIndexIfAbsent(String indexName, String binName, IndexType type) {
        try {
            session.createIndex(dataSet, indexName, binName, type, IndexCollectionType.DEFAULT)
                .waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }
    }

    private static int count(RecordStream rs) {
        try {
            int n = 0;
            while (rs.hasNext()) {
                rs.next();
                n++;
            }
            return n;
        }
        finally {
            rs.close();
        }
    }
}
