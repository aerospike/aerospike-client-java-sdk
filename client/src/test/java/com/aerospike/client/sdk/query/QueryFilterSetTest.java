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

import java.util.Calendar;
import java.util.GregorianCalendar;

import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.exp.Exp;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class QueryFilterSetTest extends ClusterTest {
    private static final String set1 = "tqps1";
    private static final String set2 = "tqps2";
    private static final String set3 = "tqps3";
    private static final String binA = "a";
    private static final String binB = "b";

    private static DataSet dataSet1;
    private static DataSet dataSet2;
    private static DataSet dataSet3;

    @BeforeAll
    public static void prepare() {
        dataSet1 = DataSet.of(args.namespace, set1);
        dataSet2 = DataSet.of(args.namespace, set2);
        dataSet3 = DataSet.of(args.namespace, set3);

        // Clean up any existing test data
        for (int i = 1; i <= 5; i++) {
            session.delete(dataSet1.ids(i));
        }
        for (int i = 20; i <= 22; i++) {
            session.delete(dataSet2.ids(i));
        }
        for (int i = 31; i <= 40; i++) {
            session.delete(dataSet3.ids(i));
            String strKey = "key-p3-" + i;
            session.delete(dataSet3.ids(strKey));
        }
        session.delete(dataSet3.ids(25));

        // Insert fresh test data
        for (int i = 1; i <= 5; i++) {
            if (args.hasTtl) {
                session.upsert(dataSet1.ids(i))
                    .expireRecordAfterSeconds(i * 60)
                    .bins(binA)
                    .values(i)
                    .execute();
            }
            else {
                session.upsert(dataSet1.ids(i))
                    .bins(binA)
                    .values(i)
                    .execute();
            }
        }

        for (int i = 20; i <= 22; i++) {
            session.upsert(dataSet2.ids(i))
                .bins(binA, binB)
                .values(i, (double) i)
                .execute();
        }

        for (int i = 31; i <= 40; i++) {
            session.upsert(dataSet3.ids(i))
                .sendKey()
                .bins(binA)
                .values(i)
                .execute();

            String strKey = "key-p3-" + i;
            session.upsert(dataSet3.ids(strKey))
                .sendKey()
                .bins(binA)
                .values(i)
                .execute();
        }

        session.upsert(dataSet3.ids(25))
            .bins(binA)
            .values(25)
            .execute();
    }

    @AfterAll
    public static void destroy() {
        for (int i = 1; i <= 5; i++) {
            session.delete(dataSet1.ids(i));
        }
        for (int i = 20; i <= 22; i++) {
            session.delete(dataSet2.ids(i));
        }
        for (int i = 31; i <= 40; i++) {
            session.delete(dataSet3.ids(i));
            String strKey = "key-p3-" + i;
            session.delete(dataSet3.ids(strKey));
        }
        session.delete(dataSet3.ids(25));
    }

    @Test
    public void querySetName() {
        DataSet allNamespace = DataSet.of(args.namespace, null);
        Exp filterExp = Exp.eq(Exp.setName(), Exp.val(set2));

        RecordStream rs = session.query(allNamespace)
            .where(filterExp)
            .execute();

        try {
            int count = 0;
            while (rs.hasNext()) {
                rs.next();
                count++;
            }

            assertEquals(3, count);
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void queryDouble() {
        Exp filterExp = Exp.gt(Exp.floatBin(binB), Exp.val(21.5));

        RecordStream rs = session.query(dataSet2)
            .where(filterExp)
            .execute();

        try {
            int count = 0;
            while (rs.hasNext()) {
                rs.next();
                count++;
            }

            assertEquals(1, count);
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void queryKeyString() {
        Exp filterExp = Exp.regexCompare("^key-.*-35$", 0, Exp.key(Exp.Type.STRING));

        RecordStream rs = session.query(dataSet3)
            .where(filterExp)
            .execute();

        try {
            int count = 0;
            while (rs.hasNext()) {
                rs.next();
                count++;
            }

            assertEquals(1, count);
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void queryKeyInt() {
        Exp filterExp = Exp.lt(Exp.key(Exp.Type.INT), Exp.val(35));

        RecordStream rs = session.query(dataSet3)
            .where(filterExp)
            .execute();

        try {
            int count = 0;
            while (rs.hasNext()) {
                rs.next();
                count++;
            }

            assertEquals(5, count);
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void queryKeyExists() {
        Exp filterExp = Exp.keyExists();

        RecordStream rs = session.query(dataSet3)
            .where(filterExp)
            .execute();

        try {
            int count = 0;
            while (rs.hasNext()) {
                rs.next();
                count++;
            }

            assertEquals(21, count);
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void queryVoidTime() {
        Assumptions.assumeTrue(args.hasTtl);

        GregorianCalendar now = new GregorianCalendar();
        GregorianCalendar end = new GregorianCalendar();
        end.add(Calendar.MINUTE, 2);

        Exp filterExp = Exp.and(
            Exp.ge(Exp.voidTime(), Exp.val(now)),
            Exp.lt(Exp.voidTime(), Exp.val(end)));

        RecordStream rs = session.query(dataSet1)
            .where(filterExp)
            .execute();

        try {
            int count = 0;
            while (rs.hasNext()) {
                rs.next();
                count++;
            }

            assertEquals(2, count);
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void queryTTL() {
        Assumptions.assumeTrue(args.hasTtl);

        Exp filterExp = Exp.le(Exp.ttl(), Exp.val(60));

        RecordStream rs = session.query(dataSet1)
            .where(filterExp)
            .execute();

        try {
            int count = 0;
            while (rs.hasNext()) {
                rs.next();
                count++;
            }

            assertEquals(1, count);
        }
        finally {
            rs.close();
        }
    }
}
