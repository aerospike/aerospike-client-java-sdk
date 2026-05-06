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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.Value.HLLValue;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.exp.HLLExp;
import com.aerospike.client.sdk.exp.ListExp;
import com.aerospike.client.sdk.operation.HLLWriteFlags;

public class HLLExpTest extends ClusterTest {
    String bin1 = "hllbin_1";
    String bin2 = "hllbin_2";
    String bin3 = "hllbin_3";
    HLLValue hll1;
    HLLValue hll2;
    HLLValue hll3;

    @Test
    public void hllExp() {
        Key key = args.set.id(5200);

        session.delete(key).execute();

        ArrayList<String> list1 = new ArrayList<>();
        list1.add("Akey1");
        list1.add("Akey2");
        list1.add("Akey3");

        ArrayList<String> list2 = new ArrayList<>();
        list2.add("Bkey1");
        list2.add("Bkey2");
        list2.add("Bkey3");

        ArrayList<String> list3 = new ArrayList<>();
        list3.add("Akey1");
        list3.add("Akey2");
        list3.add("Bkey1");
        list3.add("Bkey2");
        list3.add("Ckey1");
        list3.add("Ckey2");

        RecordStream rs = session.upsert(key)
            .bin(bin1).hllAdd(list1, HllConfig.of(8))
            .bin(bin2).hllAdd(list2, HllConfig.of(8))
            .bin(bin3).hllAdd(list3, HllConfig.of(8))
            .bin(bin1).get()
            .bin(bin2).get()
            .bin(bin3).get()
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.getFirstRecord();

        List<?> results = rec.getList(bin1);
        hll1 = (HLLValue)results.get(1);
        assertNotNull(hll1);

        results = rec.getList(bin2);
        hll2 = (HLLValue)results.get(1);
        assertNotNull(hll2);

        results = rec.getList(bin3);
        hll3 = (HLLValue)results.get(1);
        assertNotNull(hll3);

        count(key);
        union(key);
        intersect(key);
        similarity(key);
        describe(key);
        mayContain(key);
        add(key);
    }

    private void count(Key key) {
        // TODO: Test AEL string in addition to Expression.
        Expression exp  = Exp.build(Exp.eq(HLLExp.getCount(Exp.hllBin(bin1)), Exp.val(0)));

        RecordStream rs = session.query(key)
            .where(exp)
            .execute();

        assertFalse(rs.hasNext());

        exp  = Exp.build(Exp.gt(HLLExp.getCount(Exp.hllBin(bin1)), Exp.val(0)));

        rs = session.query(key)
            .where(exp)
            .execute();

        assertTrue(rs.hasNext());
        Record r = rs.getFirstRecord();
        assertNotNull(r);
    }

    private void union(Key key) {
        ArrayList<HLLValue> hlls = new ArrayList<HLLValue>();
        hlls.add(hll1);
        hlls.add(hll2);
        hlls.add(hll3);

        Expression exp  = Exp.build(
            Exp.ne(
                HLLExp.getCount(HLLExp.getUnion(Exp.val(hlls), Exp.hllBin(bin1))),
                HLLExp.getUnionCount(Exp.val(hlls), Exp.hllBin(bin1))));

        RecordStream rs = session.query(key)
            .where(exp)
            .execute();

        assertFalse(rs.hasNext());

        exp  = Exp.build(
            Exp.eq(
                HLLExp.getCount(HLLExp.getUnion(Exp.val(hlls), Exp.hllBin(bin1))),
                HLLExp.getUnionCount(Exp.val(hlls), Exp.hllBin(bin1))));

        rs = session.query(key)
            .where(exp)
            .execute();

        assertTrue(rs.hasNext());
        Record r = rs.getFirstRecord();
        assertNotNull(r);
    }

    private void intersect(Key key) {
        ArrayList<HLLValue> hlls2 = new ArrayList<HLLValue>();
        hlls2.add(hll2);

        ArrayList<HLLValue> hlls3 = new ArrayList<HLLValue>();
        hlls3.add(hll3);

        Expression exp  = Exp.build(
            Exp.ge(
                HLLExp.getIntersectCount(Exp.val(hlls2), Exp.hllBin(bin1)),
                HLLExp.getIntersectCount(Exp.val(hlls3), Exp.hllBin(bin1))));

        RecordStream rs = session.query(key)
            .where(exp)
            .execute();

        assertFalse(rs.hasNext());

        exp  = Exp.build(
            Exp.le(
                HLLExp.getIntersectCount(Exp.val(hlls2), Exp.hllBin(bin1)),
                HLLExp.getIntersectCount(Exp.val(hlls3), Exp.hllBin(bin1))));

        rs = session.query(key)
            .where(exp)
            .execute();

        assertTrue(rs.hasNext());
        Record r = rs.getFirstRecord();
        assertNotNull(r);
    }

    private void similarity(Key key) {
        ArrayList<HLLValue> hlls2 = new ArrayList<HLLValue>();
        hlls2.add(hll2);

        ArrayList<HLLValue> hlls3 = new ArrayList<HLLValue>();
        hlls3.add(hll3);

        Expression exp  = Exp.build(
            Exp.ge(
                HLLExp.getSimilarity(Exp.val(hlls2), Exp.hllBin(bin1)),
                HLLExp.getSimilarity(Exp.val(hlls3), Exp.hllBin(bin1))));

        RecordStream rs = session.query(key)
            .where(exp)
            .execute();

        assertFalse(rs.hasNext());

        exp  = Exp.build(
            Exp.le(
                HLLExp.getSimilarity(Exp.val(hlls2), Exp.hllBin(bin1)),
                HLLExp.getSimilarity(Exp.val(hlls3), Exp.hllBin(bin1))));

        rs = session.query(key)
            .where(exp)
            .execute();

        assertTrue(rs.hasNext());
        Record r = rs.getFirstRecord();
        assertNotNull(r);
    }

    private void describe(Key key) {
        Exp index = Exp.val(0);

        Expression exp  = Exp.build(
            Exp.ne(
                ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, index,
                    HLLExp.describe(Exp.hllBin(bin1))),
                ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, index,
                    HLLExp.describe(Exp.hllBin(bin2)))));

        RecordStream rs = session.query(key)
            .where(exp)
            .execute();

        assertFalse(rs.hasNext());

        exp  = Exp.build(
            Exp.eq(
                ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, index,
                    HLLExp.describe(Exp.hllBin(bin1))),
                ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, index,
                    HLLExp.describe(Exp.hllBin(bin2)))));

        rs = session.query(key)
            .where(exp)
            .execute();

        assertTrue(rs.hasNext());
        Record r = rs.getFirstRecord();
        assertNotNull(r);
    }

    private void mayContain(Key key) {
        ArrayList<Value> values = new ArrayList<Value>();
        values.add(Value.get("new_val"));

        Expression exp = Exp.build(
            Exp.eq(HLLExp.mayContain(Exp.val(values), Exp.hllBin(bin2)), Exp.val(1))
            );

        RecordStream rs = session.query(key)
            .where(exp)
            .execute();

        assertFalse(rs.hasNext());

        exp = Exp.build(
            Exp.ne(HLLExp.mayContain(Exp.val(values), Exp.hllBin(bin2)), Exp.val(1))
            );

        rs = session.query(key)
            .where(exp)
            .execute();

        assertTrue(rs.hasNext());
        Record r = rs.getFirstRecord();
        assertNotNull(r);
    }

    private void add(Key key) {
        ArrayList<Value> values = new ArrayList<Value>();
        values.add(Value.get("new_val"));

        Expression exp = Exp.build(
            Exp.eq(
                HLLExp.getCount(Exp.hllBin(bin1)),
                HLLExp.getCount(HLLExp.add(HLLWriteFlags.DEFAULT, Exp.val(values), Exp.hllBin(bin2)))));

        RecordStream rs = session.query(key)
            .where(exp)
            .execute();

        assertFalse(rs.hasNext());

        exp = Exp.build(
            Exp.lt(
                HLLExp.getCount(Exp.hllBin(bin1)),
                HLLExp.getCount(HLLExp.add(HLLWriteFlags.DEFAULT, Exp.val(values), Exp.hllBin(bin2)))));

        rs = session.query(key)
            .where(exp)
            .execute();

        assertTrue(rs.hasNext());
        Record r = rs.getFirstRecord();
        assertNotNull(r);
    }
}
