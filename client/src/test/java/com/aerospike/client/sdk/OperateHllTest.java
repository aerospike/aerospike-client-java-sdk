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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.Value.HLLValue;

public class OperateHllTest extends ClusterTest {
    private static boolean debug = false;

    private static final String binName = "ophbin";
    private static final Key key = args.set.id("ophkey");
    private static final Key[] keys = new Key[] {
        args.set.id("ophkey0"),
        args.set.id("ophkey1"),
        args.set.id("ophkey2")};
    private static final int nEntries = 1 << 18;

    private static final int minNIndexBits = 4;
    private static final int maxNIndexBits = 16;
    private static final int minNMinhashBits = 4;
    private static final int maxNMinhashBits = 51;

    private static final ArrayList<String> entries = new ArrayList<String>();
    private static final ArrayList<Integer> legalNIndexBits = new ArrayList<Integer>();
    private static final ArrayList<ArrayList<Integer>> legalDescriptions = new ArrayList<ArrayList<Integer>>();
    private static final ArrayList<ArrayList<Integer>> illegalDescriptions = new ArrayList<ArrayList<Integer>>();

    @BeforeAll
    public static void createData() {
        for (int i = 0; i < nEntries; i++) {
            entries.add("key " + i);
        }

        for (int nIndexBits = minNIndexBits; nIndexBits <= maxNIndexBits; nIndexBits += 4) {
            int nCombinedBits = maxNMinhashBits + nIndexBits;
            int maxAllowedNMinhashBits = maxNMinhashBits;

            if (nCombinedBits > 64) {
                maxAllowedNMinhashBits -= nCombinedBits - 64;
            }

            int midNMinhashBits = (maxAllowedNMinhashBits + nIndexBits) / 2;
            ArrayList<Integer> legalZero = new ArrayList<Integer>();
            ArrayList<Integer> legalMin = new ArrayList<Integer>();
            ArrayList<Integer> legalMid = new ArrayList<Integer>();
            ArrayList<Integer> legalMax = new ArrayList<Integer>();

            legalNIndexBits.add(nIndexBits);
            legalZero.add(nIndexBits);
            legalMin.add(nIndexBits);
            legalMid.add(nIndexBits);
            legalMax.add(nIndexBits);

            legalZero.add(0);
            legalMin.add(minNMinhashBits);
            legalMid.add(midNMinhashBits);
            legalMax.add(maxAllowedNMinhashBits);

            legalDescriptions.add(legalZero);
            legalDescriptions.add(legalMin);
            legalDescriptions.add(legalMid);
            legalDescriptions.add(legalMax);
        }

        for (int indexBits = minNIndexBits - 1; indexBits <= maxNIndexBits + 5; indexBits += 4) {
            if (indexBits < minNIndexBits || indexBits > maxNIndexBits) {
                ArrayList<Integer> illegalZero = new ArrayList<Integer>();
                ArrayList<Integer> illegalMin = new ArrayList<Integer>();
                ArrayList<Integer> illegalMax = new ArrayList<Integer>();

                illegalZero.add(indexBits);
                illegalMin.add(indexBits);
                illegalMax.add(indexBits);

                illegalZero.add(0);
                illegalMin.add(minNMinhashBits - 1);
                illegalMax.add(maxNMinhashBits);

                illegalDescriptions.add(illegalZero);
                illegalDescriptions.add(illegalMin);
                illegalDescriptions.add(illegalMax);
            }
            else {
                ArrayList<Integer> illegalMin = new ArrayList<Integer>();
                ArrayList<Integer> illegalMax = new ArrayList<Integer>();
                ArrayList<Integer> illegalMax1 = new ArrayList<Integer>();

                illegalMin.add(indexBits);
                illegalMax.add(indexBits);

                illegalMin.add(minNMinhashBits - 1);
                illegalMax.add(maxNMinhashBits + 1);

                illegalDescriptions.add(illegalMin);
                illegalDescriptions.add(illegalMax);

                if (indexBits + maxNMinhashBits > 64) {
                    illegalMax1.add(indexBits);
                    illegalMax1.add(1 + maxNMinhashBits - (64 - (indexBits + maxNMinhashBits)));
                    illegalDescriptions.add(illegalMax1);
                }
            }
        }
    }

    public void printDebug(String msg) {
        if (debug) {
            System.out.println("debug :: " + msg);
            System.out.flush();
        }
    }

    public void assertThrows(
        String msg, Key key, Class<?> eclass, int eresult, ChainableOperationBuilder builder
    ) {
        try {
            RecordStream rs = builder.execute();
            assertTrue(rs.hasNext());
            rs.next().recordOrThrow();
            assertTrue(false, msg + " succeeded?");
        }
        catch (AerospikeException e) {
            if (eclass != e.getClass() || eresult != e.getResultCode()) {
                assertEquals(eclass, e.getClass(), msg + " " + e.getClass() + " " + e);
                assertEquals(eresult, e.getResultCode(), msg + " " + e.getResultCode() + " " + e);
            }
        }
    }

    public Record assertSuccess(String msg, Key key, ChainableOperationBuilder builder) {
        Record rec;

        try {
            RecordStream rs = builder.execute();

            assertTrue(rs.hasNext());
            rec = rs.next().recordOrThrow();
        }
        catch (Exception e) {
            assertEquals(null, e, msg + " " + e);
            return null;
        }

        return rec;
    }

    public boolean checkBits(int nIndexBits, int nMinhashBits) {
        return ! (nIndexBits < minNIndexBits || nIndexBits > maxNIndexBits ||
                (nMinhashBits != 0 && nMinhashBits < minNMinhashBits) ||
                nMinhashBits > maxNMinhashBits || nIndexBits + nMinhashBits > 64);
    }

    public double relativeCountError(int nIndexBits) {
        return 1.04 / Math.sqrt(Math.pow(2, nIndexBits));
    }

    public void assertDescription(String msg, List<?>description, int nIndexBits, int nMinhashBits) {
        assertEquals(nIndexBits, (long)(Long) description.get(0), msg);
        assertEquals(nMinhashBits, (long)(Long) description.get(1), msg);
    }

    public void assertInit(int nIndexBits, int nMinhashBits, boolean shouldPass) {
        String msg = "Fail - nIndexBits " + nIndexBits + " nMinhashBits " + nMinhashBits;

        ChainableOperationBuilder builder = session.upsert(key)
            .bin(binName).hllInit(HllConfig.of(nIndexBits, nMinhashBits))
            .bin(binName).hllGetCount()
            .bin(binName).hllRefreshCount()
            .bin(binName).hllDescribe();

        if (! shouldPass) {
            assertThrows(msg, key, AerospikeException.class, ResultCode.PARAMETER_ERROR, builder);
            return;
        }

        Record record = assertSuccess(msg, key, builder);
        List<?> resultList = record.getList(binName);
        long count = (Long)resultList.get(1);
        long count1 = (Long)resultList.get(2);
        List<?> description = (List<?>)resultList.get(3);

        assertDescription(msg, description, nIndexBits, nMinhashBits);
        assertEquals(0, count);
        assertEquals(0, count1);
    }

    @Test
    public void operateHLLInit() {
        session.delete(key).execute();

        for (ArrayList<Integer> desc : legalDescriptions) {
            assertInit(desc.get(0), desc.get(1), true);
        }

        for (ArrayList<Integer> desc : illegalDescriptions) {
            assertInit(desc.get(0), desc.get(1), false);
        }
    }

    @Test
    public void operateHLLFlags() {
        int nIndexBits = 4;

        // Keep record around win binName is removed.
        ChainableOperationBuilder builder = session.upsert(key)
            .deleteRecord()
            .bin(binName + "other").hllInit(HllConfig.of(nIndexBits));

        assertSuccess("other bin", key, builder);

        // create_only
        builder = session.upsert(key)
            .bin(binName).hllInit(HllConfig.of(nIndexBits), opt -> opt.createOnly());

        assertSuccess("create_only", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllInit(HllConfig.of(nIndexBits), opt -> opt.createOnly());

        assertThrows("create_only - error", key, AerospikeException.BinExistsException.class,
            ResultCode.BIN_EXISTS_ERROR, builder);

        // update_only
        builder = session.upsert(key)
            .bin(binName).hllInit(HllConfig.of(nIndexBits), opt -> opt.updateOnly());

        assertSuccess("update_only", key, builder);

        builder = session.upsert(key)
            .bin(binName).remove();

        assertSuccess("remove bin", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllInit(HllConfig.of(nIndexBits), opt -> opt.updateOnly());

        assertThrows("update_only - error", key, AerospikeException.BinNotFoundException.class,
            ResultCode.BIN_NOT_FOUND, builder);

        // create_only no_fail
        builder = session.upsert(key)
            .bin(binName).hllInit(HllConfig.of(nIndexBits), opt -> opt.createOnly().noFail());

        assertSuccess("create_only nofail", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllInit(HllConfig.of(nIndexBits), opt -> opt.createOnly().noFail());

        assertSuccess("create_only nofail - no error", key, builder);

        // update_only no_fail
        builder = session.upsert(key)
            .bin(binName).hllInit(HllConfig.of(nIndexBits), opt -> opt.updateOnly().noFail());

        assertSuccess("update_only nofail", key, builder);

        builder = session.upsert(key)
            .bin(binName).remove();

        assertSuccess("remove bin", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllInit(HllConfig.of(nIndexBits), opt -> opt.updateOnly().noFail());

        assertSuccess("update_only nofail - no error", key, builder);

        // fold
        builder = session.upsert(key)
            .bin(binName).hllInit(HllConfig.of(nIndexBits), opt -> opt.createOnly());

        assertSuccess("create_only", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllInit(HllConfig.of(nIndexBits), opt -> opt.allowFold());

        assertThrows("fold", key, AerospikeException.class, ResultCode.PARAMETER_ERROR,
            builder);
    }

    @Test
    public void badReInit() {
        ChainableOperationBuilder builder = session.upsert(key)
            .deleteRecord()
            .bin(binName).hllInit(HllConfig.of(maxNIndexBits, 0));

        assertSuccess("create min max", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllInit(HllConfig.of(-1, maxNMinhashBits));

        assertThrows("create_only", key, AerospikeException.BinOpInvalidException.class, ResultCode.OP_NOT_APPLICABLE,
            builder);
    }

    public boolean isWithinRelativeError(long expected, long estimate, double relativeError) {
        return expected * (1 - relativeError) <= estimate || estimate <= expected * (1 + relativeError);
    }

    public void assertHLLCount(String msg, int nIndexBits, long hllCount, long expected) {
        double countErr6Sigma = relativeCountError(nIndexBits) * 6;

        msg = msg + " - err " + countErr6Sigma + " count " + hllCount + " expected " + expected + " nIndexBits " +
                nIndexBits;

        printDebug(msg);
        assertTrue(isWithinRelativeError(expected, hllCount, countErr6Sigma), msg);
    }

    public void assertAddInit(int nIndexBits, int nMinhashBits) {
        session.delete(key).execute();

        String msg = "Fail - nIdexBits " + nIndexBits + " nMinhashBits " + nMinhashBits;

        ChainableOperationBuilder builder = session.upsert(key)
            .bin(binName).hllAdd(entries, HllConfig.of(nIndexBits, nMinhashBits))
            .bin(binName).hllGetCount()
            .bin(binName).hllRefreshCount()
            .bin(binName).hllDescribe()
            .bin(binName).hllAdd(entries);

        if (!checkBits(nIndexBits, nMinhashBits)) {
            assertThrows(msg, key, AerospikeException.class, ResultCode.PARAMETER_ERROR, builder);
            return;
        }

        Record record = assertSuccess(msg, key, builder);
        List<?> resultList = record.getList(binName);
        long count = (Long) resultList.get(1);
        long count1 = (Long) resultList.get(2);
        List<?> description = (List<?>) resultList.get(3);
        long nAdded = (Long) resultList.get(4);

        assertDescription(msg, description, nIndexBits, nMinhashBits);
        assertHLLCount(msg, nIndexBits, count, entries.size());
        assertEquals(count, count1);
        assertEquals(nAdded, 0);
    }

    @Test
    public void operateHLLAddInit() {
        for (ArrayList<Integer> desc : legalDescriptions) {
            assertAddInit(desc.get(0), desc.get(1));
        }
    }

    @Test
    public void operateAddFlags() {
        int nIndexBits = 4;

        // Keep record around win binName is removed.
        ChainableOperationBuilder builder = session.upsert(key)
            .deleteRecord()
            .bin(binName + "other").hllInit(HllConfig.of(nIndexBits, 0));

        assertSuccess("other bin", key, builder);

        // create_only
        builder = session.upsert(key)
            .bin(binName).hllAdd(entries, HllConfig.of(nIndexBits), opt -> opt.createOnly());

        assertSuccess("create_only", key, builder);
        assertThrows("create_only - error", key, AerospikeException.BinExistsException.class,
            ResultCode.BIN_EXISTS_ERROR, builder);

        // update_only
        builder = session.upsert(key)
            .bin(binName).hllAdd(entries, HllConfig.of(nIndexBits), opt -> opt.updateOnly());

        assertThrows("update_only - error", key, AerospikeException.class,
            ResultCode.PARAMETER_ERROR, builder);

        // create_only no_fail
        builder = session.upsert(key)
            .bin(binName).hllAdd(entries, HllConfig.of(nIndexBits), opt -> opt.createOnly().noFail());

        assertSuccess("create_only nofail", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllAdd(entries, HllConfig.of(nIndexBits), opt -> opt.createOnly().noFail());

        assertSuccess("create_only nofail - no error", key, builder);

        // fold
        builder = session.upsert(key)
            .bin(binName).hllInit(HllConfig.of(nIndexBits));

        assertSuccess("init", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllAdd(entries, HllConfig.of(nIndexBits), opt -> opt.allowFold());

        assertThrows("fold", key, AerospikeException.class, ResultCode.PARAMETER_ERROR,
            builder);
    }

    public void assertFold(List<String> vals0, List<String> vals1, int nIndexBits) {
        String msg = "Fail - nIndexBits " + nIndexBits;

        for (int ix = minNIndexBits; ix <= nIndexBits; ix++) {
            if (!checkBits(nIndexBits, 0) || ! checkBits(ix, 0)) {
                assertTrue(false, "Expected valid inputs: " + msg);
            }

            ChainableOperationBuilder builder = session.upsert(key)
                .deleteRecord()
                .bin(binName).hllAdd(vals0, HllConfig.of(nIndexBits))
                .bin(binName).hllGetCount()
                .bin(binName).hllRefreshCount()
                .bin(binName).hllDescribe();

            Record recorda = assertSuccess(msg, key, builder);

            List<?> resultAList = recorda.getList(binName);
            long counta = (Long) resultAList.get(1);
            long counta1 = (Long) resultAList.get(2);
            List<?> descriptiona = (List<?>) resultAList.get(3);

            assertDescription(msg, descriptiona, nIndexBits, 0);
            assertHLLCount(msg, nIndexBits, counta, vals0.size());
            assertEquals(counta, counta1);

            builder = session.upsert(key)
                .bin(binName).hllFold(ix)
                .bin(binName).hllGetCount()
                .bin(binName).hllAdd(vals0)
                .bin(binName).hllAdd(vals1)
                .bin(binName).hllGetCount()
                .bin(binName).hllDescribe();

            Record recordb = assertSuccess(msg, key, builder);

            List<?> resultBList = recordb.getList(binName);
            long countb = (Long) resultBList.get(1);
            long nAdded0 = (Long) resultBList.get(2);
            long countb1 = (Long) resultBList.get(4);
            List<?> descriptionb = (List<?>) resultBList.get(5);

            assertEquals(0, nAdded0);
            assertDescription(msg, descriptionb, ix, 0);
            assertHLLCount(msg, ix, countb, vals0.size());
            assertHLLCount(msg, ix, countb1, vals0.size() + vals1.size());
        }
    }

    @Test
    public void operateFold() {
        List<String> vals0 = new ArrayList<>();
        List<String> vals1 = new ArrayList<>();

        for (int i = 0; i < nEntries / 2; i++) {
            vals0.add("key " + i);
        }

        for (int i = nEntries / 2; i < nEntries; i++) {
            vals1.add("key " + i);
        }

        for (int nIndexBits = 4; nIndexBits < maxNIndexBits; nIndexBits++) {
            assertFold(vals0, vals1, nIndexBits);
        }
    }

    @Test
    public void operateFoldExists() {
        int nIndexBits = 10;
        int foldDown = 4;
        int foldUp = 16;

        // Keep record around win binName is removed.
        ChainableOperationBuilder builder = session.upsert(key)
            .deleteRecord()
            .bin(binName + "other").hllInit(HllConfig.of(nIndexBits))
            .bin(binName).hllInit(HllConfig.of(nIndexBits));

        assertSuccess("other bin", key, builder);

        // Exists.
        builder = session.upsert(key)
            .bin(binName).hllFold(foldDown);

        assertSuccess("exists fold down", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllFold(foldUp);

        assertThrows("exists fold up", key, AerospikeException.BinOpInvalidException.class,
            ResultCode.OP_NOT_APPLICABLE, builder);

        // Does not exist.
        builder = session.upsert(key)
            .bin(binName).remove();

        assertSuccess("remove bin", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllFold(foldDown);

        assertThrows("create_only - error", key, AerospikeException.BinNotFoundException.class,
            ResultCode.BIN_NOT_FOUND, builder);
    }

    public void assertSetUnion(List<List<String>> vals, int nIndexBits, boolean folding, boolean allowFolding) {
        String msg = "Fail - nIndexBits " + nIndexBits;
        long unionExpected = 0;
        boolean folded = false;

        for (int i = 0; i < keys.length; i++) {
            int ix = nIndexBits;

            if (folding) {
                ix -= i;

                if (ix < minNIndexBits) {
                    ix = minNIndexBits;
                }

                if (ix < nIndexBits) {
                    folded = true;
                }
            }

            List<String> subVals = vals.get(i);

            unionExpected += subVals.size();

            ChainableOperationBuilder builder = session.upsert(keys[i])
                .deleteRecord()
                .bin(binName).hllAdd(subVals, HllConfig.of(ix))
                .bin(binName).hllGetCount();

            Record record = assertSuccess(msg, keys[i], builder);
            List<?> resultList = record.getList(binName);
            long count = (Long) resultList.get(1);

            assertHLLCount(msg, ix, count, subVals.size());
        }

        ArrayList<HLLValue> hlls = new ArrayList<>();

        for (int i = 0; i < keys.length; i++) {
            ChainableOperationBuilder builder = session.upsert(keys[i])
                .bin(binName).get()
                .bin(binName).hllGetCount();

            Record record = assertSuccess(msg, keys[i], builder);
            List<?> resultList = record.getList(binName);
            HLLValue hll = (HLLValue)resultList.get(0);

            assertNotEquals(null, hll);
            hlls.add(hll);
        }

        ChainableOperationBuilder builder = session.upsert(key)
            .deleteRecord()
            .bin(binName).hllInit(HllConfig.of(nIndexBits));

        if (allowFolding) {
            builder = builder.bin(binName).hllSetUnion(hlls, opt -> opt.allowFold());
        }
        else {
            builder = builder.bin(binName).hllSetUnion(hlls);
        }

        builder = builder
            .bin(binName).hllGetCount()
            .deleteRecord() // And recreate it to test creating empty.
            .bin(binName).hllSetUnion(hlls)
            .bin(binName).hllGetCount();

        if (folded && ! allowFolding) {
            assertThrows(msg, key, AerospikeException.BinOpInvalidException.class,
                ResultCode.OP_NOT_APPLICABLE, builder);
            return;
        }

        Record recordUnion = assertSuccess(msg, key, builder);
        List<?> unionResultList = recordUnion.getList(binName);
        long unionCount = (Long) unionResultList.get(2);
        long unionCount2 = (Long) unionResultList.get(4);

        assertHLLCount(msg, nIndexBits, unionCount, unionExpected);
        assertEquals(unionCount, unionCount2, msg);

        for (int i = 0; i < keys.length; i++) {
            List<String> subVals = vals.get(i);

            builder = session.upsert(key)
                .bin(binName).hllAdd(subVals, HllConfig.of(nIndexBits))
                .bin(binName).hllGetCount();

            Record record = assertSuccess(msg, key, builder);
            List<?> resultList = record.getList(binName);
            long nAdded = (Long) resultList.get(0);
            long count = (Long) resultList.get(1);

            assertEquals(0, nAdded, msg);
            assertEquals(unionCount, count, msg);
            assertHLLCount(msg, nIndexBits, count, unionExpected);
        }
    }

    @Test
    public void operateSetUnion() {
        ArrayList<List<String>> vals = new ArrayList<List<String>>();

        for (int i = 0; i < keys.length; i++) {
            ArrayList<String> subVals = new ArrayList<>();

            for (int j = 0; j < nEntries / 3; j++) {
                subVals.add("key" + i + " " + j);
            }

            vals.add(subVals);
        }

        for (Integer nIndexBits : legalNIndexBits) {
            assertSetUnion(vals, nIndexBits, false, false);
            assertSetUnion(vals, nIndexBits, false, true);
            assertSetUnion(vals, nIndexBits, true, false);
            assertSetUnion(vals, nIndexBits, true, true);
        }
    }

    @Test
    public void operateSetUnionFlags() {
        int nIndexBits = 6;
        int lowNBits = 4;
        int highNBits = 8;
        String otherName = binName + "o";

        // Keep record around win binName is removed.
        ArrayList<HLLValue> hlls = new ArrayList<HLLValue>();

        ChainableOperationBuilder builder = session.upsert(key)
            .deleteRecord()
            .bin(otherName).hllAdd(entries, HllConfig.of(nIndexBits))
            .bin(otherName).get();

        Record record = assertSuccess("other bin", key, builder);
        List<?> resultList = record.getList(otherName);
        HLLValue hll = (HLLValue) resultList.get(1);

        hlls.add(hll);

        // create_only
        builder = session.upsert(key)
            .bin(binName).hllSetUnion(hlls, opt -> opt.createOnly());

        assertSuccess("create_only", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllSetUnion(hlls, opt -> opt.createOnly());

        assertThrows("create_only - error", key, AerospikeException.BinExistsException.class,
            ResultCode.BIN_EXISTS_ERROR, builder);

        // update_only
        builder = session.upsert(key)
            .bin(binName).hllSetUnion(hlls, opt -> opt.updateOnly());

        assertSuccess("update_only", key, builder);

        builder = session.upsert(key)
            .bin(binName).remove();

        assertSuccess("remove bin", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllSetUnion(hlls, opt -> opt.updateOnly());

        assertThrows("update_only - error", key, AerospikeException.BinNotFoundException.class,
            ResultCode.BIN_NOT_FOUND, builder);

        // create_only no_fail
        builder = session.upsert(key)
            .bin(binName).hllSetUnion(hlls, opt -> opt.createOnly().noFail());

        assertSuccess("create_only nofail", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllSetUnion(hlls, opt -> opt.createOnly().noFail());

        assertSuccess("create_only nofail - no error", key, builder);

        // update_only no_fail
        builder = session.upsert(key)
            .bin(binName).hllSetUnion(hlls, opt -> opt.updateOnly().noFail());

        assertSuccess("update_only nofail", key, builder);

        builder = session.upsert(key)
            .bin(binName).remove();

        assertSuccess("remove bin", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllSetUnion(hlls, opt -> opt.updateOnly().noFail());

        assertSuccess("update_only nofail - no error", key, builder);

        // fold down
        builder = session.upsert(key)
            .bin(binName).hllInit(HllConfig.of(highNBits));

        assertSuccess("size up", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllSetUnion(hlls, opt -> opt.allowFold());

        assertSuccess("fold down to index_bits", key, builder);

        // fold up
        builder = session.upsert(key)
            .bin(binName).hllInit(HllConfig.of(lowNBits));

        assertSuccess("size down", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllSetUnion(hlls, opt -> opt.allowFold());

        assertSuccess("fold down to low_n_bits", key, builder);
    }

    @Test
    public void operateRefreshCount() {
        int nIndexBits = 6;

        // Keep record around win binName is removed.
        ChainableOperationBuilder builder = session.upsert(key)
            .deleteRecord()
            .bin(binName + "other").hllInit(HllConfig.of(nIndexBits))
            .bin(binName).hllInit(HllConfig.of(nIndexBits));

        assertSuccess("other bin", key, builder);

        // Exists.
        builder = session.upsert(key)
            .bin(binName).hllRefreshCount()
            .bin(binName).hllRefreshCount();

        assertSuccess("refresh zero count", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllAdd(entries, HllConfig.of(nIndexBits));

        assertSuccess("add items", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllRefreshCount()
            .bin(binName).hllRefreshCount();

        assertSuccess("refresh count", key, builder);

        // Does not exist.
        builder = session.upsert(key)
            .bin(binName).remove();

        assertSuccess("remove bin", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllRefreshCount();

        assertThrows("refresh nonexistant count", key, AerospikeException.BinNotFoundException.class,
            ResultCode.BIN_NOT_FOUND, builder);
    }

    @Test
    public void operateGetCount() {
        int nIndexBits = 6;

        // Keep record around win binName is removed.
        ChainableOperationBuilder builder = session.upsert(key)
            .deleteRecord()
            .bin(binName + "other").hllInit(HllConfig.of(nIndexBits))
            .bin(binName).hllAdd(entries, HllConfig.of(nIndexBits));

        assertSuccess("other bin", key, builder);

        // Exists.
        builder = session.upsert(key)
            .bin(binName).hllGetCount();

        Record record = assertSuccess("exists count", key, builder);
        long count = record.getLong(binName);
        assertHLLCount("check count", nIndexBits, count, entries.size());

        // Does not exist.
        builder = session.upsert(key)
            .bin(binName).remove();

        assertSuccess("remove bin", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllGetCount();

        record = assertSuccess("exists count", key, builder);
        assertEquals(null, record.getValue(binName));
    }

    @Test
    public void operateGetUnion() {
        int nIndexBits = 14;
        long expectedUnionCount = 0;
        ArrayList<List<String>> vals = new ArrayList<List<String>>();
        List<HLLValue> hlls = new ArrayList<HLLValue>();

        for (int i = 0; i < keys.length; i++) {
            ArrayList<String> subVals = new ArrayList<>();

            for (int j = 0; j < nEntries / 3; j++) {
                subVals.add("key" + i + " " + j);
            }

            ChainableOperationBuilder builder = session.upsert(keys[i])
                .deleteRecord()
                .bin(binName).hllAdd(subVals, HllConfig.of(nIndexBits))
                .bin(binName).get();

            Record record = assertSuccess("init other keys", keys[i], builder);

            List<?> resultList = record.getList(binName);
            hlls.add((HLLValue)resultList.get(1));
            expectedUnionCount += subVals.size();
            vals.add(subVals);
        }

        // Keep record around win binName is removed.
        ChainableOperationBuilder builder = session.upsert(key)
            .deleteRecord()
            .bin(binName + "other").hllInit(HllConfig.of(nIndexBits))
            .bin(binName).hllAdd(vals.get(0), HllConfig.of(nIndexBits));

        assertSuccess("other bin", key, builder);

        builder = session.upsert(key)
            .bin(binName).hllGetUnion(hlls)
            .bin(binName).hllGetUnionCount(hlls);

        Record record = assertSuccess("union and unionCount", key, builder);
        List<?> resultList = record.getList(binName);
        long unionCount = (Long)resultList.get(1);

        assertHLLCount("verify union count", nIndexBits, unionCount, expectedUnionCount);

        HLLValue unionHll = (HLLValue)resultList.get(0);

        builder = session.upsert(key)
            .bin(binName).setTo(unionHll)
            .bin(binName).hllGetCount();

        record = assertSuccess("", key, builder);
        resultList = record.getList(binName);
        long unionCount2 = (Long)resultList.get(1);

        assertEquals(unionCount, unionCount2, "unions equal");
    }

    @Test
    public void getPut() {
        for (ArrayList<Integer> desc : legalDescriptions) {
            int nIndexBits = desc.get(0);
            int nMinhashBits = desc.get(1);

            ChainableOperationBuilder builder = session.upsert(key)
                .deleteRecord()
                .bin(binName).hllInit(HllConfig.of(nIndexBits, nMinhashBits));

            assertSuccess("init record", key, builder);

            RecordStream rs = session.query(key).execute();
            assertTrue(rs.hasNext());

            Record rec = rs.next().recordOrThrow();
            HLLValue hll = rec.getHLLValue(binName);

            session.delete(key).execute();

            session.upsert(key)
                .bin(binName).setTo(hll)
                .execute();

            builder = session.upsert(key)
                .bin(binName).hllGetCount()
                .bin(binName).hllDescribe();

            rec = assertSuccess("describe", key, builder);

            List<?> resultList = rec.getList(binName);
            long count = (Long)resultList.get(0);
            List<?> description = (List<?>)resultList.get(1);

            assertEquals(0, count);
            assertDescription("Check description", description, nIndexBits, nMinhashBits);
        }
    }

    public double absoluteSimilarityError(int nIndexBits, int nMinhashBits, double expectedSimilarity) {
        double minErrIndex = 1 / Math.sqrt(1 << nIndexBits);
        double minErrMinhash = 6 * Math.pow(Math.E, nMinhashBits * -1) / expectedSimilarity;

        printDebug("minErrIndex " + minErrIndex + " minErrMinhash " + minErrMinhash + " nIndexBits " + nIndexBits +
                " nMinhashBits " + nMinhashBits + " expectedSimilarity " + expectedSimilarity);

        return Math.max(minErrIndex, minErrMinhash);
    }

    public void assertHMHSimilarity(String msg, int nIndexBits, int nMinhashBits, double similarity,
            double expectedSimilarity, long intersectCount, long expectedIntersectCount) {
        double simErr6Sigma = 0;

        if (nMinhashBits != 0) {
            simErr6Sigma = 6 * absoluteSimilarityError(nIndexBits, nMinhashBits, expectedSimilarity);
        }

        msg = msg + " - err " + simErr6Sigma + " nIindexBits " + nIndexBits + " nMinhashBits " + nMinhashBits +
                "\n\t- similarity " + similarity + " expectedSimilarity " + expectedSimilarity +
                "\n\t- intersectCount " + intersectCount + " expectedIntersectCount " + expectedIntersectCount;

        printDebug(msg);

        if (nMinhashBits == 0) {
            return;
        }

        assertTrue(simErr6Sigma > Math.abs(expectedSimilarity - similarity), msg);
        assertTrue(isWithinRelativeError(expectedIntersectCount, intersectCount, simErr6Sigma), msg);
    }

    public void assertSimilarityOp(double overlap, List<String> common, List<List<String>> vals, int nIndexBits,
            int nMinhashBits) {
        List<HLLValue> hlls = new ArrayList<HLLValue>();

        for (int i = 0; i < keys.length; i++) {
            ChainableOperationBuilder builder = session.upsert(keys[i])
                .deleteRecord()
                .bin(binName).hllAdd(vals.get(i), HllConfig.of(nIndexBits, nMinhashBits))
                .bin(binName).hllAdd(common, HllConfig.of(nIndexBits, nMinhashBits))
                .bin(binName).get();

            Record record = assertSuccess("init other keys", keys[i], builder);

            List<?> resultList = record.getList(binName);
            hlls.add((HLLValue)resultList.get(2));
        }

        // Keep record around win binName is removed.
        ChainableOperationBuilder builder = session.upsert(key)
            .deleteRecord()
            .bin(binName + "other").hllInit(HllConfig.of(nIndexBits, nMinhashBits))
            .bin(binName).hllSetUnion(hlls)
            .bin(binName).hllDescribe();

        Record rec = assertSuccess("other bin", key, builder);
        List<?> resultList = rec.getList(binName);
        List<?> description = (List<?>)resultList.get(1);

        assertDescription("check desc", description, nIndexBits, nMinhashBits);

        builder = session.upsert(key)
            .bin(binName).hllGetSimilarity(hlls)
            .bin(binName).hllGetIntersectCount(hlls);

        rec = assertSuccess("similarity and intersectCount", key, builder);
        resultList = rec.getList(binName);
        double sim = (Double)resultList.get(0);
        long intersectCount = (Long)resultList.get(1);
        double expectedSimilarity = overlap;
        long expectedIntersectCount = common.size();

        assertHMHSimilarity("check sim", nIndexBits, nMinhashBits, sim, expectedSimilarity, intersectCount,
                expectedIntersectCount);
    }

    @Test
    public void operateSimilarity() {
        double[] overlaps = new double[] {0.0001, 0.001, 0.01, 0.1, 0.5};

        for (double overlap : overlaps) {
            long expectedIntersectCount = (long)(nEntries * overlap);
            ArrayList<String> common = new ArrayList<>();

            for (int i = 0; i < expectedIntersectCount; i++) {
                common.add("common" + i);
            }

            ArrayList<List<String>> vals = new ArrayList<List<String>>();
            long uniqueEntriesPerNode = (nEntries - expectedIntersectCount) / 3;

            for (int i = 0; i < keys.length; i++) {
                ArrayList<String> subVals = new ArrayList<>();

                for (int j = 0; j < uniqueEntriesPerNode; j++) {
                    subVals.add("key" + i + " " + j);
                }

                vals.add(subVals);
            }

            for (ArrayList<Integer> desc : legalDescriptions) {
                int nIndexBits = desc.get(0);
                int nMinhashBits = desc.get(1);

                if (nMinhashBits == 0) {
                    continue;
                }

                assertSimilarityOp(overlap, common, vals, nIndexBits, nMinhashBits);
            }
        }
    }

    @Test
    public void operateEmptySimilarity() {
        for (ArrayList<Integer> desc : legalDescriptions) {
            int nIndexBits = desc.get(0);
            int nMinhashBits = desc.get(1);

            ChainableOperationBuilder builder = session.upsert(key)
                .deleteRecord()
                .bin(binName).hllInit(HllConfig.of(nIndexBits, nMinhashBits))
                .bin(binName).get();

            Record rec = assertSuccess("init", key, builder);

            List<?> resultList = rec.getList(binName);
            List<HLLValue> hlls = new ArrayList<HLLValue>();

            hlls.add((HLLValue)resultList.get(1));

            builder = session.upsert(key)
                .bin(binName).hllGetSimilarity(hlls)
                .bin(binName).hllGetIntersectCount(hlls);

            rec = assertSuccess("test", key, builder);

            resultList = rec.getList(binName);

            double sim = (Double)resultList.get(0);
            long intersectCount = (Long)resultList.get(1);

            String msg = "(" + nIndexBits + ", " + nMinhashBits + ")";

            assertEquals(0, intersectCount, msg);
            assertEquals(Double.NaN, sim, 0.0, msg);
        }
    }

    @Test
    public void operateIntersectHLL() {
        String otherBinName = binName + "other";

        for (ArrayList<Integer> desc : legalDescriptions) {
            int indexBits = desc.get(0);
            int minhashBits = desc.get(1);

            if (minhashBits != 0) {
                break;
            }

            ChainableOperationBuilder builder = session.upsert(key)
                .deleteRecord()
                .bin(binName).hllAdd(entries, HllConfig.of(indexBits, minhashBits))
                .bin(binName).get()
                .bin(otherBinName).hllAdd(entries, HllConfig.of(indexBits, 4))
                .bin(otherBinName).get();

            Record record = assertSuccess("init", key, builder);

            List<HLLValue> hlls = new ArrayList<HLLValue>();
            List<HLLValue> hmhs = new ArrayList<HLLValue>();
            List<?> resultList = record.getList(binName);

            hlls.add((HLLValue)resultList.get(1));
            hlls.add(hlls.get(0));

            resultList = record.getList(otherBinName);
            hmhs.add((HLLValue)resultList.get(1));
            hmhs.add(hmhs.get(0));

            builder = session.upsert(key)
                .bin(binName).hllGetIntersectCount(hlls)
                .bin(binName).hllGetSimilarity(hlls);

            record = assertSuccess("intersect", key, builder);
            resultList = record.getList(binName);

            long intersectCount = (Long)resultList.get(0);

            assertTrue(intersectCount < 1.8 * entries.size(), "intersect value too high");

            hlls.add(hlls.get(0));

            builder = session.upsert(key)
                .bin(binName).hllGetIntersectCount(hlls);

            assertThrows("Expect parameter error", key, AerospikeException.class,
                ResultCode.PARAMETER_ERROR, builder);

            builder = session.upsert(key)
                .bin(binName).hllGetSimilarity(hlls);

            assertThrows("Expect parameter error", key, AerospikeException.class,
                ResultCode.PARAMETER_ERROR, builder);

            builder = session.upsert(key)
                .bin(binName).hllGetIntersectCount(hmhs)
                .bin(binName).hllGetSimilarity(hmhs);

            record = assertSuccess("intersect", key, builder);
            resultList = record.getList(binName);
            intersectCount = (Long)resultList.get(0);

            assertTrue(intersectCount < 1.8 * entries.size(), "intersect value too high");

            hmhs.add(hmhs.get(0));

            builder = session.upsert(key)
                .bin(binName).hllGetIntersectCount(hmhs);

            assertThrows("Expect parameter error", key, AerospikeException.BinOpInvalidException.class,
                ResultCode.OP_NOT_APPLICABLE, builder);

            builder = session.upsert(key)
                .bin(binName).hllGetSimilarity(hmhs);

            assertThrows("Expect parameter error", key, AerospikeException.BinOpInvalidException.class,
                ResultCode.OP_NOT_APPLICABLE, builder);
        }
    }
}
