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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.SubCode;

/**
 * Server-backed AEL integration tests for CDT bulk writes ({@code *Items}) and
 * Aug-12 flag surface ({@code ADD_UNIQUE}, {@code UNSORTED_UNBOUND}, {@code :NO_FAIL}).
 *
 * <p>Compile-time rejection of the removed and renamed flag spellings lives in
 * {@link AelWriteFlagSurfaceTest}; this class covers observable write behavior.</p>
 */
public class AelPathWriteTest extends ClusterTest {
    private static final String KEY = "ael_path_write";
    private static final String BIN_MAP = "m";
    private static final String BIN_LIST = "l";
    /** Element count seeded into {@link #BIN_LIST} by {@link #seedRecord()}. */
    private static final long SEEDED_LIST_SIZE = 5L;
    /** Well past the seeded list end, so a padding-capable write has to nil-pad to reach it. */
    private static final int PAST_END_INDEX = 8;

    private static boolean serverSupportsBulkMapWrites;
    private static boolean serverSupportsMapUpdateItems;
    private static boolean serverSupportsBulkListWrites;
    private static boolean serverSupportsInsertItemsModifyFlag;
    private static boolean serverSupportsMapInsertItems;
    private static boolean serverSupportsAddUniqueFlag;
    private static boolean serverSupportsUnsortedUnboundFlag;
    private static boolean serverBoundsListWritesByDefault;

    private Key key;

    @BeforeAll
    public static void requireAelServerAndProbeFeatures() {
        Assumptions.assumeTrue(cluster.supportsAel(), "cluster does not report AEL support");

        Key probeKey = args.set.id(KEY + "_probe");
        session.delete(probeKey).execute();
        session.upsert(probeKey)
            .bin(BIN_MAP).setTo(Map.of("a", 1))
            .bin(BIN_LIST).setTo(List.of(1, 2, 3))
            .execute();

        serverSupportsBulkMapWrites = probeUpsert(probeKey, BIN_MAP,
            "$." + BIN_MAP + ":MAP.putItems({b: 2})");
        // updateItems arrived after putItems, so it needs its own probe.
        serverSupportsMapUpdateItems = probeUpsert(probeKey, BIN_MAP,
            "$." + BIN_MAP + ":MAP.updateItems({b: 3})");
        serverSupportsBulkListWrites = probeUpsert(probeKey, BIN_LIST,
            "$." + BIN_LIST + ":LIST.appendItems([4])");
        serverSupportsInsertItemsModifyFlag = probeUpsert(probeKey, BIN_LIST,
            "$." + BIN_LIST + ":LIST.[1].insertItems([5]):NO_FAIL");
        serverSupportsMapInsertItems = probeUpsert(probeKey, BIN_MAP,
            "$." + BIN_MAP + ":MAP.insertItems({probe_c: 3})");
        serverSupportsAddUniqueFlag = probeUpsert(probeKey, BIN_LIST,
            "$." + BIN_LIST + ":LIST.append(99):ADD_UNIQUE:NO_FAIL");
        // The create-order property rides the segment, not the op: setTo()'s own flag
        // mask never carried it, and the leaf segment's create bits are merged into
        // the op during finalization. A :TYPE pin on the same segment is rejected,
        // since the order already implies the container.
        serverSupportsUnsortedUnboundFlag = probeUpsert(probeKey, BIN_LIST,
            "$." + BIN_LIST + ":LIST.[20]:UNSORTED_UNBOUND.setTo(99)");
        serverBoundsListWritesByDefault = probeUpsertFailsBounded(probeKey, BIN_LIST,
            "$." + BIN_LIST + ":LIST.[40]:INT.setTo(99)");

        session.delete(probeKey).execute();
    }

    @BeforeEach
    public void seedRecord() {
        key = args.set.id(KEY);
        session.delete(key).execute();

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("alpha", 10);
        map.put("beta", 20);
        map.put("gamma", 30);

        session.upsert(key)
            .bin(BIN_MAP).setTo(map)
            .bin(BIN_LIST).setTo(List.of(100, 200, 300, 400, 500))
            .execute();
    }

    @Test
    public void mapPutItemsAddsEntries() {
        Assumptions.assumeTrue(serverSupportsBulkMapWrites,
            "server does not accept putItems() AEL yet");

        upsertPath(BIN_MAP, "$." + BIN_MAP + ":MAP.putItems({delta: 40, epsilon: 50})");
        assertEquals(40L, mapLong("delta"));
        assertEquals(50L, mapLong("epsilon"));
        assertEquals(5L, selectLong("size", "$." + BIN_MAP + ":MAP.count()"));
    }

    @Test
    public void mapUpdateItemsChangesExistingKeysOnly() {
        Assumptions.assumeTrue(serverSupportsMapUpdateItems,
            "server does not accept updateItems() AEL yet");

        upsertPath(BIN_MAP, "$." + BIN_MAP + ":MAP.updateItems({alpha: 11, beta: 22})");
        assertEquals(11L, mapLong("alpha"));
        assertEquals(22L, mapLong("beta"));
        assertEquals(30L, mapLong("gamma"));
        assertThat(mapLong("delta")).isNull();
    }

    @Test
    public void mapUpdateItemsRejectsMissingKey() {
        Assumptions.assumeTrue(serverSupportsMapUpdateItems,
            "server does not accept updateItems() AEL yet");

        // The verb presets NO_CREATE, so a missing key fails the whole write. The CDT
        // reports ELEMENT_NOT_FOUND, which the expression runtime folds into an eval
        // fault — inside AEL every sub-op failure arrives as OP_NOT_APPLICABLE.
        AerospikeException ex = assertThrows(AerospikeException.class,
            () -> upsertPath(BIN_MAP, "$." + BIN_MAP + ":MAP.updateItems({alpha: 11, delta: 40})"));
        assertEquals(ResultCode.OP_NOT_APPLICABLE, ex.getResultCode());
        assertEquals(10L, mapLong("alpha"));
        assertThat(mapLong("delta")).isNull();
    }

    @Test
    public void mapInsertItemsAddsOnlyNewKeys() {
        Assumptions.assumeTrue(serverSupportsMapInsertItems,
            "server does not accept insertItems() on a map receiver yet");

        upsertPath(BIN_MAP, "$." + BIN_MAP + ":MAP.insertItems({delta: 40, epsilon: 50})");
        assertEquals(40L, mapLong("delta"));
        assertEquals(50L, mapLong("epsilon"));
        assertEquals(10L, mapLong("alpha"));
        assertEquals(5L, selectLong("size", "$." + BIN_MAP + ":MAP.count()"));
    }

    @Test
    public void mapInsertItemsRejectsExistingKey() {
        Assumptions.assumeTrue(serverSupportsMapInsertItems,
            "server does not accept insertItems() on a map receiver yet");

        // NO_OVERWRITE is preset by the verb; the CDT's ELEMENT_EXISTS surfaces as an
        // expression eval fault, and no entry from the batch is applied.
        AerospikeException ex = assertThrows(AerospikeException.class,
            () -> upsertPath(BIN_MAP, "$." + BIN_MAP + ":MAP.insertItems({alpha: 99, delta: 40})"));
        assertEquals(ResultCode.OP_NOT_APPLICABLE, ex.getResultCode());
        assertEquals(10L, mapLong("alpha"));
        assertThat(mapLong("delta")).isNull();
    }

    @Test
    public void listAppendItemsExtendsList() {
        Assumptions.assumeTrue(serverSupportsBulkListWrites,
            "server does not accept appendItems() AEL yet");

        upsertPath(BIN_LIST, "$." + BIN_LIST + ":LIST.appendItems([601, 602])");
        assertEquals(601L, selectLong("v", "$." + BIN_LIST + ":LIST.[5]:INT"));
        assertEquals(602L, selectLong("v", "$." + BIN_LIST + ":LIST.[6]:INT"));
        assertEquals(7L, selectLong("size", "$." + BIN_LIST + ":LIST.count()"));
    }

    @Test
    public void listInsertItemsAtIndex() {
        Assumptions.assumeTrue(serverSupportsBulkListWrites,
            "server does not accept insertItems() AEL yet");

        upsertPath(BIN_LIST, "$." + BIN_LIST + ":LIST.[2].insertItems([111, 222])");
        assertEquals(111L, selectLong("v", "$." + BIN_LIST + ":LIST.[2]:INT"));
        assertEquals(222L, selectLong("v", "$." + BIN_LIST + ":LIST.[3]:INT"));
        assertEquals(300L, selectLong("v", "$." + BIN_LIST + ":LIST.[4]:INT"));
        assertEquals(7L, selectLong("size", "$." + BIN_LIST + ":LIST.count()"));
    }

    @Test
    public void listInsertItemsCarriesModifyFlag() {
        // LIST_INSERT_ITEMS takes one optional FLAGS arg. It used to be listed as
        // owning a create-flags slot too, so any modify flag emitted a pad plus the
        // word — one argument more than the op accepts.
        Assumptions.assumeTrue(serverSupportsInsertItemsModifyFlag,
            "server does not accept a modify flag on list insertItems() yet");

        upsertPath(BIN_LIST, "$." + BIN_LIST + ":LIST.[2].insertItems([111, 222]):NO_FAIL");
        assertEquals(111L, selectLong("v", "$." + BIN_LIST + ":LIST.[2]:INT"));
        assertEquals(222L, selectLong("v", "$." + BIN_LIST + ":LIST.[3]:INT"));
        assertEquals(7L, selectLong("size", "$." + BIN_LIST + ":LIST.count()"));
    }

    @Test
    public void listAppendAddUniqueRejectsDuplicateWithoutNoFail() {
        Assumptions.assumeTrue(serverSupportsAddUniqueFlag,
            "server does not accept :ADD_UNIQUE on list append yet");

        AerospikeException ex = assertThrows(AerospikeException.class,
            () -> upsertPath(BIN_LIST, "$." + BIN_LIST + ":LIST.append(200):ADD_UNIQUE"));
        assertTrue(ex.getResultCode() != ResultCode.OK,
            () -> "expected failure when appending duplicate with ADD_UNIQUE: " + ex.getResultCode());
    }

    @Test
    public void listAppendAddUniqueWithNoFailSkipsDuplicate() {
        Assumptions.assumeTrue(serverSupportsAddUniqueFlag,
            "server does not accept :ADD_UNIQUE on list append yet");

        upsertPath(BIN_LIST, "$." + BIN_LIST + ":LIST.append(200):ADD_UNIQUE:NO_FAIL");
        assertEquals(5L, selectLong("size", "$." + BIN_LIST + ":LIST.count()"));
        assertEquals(200L, selectLong("v", "$." + BIN_LIST + ":LIST.[1]:INT"));
    }

    // --- list writes are bounded by default (AEL only; the wire path still pads) ---

    @Test
    public void listSetToBeyondEndIsBounded() {
        assertBoundedListWrite("$." + BIN_LIST + ":LIST.[" + PAST_END_INDEX + "]:INT.setTo(999)");
    }

    @Test
    public void listInsertBeyondEndIsBounded() {
        assertBoundedListWrite("$." + BIN_LIST + ":LIST.[" + PAST_END_INDEX + "].insert(999)");
    }

    @Test
    public void listInsertItemsBeyondEndIsBounded() {
        assertBoundedListWrite("$." + BIN_LIST + ":LIST.[" + PAST_END_INDEX + "].insertItems([888, 999])");
    }

    @Test
    public void listAddBeyondEndIsBounded() {
        assertBoundedListWrite("$." + BIN_LIST + ":LIST.[" + PAST_END_INDEX + "].add(1)");
    }

    @Test
    public void listSetToWithUnsortedUnboundPadsToIndex() {
        Assumptions.assumeTrue(serverSupportsUnsortedUnboundFlag,
            "server does not accept :UNSORTED_UNBOUND list padding yet");

        upsertPath(BIN_LIST, "$." + BIN_LIST + ":LIST.[" + PAST_END_INDEX + "]:UNSORTED_UNBOUND.setTo(999)");
        assertEquals(999L, selectLong("v", "$." + BIN_LIST + ":LIST.[" + PAST_END_INDEX + "]:INT"));
        assertEquals(PAST_END_INDEX + 1L, selectLong("size", "$." + BIN_LIST + ":LIST.count()"));
    }

    @Test
    public void listInsertItemsWithUnsortedUnboundPadsToIndex() {
        Assumptions.assumeTrue(serverSupportsUnsortedUnboundFlag,
            "server does not accept :UNSORTED_UNBOUND list padding yet");

        upsertPath(BIN_LIST,
            "$." + BIN_LIST + ":LIST.[" + PAST_END_INDEX + "]:UNSORTED_UNBOUND.insertItems([888, 999])");
        assertEquals(888L, selectLong("v", "$." + BIN_LIST + ":LIST.[" + PAST_END_INDEX + "]:INT"));
        assertEquals(PAST_END_INDEX + 2L, selectLong("size", "$." + BIN_LIST + ":LIST.count()"));
    }

    /**
     * Bounded rejection is {@link ResultCode#OP_NOT_APPLICABLE} with a dedicated subcode, so
     * this cannot be satisfied by an AEL that merely fails to compile — that is
     * {@link ResultCode#PARAMETER_ERROR}.
     */
    private void assertBoundedListWrite(String ael) {
        Assumptions.assumeTrue(serverBoundsListWritesByDefault,
            "server does not bound padding-capable list writes by default yet");

        AerospikeException ex = assertThrows(AerospikeException.class, () -> upsertPath(BIN_LIST, ael));
        assertEquals(ResultCode.OP_NOT_APPLICABLE, ex.getResultCode(),
            () -> "expected a bounded-write rejection for AEL: " + ael);
        assertEquals(SubCode.OPNOT_CDT_BOUNDED_LIST_OVERFLOW, ex.getSubCode(),
            () -> "expected the bounded-list-overflow subcode for AEL: " + ael);
        assertEquals(SEEDED_LIST_SIZE, selectLong("size", "$." + BIN_LIST + ":LIST.count()"),
            "a bounded write must leave the list unchanged");
    }

    // --- helpers ---

    private static boolean probeUpsert(Key probeKey, String binName, String ael) {
        try {
            session.update(probeKey)
                .bin(binName).upsertFrom(ael)
                .execute();
            return true;
        }
        catch (AerospikeException ex) {
            return false;
        }
    }

    /** {@code true} only when the write is refused for overflowing a bounded list. */
    private static boolean probeUpsertFailsBounded(Key probeKey, String binName, String ael) {
        try {
            session.update(probeKey)
                .bin(binName).upsertFrom(ael)
                .execute();
            return false;
        }
        catch (AerospikeException ex) {
            return ex.getResultCode() == ResultCode.OP_NOT_APPLICABLE
                && ex.getSubCode() == SubCode.OPNOT_CDT_BOUNDED_LIST_OVERFLOW;
        }
    }

    private void upsertPath(String binName, String ael) {
        session.update(key)
            .bin(binName).upsertFrom(ael)
            .execute();
    }

    private long selectLong(String resultBin, String ael) {
        try (RecordStream rs = session.query(key)
            .bin(resultBin)
            .selectFrom(ael)
            .execute()) {
            assertTrue(rs.hasNext(), () -> "no record for AEL: " + ael);
            Record rec = rs.next().recordOrThrow();
            Object value = rec.getValue(resultBin);
            assertNotNull(value, () -> "null result for AEL: " + ael);
            return ((Number) value).longValue();
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> mapBin() {
        Record rec = session.query(key).execute().getFirstRecord();
        assertNotNull(rec);
        return (Map<String, Object>) rec.getValue(BIN_MAP);
    }

    private Long mapLong(String mapKey) {
        Object value = mapBin().get(mapKey);
        return value == null ? null : ((Number) value).longValue();
    }
}
