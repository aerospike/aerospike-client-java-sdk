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

/**
 * Server-backed AEL integration tests for CDT bulk writes ({@code *Items}) and
 * Aug-12 flag surface ({@code ADD_UNIQUE}, {@code UNSORTED_UNBOUND}, {@code :NO_FAIL}).
 */
public class AelPathWriteTest extends ClusterTest {
    private static final String KEY = "ael_path_write";
    private static final String BIN_MAP = "m";
    private static final String BIN_LIST = "l";

    private static boolean serverSupportsBulkMapWrites;
    private static boolean serverSupportsBulkListWrites;
    private static boolean serverSupportsAddUniqueFlag;
    private static boolean serverSupportsUnsortedUnboundFlag;

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
        serverSupportsBulkListWrites = probeUpsert(probeKey, BIN_LIST,
            "$." + BIN_LIST + ":LIST.appendItems([4])");
        serverSupportsAddUniqueFlag = probeUpsert(probeKey, BIN_LIST,
            "$." + BIN_LIST + ":LIST.append(99):ADD_UNIQUE:NO_FAIL");
        serverSupportsUnsortedUnboundFlag = probeUpsert(probeKey, BIN_LIST,
            "$." + BIN_LIST + ":LIST.[6]:INT.setTo(99):UNSORTED_UNBOUND");

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
        Assumptions.assumeTrue(serverSupportsBulkMapWrites,
            "server does not accept updateItems() AEL yet");

        upsertPath(BIN_MAP, "$." + BIN_MAP + ":MAP.updateItems({alpha: 11, beta: 22})");
        assertEquals(11L, mapLong("alpha"));
        assertEquals(22L, mapLong("beta"));
        assertEquals(30L, mapLong("gamma"));
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

    @Test
    public void listSetToWithUnsortedUnboundPadsToIndex() {
        Assumptions.assumeTrue(serverSupportsUnsortedUnboundFlag,
            "server does not accept :UNSORTED_UNBOUND list padding yet");

        upsertPath(BIN_LIST, "$." + BIN_LIST + ":LIST.[8]:INT.setTo(999):UNSORTED_UNBOUND");
        assertEquals(999L, selectLong("v", "$." + BIN_LIST + ":LIST.[8]:INT"));
        assertEquals(9L, selectLong("size", "$." + BIN_LIST + ":LIST.count()"));
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
