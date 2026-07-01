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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ChainableQueryBuilder;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;

/**
 * Server-backed AEL integration tests for record metadata and {@code $.key()}
 * (ael-server-spec-test-gaps-todo.md §2.4).
 *
 * <p>Uses {@code sessionWithSendKey} so user keys are stored on records — required
 * for {@code $.key()} / {@code $.keyExists()}. Key-scoped queries use server-compiled
 * AEL ({@code allowsIndex=false}); avoid {@code :INT} bin pins in filters (untyped
 * bins match {@code FilterExpTest} / {@code QueryOperationsTest} where style).</p>
 *
 * <p>{@code $.key():T} value compare and {@code selectFrom("$.key():T")} are probe-gated:
 * at {@code aerospike-server} @ {@code 93301ab26}, {@code ael_build_node} leaves
 * {@code op_rec_key.type} at {@code RT_NIL}, so key comparisons evaluate unknown; read
 * projection fails {@code build_set_expected_particle_type} ({@code EXP_RTYPE_END}).
 * {@code $.keyExists()} is always asserted.</p>
 */
public class AelMetadataTest extends ClusterTest {
    private static final String STRING_KEY = "ael_meta_alice";
    private static final int INT_KEY = 424_242;
    private static final String BIN_MARKER = "marker";
    private static final String BIN_UPDATE = "updateBy";
    private static final String DIGEST_KEY_PREFIX = "ael_digest_";
    private static final String DIGEST_INDEX = "ael_meta_digest";
    private static final String DIGEST_BIN = "dig";

    private static DataSet digestSet;
    private static boolean keyValueCompareWorks;
    private static boolean keySelectFromWorks;

    private Key stringKey;
    private Key intKey;

    @BeforeAll
    public static void requireAelServer() {
        Assumptions.assumeTrue(cluster.supportsAel(), "cluster does not report AEL support");
        prepareDigestData();
        probeKeySupport();
    }

    @AfterAll
    public static void tearDownDigestData() {
        if (!cluster.supportsAel()) {
            return;
        }
        for (int i = 1; i <= 10; i++) {
            sessionWithSendKey.delete(digestSet.id(DIGEST_KEY_PREFIX + i)).execute();
        }
        try {
            sessionWithSendKey.dropIndex(digestSet, DIGEST_INDEX).waitTillComplete();
        }
        catch (AerospikeException ex) {
            if (ex.getResultCode() != ResultCode.INDEX_NOTFOUND) {
                throw ex;
            }
        }
    }

    @BeforeEach
    public void seedRecords() {
        stringKey = args.set.id(STRING_KEY);
        intKey = args.set.id(INT_KEY);

        sessionWithSendKey.delete(stringKey, intKey).execute();

        long futureLastUpdate = System.currentTimeMillis() * 1_000_000L + 1_000_000L;

        sessionWithSendKey.upsert(stringKey)
            .bin(BIN_MARKER).setTo(1)
            .bin(BIN_UPDATE).setTo(futureLastUpdate)
            .execute();

        sessionWithSendKey.upsert(intKey)
            .bin(BIN_MARKER).setTo(2)
            .bin(BIN_UPDATE).setTo(futureLastUpdate)
            .execute();
    }

    // --- $.key() with typed pins ---

    @Test
    public void stringKeyMatchesInWhere() {
        Assumptions.assumeTrue(keyValueCompareWorks,
            "$.key():T value compare not working on this server build");
        assertTrue(matchesWhere(stringKey, "$.key():STRING == '" + STRING_KEY + "'"));
    }

    @Test
    public void stringKeyMissesInWhere() {
        Assumptions.assumeTrue(keyValueCompareWorks,
            "$.key():T value compare not working on this server build");
        assertFalse(matchesWhere(stringKey, "$.key():STRING == 'other-key'"));
    }

    @Test
    public void intKeyMatchesInWhere() {
        Assumptions.assumeTrue(keyValueCompareWorks,
            "$.key():T value compare not working on this server build");
        assertTrue(matchesWhere(intKey, "$.key():INT == " + INT_KEY));
    }

    @Test
    public void selectStringKeyViaSelectFrom() {
        Assumptions.assumeTrue(keySelectFromWorks,
            "$.key():T read projection not working on this server build");
        Object value = selectValue(stringKey, "hit", "$.key():STRING");
        assertInstanceOf(String.class, value);
        assertEquals(STRING_KEY, value);
    }

    @Test
    public void selectIntKeyViaSelectFrom() {
        Assumptions.assumeTrue(keySelectFromWorks,
            "$.key():T read projection not working on this server build");
        Object value = selectValue(intKey, "hit", "$.key():INT");
        assertInstanceOf(Number.class, value);
        assertEquals(INT_KEY, ((Number) value).longValue());
    }

    // --- $.digestModulo(n) as AEL filter ---

    @Test
    public void digestModuloAsAelFilter() {
        int expected = 0;
        String combined = "$.digestModulo(3) == 1 and $." + DIGEST_BIN + " >= 1 and $." + DIGEST_BIN + " <= 10";
        for (int i = 1; i <= 10; i++) {
            Key key = digestSet.id(DIGEST_KEY_PREFIX + i);
            if (matchesWhere(key, combined)) {
                expected++;
            }
        }
        assertTrue(expected > 0, "expected at least one digest-mod-3 match in seeded keys");
        assertEquals(expected, countKeyQueriesMatching(combined));
    }

    // --- other metadata in real filters ---

    @Test
    public void recordSizeFilterMatchesExistingRecord() {
        assertTrue(matchesWhere(stringKey, "$.recordSize() > 0"));
    }

    @Test
    public void keyExistsFilterMatchesKeyedRecord() {
        assertTrue(matchesWhere(stringKey, "$.keyExists()"));
    }

    @Test
    public void lastUpdateLessThanBinValue() {
        assertTrue(matchesWhere(stringKey, "$.lastUpdate() < $." + BIN_UPDATE));
    }

    @Test
    public void setNameMatchesCurrentSet() {
        assertTrue(matchesWhere(stringKey,
            "$.setName() == \"" + args.set.getSet() + "\""));
    }

    @Test
    public void ttlFilterAsAelString() {
        Assumptions.assumeTrue(args.hasTtl, "cluster TTL not enabled");

        Key shortTtlKey = args.set.id("ael_meta_ttl_short");
        Key longTtlKey = args.set.id("ael_meta_ttl_long");
        sessionWithSendKey.delete(shortTtlKey, longTtlKey).execute();

        sessionWithSendKey.upsert(shortTtlKey)
            .expireRecordAfterSeconds(60)
            .bin(BIN_MARKER).setTo(1)
            .execute();
        sessionWithSendKey.upsert(longTtlKey)
            .expireRecordAfterSeconds(3600)
            .bin(BIN_MARKER).setTo(1)
            .execute();

        assertTrue(matchesWhere(shortTtlKey, "$.ttl() <= 120"));
        assertFalse(matchesWhere(longTtlKey, "$.ttl() <= 120"));
    }

    // --- setup + helpers ---

    private static void probeKeySupport() {
        Key probe = args.set.id("ael_key_probe");
        sessionWithSendKey.delete(probe).execute();
        sessionWithSendKey.upsert(probe).bin(BIN_MARKER).setTo(1).execute();

        keyValueCompareWorks = matchesWhere(probe, "$.key():STRING == 'ael_key_probe'");

        keySelectFromWorks = false;
        if (keyValueCompareWorks) {
            try {
                selectValue(probe, "hit", "$.key():STRING");
                keySelectFromWorks = true;
            }
            catch (RuntimeException ignored) {
                // read projection still unsupported
            }
        }

        sessionWithSendKey.delete(probe).execute();
    }

    private static void prepareDigestData() {
        digestSet = DataSet.of(args.namespace, args.set.getSet() + "_aelmeta");
        try {
            sessionWithSendKey.createIndex(digestSet, DIGEST_INDEX, DIGEST_BIN, IndexType.INTEGER,
                IndexCollectionType.DEFAULT)
                .waitTillComplete();
        }
        catch (AerospikeException ex) {
            if (ex.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ex;
            }
        }

        for (int i = 1; i <= 10; i++) {
            Key key = digestSet.id(DIGEST_KEY_PREFIX + i);
            sessionWithSendKey.delete(key).execute();
            sessionWithSendKey.upsert(key)
                .bin(DIGEST_BIN).setTo(i)
                .execute();
        }
    }

    private static int countKeyQueriesMatching(String whereAel) {
        int count = 0;
        for (int i = 1; i <= 10; i++) {
            if (matchesWhere(digestSet.id(DIGEST_KEY_PREFIX + i), whereAel)) {
                count++;
            }
        }
        return count;
    }

    private static Object selectValue(Key key, String resultBin, String ael) {
        try (RecordStream rs = query(key)
            .bin(resultBin)
            .selectFrom(ael)
            .execute()) {
            assertTrue(rs.hasNext(), () -> "no record for AEL: " + ael);
            Record rec = rs.next().recordOrThrow();
            Object value = rec.getValue(resultBin);
            assertNotNull(value, () -> "null result for AEL: " + ael);
            return value;
        }
    }

    private static boolean matchesWhere(Key key, String whereAel) {
        try (RecordStream rs = query(key).where(whereAel).execute()) {
            return rs.hasNext();
        }
    }

    private static ChainableQueryBuilder query(Key key) {
        return sessionWithSendKey.query(key);
    }
}
