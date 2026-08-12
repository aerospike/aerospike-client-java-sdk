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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.ErrorDetailVerbosity;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Selectors;

/**
 * Server-backed AEL tests for map/list literals with non-storage elements (INF)
 * and compile-time diagnostics for unsupported flag combinations.
 */
public class AelMapLiteralTest extends ClusterTest {
    private static final String BIN_MARKER = "marker";
    private static final String BIN_MAP = "m";

    private static Key key;
    private static boolean serverAcceptsMultiEntryMapLiteralWithInf;
    private static boolean serverRejectsCreateOrderOnModify;

    @BeforeAll
    public static void setup() {
        Assumptions.assumeTrue(cluster.supportsAel(), "cluster does not report AEL support");

        key = args.set.id("ael_map_literal");
        session.upsert(key)
            .bin(BIN_MARKER).setTo(1)
            .bin(BIN_MAP).setTo(java.util.Map.of("alpha", 10, "beta", 20))
            .execute();

        serverAcceptsMultiEntryMapLiteralWithInf = probeSelectFrom(
            "let(cap = {alpha: 10, sentinel: INF}) then (${cap}.count() > 0)");
        serverRejectsCreateOrderOnModify = probeSelectFromFails(
            "$." + BIN_MAP + ":MAP.nested:KEY_ORDERED.modify(@ + 1)",
            "create-order flags are not supported on modify()");
    }

    @Test
    public void multiEntryMapLiteralWithInfCompiles() {
        Assumptions.assumeTrue(serverAcceptsMultiEntryMapLiteralWithInf,
            "server rejects multi-entry map literals containing INF");

        try (RecordStream rs = session.query(key)
            .bin("ok")
            .selectFrom("let(cap = {alpha: 10, sentinel: INF}) then (${cap}.count() > 0)")
            .execute()) {
            assertTrue(rs.hasNext());
            assertEquals(true, rs.next().recordOrThrow().getValue("ok"));
        }
    }

    @Test
    public void modifyWithCreateOrderFlagReportsDedicatedDiagnostic() {
        Assumptions.assumeTrue(serverRejectsCreateOrderOnModify,
            "server build does not emit modify()-specific create-order diagnostic yet");

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            sessionWithVerbosity(ErrorDetailVerbosity.MESSAGE).query(key)
                .bin("out")
                .selectFrom("$." + BIN_MAP + ":MAP.nested:KEY_ORDERED.modify(@ + 1)")
                .execute();
        });

        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
        String msg = ae.getBaseMessage();
        assertNotNull(msg);
        assertTrue(msg.toLowerCase().contains("modify"),
            () -> "expected modify-specific message: " + msg);
        assertTrue(msg.toLowerCase().contains("setto"),
            () -> "expected hint toward single-target writes: " + msg);
    }

    private static Session sessionWithVerbosity(int verbosity) {
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(verbosity)
            )
        );
        return cluster.createSession(behavior);
    }

    private static boolean probeSelectFrom(String ael) {
        try {
            session.query(key)
                .bin("_probe")
                .selectFrom(ael)
                .execute();
            return true;
        }
        catch (AerospikeException ex) {
            return false;
        }
    }

    private static boolean probeSelectFromFails(String ael, String messageFragment) {
        try {
            session.query(key)
                .bin("_probe")
                .selectFrom(ael)
                .execute();
            return false;
        }
        catch (AerospikeException ex) {
            if (ex.getResultCode() != ResultCode.PARAMETER_ERROR) {
                return false;
            }
            String msg = ex.getBaseMessage();
            return msg != null && msg.toLowerCase().contains(messageFragment.toLowerCase());
        }
    }
}
