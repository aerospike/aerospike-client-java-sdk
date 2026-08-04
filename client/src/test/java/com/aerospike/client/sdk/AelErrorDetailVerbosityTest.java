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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Selectors;

/**
 * Extended error-detail coverage for server-compiled AEL (SERVER-1137 / AER-6932).
 *
 * <p>Complements {@link ErrorDetailVerbosityTest}, which exercises wire msgpack {@code Exp}
 * build failures. These tests send textual AEL and assert folded compile diagnostics and,
 * at verbosity 3, {@link ExpressionTrace} with {@link ExpressionTrace#LANG_AEL}.</p>
 */
public class AelErrorDetailVerbosityTest extends ClusterTest {
    private static final String binName = "aedvbin";
    private static Key intKey;

    /** Trailing {@code and} with no right-hand operand — AEL parse failure. */
    private static String badFilterAel() {
        return "$." + binName + ":INT > 30 and";
    }

    /** Integer bin plus string literal — AEL type error at compile time. */
    private static String badSelectAel() {
        return "$." + binName + ":INT + 'x'";
    }

    @BeforeAll
    public static void setup() {
        Assumptions.assumeTrue(args.serverVersion.isGreaterOrEqual(8, 1, 3, 0),
            "Extended error-detail requires server version 8.1.3 or later");
        assumeSupportsAel();

        intKey = args.set.id("aedv-int-key");

        session.upsert(intKey)
            .bin(binName).setTo(1)
            .execute();
    }

    @Test
    public void testAelFilterBuildFailureMessage() {
        Session session1 = sessionWithVerbosity(ErrorDetailVerbosity.MESSAGE);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.query(intKey)
                .where(badFilterAel())
                .execute();
        });

        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
        assertEquals(SubCode.NONE, ae.getSubCode());

        String msg = ae.getBaseMessage();
        assertNotNull(msg);
        assertTrue(msg.contains("invalid metadata expression in request"),
            "Expected filter-build context in: " + msg);
        assertTrue(msg.length() > "invalid metadata expression in request".length(),
            "Expected AEL compile diagnostic folded into message: " + msg);
    }

    @Test
    public void testAelFilterBuildFailureTrace() {
        Session session1 = sessionWithVerbosity(ErrorDetailVerbosity.EXPRESSION_TRACE);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.query(intKey)
                .where(badFilterAel())
                .execute();
        });

        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
        assertEquals(SubCode.NONE, ae.getSubCode());

        ExpressionTrace trace = ae.getExpressionTrace();
        assertNotNull(trace, "Expected a non-null AEL build trace at verbosity 3");
        assertEquals(ExpressionTrace.PHASE_BUILD, trace.getPhase());
        assertEquals(ExpressionTrace.LANG_AEL, trace.getLang());
        assertTrue(trace.getAelOffset() >= 0, "Expected AEL source offset in trace");
    }

    @Test
    public void testAelFilterBuildFailureVerbosity2HasNoTrace() {
        Session session1 = sessionWithVerbosity(ErrorDetailVerbosity.MESSAGE);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.query(intKey)
                .where(badFilterAel())
                .execute();
        });

        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
        assertNull(ae.getExpressionTrace(), "Verbosity 2 must surface NO expression trace");
    }

    @Test
    public void testAelSelectFromBuildFailureMessage() {
        Session session1 = sessionWithVerbosity(ErrorDetailVerbosity.MESSAGE);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.query(intKey)
                .bin("out").selectFrom(badSelectAel())
                .execute();
        });

        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
        assertEquals(SubCode.NONE, ae.getSubCode());

        String msg = ae.getBaseMessage();
        assertNotNull(msg);
        assertTrue(msg.contains("invalid expression in operation request"),
            "Expected exp-op build context in: " + msg);
        assertTrue(msg.length() > "invalid expression in operation request".length(),
            "Expected AEL compile diagnostic folded into message: " + msg);
    }

    @Test
    public void testAelSelectFromBuildFailureTrace() {
        Session session1 = sessionWithVerbosity(ErrorDetailVerbosity.EXPRESSION_TRACE);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.query(intKey)
                .bin("out").selectFrom(badSelectAel())
                .execute();
        });

        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());

        ExpressionTrace trace = ae.getExpressionTrace();
        assertNotNull(trace, "Expected a non-null AEL build trace at verbosity 3");
        assertEquals(ExpressionTrace.PHASE_BUILD, trace.getPhase());
        assertEquals(ExpressionTrace.LANG_AEL, trace.getLang());
    }

    @Test
    public void testAelFilterFilteredOutMessage() {
        Session session1 = sessionWithVerbosity(ErrorDetailVerbosity.MESSAGE);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.query(intKey)
                .where("$." + binName + " == 99")
                .failOnFilteredOut()
                .execute();
        });

        assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
        assertEquals(SubCode.NONE, ae.getSubCode());

        String msg = ae.getBaseMessage();
        assertNotNull(msg);
        assertTrue(msg.contains("filtered out"), "Expected contextual message in: " + msg);
        assertFalse(msg.contains("subcode="), "Expected NO subcode suffix in: " + msg);
    }

    @Test
    public void testAelFilterFilteredOutExplainerTrace() {
        Session session1 = sessionWithVerbosity(ErrorDetailVerbosity.EXPRESSION_TRACE);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.query(intKey)
                .where("$." + binName + " == 99")
                .failOnFilteredOut()
                .execute();
        });

        assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

        ExpressionTrace trace = ae.getExpressionTrace();
        assertNotNull(trace, "Expected filter-decision explainer trace at verbosity 3");
        assertEquals(ExpressionTrace.PHASE_EVAL, trace.getPhase());
        assertEquals(ExpressionTrace.LANG_AEL, trace.getLang());
    }

    private static Session sessionWithVerbosity(int verbosity) {
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(verbosity)
            )
        );
        return cluster.createSession(behavior);
    }
}
