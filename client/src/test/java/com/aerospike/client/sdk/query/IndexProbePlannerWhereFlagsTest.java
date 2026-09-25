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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.policy.Behavior.Selectors;
import com.aerospike.client.sdk.policy.QueryDuration;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.plan.QueryWhereWire;

/**
 * CLIENT-5483: the behavior-level {@code allowScansWithWhere} setting must reach the field
 * {@code 44} WHERE flags whether or not the query carries a {@link QueryHint}.
 *
 * <p>{@code REQUIRE_INDEX} is what makes the server answer {@code INDEX_NOTFOUND} instead of
 * falling back to a primary-index scan, so it must be set exactly when the effective policy is
 * "no scans". Before the fix a hintless query returned early and never set it, which turned a
 * dropped or missing index into a silent full scan under the shipped default.</p>
 *
 * <p>No cluster is required: {@link Behavior} resolves its settings client-side.</p>
 */
public class IndexProbePlannerWhereFlagsTest {

    /** The setting under test, at each of the three states an application can configure. */
    private static ResolvedSettings settings(Boolean allowScansWithWhere) {
        Behavior behavior = (allowScansWithWhere == null)
            ? Behavior.DEFAULT
            : Behavior.DEFAULT.deriveWithChanges("scanPolicy",
                b -> b.on(Selectors.reads().query(), ops -> ops.allowScansWithWhere(allowScansWithWhere)));

        return behavior.getSettings(OpKind.READ, OpShape.QUERY, Mode.ANY);
    }

    private static boolean requiresIndex(ResolvedSettings settings, QueryHint.Result hint) {
        return (IndexProbePlanner.explainWhereFlags(settings, hint) & QueryWhereWire.FLAG_REQUIRE_INDEX) != 0;
    }

    /** A hint that says nothing about scans, to prove the mere presence of a hint changes nothing. */
    private static QueryHint.Result neutralHint() {
        return QueryHint.create().queryDuration(QueryDuration.LONG);
    }

    private static QueryHint.Result allowHint() {
        return QueryHint.create().allowScansWithWhere();
    }

    private static QueryHint.Result denyHint() {
        return QueryHint.create().disallowScansWithWhere();
    }

    /**
     * The matrix from CLIENT-5483: REQUIRE_INDEX is set exactly when the effective policy forbids
     * scans, which is the hint's value when it states one and the behavior setting otherwise.
     */
    static Stream<Arguments> scanPolicyMatrix() {
        return Stream.of(
            //          label,                    behavior setting, hint,           expect REQUIRE_INDEX
            Arguments.of("DEFAULT, no hint",      null,             null,           true),
            Arguments.of("DEFAULT, neutral hint", null,             neutralHint(),  true),
            Arguments.of("DEFAULT, allow hint",   null,             allowHint(),    false),
            Arguments.of("DEFAULT, deny hint",    null,             denyHint(),     true),

            Arguments.of("false, no hint",        Boolean.FALSE,    null,           true),
            Arguments.of("false, neutral hint",   Boolean.FALSE,    neutralHint(),  true),
            Arguments.of("false, allow hint",     Boolean.FALSE,    allowHint(),    false),
            Arguments.of("false, deny hint",      Boolean.FALSE,    denyHint(),     true),

            Arguments.of("true, no hint",         Boolean.TRUE,     null,           false),
            Arguments.of("true, neutral hint",    Boolean.TRUE,     neutralHint(),  false),
            Arguments.of("true, allow hint",      Boolean.TRUE,     allowHint(),    false),
            Arguments.of("true, deny hint",       Boolean.TRUE,     denyHint(),     true));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("scanPolicyMatrix")
    void requireIndexFollowsTheEffectiveScanPolicy(
        String label,
        Boolean behaviorSetting,
        QueryHint.Result hint,
        boolean expectRequireIndex
    ) {
        assertEquals(expectRequireIndex, requiresIndex(settings(behaviorSetting), hint),
            label + ": REQUIRE_INDEX should be " + expectRequireIndex);
    }

    /** The shipped default forbids scans, so the protection must apply with no configuration at all. */
    @Test
    void hintlessQueryUnderDefaultBehaviorRequiresAnIndex() {
        assertTrue(requiresIndex(settings(null), null),
            "Behavior.DEFAULT carries allowScansWithWhere=false, so a hintless query must require an index");
    }

    /** EXPLAIN is unconditional; the fix must not disturb it or set HARD_HINT without a hint. */
    @Test
    void hintlessQueryStillExplainsAndDoesNotHardHint() {
        int flags = IndexProbePlanner.explainWhereFlags(settings(null), null);

        assertEquals(QueryWhereWire.FLAG_EXPLAIN, flags & QueryWhereWire.FLAG_EXPLAIN, "EXPLAIN");
        assertEquals(0, flags & QueryWhereWire.FLAG_HARD_HINT, "HARD_HINT without a hint");
    }

    /** A hard hint still carries HARD_HINT, alongside the resolved scan policy. */
    @Test
    void hardHintSetsHardHintFlag() {
        QueryHint.Result hint = QueryHint.create().forIndex("age_idx").hardHint();
        int flags = IndexProbePlanner.explainWhereFlags(settings(null), hint);

        assertEquals(QueryWhereWire.FLAG_HARD_HINT, flags & QueryWhereWire.FLAG_HARD_HINT, "HARD_HINT");
        assertTrue((flags & QueryWhereWire.FLAG_REQUIRE_INDEX) != 0, "REQUIRE_INDEX under DEFAULT");
    }
}
