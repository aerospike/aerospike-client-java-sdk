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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.TestClusters;
import com.aerospike.client.sdk.ael.Ael;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.util.Version;

/**
 * Routing for {@link IndexProbePlanner#useServerQuerySelection(
 * com.aerospike.client.sdk.Cluster, WhereClauseProcessor, QueryHint.Result)}.
 *
 * <p>Only query-selection cluster support, {@link WhereClauseProcessor#hasStringAel()}, and
 * {@code hint.getBinName()} are consulted — AEL text shape (range, blob, map keys, etc.) is not.
 * Blank AEL is validated later in {@link IndexProbePlanner#plan}.</p>
 */
public class IndexProbePlannerRoutingTest {

    private static Cluster clusterWithQuerySelection() {
        return TestClusters.disconnected(Version.SERVER_VERSION_8_1_3);
    }

    @Test
    void useServerQuerySelection_stringAel_usesServerExplain() {
        WhereClauseProcessor where = WhereClauseProcessor.from("$.age > 30");

        try (Cluster cluster = clusterWithQuerySelection()) {
            assertTrue(IndexProbePlanner.useServerQuerySelection(cluster, where, null));
        }
    }

    static Stream<Arguments> nonStringAelWhereClauses() {
        return Stream.of(
            Arguments.of(WhereClauseProcessor.from(Ael.longBin("age").gt(30)), false),
            Arguments.of(WhereClauseProcessor.from(Exp.eq(Exp.intBin("age"), Exp.val(30))), false));
    }

    @ParameterizedTest
    @MethodSource("nonStringAelWhereClauses")
    void nonStringAelWhere_staysLegacyWithoutAelString(
        WhereClauseProcessor where, boolean expectsStringAel
    ) {
        assertEquals(expectsStringAel, where.hasStringAel());
        assertThrows(IllegalStateException.class, where::getAelString);

        try (Cluster cluster = clusterWithQuerySelection()) {
            assertFalse(IndexProbePlanner.useServerQuerySelection(cluster, where, null));
        }
    }

    @Test
    void useServerQuerySelection_forBinHint_staysLegacy() {
        WhereClauseProcessor where = WhereClauseProcessor.from("$.age > 30");
        QueryHint.Result hint = QueryHint.create().forBin("age");

        try (Cluster cluster = clusterWithQuerySelection()) {
            assertFalse(IndexProbePlanner.useServerQuerySelection(cluster, where, hint));
        }
    }

    @Test
    void planRejectsBlankAel() {
        WhereClauseProcessor where = WhereClauseProcessor.from("   ");

        try (Cluster cluster = clusterWithQuerySelection()) {
            Session session = cluster.createSession(Behavior.DEFAULT);
            DataSet dataSet = DataSet.of("test", "users");

            assertThrows(IllegalArgumentException.class,
                () -> IndexProbePlanner.plan(session, dataSet, where, null));
        }
    }
}
