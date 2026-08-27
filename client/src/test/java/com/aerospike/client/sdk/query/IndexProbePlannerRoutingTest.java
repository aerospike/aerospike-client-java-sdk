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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.exp.Exp;

/**
 * Routing for {@link IndexProbePlanner#useServerQuerySelection(
 * com.aerospike.client.sdk.Cluster, WhereClauseProcessor, QueryHint.Result)}.
 *
 * <p>Only query-selection cluster support, {@link WhereClauseProcessor#hasStringAel()}, and
 * {@code hint.getBinName()} are consulted — AEL text shape (range, blob, map keys, etc.) is not.
 * Blank AEL is validated later in {@link IndexProbePlanner#plan}.</p>
 */
public class IndexProbePlannerRoutingTest extends ClusterTest {

    @BeforeAll
    static void requireQuerySelection() {
        assumeTrue(cluster.supportsQuerySelection(), "server does not support query selection");
    }

    @Test
    void useServerQuerySelection_stringAel_usesServerExplain() {
        WhereClauseProcessor where = WhereClauseProcessor.from("$.age > 30");

        assertTrue(IndexProbePlanner.useServerQuerySelection(cluster, where, null));
    }

    @Test
    void useServerQuerySelection_expWhere_staysLegacy() {
        WhereClauseProcessor where = WhereClauseProcessor.from(Exp.eq(Exp.intBin("age"), Exp.val(30)));

        assertFalse(IndexProbePlanner.useServerQuerySelection(cluster, where, null));
    }

    @Test
    void useServerQuerySelection_forBinHint_staysLegacy() {
        WhereClauseProcessor where = WhereClauseProcessor.from("$.age > 30");
        QueryHint.Result hint = QueryHint.create().forBin("age");

        assertFalse(IndexProbePlanner.useServerQuerySelection(cluster, where, hint));
    }

    @Test
    void planRejectsBlankAel() {
        WhereClauseProcessor where = WhereClauseProcessor.from("   ");

        assertThrows(IllegalArgumentException.class,
            () -> IndexProbePlanner.plan(session, args.set, where, null));
    }
}
