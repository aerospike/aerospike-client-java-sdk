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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.util.Version;

class IndexProbePlannerRoutingTest extends ClusterTest {

    @Test
    void useServerQuerySelection_stringAel_ignoresAllowsIndexFalse() {
        Version saved = cluster.getVersion();
        setQuerySelectionGate(true);
        try {
            WhereClauseProcessor where = WhereClauseProcessor.from(false, "$.age > 30");

            assertTrue(where.hasStringAel());
            assertFalse(where.allowsIndex());
            assertTrue(IndexProbePlanner.useServerQuerySelection(cluster, where, null));
        } finally {
            restoreClusterVersion(saved);
        }
    }

    @Test
    void useServerQuerySelection_expWhere_staysLegacy() {
        Version saved = cluster.getVersion();
        setQuerySelectionGate(true);
        try {
            WhereClauseProcessor where = WhereClauseProcessor.from(Exp.eq(Exp.intBin("age"), Exp.val(30)));

            assertFalse(where.hasStringAel());
            assertFalse(IndexProbePlanner.useServerQuerySelection(cluster, where, null));
        } finally {
            restoreClusterVersion(saved);
        }
    }

    @Test
    void useServerQuerySelection_forBinHint_staysLegacy() {
        Version saved = cluster.getVersion();
        setQuerySelectionGate(true);
        try {
            WhereClauseProcessor where = WhereClauseProcessor.from(true, "$.age > 30");
            QueryHint.Result hint = QueryHint.create().forBin("age");

            assertFalse(IndexProbePlanner.useServerQuerySelection(cluster, where, hint));
        } finally {
            restoreClusterVersion(saved);
        }
    }

    @Test
    void useServerQuerySelection_gateOff_staysLegacy() {
        Version saved = cluster.getVersion();
        setQuerySelectionGate(false);
        try {
            WhereClauseProcessor where = WhereClauseProcessor.from(true, "$.age > 30");
            assertFalse(IndexProbePlanner.useServerQuerySelection(cluster, where, null));
        } finally {
            restoreClusterVersion(saved);
        }
    }

    private void setQuerySelectionGate(boolean enabled) {
        cluster.setVersion(enabled ? Version.SERVER_VERSION_8_1_3 : Version.SERVER_VERSION_8_1_2);
    }

    private void restoreClusterVersion(Version saved) {
        cluster.setVersion(saved);
    }
}
