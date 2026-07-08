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

import com.aerospike.client.sdk.AelMaterializer;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.command.IndexProbeCommand;
import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QueryWhereWire;

/**
 * Package-private probe orchestration for two-phase server index selection.
 */
final class IndexProbePlanner {

    private IndexProbePlanner() {
    }

    static QueryPlan plan(
        Session session,
        DataSet dataSet,
        WhereClauseProcessor where,
        QueryHint.Result hint
    ) {
        String ael = where.getAelString();
        QueryWhereWire.requireAel(ael);

        Cluster cluster = session.getCluster();
        ResolvedSettings settings = session.getBehavior().getSettings(OpKind.READ, OpShape.QUERY, Mode.ANY);

        IndexProbeCommand cmd = new IndexProbeCommand(
            cluster,
            dataSet.getNamespace(),
            dataSet.getSet(),
            ael,
            indexNameHintForProbe(hint),
            settings
        );
        return cmd.execute();
    }

    /**
     * Whether index-query {@code execute()} should probe the server and replay the returned plan.
     *
     * <p>String or prepared AEL uses field {@code 44} (SI or PI) when server explain supports the
     * index shape (scalar INTEGER/STRING, {@link com.aerospike.client.sdk.query.IndexCollectionType#DEFAULT}).
     * BLOB and collection indexes (LIST, MAPKEYS, …) stay on the legacy field {@code 43} path until
     * server explain handles them. Non-textual WHERE ({@code Exp}, {@code BooleanExpression}) always
     * uses legacy.</p>
     */
    static boolean useServerQuerySelection(
        Cluster cluster,
        Session session,
        DataSet dataSet,
        WhereClauseProcessor where,
        QueryHint.Result hint
    ) {
        if (!cluster.supportsQuerySelection()) {
            return false;
        }
        if (where == null || !where.hasStringAel()) {
            return false;
        }
        if (hint != null && hint.getBinName() != null) {
            return false;
        }
        if (AelMaterializer.requiresLegacyClientIndexSelection(
            session,
            where.allowsIndex(),
            dataSet.getNamespace(),
            dataSet.getSet(),
            where.getAelString()
        )) {
            return false;
        }
        return true;
    }

    /**
     * On the new probe path only an explicit index name hint is sent (field {@code 21}).
     * {@code forBin} hints apply to the legacy execute path only.
     */
    static String indexNameHintForProbe(QueryHint.Result hint) {
        if (hint == null) {
            return null;
        }
        String indexName = hint.getIndexName();
        if (indexName == null || indexName.isBlank()) {
            return null;
        }
        return indexName;
    }
}
