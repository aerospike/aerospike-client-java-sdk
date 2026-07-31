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

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.command.IndexProbeCommand;
import com.aerospike.client.sdk.command.QueryCommand;
import com.aerospike.client.sdk.exp.Expression;
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
            explainWhereFlags(hint),
            settings
        );
        return cmd.execute();
    }

    /**
     * Builds a dataset {@link QueryCommand}: server explain → execute when eligible, else legacy
     * field {@code 43}. Explain failures (including {@code PARAMETER}) propagate to the caller.
     */
    static QueryCommand buildCommand(
        Session session,
        DataSet dataSet,
        WhereClauseProcessor where,
        QueryHint.Result hint,
        ResolvedSettings policy,
        QueryBuilder qb
    ) {
        Cluster cluster = session.getCluster();
        if (!useServerQuerySelection(cluster, where, hint)) {
            return legacyCommand(cluster, dataSet, where, policy, qb);
        }
        QueryPlan plan = plan(session, dataSet, where, hint);
        return QueryCommand.forPlan(cluster, dataSet, plan, policy, qb);
    }

    /**
     * Whether index-query {@code execute()} should attempt server explain (field {@code 44}).
     *
     * <p>String or prepared AEL uses field {@code 44} when the cluster supports query selection.
     * The client does not parse AEL or inspect index shape to decide routing. Non-textual WHERE
     * ({@code Exp}, {@code BooleanExpression}) and {@code forBin} hints use legacy field
     * {@code 43}.</p>
     */
    static boolean useServerQuerySelection(
        Cluster cluster,
        WhereClauseProcessor where,
        QueryHint.Result hint
    ) {
        if (!cluster.supportsQuerySelection()) {
            return false;
        }
        if (where == null || !where.hasStringAel()) {
            return false;
        }
        return hint == null || hint.getBinName() == null;
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

    /**
     * Field {@code 44} WHERE flag byte for explain from hint policy flags.
     */
    static int explainWhereFlags(QueryHint.Result hint) {
        int flags = QueryWhereWire.FLAG_EXPLAIN;
        if (hint == null) {
            return flags;
        }
        if (hint.isRequireIndex()) {
            flags |= QueryWhereWire.FLAG_REQUIRE_INDEX;
        }
        if (hint.isHardHint()) {
            flags |= QueryWhereWire.FLAG_HARD_HINT;
        }
        return flags;
    }

    private static QueryCommand legacyCommand(
        Cluster cluster,
        DataSet dataSet,
        WhereClauseProcessor where,
        ResolvedSettings policy,
        QueryBuilder qb
    ) {
        Expression filterExp = null;
        if (where != null) {
            filterExp = where.toFilterExpression(cluster);
        }
        return new QueryCommand(cluster, dataSet, null, filterExp, policy, qb);
    }
}
