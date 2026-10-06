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
import com.aerospike.client.sdk.command.InlinePlan;
import com.aerospike.client.sdk.command.QueryCommand;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QueryPlanCache;
import com.aerospike.client.sdk.query.plan.QueryPlanSettings;
import com.aerospike.client.sdk.query.plan.QueryPlanStats;
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
            explainWhereFlags(settings, hint),
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
        if (!useServerQuerySelection(cluster, where, hint, qb.getFilter())) {
            return legacyCommand(cluster, dataSet, where, policy, qb);
        }
        if (QueryPlanSettings.cacheEnabled()) {
            return cachedCommand(session, dataSet, where, hint, policy, qb);
        }

        if (QueryPlanSettings.autoEnabled()) {
            return autoCommand(session, dataSet, where, hint, policy, qb);
        }

        if (QueryPlanSettings.inlineEnabled()) {
            return inlineCommand(session, dataSet, where, hint, policy, qb);
        }

        QueryPlan plan = plan(session, dataSet, where, hint);
        return QueryCommand.forPlan(cluster, dataSet, plan, policy, qb);
    }

    /**
     * A2: no explain - the execute command carries AUTO_PLAN and every node plans inline. An index
     * name travels only with a hard hint (the server treats a name on AUTO_PLAN as a pin).
     */
    private static QueryCommand inlineCommand(
        Session session,
        DataSet dataSet,
        WhereClauseProcessor where,
        QueryHint.Result hint,
        ResolvedSettings policy,
        QueryBuilder qb
    ) {
        String ael = where.getAelString();
        QueryWhereWire.requireAel(ael);

        ResolvedSettings settings = session.getBehavior().getSettings(OpKind.READ, OpShape.QUERY, Mode.ANY);
        int policyFlags = explainWhereFlags(settings, hint) & ~QueryWhereWire.FLAG_EXPLAIN;
        String pin = (policyFlags & QueryWhereWire.FLAG_HARD_HINT) != 0 ? indexNameHintForProbe(hint) : null;

        return QueryCommand.forInline(session.getCluster(), dataSet, ael, policyFlags, pin, policy, qb);
    }

    /**
     * A5: one round trip in every case. A cache hit pins the cached plan from the first command
     * (no planning on the nodes); a miss plans inline with per-partition pinning and caches the
     * plan if every node agreed on it.
     */
    private static QueryCommand autoCommand(
        Session session,
        DataSet dataSet,
        WhereClauseProcessor where,
        QueryHint.Result hint,
        ResolvedSettings policy,
        QueryBuilder qb
    ) {
        QueryCommand cmd = inlineCommand(session, dataSet, where, hint, policy, qb);
        InlinePlan inline = cmd.getInlinePlan();
        ResolvedSettings settings = session.getBehavior().getSettings(OpKind.READ, OpShape.QUERY, Mode.ANY);
        int policyFlags = explainWhereFlags(settings, hint) & ~QueryWhereWire.FLAG_EXPLAIN;
        QueryPlanCache cache = session.getCluster().getQueryPlanCache();
        QueryPlanCache.Key key = new QueryPlanCache.Key(
            dataSet.getNamespace(),
            dataSet.getSet(),
            where.getAelString(),
            policyFlags | QueryWhereWire.FLAG_AUTO_PLAN,
            indexNameHintForProbe(hint)
        );

        InlinePlan.Choice cached = cache.getChoice(key);

        if (cached != null) {
            inline.preset(cached);
        }
        else {
            inline.learnInto(cache, key);
        }

        cmd.setPlanCacheKey(key);
        return cmd;
    }

    /**
     * A1: reuse a cached plan when one exists, otherwise explain and cache the result. The cached
     * plan is pinned for every chunk exactly as a freshly explained plan would be.
     */
    private static QueryCommand cachedCommand(
        Session session,
        DataSet dataSet,
        WhereClauseProcessor where,
        QueryHint.Result hint,
        ResolvedSettings policy,
        QueryBuilder qb
    ) {
        Cluster cluster = session.getCluster();
        ResolvedSettings settings = session.getBehavior().getSettings(OpKind.READ, OpShape.QUERY, Mode.ANY);
        QueryPlanCache cache = cluster.getQueryPlanCache();
        QueryPlanCache.Key key = new QueryPlanCache.Key(
            dataSet.getNamespace(),
            dataSet.getSet(),
            where.getAelString(),
            explainWhereFlags(settings, hint),
            indexNameHintForProbe(hint)
        );

        QueryPlan plan = cache.get(key);

        if (plan == null) {
            plan = plan(session, dataSet, where, hint);
            cache.put(key, plan);
        }
        else {
            QueryPlanStats.recordPlan(QueryPlanStats.describe(plan.getSelection(), plan.getIndexName()));
        }

        QueryCommand cmd = QueryCommand.forPlan(cluster, dataSet, plan, policy, qb);
        cmd.setPlanCacheKey(key);
        return cmd;
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
        QueryHint.Result hint,
        Filter filter
    ) {
        if (filter != null) {
            return false;
        }
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
     *
     * <p>The scan policy is resolved before any hint-specific handling: a hint overrides the
     * behavior setting when it states one, but a query with no hint still gets the setting's
     * value. Returning early on a null hint would leave {@code REQUIRE_INDEX} unreachable and
     * silently turn an unservable where clause into a primary-index scan (CLIENT-5483).</p>
     */
    static int explainWhereFlags(ResolvedSettings settings, QueryHint.Result hint) {
        int flags = QueryWhereWire.FLAG_EXPLAIN;

        Boolean b = (hint != null)? hint.getAllowScansWithWhere() : null;
        boolean allowScansWithWhere = (b != null)? b : settings.getAllowScansWithWhere();

        if (!allowScansWithWhere) {
            flags |= QueryWhereWire.FLAG_REQUIRE_INDEX;
        }
        if (hint != null && hint.isHardHint()) {
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
        if (qb.getFilter() != null) {
            return QueryCommand.forExplicitFilter(cluster, dataSet, qb.getFilter(), filterExp, policy, qb);
        }
        return new QueryCommand(cluster, dataSet, null, filterExp, policy, qb);
    }
}
