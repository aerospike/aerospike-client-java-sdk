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

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.command.IndexProbeCommand;
import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.util.Version;

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
        Cluster cluster = session.getCluster();
        if (!cluster.supportsQuerySelection()) {
            Version version = cluster.getVersion();
            String versionText = version != null ? version.toString() : "unknown";
            throw new AerospikeException(
                "Server query selection requires cluster minimum version "
                    + Version.SERVER_VERSION_8_1_3 + ". Current version is " + versionText
            );
        }

        ResolvedSettings settings = session.getBehavior().getSettings(OpKind.READ, OpShape.QUERY, Mode.ANY);

        IndexProbeCommand cmd = new IndexProbeCommand(
            cluster,
            dataSet.getNamespace(),
            dataSet.getSet(),
            where.toProbeExpression(session),
            indexNameHintForProbe(hint),
            settings
        );
        return cmd.execute();
    }

    /**
     * Whether index-query {@code execute()} should probe the server and replay the returned plan.
     */
    static boolean useServerQuerySelection(
        Cluster cluster,
        WhereClauseProcessor where,
        QueryHint.Result hint
    ) {
        if (!cluster.supportsQuerySelection()) {
            return false;
        }
        if (where == null || !where.allowsIndex()) {
            return false;
        }
        if (hint != null && hint.getBinName() != null) {
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
