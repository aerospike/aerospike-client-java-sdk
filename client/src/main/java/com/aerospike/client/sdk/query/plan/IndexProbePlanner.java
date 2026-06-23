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
package com.aerospike.client.sdk.query.plan;

import java.util.Objects;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.command.IndexProbeCommand;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.QueryHint;
import com.aerospike.client.sdk.query.WhereClauseProcessor;
import com.aerospike.client.sdk.util.Version;

/**
 * Runs a server query-plan probe and returns an immutable {@link QueryPlan}.
 */
public final class IndexProbePlanner {

    private IndexProbePlanner() {
    }

    /**
     * Probe the server for index selection on a dataset WHERE clause.
     *
     * @param session  session supplying cluster and policies
     * @param dataSet  namespace and set to query
     * @param where    WHERE clause (required)
     * @param hint     optional query hint; only {@link QueryHint.Result#getIndexName()} is sent on probe
     * @return server query plan
     */
    public static QueryPlan plan(
        Session session,
        DataSet dataSet,
        WhereClauseProcessor where,
        QueryHint.Result hint
    ) {
        Objects.requireNonNull(session, "session must not be null");
        Objects.requireNonNull(dataSet, "dataSet must not be null");
        Objects.requireNonNull(where, "Query plan requires a where clause");

        Cluster cluster = session.getCluster();
        if (!cluster.supportsQuerySelection()) {
            Version version = cluster.getVersion();
            String versionText = version != null ? version.toString() : "unknown";
            throw new AerospikeException(
                "Server query selection requires cluster minimum version "
                    + Version.SERVER_VERSION_8_1_3 + ". Current version is " + versionText
            );
        }

        Expression predicate = where.toProbeExpression(session);
        String indexNameHint = indexNameHintForProbe(hint);
        ResolvedSettings settings = session.getBehavior().getSettings(OpKind.READ, OpShape.QUERY, Mode.ANY);

        IndexProbeCommand cmd = new IndexProbeCommand(
            cluster,
            dataSet.getNamespace(),
            dataSet.getSet(),
            predicate,
            indexNameHint,
            settings
        );
        return cmd.execute();
    }

    /**
     * On the new probe path only an explicit index name hint is sent (field {@code 21}).
     * {@code forBin} hints apply to the legacy execute path only.
     */
    public static String indexNameHintForProbe(QueryHint.Result hint) {
        if (hint == null) {
            return null;
        }
        return hint.getIndexName();
    }
}
