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

import com.aerospike.ael.ParseResult;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.query.AelPlaceholderBinder;
import com.aerospike.client.sdk.query.Filter;
import com.aerospike.client.sdk.query.PreparedAel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AelMaterializer {
    private static final Logger log = LoggerFactory.getLogger(Loggers.AEL);

    private AelMaterializer() {
    }

    /**
     * String AEL for filter/read/write ops: server-compiled payload when supported, else client parse.
     */
    public static Expression expressionFromString(Cluster cluster, String ael) {
        if (cluster.supportsAel()) {
            return Expression.fromServerCompiledFilter(ael);
        }
        throw AerospikeException.toException(ResultCode.OP_NOT_APPLICABLE,
                "Aerospike Expression Language (AEL) requires server version 8.1.3+. Server version is " +
                        cluster.getVersion());
    }

    public static Expression expressionFromString(Cluster cluster, String ael, Object[] params) {
        return expressionFromString(cluster, AelPlaceholderBinder.bind(ael, params));
    }

    public static Expression expressionFromPrepared(Cluster cluster, PreparedAel ael, Object[] params) {
        return expressionFromString(cluster, ael.formValue(params));
    }

    /**
     * WHERE from string AEL for paths that still use field {@code 43} (keyed query, batch filter,
     * legacy dataset query with client SI).
     *
     * <p>When {@code allowsIndex} is {@code false} and {@link Cluster#supportsAel()}, returns
     * server-compiled filter bytes ({@code [128, ael]}). Otherwise full client parse (including
     * secondary index {@link Filter} when {@code allowsIndex} is {@code true}).</p>
     *
     * <p>String-AEL dataset queries on {@link Cluster#supportsQuerySelection()} clusters use field
     * {@code 44} via {@link com.aerospike.client.sdk.query.IndexProbePlanner} instead.</p>
     */
    public static ParseResult parseWhereFromString(Session session, String ael) {
        if (!session.getCluster().supportsAel()) {
            throw AerospikeException.toException(ResultCode.OP_NOT_APPLICABLE,
                    "Aerospike Expression Language (AEL) requires server version 8.1.3+. Server version is " +
                            session.getCluster().getVersion());
        }

        return serverCompiledFilterResult(ael);
    }

    private static ParseResult serverCompiledFilterResult(String ael) {
        return new ParseResult(null, Exp.expr(Expression.fromServerCompiledFilter(ael)));
    }

}
