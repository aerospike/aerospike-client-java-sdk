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

import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.query.AelPlaceholderBinder;
import com.aerospike.client.sdk.query.PreparedAel;

public final class AelMaterializer {

    private AelMaterializer() {
    }

    /**
     * String AEL for filter/read/write ops: server-compiled payload when supported.
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
}
