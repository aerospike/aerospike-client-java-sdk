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
package com.aerospike.client.sdk.junit;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.util.Version;

/** Cluster capabilities gated in integration tests (all require {@link Version#SERVER_VERSION_8_1_3}+). */
public enum ServerFeature {
    AEL("server does not support AEL"),
    STRING_OPS("server does not support string operations"),
    EXTENDED_ERROR_DETAIL("Extended error-detail"),
    QUERY_SELECTION("server does not support query selection");

    private final String reason;

    ServerFeature(String reason) {
        this.reason = reason;
    }

    public boolean isSupported() {
        Cluster cluster = ClusterTest.cluster;
        if (cluster == null) {
            return false;
        }
        return switch (this) {
            case AEL, EXTENDED_ERROR_DETAIL -> cluster.supportsAel();
            case STRING_OPS -> cluster.supportsStringOperations();
            case QUERY_SELECTION -> cluster.supportsQuerySelection();
        };
    }

    public String skipMessage() {
        return reason + " (requires " + Version.SERVER_VERSION_8_1_3 + "+)";
    }
}
