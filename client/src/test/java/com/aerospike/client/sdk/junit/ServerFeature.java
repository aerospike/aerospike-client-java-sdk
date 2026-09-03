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
        Cluster cluster = clusterForFeatureCheck();
        return switch (this) {
            case AEL, EXTENDED_ERROR_DETAIL -> cluster.supportsAel();
            case STRING_OPS -> cluster.supportsStringOperations();
            case QUERY_SELECTION -> cluster.supportsQuerySelection();
        };
    }

    /**
     * The cluster to ask about capabilities, connecting first if nothing has yet.
     *
     * <p>{@link RequiresServerFeatureExtension} is an execution condition, so it is evaluated before
     * {@code @BeforeAll}. A class run on its own therefore arrives here with {@link ClusterTest#cluster}
     * still null; only a suite run has already connected, in its {@code @BeforeSuite}. Answering
     * "unsupported" for null disabled the whole class — {@code ErrorDetailVerbosityTest} reported 36 tests
     * skipped and the build green, so a gated class could stay silently excluded indefinitely.</p>
     *
     * <p>{@link ClusterTest#initCluster()} is idempotent, so connecting here simply makes a standalone run
     * agree with a suite run.</p>
     */
    private static Cluster clusterForFeatureCheck() {
        if (ClusterTest.cluster == null) {
            ClusterTest.initCluster();
        }

        Cluster cluster = ClusterTest.cluster;

        if (cluster == null) {
            throw new IllegalStateException(
                "No cluster available to evaluate @RequiresServerFeature. Reporting the feature as "
                    + "unsupported here would skip every test in the class and still pass the build, so "
                    + "fail loudly instead.");
        }
        return cluster;
    }

    public String skipMessage() {
        return reason + " (requires " + Version.SERVER_VERSION_8_1_3 + "+)";
    }
}
