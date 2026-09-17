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

import com.aerospike.client.sdk.util.Version;

/** Helpers for unit tests that need a {@link Cluster} without a live server. */
public final class TestClusters {

    private TestClusters() {
    }

    /** Disconnected cluster with the given minimum server version. Caller must close it. */
    public static Cluster disconnected(Version version) {
        ClusterDefinition def = new ClusterDefinition("host", 3000)
            .failIfNotConnected(false);
        Cluster cluster = new Cluster(def, SystemSettings.DEFAULT);
        cluster.setVersion(version);
        return cluster;
    }
}
