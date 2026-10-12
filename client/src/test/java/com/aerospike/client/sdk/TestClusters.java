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

import java.lang.reflect.Field;

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
        setServerVersion(cluster, version);
        return cluster;
    }

    /**
     * Force the cluster's minimum server version (and derived feature flags) for testing.
     * {@code Cluster} derives this privately from its nodes, so tests set the fields via reflection.
     * A later {@code setNodes()} call from cluster tend may recompute and overwrite it.
     */
    public static void setServerVersion(Cluster cluster, Version version) {
        set(cluster, "serverVersion", version);
        set(cluster, "serverVersionGE8", version != null && version.isGreaterOrEqual(Version.SERVER_VERSION_8_0));
        set(cluster, "serverVersionGE812", version != null && version.isGreaterOrEqual(Version.SERVER_VERSION_8_1_2));
        set(cluster, "serverVersionGE82", version != null && version.isGreaterOrEqual(Version.SERVER_VERSION_8_2));
    }

    private static void set(Cluster cluster, String name, Object value) {
        try {
            Field f = Cluster.class.getDeclaredField(name);
            f.setAccessible(true);
            f.set(cluster, value);
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Unable to set Cluster." + name, e);
        }
    }
}
