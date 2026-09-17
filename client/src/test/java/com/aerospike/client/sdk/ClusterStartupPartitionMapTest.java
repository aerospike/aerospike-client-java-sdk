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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.policy.Behavior;

/**
 * connect() must not return until the partition map is populated,
 * so the first command after connect() cannot fail with "Partition map empty".
 */
public class ClusterStartupPartitionMapTest {
    public static Args args = Args.Instance;

    @Test
    public void firstCommandAfterConnectSucceeds() {
        Host[] hosts = Host.parseHosts(args.host, args.port);
        DataSet ds = DataSet.of(args.namespace, "CLIENT_5373");

        // Several fresh clients in one JVM: the first is cold, later ones warm
        // enough to race the tend thread on the buggy code.
        for (int i = 0; i < 5; i++) {
            try (Cluster cluster = new ClusterDefinition(hosts).failIfNotConnected(false).connect()) {
                Session session = cluster.createSession(Behavior.DEFAULT);

                // First command after connect() must succeed. On the buggy code it
                // fails with "Partition map empty" because connect() returned before
                // the partition map was populated - assert that does not happen.
                try (RecordStream rs = session.upsert(ds.id(1)).bin("name").setTo("dummy").execute()) {
                    assertNotNull(rs.popRecord(), "Record not returned");
                }
                catch (AerospikeException.InvalidNamespace e) {
                    fail("first command failed with 'Partition map empty'. " +
                        "connect() returned before the client could route operations. " + e.getMessage());
                }
            }
        }
    }
}
