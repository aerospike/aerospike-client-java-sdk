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

import java.io.File;

import org.junit.platform.suite.api.AfterSuite;
import org.junit.platform.suite.api.BeforeSuite;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.query.ExpSecondaryIndexTest;
import com.aerospike.client.sdk.query.QueryBlobTest;
import com.aerospike.client.sdk.query.QueryChildrenTest;
import com.aerospike.client.sdk.query.QueryCollectionTest;
import com.aerospike.client.sdk.query.QueryContextTest;
import com.aerospike.client.sdk.query.QueryExecuteTest;
import com.aerospike.client.sdk.query.QueryFilterExpTest;
import com.aerospike.client.sdk.query.QueryFilterSetTest;
import com.aerospike.client.sdk.query.QueryGeoTest;
import com.aerospike.client.sdk.query.QueryIndexTest;
import com.aerospike.client.sdk.query.QueryIntegerTest;
import com.aerospike.client.sdk.query.QueryKeyTest;
import com.aerospike.client.sdk.query.QueryRPSTest;
import com.aerospike.client.sdk.query.QueryStringTest;

@Suite
@SelectClasses({
    // Base
    AddTest.class,
    AppendTest.class,
    BackgroundTaskTest.class,
    BatchTest.class,
    BitExpTest.class,
//  CdtExpTest.class,
//  CdtOperateTest.class,
    DeleteBinTest.class,
    ExpireTest.class,
    ExpOperationTest.class,
    FilterExpTest.class,
    GenerationTest.class,
//  HLLExpTest.class,
    ListExpTest.class,
    ListMapTest.class,
    MapExpTest.class,
    NodeChurnPartitionBehaviorTest.class,
//  OperateBitTest.class,
//  OperateHllTest.class,
//  OperateListTest.class,
//  OperateMapTest.class,
    OperateTest.class,
    OpTypeTest.class,
    PutGetTest.class,
    QueryOperationsTest.class,
    RecordStreamAdapterTest.class,
    ReplaceTest.class,
    ServerInfoTest.class,
    TouchTest.class,
    TxnTest.class,
    UdfTest.class,
    // Query
    ExpSecondaryIndexTest.class,
    QueryBlobTest.class,
    QueryChildrenTest.class,
    QueryCollectionTest.class,
    QueryContextTest.class,
    QueryExecuteTest.class,
    QueryFilterExpTest.class,
    QueryFilterSetTest.class,
    QueryGeoTest.class,
    QueryIndexTest.class,
    QueryIntegerTest.class,
    QueryKeyTest.class,
    QueryRPSTest.class,
    QueryStringTest.class,
    QueryWithPartitionPaginationTest.class
})
public class SuiteCluster {
    @BeforeSuite
    public static void beforeSuite() {
        System.out.println("Begin AerospikeClient");
        Log.setCallback(null);

        Args args = Args.Instance;

        Host[] hosts = Host.parseHosts(args.host, args.port);

        ClusterDefinition def = new ClusterDefinition(hosts)
            .withLogLevel(Log.Level.DEBUG)
            .clusterName(args.clusterName)
            .withSystemSettings(SystemSettings.builder()
                    .connections(ops -> ops.maximumConnectionsPerNode(200)).build()
                    .mergeWith(SystemSettings.DEFAULT));

        // Handle authenticated requests if provided
        if (args.user != null && args.password != null) {
            switch (args.authMode) {
                case INTERNAL:
                    def.withNativeCredentials(args.user, args.password);
                    break;
                case EXTERNAL:
                    def.withExternalCredentials(args.user, args.password);
                    break;
                case EXTERNAL_INSECURE:
                    def.withExternalInsecureCredentials(args.user, args.password);
                    break;
                default:
                    break;
            }
        }

        if (args.tlsName != null) {
            String certHome = System.getenv("CERT_HOME");

            if (certHome == null) {
                certHome = "";
            }

            String caFile = resolvePath(certHome, args.caFile);
            String clientCertFile = resolvePath(certHome, args.clientCertFile);
            String clientKeyFile = resolvePath(certHome, args.clientKeyFile);

            def.withTlsConfig(tls -> tls
                .tlsName(args.tlsName)
                .caFile(caFile)
                .clientCertFile(clientCertFile)
                .clientKeyFile(clientKeyFile)
            );
        }

        Cluster cluster = def.connect();
        Session session;

        try {
            session = cluster.createSession(Behavior.DEFAULT);
            args.setServerSpecific(cluster);
        }
        catch (RuntimeException re) {
            cluster.close();
            throw re;
        }

        ClusterTest.cluster = cluster;
        ClusterTest.session = session;
        ClusterTest.initializedBySuite = true;
    }

    private static String resolvePath(String dir, String path) {
        File file = new File(path);

        if (file.isAbsolute()) {
            return path;
        }

        file = new File(dir, path);
        return file.getAbsolutePath();
    }

    @AfterSuite
    public static void afterSuite() {
        System.out.println("End AerospikeClient");
        if (ClusterTest.cluster != null) {
            ClusterTest.cluster.close();
            ClusterTest.cluster = null;
            ClusterTest.session = null;
        }
        ClusterTest.initializedBySuite = false;
    }
}
