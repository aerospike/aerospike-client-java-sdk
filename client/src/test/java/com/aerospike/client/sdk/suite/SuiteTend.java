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
package com.aerospike.client.sdk.suite;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import com.aerospike.client.sdk.tend.AddressTranslationTest;
import com.aerospike.client.sdk.tend.AddressTranslatorCloseTest;
import com.aerospike.client.sdk.tend.AlternateAddressMissingTest;
import com.aerospike.client.sdk.tend.AwsTagsSeedRefresherTest;
import com.aerospike.client.sdk.tend.ClusterViewChangeTest;
import com.aerospike.client.sdk.tend.DefaultTranslationTest;
import com.aerospike.client.sdk.tend.DiscoveryRefreshTest;
import com.aerospike.client.sdk.tend.DiscoveryRefreshThrowTest;
import com.aerospike.client.sdk.tend.DiscoveryResultCodeTest;
import com.aerospike.client.sdk.tend.DnsSeedRefresherTest;
import com.aerospike.client.sdk.tend.DuplicateNodeTest;
import com.aerospike.client.sdk.tend.EndpointSourceListTest;
import com.aerospike.client.sdk.tend.HostnameAddressMoveTest;
import com.aerospike.client.sdk.tend.IdleReapTest;
import com.aerospike.client.sdk.tend.PartitionParserFullOwnerTest;
import com.aerospike.client.sdk.tend.PassThroughTranslationTest;
import com.aerospike.client.sdk.tend.PeerParserTest;
import com.aerospike.client.sdk.tend.PortMapTranslationTest;
import com.aerospike.client.sdk.tend.SeedCandidateProviderSpiTest;
import com.aerospike.client.sdk.tend.SeedMergePolicyTest;
import com.aerospike.client.sdk.tend.SeedProviderCloseTest;
import com.aerospike.client.sdk.tend.SeedRefresherTest;
import com.aerospike.client.sdk.tend.StaggeredConnectTest;
import com.aerospike.client.sdk.tend.StaleConnectionRetryTest;
import com.aerospike.client.sdk.tend.ThreadModeTest;
import com.aerospike.client.sdk.tend.TranslationCollisionTest;
import com.aerospike.client.sdk.tend.ZeroNodeReseedTest;

@Suite
@SelectClasses({
    AddressTranslationTest.class,
    AddressTranslatorCloseTest.class,
    AlternateAddressMissingTest.class,
    AwsTagsSeedRefresherTest.class,
    ClusterViewChangeTest.class,
    DefaultTranslationTest.class,
    DiscoveryRefreshTest.class,
    DiscoveryRefreshThrowTest.class,
    DiscoveryResultCodeTest.class,
    DnsSeedRefresherTest.class,
    DuplicateNodeTest.class,
    EndpointSourceListTest.class,
    HostnameAddressMoveTest.class,
    IdleReapTest.class,
    PartitionParserFullOwnerTest.class,
    PassThroughTranslationTest.class,
    PeerParserTest.class,
    PortMapTranslationTest.class,
    SeedCandidateProviderSpiTest.class,
    SeedMergePolicyTest.class,
    SeedProviderCloseTest.class,
    SeedRefresherTest.class,
    StaggeredConnectTest.class,
    StaleConnectionRetryTest.class,
    ThreadModeTest.class,
    TranslationCollisionTest.class,
    ZeroNodeReseedTest.class
})
public class SuiteTend {
}
