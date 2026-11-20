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

import com.aerospike.client.sdk.discovery.AwsCredentialChainTest;
import com.aerospike.client.sdk.discovery.AwsEc2TagSeedCandidateProviderTest;
import com.aerospike.client.sdk.discovery.AwsSigV4Test;
import com.aerospike.client.sdk.discovery.DnsSeedCandidateProviderTest;
import com.aerospike.client.sdk.discovery.NlbIdentifierTest;
import com.aerospike.client.sdk.discovery.PassThroughAddressTranslatorNoIoTest;
import com.aerospike.client.sdk.discovery.PassThroughAddressTranslatorTest;
import com.aerospike.client.sdk.discovery.PluginGuideExamplesTest;
import com.aerospike.client.sdk.discovery.PortMapAddressTranslatorTest;
import com.aerospike.client.sdk.discovery.StaticMapAddressTranslatorNoIoTest;
import com.aerospike.client.sdk.discovery.StaticMapAddressTranslatorTest;
import com.aerospike.client.sdk.discovery.StaticSeedCandidateProviderTest;
import com.aerospike.client.sdk.discovery.TranslateLatencyTest;

@Suite
@SelectClasses({
    AwsCredentialChainTest.class,
    AwsEc2TagSeedCandidateProviderTest.class,
    AwsSigV4Test.class,
    DnsSeedCandidateProviderTest.class,
    NlbIdentifierTest.class,
    PassThroughAddressTranslatorNoIoTest.class,
    PassThroughAddressTranslatorTest.class,
    PluginGuideExamplesTest.class,
    PortMapAddressTranslatorTest.class,
    StaticMapAddressTranslatorNoIoTest.class,
    StaticMapAddressTranslatorTest.class,
    StaticSeedCandidateProviderTest.class,
    TranslateLatencyTest.class
})
public class SuiteDiscovery {
}
