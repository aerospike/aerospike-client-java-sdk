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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.util.Version;

/** Unit tests for {@link Cluster#supportsServerCompiledFilterExpression()} version floor. */
class ClusterServerCompiledFilterPredicateTest {

    @Test
    void versionSupportsServerCompiledFilterExpression_respects_8_4_floor() {
        assertThat(Cluster.versionSupportsServerCompiledFilterExpression(Version.SERVER_VERSION_8_4)).isTrue();
        assertThat(Cluster.versionSupportsServerCompiledFilterExpression(new Version(8, 3, 0, 0))).isFalse();
        assertThat(Cluster.versionSupportsServerCompiledFilterExpression(null)).isFalse();
    }
}
