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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.BehaviorYamlLoader;
import com.aerospike.client.sdk.policy.ResolvedSettings;

/**
 * Ensures {@link Session#getBehavior()} tracks registry updates (e.g. YAML file reloads).
 */
class SessionBehaviorReloadTest {

    @AfterEach
    void resetRegistry() {
        Behavior.restoreBehaviorRegistry();
    }

    @Test
    @DisplayName("Session follows DEFAULT profile when YAML is reloaded")
    void sessionTracksRegistryDefaultAfterYamlReload() throws IOException {
        BehaviorYamlLoader.loadBehaviorsFromString("""
            behaviors:
              DEFAULT:
                allOperations:
                  abandonCallAfter: 5s
            """);

        Session session = new Session(null, Behavior.DEFAULT);

        ResolvedSettings first = session.getBehavior().getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
            Behavior.Mode.AP);
        assertEquals(5000, first.getAbandonCallAfterMs());

        BehaviorYamlLoader.loadBehaviorsFromString("""
            behaviors:
              DEFAULT:
                allOperations:
                  abandonCallAfter: 12s
            """);

        ResolvedSettings second = session.getBehavior().getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
            Behavior.Mode.AP);
        assertEquals(12000, second.getAbandonCallAfterMs());
    }
}
