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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.policy.Behavior;

class SessionExtensionTest extends ClusterTest {

    static class CustomSession extends Session {
        private final String label;

        protected CustomSession(Cluster cluster, Behavior behavior, String label) {
            super(cluster, behavior);
            this.label = label;
        }

        String getLabel() {
            return label;
        }
    }

    static class AnotherSession extends Session {
        protected AnotherSession(Cluster cluster, Behavior behavior) {
            super(cluster, behavior);
        }
    }

    static class CustomSessionExtension implements SessionExtension<CustomSession> {
        private final String label;

        CustomSessionExtension(String label) {
            this.label = label;
        }

        @Override
        public CustomSession create(Cluster cluster, Behavior behavior) {
            return new CustomSession(cluster, behavior, label);
        }
    }

    static class AnotherSessionExtension implements SessionExtension<AnotherSession> {
        @Override
        public AnotherSession create(Cluster cluster, Behavior behavior) {
            return new AnotherSession(cluster, behavior);
        }
    }

    @Test
    @DisplayName("createSession with extension returns the correct subtype")
    void createSessionWithExtensionReturnsSubtype() {
        SessionExtension<CustomSession> extension = new CustomSessionExtension("test-label");

        CustomSession custom = cluster.createSession(Behavior.DEFAULT, extension);

        assertInstanceOf(CustomSession.class, custom);
        assertEquals("test-label", custom.getLabel());
        assertSame(Behavior.DEFAULT, custom.getBehavior());
        assertSame(cluster, custom.getCluster());
    }

    @Test
    @DisplayName("createSession with extension passes cluster and behavior to extension")
    void createSessionPassesArguments() {
        Behavior derived = Behavior.DEFAULT.deriveWithChanges(
            "extensionTest",
            b -> b.on(Behavior.Selectors.all(), ops -> ops.sendKey(true))
        );
        SessionExtension<CustomSession> extension = new CustomSessionExtension("arg-check");

        CustomSession custom = cluster.createSession(derived, extension);

        assertSame(cluster, custom.getCluster());
        assertSame(derived, custom.getBehavior());
    }

    @Test
    @DisplayName("sessionFor returns a new Session with the specified behavior")
    void sessionForReturnsNewSession() {
        Behavior derived = Behavior.DEFAULT.deriveWithChanges(
            "sessionForTest",
            b -> b.on(Behavior.Selectors.all(), ops -> ops.sendKey(true))
        );

        Session original = cluster.createSession(Behavior.DEFAULT);
        Session newSession = original.sessionFor(derived);

        assertNotSame(original, newSession);
        assertSame(derived, newSession.getBehavior());
        assertSame(cluster, newSession.getCluster());
    }

    @Test
    @DisplayName("sessionFor preserves the cluster reference")
    void sessionForPreservesCluster() {
        Session original = cluster.createSession(Behavior.DEFAULT);
        Session other = original.sessionFor(Behavior.DEFAULT);

        assertNotSame(original, other);
        assertSame(original.getCluster(), other.getCluster());
    }

    @Test
    @DisplayName("multiple extension types coexist on the same cluster")
    void multipleExtensionsCoexist() {
        CustomSession custom = cluster.createSession(Behavior.DEFAULT, new CustomSessionExtension("one"));
        AnotherSession another = cluster.createSession(Behavior.DEFAULT, new AnotherSessionExtension());

        assertInstanceOf(CustomSession.class, custom);
        assertInstanceOf(AnotherSession.class, another);
        assertSame(cluster, custom.getCluster());
        assertSame(cluster, another.getCluster());
    }

    @Test
    @DisplayName("extended session is a fully functional Session")
    void extendedSessionCanOperate() {
        CustomSession custom = cluster.createSession(Behavior.DEFAULT, new CustomSessionExtension("functional"));

        assertNotNull(custom.info());
        assertEquals("functional", custom.getLabel());
    }
}
