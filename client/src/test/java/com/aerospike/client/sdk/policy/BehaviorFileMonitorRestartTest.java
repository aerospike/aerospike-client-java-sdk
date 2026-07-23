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
package com.aerospike.client.sdk.policy;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for {@link BehaviorFileMonitor} lifecycle.
 *
 * <p>The monitor is a process-wide singleton backed by a scheduled executor. A
 * prior bug shut that executor down on {@code close()}/{@code shutdown()} and
 * never recreated it, so the second {@code startMonitoring} in a JVM failed with
 * {@code RejectedExecutionException}. Running examples in separate JVMs hid this;
 * running them in one process (or any real app that stops and restarts
 * monitoring) surfaced it. These tests lock in that monitoring is restartable.
 */
@DisplayName("BehaviorFileMonitor Restart Tests")
public class BehaviorFileMonitorRestartTest {

    private static final String YAML = """
        behaviors:
          simple:
            allOperations:
              abandonCallAfter: 5s
              maximumNumberOfCallAttempts: 3
        """;

    private Path writeConfig(Path dir) throws IOException {
        Path file = dir.resolve("behavior-monitor-test.yml");
        Files.writeString(file, YAML);
        return file;
    }

    @AfterEach
    void tearDown() {
        // Reset shared singleton state so tests do not leak into one another.
        Behavior.shutdownMonitor();
    }

    @Test
    @DisplayName("Should restart monitoring after a try-with-resources close in the same process")
    void shouldRestartAfterClose(@TempDir Path tempDir) throws IOException {
        Path config = writeConfig(tempDir);

        try (Closeable monitor = Behavior.startMonitoringWithResource(config.toString())) {
            assertNotNull(monitor);
            assertTrue(Behavior.isMonitoring(), "monitoring should be active during first session");
        }
        assertFalse(Behavior.isMonitoring(), "monitoring should stop after close");

        // Before the fix, this second start reused the terminated singleton
        // executor and threw RejectedExecutionException.
        assertDoesNotThrow(() -> {
            try (Closeable monitor = Behavior.startMonitoringWithResource(config.toString())) {
                assertNotNull(monitor);
                assertTrue(Behavior.isMonitoring(), "monitoring should be active during second session");
            }
        });
    }

    @Test
    @DisplayName("Should restart monitoring after an explicit shutdown")
    void shouldRestartAfterExplicitShutdown(@TempDir Path tempDir) throws IOException {
        Path config = writeConfig(tempDir);

        Behavior.startMonitoring(config.toString());
        assertTrue(Behavior.isMonitoring());
        Behavior.shutdownMonitor();
        assertFalse(Behavior.isMonitoring());

        assertDoesNotThrow(() -> Behavior.startMonitoring(config.toString()));
        assertTrue(Behavior.isMonitoring());
    }

    @Test
    @DisplayName("Should support several sequential monitoring sessions")
    void shouldSupportSeveralSequentialSessions(@TempDir Path tempDir) throws IOException {
        Path config = writeConfig(tempDir);

        assertDoesNotThrow(() -> {
            for (int i = 0; i < 3; i++) {
                try (Closeable monitor = Behavior.startMonitoringWithResource(config.toString())) {
                    assertNotNull(monitor);
                    assertTrue(Behavior.isMonitoring(), "monitoring should be active in session " + i);
                }
            }
        });
    }
}
