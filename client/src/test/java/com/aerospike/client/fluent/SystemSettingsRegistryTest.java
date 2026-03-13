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
package com.aerospike.client.fluent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SystemSettingsRegistry}, especially that code-provided
 * settings (e.g. from {@link ClusterDefinition#withSystemSettings}) are
 * honored when no YAML defaults have been loaded.
 */
public class SystemSettingsRegistryTest {

    @Test
    public void codeProvidedTransactionSettingsHonoredWhenNoYamlLoaded() {
        SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();

        SystemSettings codeProvided = SystemSettings.builder()
            .transactions(ops -> ops
                .numberOfAttempts(1)
                .sleepBetweenAttempts(Duration.ZERO))
            .build();

        SystemSettings effective = registry.getEffectiveSettings("test-cluster", codeProvided);

        assertEquals(
            Integer.valueOf(1),
            effective.getNumberOfAttempts(),
            "Code-provided numberOfAttempts(1) should be honored when no YAML default is loaded; "
                + "currently the registry overwrites it with DEFAULT (5) in Layer 3");
        assertEquals(
            Duration.ZERO,
            effective.getSleepBetweenAttempts(),
            "Code-provided sleepBetweenAttempts(ZERO) should be honored when no YAML default is loaded");
    }

    @Test
    public void yamlDefaultOverridesCodeProvidedWhenExplicitlySet() {
        SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();

        // Simulate YAML default loaded: explicit default with numberOfAttempts(3)
        SystemSettings yamlDefault = SystemSettings.builder()
            .transactions(ops -> ops
                .numberOfAttempts(3)
                .sleepBetweenAttempts(Duration.ofMillis(500)))
            .build();
        registry.updateDefaultSettings(yamlDefault);
        try {
            // Code-provided would say 1, but Layer 3 (YAML default) should win
            SystemSettings codeProvided = SystemSettings.builder()
                .transactions(ops -> ops
                    .numberOfAttempts(1)
                    .sleepBetweenAttempts(Duration.ZERO))
                .build();

            SystemSettings effective = registry.getEffectiveSettings("test-cluster-2", codeProvided);

            assertEquals(
                Integer.valueOf(3),
                effective.getNumberOfAttempts(),
                "When registry default is explicitly set (e.g. YAML), Layer 3 should apply and override code-provided");
            assertEquals(
                Duration.ofMillis(500),
                effective.getSleepBetweenAttempts(),
                "When registry default is explicitly set, its transaction settings should win");
        } finally {
            // Restore so other tests see default behavior
            registry.updateDefaultSettings(SystemSettings.DEFAULT);
        }
    }
}
