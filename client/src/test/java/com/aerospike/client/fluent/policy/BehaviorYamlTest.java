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
package com.aerospike.client.fluent.policy;

import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.SystemSettings;
import com.aerospike.client.fluent.SystemSettingsRegistry;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.DisplayName;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Behavior YAML Serialization Tests")
public class BehaviorYamlTest {

    /**
     * Helper method to load behaviors from a text block string to map
     */
    private Map<String, Behavior> loadFromYamlString(String yaml) throws IOException {
        return BehaviorYamlLoader.loadBehaviorsFromString(yaml);
    }

    @Nested
    @DisplayName("Basic Configuration Tests")
    class BasicConfigurationTests {

        @Test
        @DisplayName("Should load simple behavior with basic settings")
        void testSimpleBehavior() throws IOException {
            String yaml = """
                behaviors:
                  simple:
                    allOperations:
                      abandonCallAfter: 5s
                      maximumNumberOfCallAttempts: 3
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);

            assertNotNull(behaviors);
            assertEquals(1, behaviors.size());
            assertTrue(behaviors.containsKey("simple"));

            Behavior simple = behaviors.get("simple");
            Settings settings = simple.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            assertNotNull(settings);
            assertEquals(Duration.ofSeconds(5), settings.abandonCallAfter);
            assertEquals(3, settings.maximumNumberOfCallAttempts);
        }

        @Test
        @DisplayName("Should load behavior with multiple operation types")
        void testMultipleOperationTypes() throws IOException {
            String yaml = """
                behaviors:
                  multi-op:
                    consistencyModeReads:
                      abandonCallAfter: 2s
                      readConsistency: SESSION
                    retryableWrites:
                      abandonCallAfter: 10s
                      useDurableDelete: true
                      maximumNumberOfCallAttempts: 5
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior multiOp = behaviors.get("multi-op");

            // Test CP read settings
            Settings readCp = multiOp.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.CP);
            assertNotNull(readCp);
            assertEquals(Duration.ofSeconds(2), readCp.abandonCallAfter);
            assertEquals(ReadModeSC.SESSION, readCp.readModeSC);

            // Test retryable write settings
            Settings writeRetry = multiOp.getSettings(Behavior.OpKind.WRITE_RETRYABLE, Behavior.OpShape.POINT, Behavior.Mode.AP);
            assertNotNull(writeRetry);
            assertEquals(Duration.ofSeconds(10), writeRetry.abandonCallAfter);
            assertTrue(writeRetry.useDurableDelete);
            assertEquals(5, writeRetry.maximumNumberOfCallAttempts);
        }
    }

    @Nested
    @DisplayName("Duration Parsing Tests")
    class DurationParsingTests {

        @Test
        @DisplayName("Should parse durations in seconds")
        void testSecondsFormat() throws IOException {
            String yaml = """
                behaviors:
                  seconds-test:
                    allOperations:
                      abandonCallAfter: 30s
                      delayBetweenRetries: 1s
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("seconds-test");
            Settings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            assertEquals(Duration.ofSeconds(30), settings.abandonCallAfter);
            assertEquals(Duration.ofSeconds(1), settings.delayBetweenRetries);
        }

        @Test
        @DisplayName("Should parse durations in milliseconds")
        void testMillisecondsFormat() throws IOException {
            String yaml = """
                behaviors:
                  millis-test:
                    allOperations:
                      abandonCallAfter: 500ms
                      delayBetweenRetries: 100ms
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("millis-test");
            Settings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            assertEquals(Duration.ofMillis(500), settings.abandonCallAfter);
            assertEquals(Duration.ofMillis(100), settings.delayBetweenRetries);
        }

        @Test
        @DisplayName("Should parse durations in minutes and hours")
        void testMinutesAndHours() throws IOException {
            String yaml = """
                behaviors:
                  time-test:
                    allOperations:
                      abandonCallAfter: 2m
                      waitForCallToComplete: 1h
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("time-test");
            Settings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            assertEquals(Duration.ofMinutes(2), settings.abandonCallAfter);
            assertEquals(Duration.ofHours(1), settings.waitForCallToComplete);
        }
    }

    @Nested
    @DisplayName("Unit Conversion Tests")
    class UnitConversionTests {

        @Test
        @DisplayName("Should parse nanoseconds with various formats")
        void testNanosecondsFormats() throws IOException {
            String yaml = """
                behaviors:
                  nanos-test:
                    allOperations:
                      abandonCallAfter: 1000ns
                      delayBetweenRetries: 500nanos
                      waitForCallToComplete: 2000nanosecond
                      waitForConnectionToComplete: 3000nanoseconds
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("nanos-test");
            Settings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            assertEquals(Duration.ofNanos(1000), settings.abandonCallAfter);
            assertEquals(Duration.ofNanos(500), settings.delayBetweenRetries);
            assertEquals(Duration.ofNanos(2000), settings.waitForCallToComplete);
            assertEquals(Duration.ofNanos(3000), settings.waitForConnectionToComplete);
        }

        @Test
        @DisplayName("Should parse microseconds with various formats")
        void testMicrosecondsFormats() throws IOException {
            String yaml = """
                behaviors:
                  micros-test:
                    allOperations:
                      abandonCallAfter: 1000us
                      delayBetweenRetries: 500micros
                      waitForCallToComplete: 2000microsecond
                      waitForConnectionToComplete: 3000microseconds
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("micros-test");
            Settings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            // 1 microsecond = 1000 nanoseconds
            assertEquals(Duration.ofNanos(1000 * 1000), settings.abandonCallAfter);
            assertEquals(Duration.ofNanos(500 * 1000), settings.delayBetweenRetries);
            assertEquals(Duration.ofNanos(2000 * 1000), settings.waitForCallToComplete);
            assertEquals(Duration.ofNanos(3000 * 1000), settings.waitForConnectionToComplete);
        }

        @Test
        @DisplayName("Should parse milliseconds with various formats")
        void testMillisecondsFormats() throws IOException {
            String yaml = """
                behaviors:
                  millis-test:
                    allOperations:
                      abandonCallAfter: 100ms
                      delayBetweenRetries: 50millis
                      waitForCallToComplete: 200millisecond
                      waitForConnectionToComplete: 300milliseconds
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("millis-test");
            Settings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            assertEquals(Duration.ofMillis(100), settings.abandonCallAfter);
            assertEquals(Duration.ofMillis(50), settings.delayBetweenRetries);
            assertEquals(Duration.ofMillis(200), settings.waitForCallToComplete);
            assertEquals(Duration.ofMillis(300), settings.waitForConnectionToComplete);
        }

        @Test
        @DisplayName("Should parse seconds with various formats")
        void testSecondsFormats() throws IOException {
            String yaml = """
                behaviors:
                  seconds-test:
                    allOperations:
                      abandonCallAfter: 10s
                      delayBetweenRetries: 5sec
                      waitForCallToComplete: 20second
                      waitForConnectionToComplete: 30seconds
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("seconds-test");
            Settings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            assertEquals(Duration.ofSeconds(10), settings.abandonCallAfter);
            assertEquals(Duration.ofSeconds(5), settings.delayBetweenRetries);
            assertEquals(Duration.ofSeconds(20), settings.waitForCallToComplete);
            assertEquals(Duration.ofSeconds(30), settings.waitForConnectionToComplete);
        }

        @Test
        @DisplayName("Should parse minutes with various formats")
        void testMinutesFormats() throws IOException {
            String yaml = """
                behaviors:
                  minutes-test:
                    allOperations:
                      abandonCallAfter: 1m
                      delayBetweenRetries: 2min
                      waitForCallToComplete: 3minute
                      waitForConnectionToComplete: 5minutes
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("minutes-test");
            Settings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            assertEquals(Duration.ofMinutes(1), settings.abandonCallAfter);
            assertEquals(Duration.ofMinutes(2), settings.delayBetweenRetries);
            assertEquals(Duration.ofMinutes(3), settings.waitForCallToComplete);
            assertEquals(Duration.ofMinutes(5), settings.waitForConnectionToComplete);
        }

        @Test
        @DisplayName("Should parse hours with various formats")
        void testHoursFormats() throws IOException {
            String yaml = """
                behaviors:
                  hours-test:
                    allOperations:
                      abandonCallAfter: 1h
                      delayBetweenRetries: 2hr
                      waitForCallToComplete: 3hour
                      waitForConnectionToComplete: 4hours
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("hours-test");
            Settings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            assertEquals(Duration.ofHours(1), settings.abandonCallAfter);
            assertEquals(Duration.ofHours(2), settings.delayBetweenRetries);
            assertEquals(Duration.ofHours(3), settings.waitForCallToComplete);
            assertEquals(Duration.ofHours(4), settings.waitForConnectionToComplete);
        }

        @Test
        @DisplayName("Should parse days with various formats")
        void testDaysFormats() throws IOException {
            String yaml = """
                behaviors:
                  days-test:
                    allOperations:
                      abandonCallAfter: 1d
                      delayBetweenRetries: 2day
                      waitForCallToComplete: 3days
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("days-test");
            Settings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            assertEquals(Duration.ofDays(1), settings.abandonCallAfter);
            assertEquals(Duration.ofDays(2), settings.delayBetweenRetries);
            assertEquals(Duration.ofDays(3), settings.waitForCallToComplete);
        }

        @Test
        @DisplayName("Should correctly convert between units - milliseconds to seconds")
        void testMillisecondsToSecondsConversion() throws IOException {
            String yaml = """
                behaviors:
                  conversion-test:
                    allOperations:
                      abandonCallAfter: 5000ms
                      delayBetweenRetries: 5s
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("conversion-test");
            Settings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            // 5000ms should equal 5s
            assertEquals(settings.abandonCallAfter, settings.delayBetweenRetries);
            assertEquals(Duration.ofSeconds(5), settings.abandonCallAfter);
        }

        @Test
        @DisplayName("Should correctly convert between units - seconds to minutes")
        void testSecondsToMinutesConversion() throws IOException {
            String yaml = """
                behaviors:
                  conversion-test:
                    allOperations:
                      abandonCallAfter: 120s
                      delayBetweenRetries: 2m
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("conversion-test");
            Settings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            // 120s should equal 2m
            assertEquals(settings.abandonCallAfter, settings.delayBetweenRetries);
            assertEquals(Duration.ofMinutes(2), settings.abandonCallAfter);
        }

        @Test
        @DisplayName("Should correctly convert between units - minutes to hours")
        void testMinutesToHoursConversion() throws IOException {
            String yaml = """
                behaviors:
                  conversion-test:
                    allOperations:
                      abandonCallAfter: 60m
                      delayBetweenRetries: 1h
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("conversion-test");
            Settings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            // 60m should equal 1h
            assertEquals(settings.abandonCallAfter, settings.delayBetweenRetries);
            assertEquals(Duration.ofHours(1), settings.abandonCallAfter);
        }

        @Test
        @DisplayName("Should correctly convert between units - hours to days")
        void testHoursToDaysConversion() throws IOException {
            String yaml = """
                behaviors:
                  conversion-test:
                    allOperations:
                      abandonCallAfter: 24h
                      delayBetweenRetries: 1d
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("conversion-test");
            Settings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            // 24h should equal 1d
            assertEquals(settings.abandonCallAfter, settings.delayBetweenRetries);
            assertEquals(Duration.ofDays(1), settings.abandonCallAfter);
        }

        @Test
        @DisplayName("Should handle microseconds to milliseconds conversion")
        void testMicrosecondsToMillisecondsConversion() throws IOException {
            String yaml = """
                behaviors:
                  conversion-test:
                    allOperations:
                      abandonCallAfter: 5000us
                      delayBetweenRetries: 5ms
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("conversion-test");
            Settings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            // 5000us should equal 5ms
            assertEquals(settings.abandonCallAfter, settings.delayBetweenRetries);
            assertEquals(Duration.ofMillis(5), settings.abandonCallAfter);
        }

        @Test
        @DisplayName("Should handle large values correctly")
        void testLargeValues() throws IOException {
            String yaml = """
                behaviors:
                  large-values-test:
                    allOperations:
                      abandonCallAfter: 999999ms
                      delayBetweenRetries: 86400s
                      waitForCallToComplete: 1440m
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("large-values-test");
            Settings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            assertEquals(Duration.ofMillis(999999), settings.abandonCallAfter);
            assertEquals(Duration.ofSeconds(86400), settings.delayBetweenRetries); // 1 day in seconds
            assertEquals(Duration.ofMinutes(1440), settings.waitForCallToComplete); // 1 day in minutes
        }

        @Test
        @DisplayName("Should handle mixed unit configurations")
        void testMixedUnits() throws IOException {
            String yaml = """
                behaviors:
                  mixed-units:
                    allOperations:
                      abandonCallAfter: 500ms
                    consistencyModeReads:
                      abandonCallAfter: 2s
                    retryableWrites:
                      abandonCallAfter: 1m
                    batchReads:
                      abandonCallAfter: 5m
                    query:
                      abandonCallAfter: 1h
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("mixed-units");

            Settings allOps = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);
            Settings readCp = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.CP);
            Settings writes = behavior.getSettings(Behavior.OpKind.WRITE_RETRYABLE, Behavior.OpShape.POINT, Behavior.Mode.AP);
            Settings batch = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.BATCH, Behavior.Mode.AP);
            Settings query = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.QUERY, Behavior.Mode.AP);

            assertEquals(Duration.ofMillis(500), allOps.abandonCallAfter);
            assertEquals(Duration.ofSeconds(2), readCp.abandonCallAfter);
            assertEquals(Duration.ofMinutes(1), writes.abandonCallAfter);
            assertEquals(Duration.ofMinutes(5), batch.abandonCallAfter);
            assertEquals(Duration.ofHours(1), query.abandonCallAfter);
        }

        @Test
        @DisplayName("Should parse duration with whitespace between number and unit")
        void testDurationWithWhitespace() throws IOException {
            String yaml = """
                behaviors:
                  whitespace-test:
                    allOperations:
                      abandonCallAfter: 10 s
                      delayBetweenRetries: 500 ms
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior behavior = behaviors.get("whitespace-test");
            Settings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            assertEquals(Duration.ofSeconds(10), settings.abandonCallAfter);
            assertEquals(Duration.ofMillis(500), settings.delayBetweenRetries);
        }
    }

    @Nested
    @DisplayName("Inheritance Tests")
    class InheritanceTests {

        @Test
        @DisplayName("Should inherit settings from parent behavior")
        void testParentChildInheritance() throws IOException {
            String yaml = """
                behaviors:
                  parent-config:
                    allOperations:
                      abandonCallAfter: 10s
                      maximumNumberOfCallAttempts: 5
                      delayBetweenRetries: 500ms

                  child-config:
                    parent: parent-config
                    allOperations:
                      abandonCallAfter: 20s
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);

            // Register parent first
            BehaviorRegistry registry = BehaviorRegistry.getInstance();
            registry.registerBehavior(behaviors.get("parent-config"));

            // Load again to resolve parent reference
            behaviors = loadFromYamlString(yaml);

            Behavior child = behaviors.get("child-config");
            Settings settings = child.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            // Child overrides abandonCallAfter
            assertEquals(Duration.ofSeconds(20), settings.abandonCallAfter);

            // Child inherits other settings from parent
            assertEquals(5, settings.maximumNumberOfCallAttempts);
            assertEquals(Duration.ofMillis(500), settings.delayBetweenRetries);
        }

        @Test
        @DisplayName("Should handle default parent when not specified")
        void testDefaultParent() throws IOException {
            String yaml = """
                behaviors:
                  standalone:
                    allOperations:
                      abandonCallAfter: 15s
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior standalone = behaviors.get("standalone");

            assertNotNull(standalone);
            assertNotNull(standalone.getParent());
            assertEquals(Behavior.DEFAULT.name(), standalone.getParent().name());
        }
    }

    @Nested
    @DisplayName("Batch Configuration Tests")
    class BatchConfigurationTests {

        @Test
        @DisplayName("Should load batch read configuration")
        void testBatchReadConfig() throws IOException {
            String yaml = """
                behaviors:
                  batch-config:
                    batchReads:
                      abandonCallAfter: 30s
                      maxConcurrentServers: 10
                      allowInlineMemoryAccess: true
                      allowInlineSsdAccess: false
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior batchConfig = behaviors.get("batch-config");

            Settings batchSettings = batchConfig.getSettings(Behavior.OpKind.READ, Behavior.OpShape.BATCH, Behavior.Mode.AP);

            assertNotNull(batchSettings);
            assertEquals(Duration.ofSeconds(30), batchSettings.abandonCallAfter);
            assertEquals(10, batchSettings.maxConcurrentNodes);
            assertTrue(batchSettings.allowInlineMemoryAccess);
            assertFalse(batchSettings.allowInlineSsdAccess);
        }

        @Test
        @DisplayName("Should load batch write configuration")
        void testBatchWriteConfig() throws IOException {
            String yaml = """
                behaviors:
                  batch-writes:
                    batchWrites:
                      abandonCallAfter: 45s
                      maxConcurrentServers: 8
                      allowInlineMemoryAccess: false
                      allowInlineSsdAccess: true
                      maximumNumberOfCallAttempts: 4
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior batchWrites = behaviors.get("batch-writes");

            Settings batchSettings = batchWrites.getSettings(Behavior.OpKind.WRITE_RETRYABLE, Behavior.OpShape.BATCH, Behavior.Mode.AP);

            assertNotNull(batchSettings);
            assertEquals(Duration.ofSeconds(45), batchSettings.abandonCallAfter);
            assertEquals(8, batchSettings.maxConcurrentNodes);
            assertFalse(batchSettings.allowInlineMemoryAccess);
            assertTrue(batchSettings.allowInlineSsdAccess);
            assertEquals(4, batchSettings.maximumNumberOfCallAttempts);
        }
    }

    @Nested
    @DisplayName("Query Configuration Tests")
    class QueryConfigurationTests {

        @Test
        @DisplayName("Should load query configuration")
        void testQueryConfig() throws IOException {
            String yaml = """
                behaviors:
                  query-config:
                    query:
                      abandonCallAfter: 60s
                      recordQueueSize: 5000
                      maxConcurrentServers: 12
                      maximumNumberOfCallAttempts: 2
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior queryConfig = behaviors.get("query-config");

            Settings querySettings = queryConfig.getSettings(Behavior.OpKind.READ, Behavior.OpShape.QUERY, Behavior.Mode.AP);

            assertNotNull(querySettings);
            assertEquals(Duration.ofSeconds(60), querySettings.abandonCallAfter);
            assertEquals(5000, querySettings.recordQueueSize);
            assertEquals(2, querySettings.maximumNumberOfCallAttempts);
        }
    }

    @Nested
    @DisplayName("Write Configuration Tests")
    class WriteConfigurationTests {

        @Test
        @DisplayName("Should load retryable write configuration")
        void testRetryableWriteConfig() throws IOException {
            String yaml = """
                behaviors:
                  retry-writes:
                    retryableWrites:
                      abandonCallAfter: 8s
                      useDurableDelete: true
                      maximumNumberOfCallAttempts: 6
                      delayBetweenRetries: 200ms
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior retryWrites = behaviors.get("retry-writes");

            Settings writeSettings = retryWrites.getSettings(Behavior.OpKind.WRITE_RETRYABLE, Behavior.OpShape.POINT, Behavior.Mode.AP);

            assertNotNull(writeSettings);
            assertEquals(Duration.ofSeconds(8), writeSettings.abandonCallAfter);
            assertTrue(writeSettings.useDurableDelete);
            assertEquals(6, writeSettings.maximumNumberOfCallAttempts);
            assertEquals(Duration.ofMillis(200), writeSettings.delayBetweenRetries);
        }

        @Test
        @DisplayName("Should load non-retryable write configuration")
        void testNonRetryableWriteConfig() throws IOException {
            String yaml = """
                behaviors:
                  no-retry-writes:
                    nonRetryableWrites:
                      abandonCallAfter: 3s
                      useDurableDelete: false
                      maximumNumberOfCallAttempts: 1
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior noRetryWrites = behaviors.get("no-retry-writes");

            Settings writeSettings = noRetryWrites.getSettings(Behavior.OpKind.WRITE_NON_RETRYABLE, Behavior.OpShape.POINT, Behavior.Mode.AP);

            assertNotNull(writeSettings);
            assertEquals(Duration.ofSeconds(3), writeSettings.abandonCallAfter);
            assertFalse(writeSettings.useDurableDelete);
            assertEquals(1, writeSettings.maximumNumberOfCallAttempts);
        }
    }

    @Nested
    @DisplayName("System Configuration Tests")
    class SystemConfigurationTests {

        @Test
        @DisplayName("Should load system transaction verify configuration")
        void testSystemTxnVerifyConfig() throws IOException {
            String yaml = """
                behaviors:
                  sys-txn:
                    systemTxnVerify:
                      abandonCallAfter: 1s
                      consistency: LINEARIZE
                      maximumNumberOfCallAttempts: 2
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior sysTxn = behaviors.get("sys-txn");

            Settings txnSettings = sysTxn.getSettings(Behavior.OpKind.SYSTEM_TXN_VERIFY, Behavior.OpShape.POINT, Behavior.Mode.CP);

            assertNotNull(txnSettings);
            assertEquals(Duration.ofSeconds(1), txnSettings.abandonCallAfter);
            assertEquals(ReadModeSC.LINEARIZE, txnSettings.readModeSC);
            assertEquals(2, txnSettings.maximumNumberOfCallAttempts);
        }

        // Note: System settings (connections, circuit breaker, refresh) are now loaded
        // via SystemSettings/SystemSettingsRegistry, not as part of behaviors.
        // See the system: section in YAML configuration.
    }

    @Nested
    @DisplayName("System Settings Tests")
    class SystemSettingsTests {

        @Test
        @DisplayName("Should load default system settings")
        void testDefaultSystemSettings() throws IOException {
            String yaml = """
                system:
                  DEFAULT:
                    connections:
                      minimumConnectionsPerNode: 10
                      maximumConnectionsPerNode: 100
                    circuitBreaker:
                      numTendIntervalsInErrorWindow: 2
                      maximumErrorsInErrorWindow: 50
                    refresh:
                      tendInterval: 1s
                """;

            loadFromYamlString(yaml);

            SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();
            SystemSettings defaults = registry.getDefaultSettings();

            assertNotNull(defaults);
            assertEquals(10, defaults.getMinimumConnectionsPerNode());
            assertEquals(100, defaults.getMaximumConnectionsPerNode());
            assertEquals(2, defaults.getNumTendIntervalsInErrorWindow());
            assertEquals(50, defaults.getMaximumErrorsInErrorWindow());
            assertEquals(Duration.ofSeconds(1), defaults.getTendInterval());
        }

        @Test
        @DisplayName("Should load cluster-specific system settings")
        void testClusterSpecificSystemSettings() throws IOException {
            String yaml = """
                system:
                  production:
                    connections:
                      minimumConnectionsPerNode: 50
                      maximumConnectionsPerNode: 300
                    circuitBreaker:
                      numTendIntervalsInErrorWindow: 3
                      maximumErrorsInErrorWindow: 100
                    refresh:
                      tendInterval: 500ms
                """;

            loadFromYamlString(yaml);

            SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();
            SystemSettings production = registry.getClusterSettings("production");

            assertNotNull(production);
            assertEquals(50, production.getMinimumConnectionsPerNode());
            assertEquals(300, production.getMaximumConnectionsPerNode());
            assertEquals(3, production.getNumTendIntervalsInErrorWindow());
            assertEquals(100, production.getMaximumErrorsInErrorWindow());
            assertEquals(Duration.ofMillis(500), production.getTendInterval());
        }

        @Test
        @DisplayName("Should load multiple cluster settings")
        void testMultipleClusterSettings() throws IOException {
            String yaml = """
                system:
                  DEFAULT:
                    connections:
                      minimumConnectionsPerNode: 5
                      maximumConnectionsPerNode: 50
                  production:
                    connections:
                      minimumConnectionsPerNode: 100
                      maximumConnectionsPerNode: 500
                  development:
                    connections:
                      minimumConnectionsPerNode: 1
                      maximumConnectionsPerNode: 10
                """;

            loadFromYamlString(yaml);

            SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();

            SystemSettings defaults = registry.getDefaultSettings();
            assertEquals(5, defaults.getMinimumConnectionsPerNode());
            assertEquals(50, defaults.getMaximumConnectionsPerNode());

            SystemSettings production = registry.getClusterSettings("production");
            assertEquals(100, production.getMinimumConnectionsPerNode());
            assertEquals(500, production.getMaximumConnectionsPerNode());

            SystemSettings development = registry.getClusterSettings("development");
            assertEquals(1, development.getMinimumConnectionsPerNode());
            assertEquals(10, development.getMaximumConnectionsPerNode());
        }

        @Test
        @DisplayName("Should load only connection settings")
        void testConnectionSettingsOnly() throws IOException {
            String yaml = """
                system:
                  DEFAULT:
                    connections:
                      minimumConnectionsPerNode: 20
                      maximumConnectionsPerNode: 200
                      maximumSocketIdleTime: 55s
                """;

            loadFromYamlString(yaml);

            SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();
            SystemSettings defaults = registry.getDefaultSettings();

            assertNotNull(defaults);
            assertEquals(20, defaults.getMinimumConnectionsPerNode());
            assertEquals(200, defaults.getMaximumConnectionsPerNode());
            assertEquals(Duration.ofSeconds(55), defaults.getMaximumSocketIdleTime());
        }

        @Test
        @DisplayName("Should load only circuit breaker settings")
        void testCircuitBreakerSettingsOnly() throws IOException {
            String yaml = """
                system:
                  DEFAULT:
                    circuitBreaker:
                      numTendIntervalsInErrorWindow: 5
                      maximumErrorsInErrorWindow: 200
                """;

            loadFromYamlString(yaml);

            SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();
            SystemSettings defaults = registry.getDefaultSettings();

            assertNotNull(defaults);
            assertEquals(5, defaults.getNumTendIntervalsInErrorWindow());
            assertEquals(200, defaults.getMaximumErrorsInErrorWindow());
        }

        @Test
        @DisplayName("Should load only refresh settings")
        void testRefreshSettingsOnly() throws IOException {
            String yaml = """
                system:
                  DEFAULT:
                    refresh:
                      tendInterval: 2s
                """;

            loadFromYamlString(yaml);

            SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();
            SystemSettings defaults = registry.getDefaultSettings();

            assertNotNull(defaults);
            assertEquals(Duration.ofSeconds(2), defaults.getTendInterval());
        }

        @Test
        @DisplayName("Should parse duration formats in system settings")
        void testDurationFormatsInSystemSettings() throws IOException {
            String yaml = """
                system:
                  DEFAULT:
                    connections:
                      maximumSocketIdleTime: 30s
                    refresh:
                      tendInterval: 500ms
                  fast-refresh:
                    refresh:
                      tendInterval: 100ms
                  slow-refresh:
                    refresh:
                      tendInterval: 5s
                """;

            loadFromYamlString(yaml);

            SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();

            SystemSettings defaults = registry.getDefaultSettings();
            assertEquals(Duration.ofSeconds(30), defaults.getMaximumSocketIdleTime());
            assertEquals(Duration.ofMillis(500), defaults.getTendInterval());

            SystemSettings fastRefresh = registry.getClusterSettings("fast-refresh");
            assertEquals(Duration.ofMillis(100), fastRefresh.getTendInterval());

            SystemSettings slowRefresh = registry.getClusterSettings("slow-refresh");
            assertEquals(Duration.ofSeconds(5), slowRefresh.getTendInterval());
        }

        @Test
        @DisplayName("Should load both behaviors and system settings together")
        void testBehaviorsAndSystemSettingsTogether() throws IOException {
            String yaml = """
                behaviors:
                  production:
                    allOperations:
                      abandonCallAfter: 10s
                      maximumNumberOfCallAttempts: 5

                system:
                  DEFAULT:
                    connections:
                      minimumConnectionsPerNode: 25
                      maximumConnectionsPerNode: 250
                    circuitBreaker:
                      numTendIntervalsInErrorWindow: 3
                  production:
                    connections:
                      minimumConnectionsPerNode: 50
                      maximumConnectionsPerNode: 500
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);

            // Verify behaviors loaded
            assertNotNull(behaviors);
            assertTrue(behaviors.containsKey("production"));
            Behavior production = behaviors.get("production");
            Settings settings = production.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);
            assertEquals(Duration.ofSeconds(10), settings.abandonCallAfter);
            assertEquals(5, settings.maximumNumberOfCallAttempts);

            // Verify system settings loaded
            SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();

            SystemSettings defaults = registry.getDefaultSettings();
            assertEquals(25, defaults.getMinimumConnectionsPerNode());
            assertEquals(250, defaults.getMaximumConnectionsPerNode());
            assertEquals(3, defaults.getNumTendIntervalsInErrorWindow());

            SystemSettings productionSystem = registry.getClusterSettings("production");
            assertEquals(50, productionSystem.getMinimumConnectionsPerNode());
            assertEquals(500, productionSystem.getMaximumConnectionsPerNode());
        }

        @Test
        @DisplayName("Should handle empty system settings map")
        void testEmptySystemSettingsMap() throws IOException {
            String yaml = """
                system: {}
                """;

            // Should not throw exception
            loadFromYamlString(yaml);
        }

        @Test
        @DisplayName("Should handle missing system section")
        void testMissingSystemSection() throws IOException {
            String yaml = """
                behaviors:
                  simple:
                    allOperations:
                      abandonCallAfter: 5s
                """;

            // Should not throw exception when system section is missing
            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            assertNotNull(behaviors);
            assertTrue(behaviors.containsKey("simple"));
        }
    }

    @Nested
    @DisplayName("Complex Scenario Tests")
    class ComplexScenarioTests {

        @Test
        @DisplayName("Should load multiple behaviors with hierarchy")
        void testMultipleBehaviorsWithHierarchy() throws IOException {
            String yaml = """
                behaviors:
                  base-performance:
                    allOperations:
                      abandonCallAfter: 5s
                      maximumNumberOfCallAttempts: 3
                      delayBetweenRetries: 100ms

                  high-throughput:
                    parent: base-performance
                    batchReads:
                      maxConcurrentServers: 16
                      allowInlineMemoryAccess: true
                      allowInlineSsdAccess: true
                    query:
                      recordQueueSize: 10000
                      maxConcurrentServers: 16

                  low-latency:
                    parent: base-performance
                    allOperations:
                      abandonCallAfter: 1s
                    consistencyModeReads:
                      readConsistency: LINEARIZE
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);

            assertEquals(3, behaviors.size());
            assertTrue(behaviors.containsKey("base-performance"));
            assertTrue(behaviors.containsKey("high-throughput"));
            assertTrue(behaviors.containsKey("low-latency"));

            // Test high-throughput batch configuration
            Behavior highThroughput = behaviors.get("high-throughput");
            Settings batchSettings = highThroughput.getSettings(Behavior.OpKind.READ, Behavior.OpShape.BATCH, Behavior.Mode.AP);
            assertEquals(16, batchSettings.maxConcurrentNodes);
            assertTrue(batchSettings.allowInlineMemoryAccess);

            // Test low-latency overrides
            Behavior lowLatency = behaviors.get("low-latency");
            Settings lowLatSettings = lowLatency.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.CP);
            assertEquals(Duration.ofSeconds(1), lowLatSettings.abandonCallAfter);
            assertEquals(ReadModeSC.LINEARIZE, lowLatSettings.readModeSC);
        }

        @Test
        @DisplayName("Should handle comprehensive configuration")
        void testComprehensiveConfiguration() throws IOException {
            String yaml = """
                behaviors:
                  production-config:
                    sendKey: true
                    useCompression: false

                    allOperations:
                      abandonCallAfter: 10s
                      waitForCallToComplete: 5s
                      waitForConnectionToComplete: 2s
                      maximumNumberOfCallAttempts: 4
                      delayBetweenRetries: 250ms
                      replicaOrder: SEQUENCE
                      resetTtlOnReadAtPercent: 80

                    consistencyModeReads:
                      readConsistency: SESSION

                    availabilityModeReads:
                      migrationReadConsistency: ONE

                    retryableWrites:
                      useDurableDelete: true
                      maximumNumberOfCallAttempts: 5

                    batchReads:
                      maxConcurrentServers: 12
                      allowInlineMemoryAccess: true
                      allowInlineSsdAccess: false

                    query:
                      recordQueueSize: 8000
                      maxConcurrentServers: 10
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);
            Behavior prodConfig = behaviors.get("production-config");

            assertNotNull(prodConfig);

            // Test various operation types
            Settings readSettings = prodConfig.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.CP);
            assertNotNull(readSettings);
            assertEquals(Duration.ofSeconds(10), readSettings.abandonCallAfter);
            assertEquals(80, readSettings.resetTtlOnReadAtPercent);
            assertEquals(ReadModeSC.SESSION, readSettings.readModeSC);

            Settings writeSettings = prodConfig.getSettings(Behavior.OpKind.WRITE_RETRYABLE, Behavior.OpShape.POINT, Behavior.Mode.AP);
            assertNotNull(writeSettings);
            assertTrue(writeSettings.useDurableDelete);
            assertEquals(5, writeSettings.maximumNumberOfCallAttempts);

            Settings batchSettings = prodConfig.getSettings(Behavior.OpKind.READ, Behavior.OpShape.BATCH, Behavior.Mode.AP);
            assertNotNull(batchSettings);
            assertEquals(12, batchSettings.maxConcurrentNodes);
            assertTrue(batchSettings.allowInlineMemoryAccess);

            Settings querySettings = prodConfig.getSettings(Behavior.OpKind.READ, Behavior.OpShape.QUERY, Behavior.Mode.AP);
            assertNotNull(querySettings);
            assertEquals(8000, querySettings.recordQueueSize);
        }
    }

    @Nested
    @DisplayName("Error Handling Tests")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Should handle empty behaviors map")
        void testEmptyBehaviorsMap() throws IOException {
            String yaml = """
                behaviors: {}
                """;

            Map<String, Behavior> behaviors = loadFromYamlString(yaml);

            assertNotNull(behaviors);
            assertTrue(behaviors.isEmpty());
        }

        @Test
        @DisplayName("Should throw exception for invalid YAML")
        void testInvalidYaml() {
            String yaml = """
                behaviors:
                  invalid:
                    allOperations
                      invalid syntax here
                """;

            assertThrows(IOException.class, () -> loadFromYamlString(yaml));
        }
    }
}
