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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.SystemSettings;
import com.aerospike.client.sdk.SystemSettingsRegistry;

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
            ResolvedSettings settings =
                simple.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            assertNotNull(settings);
            assertEquals(5000, settings.getAbandonCallAfterMs());
            assertEquals(3, settings.getMaximumNumberOfCallAttempts());
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
            ResolvedSettings readCp =
                multiOp.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.CP);
            assertNotNull(readCp);
            assertEquals(2000, readCp.getAbandonCallAfterMs());
            assertEquals(ReadModeSC.SESSION, readCp.getReadModeSC());

            // Test retryable write settings
            ResolvedSettings writeRetry = multiOp.getSettings(Behavior.OpKind.WRITE_RETRYABLE,
                Behavior.OpShape.POINT, Behavior.Mode.AP);
            assertNotNull(writeRetry);
            assertEquals(10000, writeRetry.getAbandonCallAfterMs());
            assertTrue(writeRetry.getUseDurableDelete());
            assertEquals(5, writeRetry.getMaximumNumberOfCallAttempts());
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
            ResolvedSettings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);

            assertEquals(30000, settings.getAbandonCallAfterMs());
            assertEquals(1000, settings.getDelayBetweenRetriesMs());
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
            ResolvedSettings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);

            assertEquals(500, settings.getAbandonCallAfterMs());
            assertEquals(100, settings.getDelayBetweenRetriesMs());
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
            ResolvedSettings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);

            assertEquals(120000, settings.getAbandonCallAfterMs());
            assertEquals(3600000, settings.getWaitForCallToCompleteMs());
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
            ResolvedSettings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);

            assertEquals(0, settings.getAbandonCallAfterMs());
            assertEquals(0, settings.getDelayBetweenRetriesMs());
            assertEquals(0, settings.getWaitForCallToCompleteMs());
            assertEquals(0, settings.getWaitForConnectionToCompleteMs());
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
            ResolvedSettings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);

            // 1 microsecond = 1000 nanoseconds
            assertEquals(1, settings.getAbandonCallAfterMs());
            assertEquals(0, settings.getDelayBetweenRetriesMs());
            assertEquals(2, settings.getWaitForCallToCompleteMs());
            assertEquals(3, settings.getWaitForConnectionToCompleteMs());
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
            ResolvedSettings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);

            assertEquals(100, settings.getAbandonCallAfterMs());
            assertEquals(50, settings.getDelayBetweenRetriesMs());
            assertEquals(200, settings.getWaitForCallToCompleteMs());
            assertEquals(300, settings.getWaitForConnectionToCompleteMs());
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
            ResolvedSettings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);

            assertEquals(10000, settings.getAbandonCallAfterMs());
            assertEquals(5000, settings.getDelayBetweenRetriesMs());
            assertEquals(20000, settings.getWaitForCallToCompleteMs());
            assertEquals(30000, settings.getWaitForConnectionToCompleteMs());
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
            ResolvedSettings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);

            assertEquals(60000, settings.getAbandonCallAfterMs());
            assertEquals(120000, settings.getDelayBetweenRetriesMs());
            assertEquals(180000, settings.getWaitForCallToCompleteMs());
            assertEquals(300000, settings.getWaitForConnectionToCompleteMs());
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
            ResolvedSettings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);

            assertEquals(3600000, settings.getAbandonCallAfterMs());
            assertEquals(7200000, settings.getDelayBetweenRetriesMs());
            assertEquals(10800000, settings.getWaitForCallToCompleteMs());
            assertEquals(14400000, settings.getWaitForConnectionToCompleteMs());
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
            ResolvedSettings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);

            assertEquals(86400000, settings.getAbandonCallAfterMs());
            assertEquals(172800000, settings.getDelayBetweenRetriesMs());
            assertEquals(259200000, settings.getWaitForCallToCompleteMs());
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
            ResolvedSettings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);

            // 5000ms should equal 5s
            assertEquals(settings.getAbandonCallAfterMs(), settings.getDelayBetweenRetriesMs());
            assertEquals(5000, settings.getAbandonCallAfterMs());
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
            ResolvedSettings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);

            // 120s should equal 2m
            assertEquals(settings.getAbandonCallAfterMs(), settings.getDelayBetweenRetriesMs());
            assertEquals(120000, settings.getAbandonCallAfterMs());
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
            ResolvedSettings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);

            // 60m should equal 1h
            assertEquals(settings.getAbandonCallAfterMs(), settings.getDelayBetweenRetriesMs());
            assertEquals(3600000, settings.getAbandonCallAfterMs());
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
            ResolvedSettings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);

            // 24h should equal 1d
            assertEquals(settings.getAbandonCallAfterMs(), settings.getDelayBetweenRetriesMs());
            assertEquals(86400000, settings.getAbandonCallAfterMs());
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
            ResolvedSettings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);

            // 5000us should equal 5ms
            assertEquals(settings.getAbandonCallAfterMs(), settings.getDelayBetweenRetriesMs());
            assertEquals(5, settings.getAbandonCallAfterMs());
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
            ResolvedSettings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);

            assertEquals(999999, settings.getAbandonCallAfterMs());
            assertEquals(86400000, settings.getDelayBetweenRetriesMs()); // 1 day in seconds
            assertEquals(86400000, settings.getWaitForCallToCompleteMs()); // 1 day in minutes
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

            ResolvedSettings allOps = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);
            ResolvedSettings readCp = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.CP);
            ResolvedSettings writes = behavior.getSettings(Behavior.OpKind.WRITE_RETRYABLE,
                Behavior.OpShape.POINT, Behavior.Mode.AP);
            ResolvedSettings batch = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.BATCH,
                Behavior.Mode.AP);
            ResolvedSettings query = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.QUERY,
                Behavior.Mode.AP);

            assertEquals(500, allOps.getAbandonCallAfterMs());
            assertEquals(2000, readCp.getAbandonCallAfterMs());
            assertEquals(60000, writes.getAbandonCallAfterMs());
            assertEquals(300000, batch.getAbandonCallAfterMs());
            assertEquals(3600000, query.getAbandonCallAfterMs());
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
            ResolvedSettings settings = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);

            assertEquals(10000, settings.getAbandonCallAfterMs());
            assertEquals(500, settings.getDelayBetweenRetriesMs());
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
            ResolvedSettings settings =
                child.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            // Child overrides abandonCallAfter
            assertEquals(20000, settings.getAbandonCallAfterMs());

            // Child inherits other settings from parent
            assertEquals(5, settings.getMaximumNumberOfCallAttempts());
            assertEquals(500, settings.getDelayBetweenRetriesMs());
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

            ResolvedSettings batchSettings = batchConfig.getSettings(Behavior.OpKind.READ,
                Behavior.OpShape.BATCH, Behavior.Mode.AP);

            assertNotNull(batchSettings);
            assertEquals(30000, batchSettings.getAbandonCallAfterMs());
            assertEquals(10, batchSettings.getMaxConcurrentNodes());
            assertTrue(batchSettings.getAllowInlineMemoryAccess());
            assertFalse(batchSettings.getAllowInlineSsdAccess());
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

            ResolvedSettings batchSettings = batchWrites.getSettings(Behavior.OpKind.WRITE_RETRYABLE,
                Behavior.OpShape.BATCH, Behavior.Mode.AP);

            assertNotNull(batchSettings);
            assertEquals(45000, batchSettings.getAbandonCallAfterMs());
            assertEquals(8, batchSettings.getMaxConcurrentNodes());
            assertFalse(batchSettings.getAllowInlineMemoryAccess());
            assertTrue(batchSettings.getAllowInlineSsdAccess());
            assertEquals(4, batchSettings.getMaximumNumberOfCallAttempts());
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

            ResolvedSettings querySettings = queryConfig.getSettings(Behavior.OpKind.READ,
                Behavior.OpShape.QUERY, Behavior.Mode.AP);

            assertNotNull(querySettings);
            assertEquals(60000, querySettings.getAbandonCallAfterMs());
            assertEquals(5000, querySettings.getRecordQueueSize());
            assertEquals(2, querySettings.getMaximumNumberOfCallAttempts());
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

            ResolvedSettings writeSettings = retryWrites.getSettings(Behavior.OpKind.WRITE_RETRYABLE,
                Behavior.OpShape.POINT, Behavior.Mode.AP);

            assertNotNull(writeSettings);
            assertEquals(8000, writeSettings.getAbandonCallAfterMs());
            assertTrue(writeSettings.getUseDurableDelete());
            assertEquals(6, writeSettings.getMaximumNumberOfCallAttempts());
            assertEquals(200, writeSettings.getDelayBetweenRetriesMs());
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

            ResolvedSettings writeSettings = noRetryWrites.getSettings(Behavior.OpKind.WRITE_NON_RETRYABLE,
                Behavior.OpShape.POINT, Behavior.Mode.AP);

            assertNotNull(writeSettings);
            assertEquals(3000, writeSettings.getAbandonCallAfterMs());
            assertFalse(writeSettings.getUseDurableDelete());
            assertEquals(1, writeSettings.getMaximumNumberOfCallAttempts());
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

            ResolvedSettings txnSettings = sysTxn.getSettings(Behavior.OpKind.SYSTEM_TXN_VERIFY,
                Behavior.OpShape.POINT, Behavior.Mode.CP);

            assertNotNull(txnSettings);
            assertEquals(1000, txnSettings.getAbandonCallAfterMs());
            assertEquals(ReadModeSC.LINEARIZE, txnSettings.getReadModeSC());
            assertEquals(2, txnSettings.getMaximumNumberOfCallAttempts());
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
            ResolvedSettings settings = production.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT,
                Behavior.Mode.AP);
            assertEquals(10000, settings.getAbandonCallAfterMs());
            assertEquals(5, settings.getMaximumNumberOfCallAttempts());

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

        @Test
        @DisplayName("Should load transaction settings from YAML")
        void testTransactionSettings() throws IOException {
            String yaml = """
                system:
                  DEFAULT:
                    transactions:
                      implicitBatchWriteTransactions: true
                      sleepBetweenAttempts: 500ms
                      numberOfAttempts: 5
                """;

            loadFromYamlString(yaml);

            SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();
            SystemSettings defaults = registry.getDefaultSettings();

            assertNotNull(defaults);
            assertEquals(true, defaults.getImplicitBatchWriteTransactions());
            assertEquals(Duration.ofMillis(500), defaults.getSleepBetweenAttempts());
            assertEquals(5, defaults.getNumberOfAttempts());
        }

        @Test
        @DisplayName("Should load partial transaction settings")
        void testPartialTransactionSettings() throws IOException {
            String yaml = """
                system:
                  DEFAULT:
                    transactions:
                      numberOfAttempts: 10
                """;

            loadFromYamlString(yaml);

            SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();
            SystemSettings defaults = registry.getDefaultSettings();

            assertNotNull(defaults);
            assertEquals(10, defaults.getNumberOfAttempts());
            assertNull(defaults.getSleepBetweenAttempts());
            assertNull(defaults.getImplicitBatchWriteTransactions());
        }

        @Test
        @DisplayName("Should load cluster-specific transaction settings")
        void testClusterSpecificTransactionSettings() throws IOException {
            String yaml = """
                system:
                  DEFAULT:
                    transactions:
                      sleepBetweenAttempts: 250ms
                      numberOfAttempts: 3
                  production:
                    transactions:
                      sleepBetweenAttempts: 1s
                      numberOfAttempts: 10
                """;

            loadFromYamlString(yaml);

            SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();

            SystemSettings defaults = registry.getDefaultSettings();
            assertEquals(Duration.ofMillis(250), defaults.getSleepBetweenAttempts());
            assertEquals(3, defaults.getNumberOfAttempts());

            SystemSettings production = registry.getClusterSettings("production");
            assertEquals(Duration.ofSeconds(1), production.getSleepBetweenAttempts());
            assertEquals(10, production.getNumberOfAttempts());
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
            ResolvedSettings batchSettings = highThroughput.getSettings(Behavior.OpKind.READ,
                Behavior.OpShape.BATCH, Behavior.Mode.AP);
            assertEquals(16, batchSettings.getMaxConcurrentNodes());
            assertTrue(batchSettings.getAllowInlineMemoryAccess());

            // Test low-latency overrides
            Behavior lowLatency = behaviors.get("low-latency");
            ResolvedSettings lowLatSettings = lowLatency.getSettings(Behavior.OpKind.READ,
                Behavior.OpShape.POINT, Behavior.Mode.CP);
            assertEquals(1000, lowLatSettings.getAbandonCallAfterMs());
            assertEquals(ReadModeSC.LINEARIZE, lowLatSettings.getReadModeSC());
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
            ResolvedSettings readSettings = prodConfig.getSettings(Behavior.OpKind.READ,
                Behavior.OpShape.POINT, Behavior.Mode.CP);
            assertNotNull(readSettings);
            assertEquals(10000, readSettings.getAbandonCallAfterMs());
            assertEquals(80, readSettings.getResetTtlOnReadAtPercent());
            assertEquals(ReadModeSC.SESSION, readSettings.getReadModeSC());

            ResolvedSettings writeSettings = prodConfig.getSettings(Behavior.OpKind.WRITE_RETRYABLE,
                Behavior.OpShape.POINT, Behavior.Mode.AP);
            assertNotNull(writeSettings);
            assertTrue(writeSettings.getUseDurableDelete());
            assertEquals(5, writeSettings.getMaximumNumberOfCallAttempts());

            ResolvedSettings batchSettings = prodConfig.getSettings(Behavior.OpKind.READ,
                Behavior.OpShape.BATCH, Behavior.Mode.AP);
            assertNotNull(batchSettings);
            assertEquals(12, batchSettings.getMaxConcurrentNodes());
            assertTrue(batchSettings.getAllowInlineMemoryAccess());

            ResolvedSettings querySettings = prodConfig.getSettings(Behavior.OpKind.READ,
                Behavior.OpShape.QUERY, Behavior.Mode.AP);
            assertNotNull(querySettings);
            assertEquals(8000, querySettings.getRecordQueueSize());
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
