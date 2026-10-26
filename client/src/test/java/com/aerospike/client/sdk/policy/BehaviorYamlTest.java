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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.SystemSettings;
import com.aerospike.client.sdk.SystemSettingsRegistry;

@DisplayName("Behavior YAML Serialization Tests")
public class BehaviorYamlTest {

    @AfterEach
    void resetBehaviorRegistryAfterEachYamlTest() {
        Behavior.restoreBehaviorRegistry();
    }

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
                    all_operations:
                      abandon_call_after: 5s
                      maximum_number_of_call_attempts: 3
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
        @DisplayName("YAML DEFAULT entry updates Behavior.DEFAULT used by sessions")
        void testYamlDefaultUpdatesRootDefaultReference() throws IOException {
            ResolvedSettings before = Behavior.DEFAULT.getSettings(Behavior.OpKind.WRITE_RETRYABLE,
                Behavior.OpShape.POINT, Behavior.Mode.AP);
            assertFalse(before.getSendKey());

            String yaml = """
                behaviors:
                  DEFAULT:
                    retryable_writes:
                      send_key: true
                """;

            loadFromYamlString(yaml);

            ResolvedSettings after = Behavior.DEFAULT.getSettings(Behavior.OpKind.WRITE_RETRYABLE,
                Behavior.OpShape.POINT, Behavior.Mode.AP);
            assertTrue(after.getSendKey());
        }

        @Test
        @DisplayName("Should load behavior with multiple operation types")
        void testMultipleOperationTypes() throws IOException {
            String yaml = """
                behaviors:
                  multi-op:
                    consistency_mode_reads:
                      abandon_call_after: 2s
                      read_consistency: SESSION
                    retryable_writes:
                      abandon_call_after: 10s
                      use_durable_delete: true
                      maximum_number_of_call_attempts: 5
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
                    all_operations:
                      abandon_call_after: 30s
                      delay_between_retries: 1s
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
                    all_operations:
                      abandon_call_after: 500ms
                      delay_between_retries: 100ms
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
                    all_operations:
                      abandon_call_after: 2m
                      wait_for_call_to_complete: 1h
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
                    all_operations:
                      abandon_call_after: 1000ns
                      delay_between_retries: 500nanos
                      wait_for_call_to_complete: 2000nanosecond
                      wait_for_connection_to_complete: 3000nanoseconds
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
                    all_operations:
                      abandon_call_after: 1000us
                      delay_between_retries: 500micros
                      wait_for_call_to_complete: 2000microsecond
                      wait_for_connection_to_complete: 3000microseconds
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
                    all_operations:
                      abandon_call_after: 100ms
                      delay_between_retries: 50millis
                      wait_for_call_to_complete: 200millisecond
                      wait_for_connection_to_complete: 300milliseconds
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
                    all_operations:
                      abandon_call_after: 10s
                      delay_between_retries: 5sec
                      wait_for_call_to_complete: 20second
                      wait_for_connection_to_complete: 30seconds
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
                    all_operations:
                      abandon_call_after: 1m
                      delay_between_retries: 2min
                      wait_for_call_to_complete: 3minute
                      wait_for_connection_to_complete: 5minutes
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
                    all_operations:
                      abandon_call_after: 1h
                      delay_between_retries: 2hr
                      wait_for_call_to_complete: 3hour
                      wait_for_connection_to_complete: 4hours
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
                    all_operations:
                      abandon_call_after: 1d
                      delay_between_retries: 2day
                      wait_for_call_to_complete: 3days
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
                    all_operations:
                      abandon_call_after: 5000ms
                      delay_between_retries: 5s
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
                    all_operations:
                      abandon_call_after: 120s
                      delay_between_retries: 2m
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
                    all_operations:
                      abandon_call_after: 60m
                      delay_between_retries: 1h
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
                    all_operations:
                      abandon_call_after: 24h
                      delay_between_retries: 1d
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
                    all_operations:
                      abandon_call_after: 5000us
                      delay_between_retries: 5ms
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
                    all_operations:
                      abandon_call_after: 999999ms
                      delay_between_retries: 86400s
                      wait_for_call_to_complete: 1440m
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
                    all_operations:
                      abandon_call_after: 500ms
                    consistency_mode_reads:
                      abandon_call_after: 2s
                    retryable_writes:
                      abandon_call_after: 1m
                    batch_reads:
                      abandon_call_after: 5m
                    query:
                      abandon_call_after: 1h
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
                    all_operations:
                      abandon_call_after: 10 s
                      delay_between_retries: 500 ms
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
                    all_operations:
                      abandon_call_after: 10s
                      maximum_number_of_call_attempts: 5
                      delay_between_retries: 500ms

                  child-config:
                    parent: parent-config
                    all_operations:
                      abandon_call_after: 20s
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
                    all_operations:
                      abandon_call_after: 15s
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
                    batch_reads:
                      abandon_call_after: 30s
                      max_concurrent_servers: 10
                      allow_inline_memory_access: true
                      allow_inline_ssd_access: false
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
                    batch_writes:
                      abandon_call_after: 45s
                      max_concurrent_servers: 8
                      allow_inline_memory_access: false
                      allow_inline_ssd_access: true
                      maximum_number_of_call_attempts: 4
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
                      abandon_call_after: 60s
                      record_queue_size: 5000
                      max_concurrent_servers: 12
                      maximum_number_of_call_attempts: 2
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
                    retryable_writes:
                      abandon_call_after: 8s
                      use_durable_delete: true
                      maximum_number_of_call_attempts: 6
                      delay_between_retries: 200ms
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
                    non_retryable_writes:
                      abandon_call_after: 3s
                      use_durable_delete: false
                      maximum_number_of_call_attempts: 1
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
                    system_txn_verify:
                      abandon_call_after: 1s
                      consistency: LINEARIZE
                      maximum_number_of_call_attempts: 2
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
                      minimum_connections_per_node: 10
                      maximum_connections_per_node: 100
                    circuit_breaker:
                      num_tend_intervals_in_error_window: 2
                      maximum_errors_in_error_window: 50
                    refresh:
                      tend_interval: 1s
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
                      minimum_connections_per_node: 50
                      maximum_connections_per_node: 300
                    circuit_breaker:
                      num_tend_intervals_in_error_window: 3
                      maximum_errors_in_error_window: 100
                    refresh:
                      tend_interval: 500ms
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
                      minimum_connections_per_node: 5
                      maximum_connections_per_node: 50
                  production:
                    connections:
                      minimum_connections_per_node: 100
                      maximum_connections_per_node: 500
                  development:
                    connections:
                      minimum_connections_per_node: 1
                      maximum_connections_per_node: 10
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
                      minimum_connections_per_node: 20
                      maximum_connections_per_node: 200
                      maximum_socket_idle_time: 55s
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
                    circuit_breaker:
                      num_tend_intervals_in_error_window: 5
                      maximum_errors_in_error_window: 200
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
                      tend_interval: 2s
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
                      maximum_socket_idle_time: 30s
                    refresh:
                      tend_interval: 500ms
                  fast-refresh:
                    refresh:
                      tend_interval: 100ms
                  slow-refresh:
                    refresh:
                      tend_interval: 5s
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
                    all_operations:
                      abandon_call_after: 10s
                      maximum_number_of_call_attempts: 5

                system:
                  DEFAULT:
                    connections:
                      minimum_connections_per_node: 25
                      maximum_connections_per_node: 250
                    circuit_breaker:
                      num_tend_intervals_in_error_window: 3
                  production:
                    connections:
                      minimum_connections_per_node: 50
                      maximum_connections_per_node: 500
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
                    all_operations:
                      abandon_call_after: 5s
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
                      implicit_batch_write_transactions: true
                      sleep_between_attempts: 500ms
                      number_of_attempts: 5
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
                      number_of_attempts: 10
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
                      sleep_between_attempts: 250ms
                      number_of_attempts: 3
                  production:
                    transactions:
                      sleep_between_attempts: 1s
                      number_of_attempts: 10
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
                    all_operations:
                      abandon_call_after: 5s
                      maximum_number_of_call_attempts: 3
                      delay_between_retries: 100ms

                  high-throughput:
                    parent: base-performance
                    batch_reads:
                      max_concurrent_servers: 16
                      allow_inline_memory_access: true
                      allow_inline_ssd_access: true
                    query:
                      record_queue_size: 10000
                      max_concurrent_servers: 16

                  low-latency:
                    parent: base-performance
                    all_operations:
                      abandon_call_after: 1s
                    consistency_mode_reads:
                      read_consistency: LINEARIZE
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
                    all_operations:
                      abandon_call_after: 10s
                      wait_for_call_to_complete: 5s
                      wait_for_connection_to_complete: 2s
                      maximum_number_of_call_attempts: 4
                      delay_between_retries: 250ms
                      replica_order: SEQUENCE
                      reset_ttl_on_read_at_percent: 80
                      send_key: true
                      use_compression: false

                    consistency_mode_reads:
                      read_consistency: SESSION

                    availability_mode_reads:
                      migration_read_consistency: ONE

                    retryable_writes:
                      use_durable_delete: true
                      maximum_number_of_call_attempts: 5

                    batch_reads:
                      max_concurrent_servers: 12
                      allow_inline_memory_access: true
                      allow_inline_ssd_access: false

                    query:
                      record_queue_size: 8000
                      max_concurrent_servers: 10
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
            assertTrue(readSettings.getSendKey());
            assertFalse(readSettings.getUseCompression());

            ResolvedSettings writeSettings = prodConfig.getSettings(Behavior.OpKind.WRITE_RETRYABLE,
                Behavior.OpShape.POINT, Behavior.Mode.AP);
            assertNotNull(writeSettings);
            assertTrue(writeSettings.getUseDurableDelete());
            assertEquals(5, writeSettings.getMaximumNumberOfCallAttempts());
            assertTrue(writeSettings.getSendKey());
            assertFalse(writeSettings.getUseCompression());

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
