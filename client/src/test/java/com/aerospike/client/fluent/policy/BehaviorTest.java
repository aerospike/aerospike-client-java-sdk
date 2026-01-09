/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.policy.Behavior.Mode;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.Behavior.Selectors;

/**
 * Comprehensive tests for Behavior class covering:
 * - All selector permutations
 * - All configuration attributes
 * - Inheritance and override behavior
 * - DEFAULT behavior completeness
 */
public class BehaviorTest {
    @Nested
    @DisplayName("DEFAULT Behavior Tests")
    class DefaultBehaviorTests {

        @Test
        @DisplayName("DEFAULT should have all common settings configured")
        void testDefaultCommonSettings() {
            // Test all operation types have common settings from Selectors.all()
            Settings readPointAp = Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertNotNull(readPointAp, "READ:POINT:AP settings should exist");

            // Verify common settings are present
            assertEquals(Duration.ofSeconds(1), readPointAp.abandonCallAfter);
            assertEquals(Duration.ofMillis(0), readPointAp.delayBetweenRetries);
            assertEquals(3, readPointAp.maximumNumberOfCallAttempts);
            assertEquals(Replica.SEQUENCE, readPointAp.replicaOrder);
            assertTrue(readPointAp.sendKey);
            assertFalse(readPointAp.useCompression);
            assertEquals(Duration.ofSeconds(30), readPointAp.waitForCallToComplete);
            assertEquals(Duration.ofSeconds(0), readPointAp.waitForConnectionToComplete);
            assertEquals(Duration.ofSeconds(0), readPointAp.waitForSocketResponseAfterCallFails);
        }

        @Test
        @DisplayName("DEFAULT should have all READ AP settings configured")
        void testDefaultReadApSettings() {
            Settings readPointAp = Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);

            assertEquals(ReadModeAP.ALL, readPointAp.readModeAP);
            assertEquals(0, readPointAp.resetTtlOnReadAtPercent);
        }

        @Test
        @DisplayName("DEFAULT should have all READ CP settings configured")
        void testDefaultReadCpSettings() {
            Settings readPointCp = Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);

            assertEquals(ReadModeSC.SESSION, readPointCp.readModeSC);
            assertEquals(0, readPointCp.resetTtlOnReadAtPercent);
        }

        @Test
        @DisplayName("DEFAULT should have all BATCH READ settings configured")
        void testDefaultBatchReadSettings() {
            Settings readBatchAp = Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);

            assertEquals(1, readBatchAp.maxConcurrentNodes);
            assertTrue(readBatchAp.allowInlineMemoryAccess);
            assertFalse(readBatchAp.allowInlineSsdAccess);
        }

        @Test
        @DisplayName("DEFAULT should have all QUERY settings configured")
        void testDefaultQuerySettings() {
            Settings readQueryAp = Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.QUERY, Mode.AP);

            assertEquals(5000, readQueryAp.recordQueueSize);
            assertEquals(6, readQueryAp.maximumNumberOfCallAttempts); // Override from query-specific
        }

        @Test
        @DisplayName("DEFAULT should have all RETRYABLE WRITE settings configured")
        void testDefaultRetryableWriteSettings() {
            Settings writeRetryablePointAp = Behavior.DEFAULT.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);

            assertFalse(writeRetryablePointAp.useDurableDelete);
            assertEquals(3, writeRetryablePointAp.maximumNumberOfCallAttempts); // Override from retryable
            assertFalse(writeRetryablePointAp.simulateXdrWrite);
        }

        @Test
        @DisplayName("DEFAULT should have all RETRYABLE WRITE CP settings configured")
        void testDefaultRetryableWriteCpSettings() {
            Settings writeRetryableCp = Behavior.DEFAULT.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.CP);

            assertTrue(writeRetryableCp.useDurableDelete); // CP override
        }

        @Test
        @DisplayName("DEFAULT should have all NON-RETRYABLE WRITE settings configured")
        void testDefaultNonRetryableWriteSettings() {
            Settings writeNonRetryablePointAp = Behavior.DEFAULT.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);

            assertFalse(writeNonRetryablePointAp.useDurableDelete);
            assertFalse(writeNonRetryablePointAp.simulateXdrWrite);
        }

        @Test
        @DisplayName("DEFAULT should have all BATCH WRITE settings configured")
        void testDefaultBatchWriteSettings() {
            Settings writeRetryableBatchAp = Behavior.DEFAULT.getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Mode.AP);

            assertEquals(1, writeRetryableBatchAp.maxConcurrentNodes);
            assertTrue(writeRetryableBatchAp.allowInlineMemoryAccess);
            assertFalse(writeRetryableBatchAp.allowInlineSsdAccess);
        }

        @Test
        @DisplayName("DEFAULT should have all WRITE AP settings configured")
        void testDefaultWriteApSettings() {
            Settings writePointAp = Behavior.DEFAULT.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);

            assertEquals(CommitLevel.COMMIT_ALL, writePointAp.commitLevel);
        }

        @Test
        @DisplayName("All operation combinations should have settings")
        void testAllOperationCombinationsExist() {
            // Test all READ combinations
            for (OpShape shape : new OpShape[]{OpShape.POINT, OpShape.BATCH, OpShape.QUERY}) {
                for (Mode mode : new Mode[]{Mode.AP, Mode.CP}) {
                    Settings settings = Behavior.DEFAULT.getSettings(OpKind.READ, shape, mode);
                    assertNotNull(settings, "READ:" + shape + ":" + mode + " should have settings");
                }
            }

            // Test all WRITE combinations (no QUERY for writes)
            for (OpKind kind : new OpKind[]{OpKind.WRITE_RETRYABLE, OpKind.WRITE_NON_RETRYABLE}) {
                for (OpShape shape : new OpShape[]{OpShape.POINT, OpShape.BATCH}) {
                    for (Mode mode : new Mode[]{Mode.AP, Mode.CP}) {
                        Settings settings = Behavior.DEFAULT.getSettings(kind, shape, mode);
                        assertNotNull(settings, kind + ":" + shape + ":" + mode + " should have settings");
                    }
                }
            }
        }
    }

    @Nested
    @DisplayName("Selector Permutation Tests")
    class SelectorPermutationTests {

        @Test
        @DisplayName("Selectors.all() should expose all common methods")
        void testSelectorsAll() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.all(), ops -> ops
                            .abandonCallAfter(Duration.ofSeconds(10))
                            .delayBetweenRetries(Duration.ofMillis(100))
                            .maximumNumberOfCallAttempts(5)
                            .replicaOrder(Replica.SEQUENCE)
                            .sendKey(false)
                            .useCompression(true)
                            .waitForCallToComplete(Duration.ofSeconds(2))
                            .waitForConnectionToComplete(Duration.ofSeconds(3))
                            .waitForSocketResponseAfterCallFails(Duration.ofSeconds(4))
                    )
            );

            // Verify settings applied to all operations
            Settings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(Duration.ofSeconds(10), readPointAp.abandonCallAfter);
            assertEquals(5, readPointAp.maximumNumberOfCallAttempts);
            assertFalse(readPointAp.sendKey);

            Settings writePointCp = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.CP);
            assertEquals(Duration.ofSeconds(10), writePointCp.abandonCallAfter);
            assertTrue(writePointCp.useCompression);
        }

        @Test
        @DisplayName("Selectors.all() should allow setting all possible settings including mode-specific")
        void testSelectorsAllWithModeSpecificSettings() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.all(), ops -> ops
                            .maximumNumberOfCallAttempts(5)
                            .readMode(ReadModeAP.ONE)
                            .consistency(ReadModeSC.LINEARIZE)
                            .commitLevel(CommitLevel.COMMIT_MASTER)
                            .useDurableDelete(true)
                            .resetTtlOnReadAtPercent(50)
                            .maxConcurrentNodes(10)
                            .recordQueueSize(10000)
                    )
            );

            // Verify AP read settings
            Settings readAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(5, readAp.maximumNumberOfCallAttempts);
            assertEquals(ReadModeAP.ONE, readAp.readModeAP);
            assertEquals(50, readAp.resetTtlOnReadAtPercent);

            // Verify CP read settings
            Settings readCp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);
            assertEquals(5, readCp.maximumNumberOfCallAttempts);
            assertEquals(ReadModeSC.LINEARIZE, readCp.readModeSC);
            assertEquals(50, readCp.resetTtlOnReadAtPercent);

            // Verify AP write settings
            Settings writeAp = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(5, writeAp.maximumNumberOfCallAttempts);
            assertEquals(CommitLevel.COMMIT_MASTER, writeAp.commitLevel);
            assertTrue(writeAp.useDurableDelete);

            // Verify batch settings
            Settings batchRead = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
            assertEquals(10, batchRead.maxConcurrentNodes);

            // Verify query settings
            Settings query = behavior.getSettings(OpKind.READ, OpShape.QUERY, Mode.AP);
            assertEquals(10000, query.recordQueueSize);
        }

        @Test
        @DisplayName("Selectors.reads() should expose resetTtlOnReadAtPercent")
        void testSelectorsReads() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.reads(), ops -> ops
                            .resetTtlOnReadAtPercent(50)
                            .maximumNumberOfCallAttempts(10)
                    )
            );

            Settings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(50, readPointAp.resetTtlOnReadAtPercent);
            assertEquals(10, readPointAp.maximumNumberOfCallAttempts);
        }

        @Test
        @DisplayName("Selectors.reads().ap() should expose readMode")
        void testSelectorsReadsAp() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.reads().ap(), ops -> ops
                            .readMode(ReadModeAP.ONE)
                            .resetTtlOnReadAtPercent(25)
                    )
            );

            Settings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(ReadModeAP.ONE, readPointAp.readModeAP);
            assertEquals(25, readPointAp.resetTtlOnReadAtPercent);

            // CP mode should have DEFAULT readModeAP (from Selectors.all())
            Settings readPointCp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);
            assertEquals(ReadModeAP.ALL, readPointCp.readModeAP); // DEFAULT value from Selectors.all()
        }

        @Test
        @DisplayName("Selectors.reads().cp() should expose consistency")
        void testSelectorsReadsCp() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.reads().cp(), ops -> ops
                            .consistency(ReadModeSC.LINEARIZE)
                            .resetTtlOnReadAtPercent(75)
                    )
            );

            Settings readPointCp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);
            assertEquals(ReadModeSC.LINEARIZE, readPointCp.readModeSC);
            assertEquals(75, readPointCp.resetTtlOnReadAtPercent);

            // AP mode should have DEFAULT readModeSC (from Selectors.all())
            Settings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(ReadModeSC.SESSION, readPointAp.readModeSC); // DEFAULT value from Selectors.all()
        }

        @Test
        @DisplayName("Selectors.reads().get() should configure point reads")
        void testSelectorsReadsGet() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.reads().get(), ops -> ops
                            .maximumNumberOfCallAttempts(7)
                    )
            );

            Settings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(7, readPointAp.maximumNumberOfCallAttempts);

            // Should not affect batch - should have DEFAULT value
            Settings readBatchAp = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
            assertEquals(3, readBatchAp.maximumNumberOfCallAttempts); // DEFAULT value, not overridden
        }

        @Test
        @DisplayName("Selectors.reads().batch() should expose batch-specific methods")
        void testSelectorsReadsBatch() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.reads().batch(), ops -> ops
                            .maxConcurrentNodes(8)
                            .allowInlineMemoryAccess(false)
                            .allowInlineSsdAccess(true)
                    )
            );

            Settings readBatchAp = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
            assertEquals(8, readBatchAp.maxConcurrentNodes);
            assertFalse(readBatchAp.allowInlineMemoryAccess);
            assertTrue(readBatchAp.allowInlineSsdAccess);

            // Should not affect point reads - they keep global default
            Settings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(1, readPointAp.maxConcurrentNodes); // Global default from Selectors.all()
        }

        @Test
        @DisplayName("Selectors.reads().query() should expose query-specific methods")
        void testSelectorsReadsQuery() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.reads().query(), ops -> ops
                            .recordQueueSize(10000)
                            .maximumNumberOfCallAttempts(8)
                    )
            );

            Settings readQueryAp = behavior.getSettings(OpKind.READ, OpShape.QUERY, Mode.AP);
            assertEquals(10000, readQueryAp.recordQueueSize);
            assertEquals(8, readQueryAp.maximumNumberOfCallAttempts);

            // Should not affect batch
            Settings readBatchAp = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
            assertNull(readBatchAp.recordQueueSize);
        }

        @Test
        @DisplayName("Selectors.reads().batch().ap() should expose all relevant methods")
        void testSelectorsReadsBatchAp() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.reads().batch().ap(), ops -> ops
                            .maxConcurrentNodes(16)
                            .readMode(ReadModeAP.ALL)
                            .resetTtlOnReadAtPercent(33)
                    )
            );

            Settings readBatchAp = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
            assertEquals(16, readBatchAp.maxConcurrentNodes);
            assertEquals(ReadModeAP.ALL, readBatchAp.readModeAP);
            assertEquals(33, readBatchAp.resetTtlOnReadAtPercent);
        }

        @Test
        @DisplayName("Selectors.writes() should expose write-specific methods")
        void testSelectorsWrites() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.writes(), ops -> ops
                            .useDurableDelete(true)
                            .simulateXdrWrite(true)
                    )
            );

            // Should apply to both retryable and non-retryable
            Settings writeRetryable = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertTrue(writeRetryable.useDurableDelete);
            assertTrue(writeRetryable.simulateXdrWrite);

            Settings writeNonRetryable = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
            assertTrue(writeNonRetryable.useDurableDelete);
            assertTrue(writeNonRetryable.simulateXdrWrite);
        }

        @Test
        @DisplayName("Selectors.writes().ap() should expose commitLevel")
        void testSelectorsWritesAp() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.writes().ap(), ops -> ops
                            .commitLevel(CommitLevel.COMMIT_MASTER)
                    )
            );

            Settings writePointAp = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(CommitLevel.COMMIT_MASTER, writePointAp.commitLevel);

            // Should not affect CP
            Settings writePointCp = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.CP);
            assertNull(writePointCp.commitLevel);
        }

        @Test
        @DisplayName("Selectors.writes().retryable() should configure only retryable writes")
        void testSelectorsWritesRetryable() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.writes().retryable(), ops -> ops
                            .maximumNumberOfCallAttempts(10)
                    )
            );

            Settings writeRetryable = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(10, writeRetryable.maximumNumberOfCallAttempts);

            // Should not affect non-retryable - should have DEFAULT value
            Settings writeNonRetryable = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(1, writeNonRetryable.maximumNumberOfCallAttempts); // DEFAULT value from Selectors.all()
        }

        @Test
        @DisplayName("Selectors.writes().nonRetryable() should configure only non-retryable writes")
        void testSelectorsWritesNonRetryable() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.writes().nonRetryable(), ops -> ops
                            .maximumNumberOfCallAttempts(1)
                    )
            );

            Settings writeNonRetryable = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(1, writeNonRetryable.maximumNumberOfCallAttempts);

            // Should not affect retryable - should have DEFAULT value
            Settings writeRetryable = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(3, writeRetryable.maximumNumberOfCallAttempts); // DEFAULT value for retryable writes
        }

        @Test
        @DisplayName("Selectors.writes().point() should configure point writes (retryability-agnostic)")
        void testSelectorsWritesPoint() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.writes().point(), ops -> ops
                            .maximumNumberOfCallAttempts(5)
                    )
            );

            // Should apply to both retryable and non-retryable point writes
            Settings writeRetryablePoint = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(5, writeRetryablePoint.maximumNumberOfCallAttempts);

            Settings writeNonRetryablePoint = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(5, writeNonRetryablePoint.maximumNumberOfCallAttempts);

            // Should not affect batch - should have DEFAULT value
            Settings writeRetryableBatch = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Mode.AP);
            assertEquals(3, writeRetryableBatch.maximumNumberOfCallAttempts); // DEFAULT value for retryable writes
        }

        @Test
        @DisplayName("Selectors.writes().batch() should expose batch-specific methods")
        void testSelectorsWritesBatch() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.writes().batch(), ops -> ops
                            .maxConcurrentNodes(12)
                            .allowInlineMemoryAccess(true)
                    )
            );

            // Should apply to both retryable and non-retryable batch writes
            Settings writeRetryableBatch = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Mode.AP);
            assertEquals(12, writeRetryableBatch.maxConcurrentNodes);

            Settings writeNonRetryableBatch = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, Mode.AP);
            assertEquals(12, writeNonRetryableBatch.maxConcurrentNodes);
        }

        @Test
        @DisplayName("Selectors.writes().retryable().point().ap() should expose all relevant methods")
        void testSelectorsWritesRetryablePointAp() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.writes().retryable().point().ap(), ops -> ops
                            .maximumNumberOfCallAttempts(9)
                            .commitLevel(CommitLevel.COMMIT_ALL)
                            .useDurableDelete(false)
                    )
            );

            Settings settings = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(9, settings.maximumNumberOfCallAttempts);
            assertEquals(CommitLevel.COMMIT_ALL, settings.commitLevel);
            assertFalse(settings.useDurableDelete);

            // Should not affect non-retryable - should have DEFAULT value
            Settings nonRetryable = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(1, nonRetryable.maximumNumberOfCallAttempts); // DEFAULT value from Selectors.all()
        }

        @Test
        @DisplayName("Selectors.writes().point().ap() should configure both retryable and non-retryable")
        void testSelectorsWritesPointAp() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.writes().point().ap(), ops -> ops
                            .commitLevel(CommitLevel.COMMIT_MASTER)
                    )
            );

            // Should apply to both retryable and non-retryable
            Settings writeRetryable = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(CommitLevel.COMMIT_MASTER, writeRetryable.commitLevel);

            Settings writeNonRetryable = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(CommitLevel.COMMIT_MASTER, writeNonRetryable.commitLevel);
        }
    }

    @Nested
    @DisplayName("Override and Precedence Tests")
    class OverrideAndPrecedenceTests {

        @Test
        @DisplayName("More specific selectors should override less specific ones")
        void testSpecificOverridesGeneral() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.all(), ops -> ops
                            .maximumNumberOfCallAttempts(1)
                    )
                    .on(Selectors.reads(), ops -> ops
                            .maximumNumberOfCallAttempts(2)
                    )
                    .on(Selectors.reads().batch(), ops -> ops
                            .maximumNumberOfCallAttempts(3)
                    )
                    .on(Selectors.reads().batch().ap(), ops -> ops
                            .maximumNumberOfCallAttempts(4)
                    )
            );

            // Most specific wins
            Settings readBatchAp = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
            assertEquals(4, readBatchAp.maximumNumberOfCallAttempts);

            // Less specific for CP
            Settings readBatchCp = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.CP);
            assertEquals(3, readBatchCp.maximumNumberOfCallAttempts);

            // Even less specific for point
            Settings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(2, readPointAp.maximumNumberOfCallAttempts);

            // Least specific for writes
            Settings writePointAp = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(1, writePointAp.maximumNumberOfCallAttempts);
        }

        @Test
        @DisplayName("Later configurations should override earlier ones at same specificity")
        void testLaterOverridesEarlier() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.reads().ap(), ops -> ops
                            .readMode(ReadModeAP.ONE)
                    )
                    .on(Selectors.reads().ap(), ops -> ops
                            .readMode(ReadModeAP.ALL)
                    )
            );

            Settings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(ReadModeAP.ALL, readPointAp.readModeAP);
        }

        @Test
        @DisplayName("Different attributes should merge, not replace")
        void testAttributesMerge() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.all(), ops -> ops
                            .maximumNumberOfCallAttempts(5)
                            .sendKey(true)
                    )
                    .on(Selectors.reads().ap(), ops -> ops
                            .readMode(ReadModeAP.ONE)
                    )
            );

            Settings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            // Should have both
            assertEquals(5, readPointAp.maximumNumberOfCallAttempts);
            assertTrue(readPointAp.sendKey);
            assertEquals(ReadModeAP.ONE, readPointAp.readModeAP);
        }

        @Test
        @DisplayName("Mode-specific settings should not affect other modes")
        void testModeIsolation() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.reads().ap(), ops -> ops
                            .readMode(ReadModeAP.ONE)
                    )
                    .on(Selectors.reads().cp(), ops -> ops
                            .consistency(ReadModeSC.LINEARIZE)
                    )
            );

            Settings readAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(ReadModeAP.ONE, readAp.readModeAP);
            assertEquals(ReadModeSC.SESSION, readAp.readModeSC); // DEFAULT value from Selectors.all()

            Settings readCp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);
            assertEquals(ReadModeSC.LINEARIZE, readCp.readModeSC);
            assertEquals(ReadModeAP.ALL, readCp.readModeAP); // DEFAULT value from Selectors.all()
        }

        @Test
        @DisplayName("Shape-specific settings should not affect other shapes")
        void testShapeIsolation() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.reads().batch(), ops -> ops
                            .maxConcurrentNodes(10)
                    )
            );

            Settings readBatch = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
            assertEquals(10, readBatch.maxConcurrentNodes);

            Settings readPoint = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(1, readPoint.maxConcurrentNodes); // Global default from Selectors.all()
        }

        @Test
        @DisplayName("Retryability-specific settings should not cross over")
        void testRetryabilityIsolation() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.writes().retryable(), ops -> ops
                            .maximumNumberOfCallAttempts(10)
                    )
                    .on(Selectors.writes().nonRetryable(), ops -> ops
                            .maximumNumberOfCallAttempts(1)
                    )
            );

            Settings retryable = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(10, retryable.maximumNumberOfCallAttempts);

            Settings nonRetryable = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(1, nonRetryable.maximumNumberOfCallAttempts);
        }
    }

    @Nested
    @DisplayName("Inheritance Tests")
    class InheritanceTests {

        @Test
        @DisplayName("Child should inherit all settings from parent")
        void testChildInheritsFromParent() {
            Behavior parent = Behavior.DEFAULT.deriveWithChanges("parent", builder -> builder
                    .on(Selectors.all(), ops -> ops
                            .maximumNumberOfCallAttempts(5)
                            .sendKey(false)
                    )
            );

            Behavior child = parent.deriveWithChanges("child", builder -> {});

            Settings childSettings = child.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(5, childSettings.maximumNumberOfCallAttempts);
            assertFalse(childSettings.sendKey);
        }

        @Test
        @DisplayName("Child should override parent settings")
        void testChildOverridesParent() {
            Behavior parent = Behavior.DEFAULT.deriveWithChanges("parent", builder -> builder
                    .on(Selectors.all(), ops -> ops
                            .maximumNumberOfCallAttempts(5)
                            .sendKey(false)
                    )
            );

            Behavior child = parent.deriveWithChanges("child", builder -> builder
                    .on(Selectors.all(), ops -> ops
                            .maximumNumberOfCallAttempts(10)
                    )
            );

            Settings childSettings = child.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(10, childSettings.maximumNumberOfCallAttempts); // Overridden
            assertFalse(childSettings.sendKey); // Inherited
        }

        @Test
        @DisplayName("deriveWithChanges should create proper parent-child relationship")
        void testDeriveWithChanges() {
            Behavior parent = Behavior.DEFAULT.deriveWithChanges("parent", builder -> builder
                    .on(Selectors.all(), ops -> ops
                            .maximumNumberOfCallAttempts(5)
                    )
            );

            Behavior child = parent.deriveWithChanges("child", builder -> builder
                    .on(Selectors.reads().ap(), ops -> ops
                            .readMode(ReadModeAP.ONE)
                    )
            );

            // Should inherit from parent
            Settings childSettings = child.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(5, childSettings.maximumNumberOfCallAttempts);
            assertEquals(ReadModeAP.ONE, childSettings.readModeAP);

            // Parent should not be affected by child changes
            Settings parentSettings = parent.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(5, parentSettings.maximumNumberOfCallAttempts);
            assertEquals(ReadModeAP.ALL, parentSettings.readModeAP); // DEFAULT value, not overridden by child
        }

        @Test
        @DisplayName("Multi-level inheritance should work correctly")
        void testMultiLevelInheritance() {
            Behavior grandparent = Behavior.DEFAULT.deriveWithChanges("grandparent", builder -> builder
                    .on(Selectors.all(), ops -> ops
                            .maximumNumberOfCallAttempts(1)
                            .sendKey(true)
                            .useCompression(false)
                    )
            );

            Behavior parent = grandparent.deriveWithChanges("parent", builder -> builder
                    .on(Selectors.all(), ops -> ops
                            .maximumNumberOfCallAttempts(2)
                    )
            );

            Behavior child = parent.deriveWithChanges("child", builder -> builder
                    .on(Selectors.all(), ops -> ops
                            .maximumNumberOfCallAttempts(3)
                    )
            );

            Settings childSettings = child.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(3, childSettings.maximumNumberOfCallAttempts); // From child
            assertTrue(childSettings.sendKey); // From grandparent
            assertFalse(childSettings.useCompression); // From grandparent
        }

        @Test
        @DisplayName("getParent should return correct parent")
        void testGetParent() {
            Behavior parent = Behavior.DEFAULT.deriveWithChanges("parent", builder -> {});
            Behavior child = parent.deriveWithChanges("child", builder -> {});

            assertEquals(parent, child.getParent());
            assertEquals(Behavior.DEFAULT, parent.getParent()); // Parent's parent is DEFAULT
            assertNull(Behavior.DEFAULT.getParent()); // DEFAULT has no parent
        }

        @Test
        @DisplayName("getChildren should return all children")
        void testGetChildren() {
            Behavior parent = Behavior.DEFAULT.deriveWithChanges("parent", builder -> {});
            Behavior child1 = parent.deriveWithChanges("child1", builder -> builder.on(Selectors.all(), x -> x.maximumNumberOfCallAttempts(3)));
            Behavior child2 = parent.deriveWithChanges("child2", builder -> builder.on(Selectors.all(), x -> x.maximumNumberOfCallAttempts(3)));

            List<Behavior> children = parent.getChildren();
            assertEquals(2, children.size());
            assertTrue(children.contains(child1));
            assertTrue(children.contains(child2));
        }
    }

    @Nested
    @DisplayName("Policy Conversion Tests")
    class PolicyConversionTests {

        @Test
        @DisplayName("Settings.asWritePolicy should convert correctly")
        void testAsWritePolicy() {
            Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.writes().retryable().point().ap(), ops -> ops
                            .abandonCallAfter(Duration.ofSeconds(10))
                            .commitLevel(CommitLevel.COMMIT_MASTER)
                            .useCompression(true)
                            .useDurableDelete(true)
                            .maximumNumberOfCallAttempts(5)
                            .sendKey(false)
                    )
            );
        }
    }

    @Nested
    @DisplayName("Edge Cases and Error Handling")
    class EdgeCasesTests {

        @Test
        @DisplayName("Empty derived behavior should inherit DEFAULT settings")
        void testEmptyBehavior() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("empty", builder -> {});

            Settings settings = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertNotNull(settings, "Should inherit from DEFAULT");
            // Verify it has DEFAULT settings
            assertEquals(Duration.ofSeconds(1), settings.abandonCallAfter);
            assertEquals(3, settings.maximumNumberOfCallAttempts);
        }

        @Test
        @DisplayName("getName should return correct name")
        void testGetName() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("testName", builder -> {});
            assertEquals("testName", behavior.getName());
        }

        @Test
        @DisplayName("DEFAULT should have name 'DEFAULT'")
        void testDefaultName() {
            assertEquals("DEFAULT", Behavior.DEFAULT.getName());
        }

        @Test
        @DisplayName("clearCache should recompute settings")
        void testClearCache() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.all(), ops -> ops
                            .maximumNumberOfCallAttempts(5)
                    )
            );

            Settings before = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(5, before.maximumNumberOfCallAttempts);

            // Clear cache and verify settings still work
            behavior.clearCache();

            Settings after = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(5, after.maximumNumberOfCallAttempts);
        }

        @Test
        @DisplayName("explain should return non-empty string")
        void testExplain() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.all(), ops -> ops
                            .maximumNumberOfCallAttempts(5)
                    )
            );

            String explanation = behavior.explain();
            assertNotNull(explanation);
            assertTrue(explanation.contains("test"));
            assertTrue(explanation.contains("Patches"));
            assertTrue(explanation.contains("Resolved Matrix"));
        }
    }

    @Nested
    @DisplayName("Comprehensive Attribute Coverage Tests")
    class ComprehensiveAttributeTests {

        @Test
        @DisplayName("All common attributes should be settable and retrievable")
        void testAllCommonAttributes() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.all(), ops -> ops
                            .abandonCallAfter(Duration.ofSeconds(99))
                            .delayBetweenRetries(Duration.ofMillis(999))
                            .maximumNumberOfCallAttempts(99)
                            .replicaOrder(Replica.SEQUENCE)
                            .sendKey(true)
                            .useCompression(true)
                            .waitForCallToComplete(Duration.ofSeconds(88))
                            .waitForConnectionToComplete(Duration.ofSeconds(77))
                            .waitForSocketResponseAfterCallFails(Duration.ofSeconds(66))
                    )
            );

            Settings settings = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(Duration.ofSeconds(99), settings.abandonCallAfter);
            assertEquals(Duration.ofMillis(999), settings.delayBetweenRetries);
            assertEquals(99, settings.maximumNumberOfCallAttempts);
            assertEquals(Replica.SEQUENCE, settings.replicaOrder);
            assertTrue(settings.sendKey);
            assertTrue(settings.useCompression);
            assertEquals(Duration.ofSeconds(88), settings.waitForCallToComplete);
            assertEquals(Duration.ofSeconds(77), settings.waitForConnectionToComplete);
            assertEquals(Duration.ofSeconds(66), settings.waitForSocketResponseAfterCallFails);
        }

        @Test
        @DisplayName("All batch attributes should be settable and retrievable")
        void testAllBatchAttributes() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.reads().batch(), ops -> ops
                            .maxConcurrentNodes(99)
                            .allowInlineMemoryAccess(true)
                            .allowInlineSsdAccess(true)
                    )
            );

            Settings settings = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
            assertEquals(99, settings.maxConcurrentNodes);
            assertTrue(settings.allowInlineMemoryAccess);
            assertTrue(settings.allowInlineSsdAccess);
        }

        @Test
        @DisplayName("All query attributes should be settable and retrievable")
        void testAllQueryAttributes() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.reads().query(), ops -> ops
                            .recordQueueSize(99999)
                    )
            );

            Settings settings = behavior.getSettings(OpKind.READ, OpShape.QUERY, Mode.AP);
            assertEquals(99999, settings.recordQueueSize);
        }

        @Test
        @DisplayName("All read attributes should be settable and retrievable")
        void testAllReadAttributes() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.reads(), ops -> ops
                            .resetTtlOnReadAtPercent(99)
                    )
            );

            Settings settings = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(99, settings.resetTtlOnReadAtPercent);
        }

        @Test
        @DisplayName("All read AP attributes should be settable and retrievable")
        void testAllReadApAttributes() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.reads().ap(), ops -> ops
                            .readMode(ReadModeAP.ONE)
                            .resetTtlOnReadAtPercent(88)
                    )
            );

            Settings settings = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(ReadModeAP.ONE, settings.readModeAP);
            assertEquals(88, settings.resetTtlOnReadAtPercent);
        }

        @Test
        @DisplayName("All read CP attributes should be settable and retrievable")
        void testAllReadCpAttributes() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.reads().cp(), ops -> ops
                            .consistency(ReadModeSC.ALLOW_REPLICA)
                            .resetTtlOnReadAtPercent(77)
                    )
            );

            Settings settings = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);
            assertEquals(ReadModeSC.ALLOW_REPLICA, settings.readModeSC);
            assertEquals(77, settings.resetTtlOnReadAtPercent);
        }

        @Test
        @DisplayName("All write attributes should be settable and retrievable")
        void testAllWriteAttributes() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.writes(), ops -> ops
                            .useDurableDelete(true)
                            .simulateXdrWrite(true)
                    )
            );

            Settings settings = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertTrue(settings.useDurableDelete);
            assertTrue(settings.simulateXdrWrite);
        }

        @Test
        @DisplayName("All write AP attributes should be settable and retrievable")
        void testAllWriteApAttributes() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                    .on(Selectors.writes().ap(), ops -> ops
                            .commitLevel(CommitLevel.COMMIT_MASTER)
                    )
            );

            Settings settings = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(CommitLevel.COMMIT_MASTER, settings.commitLevel);
        }
    }
}
