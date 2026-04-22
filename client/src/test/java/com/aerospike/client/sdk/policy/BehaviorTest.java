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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.policy.Behavior.Selectors;

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
            ResolvedSettings readPointAp =
                Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertNotNull(readPointAp, "READ:POINT:AP settings should exist");

            // Verify common settings are present
            assertEquals(1000, readPointAp.getAbandonCallAfterMs());
            assertEquals(0, readPointAp.getDelayBetweenRetriesMs());
            assertEquals(3, readPointAp.getMaximumNumberOfCallAttempts());
            assertEquals(Replica.SEQUENCE, readPointAp.getReplicaOrder());
            assertTrue(readPointAp.getSendKey());
            assertFalse(readPointAp.getUseCompression());
            assertEquals(30000, readPointAp.getWaitForCallToCompleteMs());
            assertEquals(0, readPointAp.getWaitForConnectionToCompleteMs());
            assertEquals(0, readPointAp.getWaitForSocketResponseAfterCallFailsMs());
        }

        @Test
        @DisplayName("DEFAULT should have all READ AP settings configured")
        void testDefaultReadApSettings() {
            ResolvedSettings readPointAp =
                Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);

            assertEquals(ReadModeAP.ALL, readPointAp.getReadModeAP());
            assertEquals(0, readPointAp.getResetTtlOnReadAtPercent());
        }

        @Test
        @DisplayName("DEFAULT should have all READ CP settings configured")
        void testDefaultReadCpSettings() {
            ResolvedSettings readPointCp =
                Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);

            assertEquals(ReadModeSC.SESSION, readPointCp.getReadModeSC());
            assertEquals(0, readPointCp.getResetTtlOnReadAtPercent());
        }

        @Test
        @DisplayName("DEFAULT should have all BATCH READ settings configured")
        void testDefaultBatchReadSettings() {
            ResolvedSettings readBatchAp =
                Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);

            assertEquals(1, readBatchAp.getMaxConcurrentNodes());
            assertTrue(readBatchAp.getAllowInlineMemoryAccess());
            assertFalse(readBatchAp.getAllowInlineSsdAccess());
        }

        @Test
        @DisplayName("DEFAULT should have all QUERY settings configured")
        void testDefaultQuerySettings() {
            ResolvedSettings readQueryAp =
                Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.QUERY, Mode.AP);

            assertEquals(5000, readQueryAp.getRecordQueueSize());
            assertEquals(6, readQueryAp.getMaximumNumberOfCallAttempts()); // Override from query-specific
        }

        @Test
        @DisplayName("DEFAULT should have all RETRYABLE WRITE settings configured")
        void testDefaultRetryableWriteSettings() {
            ResolvedSettings writeRetryablePointAp =
                Behavior.DEFAULT.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);

            assertFalse(writeRetryablePointAp.getUseDurableDelete());
            assertEquals(3, writeRetryablePointAp.getMaximumNumberOfCallAttempts()); // Override from retryable
            assertFalse(writeRetryablePointAp.getSimulateXdrWrite());
        }

        @Test
        @DisplayName("DEFAULT should have all RETRYABLE WRITE CP settings configured")
        void testDefaultRetryableWriteCpSettings() {
            ResolvedSettings writeRetryableCp =
                Behavior.DEFAULT.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.CP);

            assertTrue(writeRetryableCp.getUseDurableDelete()); // CP override
        }

        @Test
        @DisplayName("DEFAULT should have all NON-RETRYABLE WRITE settings configured")
        void testDefaultNonRetryableWriteSettings() {
            ResolvedSettings writeNonRetryablePointAp =
                Behavior.DEFAULT.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);

            assertFalse(writeNonRetryablePointAp.getUseDurableDelete());
            assertFalse(writeNonRetryablePointAp.getSimulateXdrWrite());
        }

        @Test
        @DisplayName("DEFAULT should have all BATCH WRITE settings configured")
        void testDefaultBatchWriteSettings() {
            ResolvedSettings writeRetryableBatchAp =
                Behavior.DEFAULT.getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Mode.AP);

            assertEquals(1, writeRetryableBatchAp.getMaxConcurrentNodes());
            assertTrue(writeRetryableBatchAp.getAllowInlineMemoryAccess());
            assertFalse(writeRetryableBatchAp.getAllowInlineSsdAccess());
        }

        @Test
        @DisplayName("DEFAULT should have all WRITE AP settings configured")
        void testDefaultWriteApSettings() {
            ResolvedSettings writePointAp =
                Behavior.DEFAULT.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);

            assertEquals(CommitLevel.COMMIT_ALL, writePointAp.getCommitLevel());
        }

        @Test
        @DisplayName("All operation combinations should have settings")
        void testAllOperationCombinationsExist() {
            // Test all READ combinations
            for (OpShape shape : new OpShape[] { OpShape.POINT, OpShape.BATCH, OpShape.QUERY }) {
                for (Mode mode : new Mode[] { Mode.AP, Mode.CP }) {
                    ResolvedSettings settings = Behavior.DEFAULT.getSettings(OpKind.READ, shape, mode);
                    assertNotNull(settings, "READ:" + shape + ":" + mode + " should have settings");
                }
            }

            // Test all WRITE combinations (no QUERY for writes)
            for (OpKind kind : new OpKind[] { OpKind.WRITE_RETRYABLE,
                OpKind.WRITE_NON_RETRYABLE }) {
                for (OpShape shape : new OpShape[] { OpShape.POINT, OpShape.BATCH }) {
                    for (Mode mode : new Mode[] { Mode.AP, Mode.CP }) {
                        ResolvedSettings settings = Behavior.DEFAULT.getSettings(kind, shape, mode);
                        assertNotNull(settings,
                            kind + ":" + shape + ":" + mode + " should have settings");
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
                    .waitForSocketResponseAfterCallFails(Duration.ofSeconds(4))));

            // Verify settings applied to all operations
            ResolvedSettings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(10000, readPointAp.getAbandonCallAfterMs());
            assertEquals(5, readPointAp.getMaximumNumberOfCallAttempts());
            assertFalse(readPointAp.getSendKey());

            ResolvedSettings writePointCp =
                behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.CP);
            assertEquals(10000, writePointCp.getAbandonCallAfterMs());
            assertTrue(writePointCp.getUseCompression());
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
                    .recordQueueSize(10000)));

            // Verify AP read settings
            ResolvedSettings readAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(5, readAp.getMaximumNumberOfCallAttempts());
            assertEquals(ReadModeAP.ONE, readAp.getReadModeAP());
            assertEquals(50, readAp.getResetTtlOnReadAtPercent());

            // Verify CP read settings
            ResolvedSettings readCp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);
            assertEquals(5, readCp.getMaximumNumberOfCallAttempts());
            assertEquals(ReadModeSC.LINEARIZE, readCp.getReadModeSC());
            assertEquals(50, readCp.getResetTtlOnReadAtPercent());

            // Verify AP write settings
            ResolvedSettings writeAp = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(5, writeAp.getMaximumNumberOfCallAttempts());
            assertEquals(CommitLevel.COMMIT_MASTER, writeAp.getCommitLevel());
            assertTrue(writeAp.getUseDurableDelete());

            // Verify batch settings
            ResolvedSettings batchRead = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
            assertEquals(10, batchRead.getMaxConcurrentNodes());

            // Verify query settings
            ResolvedSettings query = behavior.getSettings(OpKind.READ, OpShape.QUERY, Mode.AP);
            assertEquals(10000, query.getRecordQueueSize());
        }

        @Test
        @DisplayName("Selectors.reads() should expose resetTtlOnReadAtPercent")
        void testSelectorsReads() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads(), ops -> ops
                    .resetTtlOnReadAtPercent(50)
                    .maximumNumberOfCallAttempts(10)));

            ResolvedSettings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(50, readPointAp.getResetTtlOnReadAtPercent());
            assertEquals(10, readPointAp.getMaximumNumberOfCallAttempts());
        }

        @Test
        @DisplayName("Selectors.reads().ap() should expose readMode")
        void testSelectorsReadsAp() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().ap(), ops -> ops
                    .readMode(ReadModeAP.ONE)
                    .resetTtlOnReadAtPercent(25)));

            ResolvedSettings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(ReadModeAP.ONE, readPointAp.getReadModeAP());
            assertEquals(25, readPointAp.getResetTtlOnReadAtPercent());

            // CP mode should have DEFAULT readModeAP (from Selectors.all())
            ResolvedSettings readPointCp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);
            assertEquals(ReadModeAP.ALL, readPointCp.getReadModeAP()); // DEFAULT value from Selectors.all()
        }

        @Test
        @DisplayName("Selectors.reads().cp() should expose consistency")
        void testSelectorsReadsCp() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().cp(), ops -> ops
                    .consistency(ReadModeSC.LINEARIZE)
                    .resetTtlOnReadAtPercent(75)));

            ResolvedSettings readPointCp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);
            assertEquals(ReadModeSC.LINEARIZE, readPointCp.getReadModeSC());
            assertEquals(75, readPointCp.getResetTtlOnReadAtPercent());

            // AP mode should have DEFAULT readModeSC (from Selectors.all())
            ResolvedSettings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(ReadModeSC.SESSION, readPointAp.getReadModeSC()); // DEFAULT value from Selectors.all()
        }

        @Test
        @DisplayName("Selectors.reads().get() should configure point reads")
        void testSelectorsReadsGet() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().get(), ops -> ops
                    .maximumNumberOfCallAttempts(7)));

            ResolvedSettings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(7, readPointAp.getMaximumNumberOfCallAttempts());

            // Should not affect batch - should have DEFAULT value
            ResolvedSettings readBatchAp = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
            assertEquals(3, readBatchAp.getMaximumNumberOfCallAttempts()); // DEFAULT value, not overridden
        }

        @Test
        @DisplayName("Selectors.reads().batch() should expose batch-specific methods")
        void testSelectorsReadsBatch() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().batch(), ops -> ops
                    .maxConcurrentNodes(8)
                    .allowInlineMemoryAccess(false)
                    .allowInlineSsdAccess(true)));

            ResolvedSettings readBatchAp = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
            assertEquals(8, readBatchAp.getMaxConcurrentNodes());
            assertFalse(readBatchAp.getAllowInlineMemoryAccess());
            assertTrue(readBatchAp.getAllowInlineSsdAccess());

            // Should not affect point reads - they keep global default
            ResolvedSettings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(1, readPointAp.getMaxConcurrentNodes()); // Global default from Selectors.all()
        }

        @Test
        @DisplayName("Selectors.reads().query() should expose query-specific methods")
        void testSelectorsReadsQuery() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().query(), ops -> ops
                    .recordQueueSize(10000)
                    .maximumNumberOfCallAttempts(8)));

            ResolvedSettings readQueryAp = behavior.getSettings(OpKind.READ, OpShape.QUERY, Mode.AP);
            assertEquals(10000, readQueryAp.getRecordQueueSize());
            assertEquals(8, readQueryAp.getMaximumNumberOfCallAttempts());

            // Should not affect batch
            ResolvedSettings readBatchAp = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
            assertEquals(5000, readBatchAp.getRecordQueueSize());
        }

        @Test
        @DisplayName("Selectors.reads().batch().ap() should expose all relevant methods")
        void testSelectorsReadsBatchAp() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().batch().ap(), ops -> ops
                    .maxConcurrentNodes(16)
                    .readMode(ReadModeAP.ALL)
                    .resetTtlOnReadAtPercent(33)));

            ResolvedSettings readBatchAp = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
            assertEquals(16, readBatchAp.getMaxConcurrentNodes());
            assertEquals(ReadModeAP.ALL, readBatchAp.getReadModeAP());
            assertEquals(33, readBatchAp.getResetTtlOnReadAtPercent());
        }

        @Test
        @DisplayName("Selectors.writes() should expose write-specific methods")
        void testSelectorsWrites() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes(), ops -> ops
                    .useDurableDelete(true)
                    .simulateXdrWrite(true)));

            // Should apply to both retryable and non-retryable
            ResolvedSettings writeRetryable =
                behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertTrue(writeRetryable.getUseDurableDelete());
            assertTrue(writeRetryable.getSimulateXdrWrite());

            ResolvedSettings writeNonRetryable =
                behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
            assertTrue(writeNonRetryable.getUseDurableDelete());
            assertTrue(writeNonRetryable.getSimulateXdrWrite());
        }

        @Test
        @DisplayName("Selectors.writes().ap() should expose commitLevel")
        void testSelectorsWritesAp() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes().ap(), ops -> ops
                    .commitLevel(CommitLevel.COMMIT_MASTER)));

            ResolvedSettings writePointAp =
                behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(CommitLevel.COMMIT_MASTER, writePointAp.getCommitLevel());

            // Should not affect CP
            ResolvedSettings writePointCp =
                behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.CP);
            assertEquals(CommitLevel.COMMIT_ALL, writePointCp.getCommitLevel());
        }

        @Test
        @DisplayName("Selectors.writes().retryable() should configure only retryable writes")
        void testSelectorsWritesRetryable() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes().retryable(), ops -> ops
                    .maximumNumberOfCallAttempts(10)));

            ResolvedSettings writeRetryable =
                behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(10, writeRetryable.getMaximumNumberOfCallAttempts());

            // Should not affect non-retryable - should have DEFAULT value
            ResolvedSettings writeNonRetryable =
                behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(1, writeNonRetryable.getMaximumNumberOfCallAttempts()); // DEFAULT value from Selectors.all()
        }

        @Test
        @DisplayName("Selectors.writes().nonRetryable() should configure only non-retryable writes")
        void testSelectorsWritesNonRetryable() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes().nonRetryable(), ops -> ops
                    .maximumNumberOfCallAttempts(1)));

            ResolvedSettings writeNonRetryable =
                behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(1, writeNonRetryable.getMaximumNumberOfCallAttempts());

            // Should not affect retryable - should have DEFAULT value
            ResolvedSettings writeRetryable =
                behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(3, writeRetryable.getMaximumNumberOfCallAttempts()); // DEFAULT value for retryable writes
        }

        @Test
        @DisplayName("Selectors.writes().point() should configure point writes (retryability-agnostic)")
        void testSelectorsWritesPoint() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes().point(), ops -> ops
                    .maximumNumberOfCallAttempts(5)));

            // Should apply to both retryable and non-retryable point writes
            ResolvedSettings writeRetryablePoint =
                behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(5, writeRetryablePoint.getMaximumNumberOfCallAttempts());

            ResolvedSettings writeNonRetryablePoint =
                behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(5, writeNonRetryablePoint.getMaximumNumberOfCallAttempts());

            // Should not affect batch - should have DEFAULT value
            ResolvedSettings writeRetryableBatch =
                behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Mode.AP);
            assertEquals(3, writeRetryableBatch.getMaximumNumberOfCallAttempts()); // DEFAULT value for retryable writes
        }

        @Test
        @DisplayName("Selectors.writes().batch() should expose batch-specific methods")
        void testSelectorsWritesBatch() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes().batch(), ops -> ops
                    .maxConcurrentNodes(12)
                    .allowInlineMemoryAccess(true)));

            // Should apply to both retryable and non-retryable batch writes
            ResolvedSettings writeRetryableBatch =
                behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Mode.AP);
            assertEquals(12, writeRetryableBatch.getMaxConcurrentNodes());

            ResolvedSettings writeNonRetryableBatch =
                behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, Mode.AP);
            assertEquals(12, writeNonRetryableBatch.getMaxConcurrentNodes());
        }

        @Test
        @DisplayName("Selectors.writes().retryable().point().ap() should expose all relevant methods")
        void testSelectorsWritesRetryablePointAp() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes().retryable().point().ap(), ops -> ops
                    .maximumNumberOfCallAttempts(9)
                    .commitLevel(CommitLevel.COMMIT_ALL)
                    .useDurableDelete(false)));

            ResolvedSettings settings =
                behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(9, settings.getMaximumNumberOfCallAttempts());
            assertEquals(CommitLevel.COMMIT_ALL, settings.getCommitLevel());
            assertFalse(settings.getUseDurableDelete());

            // Should not affect non-retryable - should have DEFAULT value
            ResolvedSettings nonRetryable =
                behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(1, nonRetryable.getMaximumNumberOfCallAttempts()); // DEFAULT value from Selectors.all()
        }

        @Test
        @DisplayName("Selectors.writes().point().ap() should configure both retryable and non-retryable")
        void testSelectorsWritesPointAp() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes().point().ap(), ops -> ops
                    .commitLevel(CommitLevel.COMMIT_MASTER)));

            // Should apply to both retryable and non-retryable
            ResolvedSettings writeRetryable =
                behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(CommitLevel.COMMIT_MASTER, writeRetryable.getCommitLevel());

            ResolvedSettings writeNonRetryable =
                behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(CommitLevel.COMMIT_MASTER, writeNonRetryable.getCommitLevel());
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
                    .maximumNumberOfCallAttempts(1))
                .on(Selectors.reads(), ops -> ops
                    .maximumNumberOfCallAttempts(2))
                .on(Selectors.reads().batch(), ops -> ops
                    .maximumNumberOfCallAttempts(3))
                .on(Selectors.reads().batch().ap(), ops -> ops
                    .maximumNumberOfCallAttempts(4)));

            // Most specific wins
            ResolvedSettings readBatchAp = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
            assertEquals(4, readBatchAp.getMaximumNumberOfCallAttempts());

            // Less specific for CP
            ResolvedSettings readBatchCp = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.CP);
            assertEquals(3, readBatchCp.getMaximumNumberOfCallAttempts());

            // Even less specific for point
            ResolvedSettings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(2, readPointAp.getMaximumNumberOfCallAttempts());

            // Least specific for writes
            ResolvedSettings writePointAp =
                behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(1, writePointAp.getMaximumNumberOfCallAttempts());
        }

        @Test
        @DisplayName("Later configurations should override earlier ones at same specificity")
        void testLaterOverridesEarlier() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().ap(), ops -> ops
                    .readMode(ReadModeAP.ONE))
                .on(Selectors.reads().ap(), ops -> ops
                    .readMode(ReadModeAP.ALL)));

            ResolvedSettings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(ReadModeAP.ALL, readPointAp.getReadModeAP());
        }

        @Test
        @DisplayName("Different attributes should merge, not replace")
        void testAttributesMerge() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.all(), ops -> ops
                    .maximumNumberOfCallAttempts(5)
                    .sendKey(true))
                .on(Selectors.reads().ap(), ops -> ops
                    .readMode(ReadModeAP.ONE)));

            ResolvedSettings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            // Should have both
            assertEquals(5, readPointAp.getMaximumNumberOfCallAttempts());
            assertTrue(readPointAp.getSendKey());
            assertEquals(ReadModeAP.ONE, readPointAp.getReadModeAP());
        }

        @Test
        @DisplayName("Mode-specific settings should not affect other modes")
        void testModeIsolation() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().ap(), ops -> ops
                    .readMode(ReadModeAP.ONE))
                .on(Selectors.reads().cp(), ops -> ops
                    .consistency(ReadModeSC.LINEARIZE)));

            ResolvedSettings readAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(ReadModeAP.ONE, readAp.getReadModeAP());
            assertEquals(ReadModeSC.SESSION, readAp.getReadModeSC()); // DEFAULT value from Selectors.all()

            ResolvedSettings readCp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);
            assertEquals(ReadModeSC.LINEARIZE, readCp.getReadModeSC());
            assertEquals(ReadModeAP.ALL, readCp.getReadModeAP()); // DEFAULT value from Selectors.all()
        }

        @Test
        @DisplayName("Shape-specific settings should not affect other shapes")
        void testShapeIsolation() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().batch(), ops -> ops
                    .maxConcurrentNodes(10)));

            ResolvedSettings readBatch = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
            assertEquals(10, readBatch.getMaxConcurrentNodes());

            ResolvedSettings readPoint = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(1, readPoint.getMaxConcurrentNodes()); // Global default from Selectors.all()
        }

        @Test
        @DisplayName("Retryability-specific settings should not cross over")
        void testRetryabilityIsolation() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes().retryable(), ops -> ops
                    .maximumNumberOfCallAttempts(10))
                .on(Selectors.writes().nonRetryable(), ops -> ops
                    .maximumNumberOfCallAttempts(1)));

            ResolvedSettings retryable =
                behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(10, retryable.getMaximumNumberOfCallAttempts());

            ResolvedSettings nonRetryable =
                behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(1, nonRetryable.getMaximumNumberOfCallAttempts());
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
                    .sendKey(false)));

            Behavior child = parent.deriveWithChanges("child", builder -> {
            });

            ResolvedSettings childSettings = child.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(5, childSettings.getMaximumNumberOfCallAttempts());
            assertFalse(childSettings.getSendKey());
        }

        @Test
        @DisplayName("Child should override parent settings")
        void testChildOverridesParent() {
            Behavior parent = Behavior.DEFAULT.deriveWithChanges("parent", builder -> builder
                .on(Selectors.all(), ops -> ops
                    .maximumNumberOfCallAttempts(5)
                    .sendKey(false)));

            Behavior child = parent.deriveWithChanges("child", builder -> builder
                .on(Selectors.all(), ops -> ops
                    .maximumNumberOfCallAttempts(10)));

            ResolvedSettings childSettings = child.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(10, childSettings.getMaximumNumberOfCallAttempts()); // Overridden
            assertFalse(childSettings.getSendKey()); // Inherited
        }

        @Test
        @DisplayName("deriveWithChanges should create proper parent-child relationship")
        void testDeriveWithChanges() {
            Behavior parent = Behavior.DEFAULT.deriveWithChanges("parent", builder -> builder
                .on(Selectors.all(), ops -> ops
                    .maximumNumberOfCallAttempts(5)));

            Behavior child = parent.deriveWithChanges("child", builder -> builder
                .on(Selectors.reads().ap(), ops -> ops
                    .readMode(ReadModeAP.ONE)));

            // Should inherit from parent
            ResolvedSettings childSettings = child.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(5, childSettings.getMaximumNumberOfCallAttempts());
            assertEquals(ReadModeAP.ONE, childSettings.getReadModeAP());

            // Parent should not be affected by child changes
            ResolvedSettings parentSettings = parent.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(5, parentSettings.getMaximumNumberOfCallAttempts());
            assertEquals(ReadModeAP.ALL, parentSettings.getReadModeAP()); // DEFAULT value, not overridden by child
        }

        @Test
        @DisplayName("Multi-level inheritance should work correctly")
        void testMultiLevelInheritance() {
            Behavior grandparent = Behavior.DEFAULT.deriveWithChanges("grandparent",
                builder -> builder
                    .on(Selectors.all(), ops -> ops
                        .maximumNumberOfCallAttempts(1)
                        .sendKey(true)
                        .useCompression(false)));

            Behavior parent = grandparent.deriveWithChanges("parent", builder -> builder
                .on(Selectors.all(), ops -> ops
                    .maximumNumberOfCallAttempts(2)));

            Behavior child = parent.deriveWithChanges("child", builder -> builder
                .on(Selectors.all(), ops -> ops
                    .maximumNumberOfCallAttempts(3)));

            ResolvedSettings childSettings = child.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(3, childSettings.getMaximumNumberOfCallAttempts()); // From child
            assertTrue(childSettings.getSendKey()); // From grandparent
            assertFalse(childSettings.getUseCompression()); // From grandparent
        }

        @Test
        @DisplayName("getParent should return correct parent")
        void testGetParent() {
            Behavior parent = Behavior.DEFAULT.deriveWithChanges("parent", builder -> {
            });
            Behavior child = parent.deriveWithChanges("child", builder -> {
            });

            assertEquals(parent, child.getParent());
            assertEquals(Behavior.DEFAULT, parent.getParent()); // Parent's parent is DEFAULT
            assertNull(Behavior.DEFAULT.getParent()); // DEFAULT has no parent
        }

        @Test
        @DisplayName("getChildren should return all children")
        void testGetChildren() {
            Behavior parent = Behavior.DEFAULT.deriveWithChanges("parent", builder -> {
            });
            Behavior child1 = parent.deriveWithChanges("child1",
                builder -> builder.on(Selectors.all(), x -> x.maximumNumberOfCallAttempts(3)));
            Behavior child2 = parent.deriveWithChanges("child2",
                builder -> builder.on(Selectors.all(), x -> x.maximumNumberOfCallAttempts(3)));

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
                    .sendKey(false)));
        }
    }

    @Nested
    @DisplayName("Edge Cases and Error Handling")
    class EdgeCasesTests {

        @Test
        @DisplayName("Empty derived behavior should inherit DEFAULT settings")
        void testEmptyBehavior() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("empty", builder -> {
            });

            ResolvedSettings settings = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertNotNull(settings, "Should inherit from DEFAULT");
            // Verify it has DEFAULT settings
            assertEquals(1000, settings.getAbandonCallAfterMs());
            assertEquals(3, settings.getMaximumNumberOfCallAttempts());
        }

        @Test
        @DisplayName("getName should return correct name")
        void testGetName() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("testName", builder -> {
            });
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
                    .maximumNumberOfCallAttempts(5)));

            ResolvedSettings before = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(5, before.getMaximumNumberOfCallAttempts());

            // Clear cache and verify settings still work
            behavior.clearCache();

            ResolvedSettings after = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(5, after.getMaximumNumberOfCallAttempts());
        }

        @Test
        @DisplayName("explain should return non-empty string")
        void testExplain() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.all(), ops -> ops
                    .maximumNumberOfCallAttempts(5)));

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
                    .waitForSocketResponseAfterCallFails(Duration.ofSeconds(66))));

            ResolvedSettings settings = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(99000, settings.getAbandonCallAfterMs());
            assertEquals(999, settings.getDelayBetweenRetriesMs());
            assertEquals(99, settings.getMaximumNumberOfCallAttempts());
            assertEquals(Replica.SEQUENCE, settings.getReplicaOrder());
            assertTrue(settings.getSendKey());
            assertTrue(settings.getUseCompression());
            assertEquals(88000, settings.getWaitForCallToCompleteMs());
            assertEquals(77000, settings.getWaitForConnectionToCompleteMs());
            assertEquals(66000, settings.getWaitForSocketResponseAfterCallFailsMs());
        }

        @Test
        @DisplayName("All batch attributes should be settable and retrievable")
        void testAllBatchAttributes() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().batch(), ops -> ops
                    .maxConcurrentNodes(99)
                    .allowInlineMemoryAccess(true)
                    .allowInlineSsdAccess(true)));

            ResolvedSettings settings = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
            assertEquals(99, settings.getMaxConcurrentNodes());
            assertTrue(settings.getAllowInlineMemoryAccess());
            assertTrue(settings.getAllowInlineSsdAccess());
        }

        @Test
        @DisplayName("All query attributes should be settable and retrievable")
        void testAllQueryAttributes() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().query(), ops -> ops
                    .recordQueueSize(99999)));

            ResolvedSettings settings = behavior.getSettings(OpKind.READ, OpShape.QUERY, Mode.AP);
            assertEquals(99999, settings.getRecordQueueSize());
        }

        @Test
        @DisplayName("All read attributes should be settable and retrievable")
        void testAllReadAttributes() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads(), ops -> ops
                    .resetTtlOnReadAtPercent(99)));

            ResolvedSettings settings = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(99, settings.getResetTtlOnReadAtPercent());
        }

        @Test
        @DisplayName("All read AP attributes should be settable and retrievable")
        void testAllReadApAttributes() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().ap(), ops -> ops
                    .readMode(ReadModeAP.ONE)
                    .resetTtlOnReadAtPercent(88)));

            ResolvedSettings settings = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
            assertEquals(ReadModeAP.ONE, settings.getReadModeAP());
            assertEquals(88, settings.getResetTtlOnReadAtPercent());
        }

        @Test
        @DisplayName("All read CP attributes should be settable and retrievable")
        void testAllReadCpAttributes() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().cp(), ops -> ops
                    .consistency(ReadModeSC.ALLOW_REPLICA)
                    .resetTtlOnReadAtPercent(77)));

            ResolvedSettings settings = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);
            assertEquals(ReadModeSC.ALLOW_REPLICA, settings.getReadModeSC());
            assertEquals(77, settings.getResetTtlOnReadAtPercent());
        }

        @Test
        @DisplayName("All write attributes should be settable and retrievable")
        void testAllWriteAttributes() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes(), ops -> ops
                    .useDurableDelete(true)
                    .simulateXdrWrite(true)));

            ResolvedSettings settings =
                behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertTrue(settings.getUseDurableDelete());
            assertTrue(settings.getSimulateXdrWrite());
        }

        @Test
        @DisplayName("All write AP attributes should be settable and retrievable")
        void testAllWriteApAttributes() {
            Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes().ap(), ops -> ops
                    .commitLevel(CommitLevel.COMMIT_MASTER)));

            ResolvedSettings settings =
                behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
            assertEquals(CommitLevel.COMMIT_MASTER, settings.getCommitLevel());
        }
    }
}
