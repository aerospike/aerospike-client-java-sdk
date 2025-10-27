package com.aerospike.client.fluent.policy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.List;

import org.junit.Test;

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
public class BehaviorTests {

    // ========================================
    // DEFAULT Behavior Tests
    // ========================================
    
    @Test
    public void testDefaultCommonSettings() {
        // DEFAULT should have all common settings configured
        // Test all operation types have common settings from Selectors.all()
        Settings readPointAp = Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
        assertNotNull("READ:POINT:AP settings should exist", readPointAp);
        
        // Verify common settings are present
        assertEquals("abandonCallAfter", Duration.ofSeconds(1), readPointAp.abandonCallAfter);
        assertEquals("delayBetweenRetries", Duration.ofMillis(0), readPointAp.delayBetweenRetries);
        assertEquals("maximumNumberOfCallAttempts", Integer.valueOf(3), readPointAp.maximumNumberOfCallAttempts);
        assertEquals(Replica.SEQUENCE, readPointAp.replicaOrder);
        assertTrue(readPointAp.sendKey);
        assertFalse(readPointAp.useCompression);
        assertEquals("waitForCallToComplete", Duration.ofSeconds(30), readPointAp.waitForCallToComplete);
        assertEquals("waitForConnectionToComplete", Duration.ofSeconds(0), readPointAp.waitForConnectionToComplete);
        assertEquals("waitForSocketResponseAfterCallFails", Duration.ofSeconds(0), readPointAp.waitForSocketResponseAfterCallFails);
    }
    
    @Test
    public void testDefaultReadApSettings() {
        // DEFAULT should have all READ AP settings configured
        Settings readPointAp = Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
        
        assertEquals(ReadModeAP.ALL, readPointAp.readModeAP);
        assertEquals("resetTtlOnReadAtPercent", Integer.valueOf(0), readPointAp.resetTtlOnReadAtPercent);
    }
    
    @Test
    public void testDefaultReadCpSettings() {
        // DEFAULT should have all READ CP settings configured
        Settings readPointCp = Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);
        
        assertEquals(ReadModeSC.SESSION, readPointCp.readModeSC);
        assertEquals("resetTtlOnReadAtPercent", Integer.valueOf(0), readPointCp.resetTtlOnReadAtPercent);
    }
    
    @Test
    public void testDefaultBatchReadSettings() {
        // DEFAULT should have all BATCH READ settings configured
        Settings readBatchAp = Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
        
        assertEquals(Integer.valueOf(1), readBatchAp.maxConcurrentNodes);
        assertTrue(readBatchAp.allowInlineMemoryAccess);
        assertFalse(readBatchAp.allowInlineSsdAccess);
    }
    
    @Test
    public void testDefaultQuerySettings() {
        // DEFAULT should have all QUERY settings configured
        Settings readQueryAp = Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.QUERY, Mode.AP);
        
        assertEquals(Integer.valueOf(5000), readQueryAp.recordQueueSize);
        assertEquals(Integer.valueOf(6), readQueryAp.maximumNumberOfCallAttempts); // Override from query-specific
    }
    
    @Test
    public void testDefaultRetryableWriteSettings() {
        // DEFAULT should have all RETRYABLE WRITE settings configured
        Settings writeRetryablePointAp = Behavior.DEFAULT.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
        
        assertFalse(writeRetryablePointAp.useDurableDelete);
        assertEquals(Integer.valueOf(3), writeRetryablePointAp.maximumNumberOfCallAttempts); // Override from retryable
        assertFalse(writeRetryablePointAp.simulateXdrWrite);
    }
    
    @Test
    public void testDefaultRetryableWriteCpSettings() {
        // DEFAULT should have all RETRYABLE WRITE CP settings configured
        Settings writeRetryableCp = Behavior.DEFAULT.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.CP);
        
        assertTrue(writeRetryableCp.useDurableDelete); // CP override
    }
    
    @Test
    public void testDefaultNonRetryableWriteSettings() {
        // DEFAULT should have all NON-RETRYABLE WRITE settings configured
        Settings writeNonRetryablePointAp = Behavior.DEFAULT.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
        
        assertFalse(writeNonRetryablePointAp.useDurableDelete);
        assertFalse(writeNonRetryablePointAp.simulateXdrWrite);
    }
    
    @Test
    public void testDefaultBatchWriteSettings() {
        // DEFAULT should have all BATCH WRITE settings configured
        Settings writeRetryableBatchAp = Behavior.DEFAULT.getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Mode.AP);
        
        assertEquals(Integer.valueOf(1), writeRetryableBatchAp.maxConcurrentNodes);
        assertTrue(writeRetryableBatchAp.allowInlineMemoryAccess);
        assertFalse(writeRetryableBatchAp.allowInlineSsdAccess);
    }
    
    @Test
    public void testDefaultWriteApSettings() {
        // DEFAULT should have all WRITE AP settings configured
        Settings writePointAp = Behavior.DEFAULT.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
        
        assertEquals(CommitLevel.COMMIT_ALL, writePointAp.commitLevel);
    }
    
    @Test
    public void testAllOperationCombinationsExist() {
        // All operation combinations should have settings
        // Test all READ combinations
        for (OpShape shape : new OpShape[]{OpShape.POINT, OpShape.BATCH, OpShape.QUERY}) {
            for (Mode mode : new Mode[]{Mode.AP, Mode.CP}) {
                Settings settings = Behavior.DEFAULT.getSettings(OpKind.READ, shape, mode);
                assertNotNull("READ:" + shape + ":" + mode + " should have settings", settings);
            }
        }
        
        // Test all WRITE combinations (no QUERY for writes)
        for (OpKind kind : new OpKind[]{OpKind.WRITE_RETRYABLE, OpKind.WRITE_NON_RETRYABLE}) {
            for (OpShape shape : new OpShape[]{OpShape.POINT, OpShape.BATCH}) {
                for (Mode mode : new Mode[]{Mode.AP, Mode.CP}) {
                    Settings settings = Behavior.DEFAULT.getSettings(kind, shape, mode);
                    assertNotNull(kind + ":" + shape + ":" + mode + " should have settings", settings);
                }
            }
        }
    }
    
    // ========================================
    // Selector Permutation Tests
    // ========================================
    
    @Test
    public void testSelectorsAll() {
        // Selectors.all() should expose all common methods
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.all(), ops -> ops
                        .abandonCallAfter(Duration.ofSeconds(10))
                        .delayBetweenRetries(Duration.ofMillis(100))
                        .maximumNumberOfCallAttempts(5)
                        .replicaOrder(Replica.MASTER_PROLES)
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
        assertEquals(Integer.valueOf(5), readPointAp.maximumNumberOfCallAttempts);
        assertFalse(readPointAp.sendKey);
        
        Settings writePointCp = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.CP);
        assertEquals(Duration.ofSeconds(10), writePointCp.abandonCallAfter);
        assertTrue(writePointCp.useCompression);
    }
    
    @Test
    public void testSelectorsAllWithModeSpecificSettings() {
        // Selectors.all() should allow setting all possible settings including mode-specific
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
        assertEquals(Integer.valueOf(5), readAp.maximumNumberOfCallAttempts);
        assertEquals(ReadModeAP.ONE, readAp.readModeAP);
        assertEquals(Integer.valueOf(50), readAp.resetTtlOnReadAtPercent);
        
        // Verify CP read settings
        Settings readCp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);
        assertEquals(Integer.valueOf(5), readCp.maximumNumberOfCallAttempts);
        assertEquals(ReadModeSC.LINEARIZE, readCp.readModeSC);
        assertEquals(Integer.valueOf(50), readCp.resetTtlOnReadAtPercent);
        
        // Verify AP write settings
        Settings writeAp = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(5), writeAp.maximumNumberOfCallAttempts);
        assertEquals(CommitLevel.COMMIT_MASTER, writeAp.commitLevel);
        assertTrue(writeAp.useDurableDelete);
        
        // Verify batch settings
        Settings batchRead = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
        assertEquals(Integer.valueOf(10), batchRead.maxConcurrentNodes);
        
        // Verify query settings
        Settings query = behavior.getSettings(OpKind.READ, OpShape.QUERY, Mode.AP);
        assertEquals(Integer.valueOf(10000), query.recordQueueSize);
    }
    
    @Test
    public void testSelectorsReads() {
        // Selectors.reads() should expose resetTtlOnReadAtPercent
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads(), ops -> ops
                        .resetTtlOnReadAtPercent(50)
                        .maximumNumberOfCallAttempts(10)
                )
        );
        
        Settings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(50), readPointAp.resetTtlOnReadAtPercent);
        assertEquals(Integer.valueOf(10), readPointAp.maximumNumberOfCallAttempts);
    }
    
    @Test
    public void testSelectorsReadsAp() {
        // Selectors.reads().ap() should expose readMode
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().ap(), ops -> ops
                        .readMode(ReadModeAP.ONE)
                        .resetTtlOnReadAtPercent(25)
                )
        );
        
        Settings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
        assertEquals(ReadModeAP.ONE, readPointAp.readModeAP);
        assertEquals(Integer.valueOf(25), readPointAp.resetTtlOnReadAtPercent);
        
        // Should not affect CP mode
        Settings readPointCp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);
        assertEquals(ReadModeAP.ALL, readPointCp.readModeAP);
    }
    
    @Test
    public void testSelectorsReadsCp() {
        // Selectors.reads().cp() should expose consistency
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().cp(), ops -> ops
                        .consistency(ReadModeSC.LINEARIZE)
                        .resetTtlOnReadAtPercent(75)
                )
        );
        
        Settings readPointCp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);
        assertEquals(ReadModeSC.LINEARIZE, readPointCp.readModeSC);
        assertEquals(Integer.valueOf(75), readPointCp.resetTtlOnReadAtPercent);
        
        // Should not affect AP mode
        Settings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
        assertEquals(ReadModeSC.SESSION, readPointAp.readModeSC);
    }
    
    @Test
    public void testSelectorsReadsGet() {
        // Selectors.reads().get() should configure point reads
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().get(), ops -> ops
                        .maximumNumberOfCallAttempts(7)
                )
        );
        
        Settings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(7), readPointAp.maximumNumberOfCallAttempts);
        
        // Should not affect batch - should have DEFAULT value
        Settings readBatchAp = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
        assertEquals(Integer.valueOf(3), readBatchAp.maximumNumberOfCallAttempts); // DEFAULT value, not overridden
    }
    
    @Test
    public void testSelectorsReadsBatch() {
        // Selectors.reads().batch() should expose batch-specific methods
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().batch(), ops -> ops
                        .maxConcurrentNodes(8)
                        .allowInlineMemoryAccess(false)
                        .allowInlineSsdAccess(true)
                )
        );
        
        Settings readBatchAp = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
        assertEquals(Integer.valueOf(8), readBatchAp.maxConcurrentNodes);
        assertFalse(readBatchAp.allowInlineMemoryAccess);
        assertTrue(readBatchAp.allowInlineSsdAccess);
        
        // Should not affect point reads
        Settings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
        assertNull(readPointAp.maxConcurrentNodes);
    }
    
    @Test
    public void testSelectorsReadsQuery() {
        // Selectors.reads().query() should expose query-specific methods
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().query(), ops -> ops
                        .recordQueueSize(10000)
                        .maximumNumberOfCallAttempts(8)
                )
        );
        
        Settings readQueryAp = behavior.getSettings(OpKind.READ, OpShape.QUERY, Mode.AP);
        assertEquals(Integer.valueOf(10000), readQueryAp.recordQueueSize);
        assertEquals(Integer.valueOf(8), readQueryAp.maximumNumberOfCallAttempts);
        
        // Should not affect batch
        Settings readBatchAp = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
        assertNull(readBatchAp.recordQueueSize);
    }
    
    @Test
    public void testSelectorsReadsBatchAp() {
        // Selectors.reads().batch().ap() should expose all relevant methods
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().batch().ap(), ops -> ops
                        .maxConcurrentNodes(16)
                        .readMode(ReadModeAP.ALL)
                        .resetTtlOnReadAtPercent(33)
                )
        );
        
        Settings readBatchAp = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
        assertEquals(Integer.valueOf(16), readBatchAp.maxConcurrentNodes);
        assertEquals(ReadModeAP.ALL, readBatchAp.readModeAP);
        assertEquals(Integer.valueOf(33), readBatchAp.resetTtlOnReadAtPercent);
    }
    
    @Test
    public void testSelectorsWrites() {
        // Selectors.writes() should expose write-specific methods
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
    public void testSelectorsWritesAp() {
        // Selectors.writes().ap() should expose commitLevel
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
    public void testSelectorsWritesRetryable() {
        // Selectors.writes().retryable() should configure only retryable writes
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes().retryable(), ops -> ops
                        .maximumNumberOfCallAttempts(10)
                )
        );
        
        Settings writeRetryable = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(10), writeRetryable.maximumNumberOfCallAttempts);
        
        // Should not affect non-retryable - should have DEFAULT value
        Settings writeNonRetryable = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(1), writeNonRetryable.maximumNumberOfCallAttempts); // DEFAULT value from Selectors.all()
    }
    
    @Test
    public void testSelectorsWritesNonRetryable() {
        // Selectors.writes().nonRetryable() should configure only non-retryable writes
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes().nonRetryable(), ops -> ops
                        .maximumNumberOfCallAttempts(1)
                )
        );
        
        Settings writeNonRetryable = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(1), writeNonRetryable.maximumNumberOfCallAttempts);
        
        // Should not affect retryable - should have DEFAULT value
        Settings writeRetryable = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(3), writeRetryable.maximumNumberOfCallAttempts); // DEFAULT value for retryable writes
    }
    
    @Test
    public void testSelectorsWritesPoint() {
        // Selectors.writes().point() should configure point writes (retryability-agnostic)
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes().point(), ops -> ops
                        .maximumNumberOfCallAttempts(5)
                )
        );
        
        // Should apply to both retryable and non-retryable point writes
        Settings writeRetryablePoint = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(5), writeRetryablePoint.maximumNumberOfCallAttempts);
        
        Settings writeNonRetryablePoint = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(5), writeNonRetryablePoint.maximumNumberOfCallAttempts);
        
        // Should not affect batch - should have DEFAULT value
        Settings writeRetryableBatch = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Mode.AP);
        assertEquals(Integer.valueOf(3), writeRetryableBatch.maximumNumberOfCallAttempts); // DEFAULT value for retryable writes
    }
    
    @Test
    public void testSelectorsWritesBatch() {
        // Selectors.writes().batch() should expose batch-specific methods
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes().batch(), ops -> ops
                        .maxConcurrentNodes(12)
                        .allowInlineMemoryAccess(true)
                )
        );
        
        // Should apply to both retryable and non-retryable batch writes
        Settings writeRetryableBatch = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Mode.AP);
        assertEquals(Integer.valueOf(12), writeRetryableBatch.maxConcurrentNodes);
        
        Settings writeNonRetryableBatch = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, Mode.AP);
        assertEquals(Integer.valueOf(12), writeNonRetryableBatch.maxConcurrentNodes);
    }
    
    @Test
    public void testSelectorsWritesRetryablePointAp() {
        // Selectors.writes().retryable().point().ap() should expose all relevant methods
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes().retryable().point().ap(), ops -> ops
                        .maximumNumberOfCallAttempts(9)
                        .commitLevel(CommitLevel.COMMIT_ALL)
                        .useDurableDelete(false)
                )
        );
        
        Settings settings = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(9), settings.maximumNumberOfCallAttempts);
        assertEquals(CommitLevel.COMMIT_ALL, settings.commitLevel);
        assertFalse(settings.useDurableDelete);
        
        // Should not affect non-retryable - should have DEFAULT value
        Settings nonRetryable = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(1), nonRetryable.maximumNumberOfCallAttempts); // DEFAULT value from Selectors.all()
    }
    
    @Test
    public void testSelectorsWritesPointAp() {
        // Selectors.writes().point().ap() should configure both retryable and non-retryable
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
    
    // ========================================
    // Override and Precedence Tests
    // ========================================
    
    @Test
    public void testSpecificOverridesGeneral() {
        // More specific selectors should override less specific ones
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
        assertEquals(Integer.valueOf(4), readBatchAp.maximumNumberOfCallAttempts);
        
        // Less specific for CP
        Settings readBatchCp = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.CP);
        assertEquals(Integer.valueOf(3), readBatchCp.maximumNumberOfCallAttempts);
        
        // Even less specific for point
        Settings readPointAp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(2), readPointAp.maximumNumberOfCallAttempts);
        
        // Least specific for writes
        Settings writePointAp = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(1), writePointAp.maximumNumberOfCallAttempts);
    }
    
    @Test
    public void testLaterOverridesEarlier() {
        // Later configurations should override earlier ones at same specificity
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
    public void testAttributesMerge() {
        // Different attributes should merge, not replace
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
        assertEquals(Integer.valueOf(5), readPointAp.maximumNumberOfCallAttempts);
        assertTrue(readPointAp.sendKey);
        assertEquals(ReadModeAP.ONE, readPointAp.readModeAP);
    }
    
    @Test
    public void testModeIsolation() {
        // Mode-specific settings should not affect other modes
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
        assertEquals(ReadModeSC.SESSION, readAp.readModeSC);
        
        Settings readCp = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);
        assertEquals(ReadModeSC.LINEARIZE, readCp.readModeSC);
        assertEquals(ReadModeAP.ALL, readCp.readModeAP);
    }
    
    @Test
    public void testShapeIsolation() {
        // Shape-specific settings should not affect other shapes
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().batch(), ops -> ops
                        .maxConcurrentNodes(10)
                )
        );
        
        Settings readBatch = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
        assertEquals(Integer.valueOf(10), readBatch.maxConcurrentNodes);
        
        Settings readPoint = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
        assertNull(readPoint.maxConcurrentNodes);
    }
    
    @Test
    public void testRetryabilityIsolation() {
        // Retryability-specific settings should not cross over
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes().retryable(), ops -> ops
                        .maximumNumberOfCallAttempts(10)
                )
                .on(Selectors.writes().nonRetryable(), ops -> ops
                        .maximumNumberOfCallAttempts(1)
                )
        );
        
        Settings retryable = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(10), retryable.maximumNumberOfCallAttempts);
        
        Settings nonRetryable = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(1), nonRetryable.maximumNumberOfCallAttempts);
    }
    
    // ========================================
    // Inheritance Tests
    // ========================================
    
    @Test
    public void testChildInheritsFromParent() {
        // Child should inherit all settings from parent
        Behavior parent = Behavior.DEFAULT.deriveWithChanges("parent", builder -> builder
                .on(Selectors.all(), ops -> ops
                        .maximumNumberOfCallAttempts(5)
                        .sendKey(false)
                )
        );
        
        Behavior child = parent.deriveWithChanges("child", builder -> {});
        
        Settings childSettings = child.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(5), childSettings.maximumNumberOfCallAttempts);
        assertFalse(childSettings.sendKey);
    }
    
    @Test
    public void testChildOverridesParent() {
        // Child should override parent settings
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
        assertEquals(Integer.valueOf(10), childSettings.maximumNumberOfCallAttempts); // Overridden
        assertFalse(childSettings.sendKey); // Inherited
    }
    
    @Test
    public void testDeriveWithChanges() {
        // deriveWithChanges should create proper parent-child relationship
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
        assertEquals(Integer.valueOf(5), childSettings.maximumNumberOfCallAttempts);
        assertEquals(ReadModeAP.ONE, childSettings.readModeAP);
        
        // Parent should not be affected by child changes
        Settings parentSettings = parent.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(5), parentSettings.maximumNumberOfCallAttempts);
        assertEquals(ReadModeAP.ALL, parentSettings.readModeAP); // DEFAULT value, not overridden by child
    }
    
    @Test
    public void testMultiLevelInheritance() {
        // Multi-level inheritance should work correctly
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
        assertEquals(Integer.valueOf(3), childSettings.maximumNumberOfCallAttempts); // From child
        assertTrue(childSettings.sendKey); // From grandparent
        assertFalse(childSettings.useCompression); // From grandparent
    }
    
    @Test
    public void testGetParent() {
        // getParent should return correct parent
        Behavior parent = Behavior.DEFAULT.deriveWithChanges("parent", builder -> {});
        Behavior child = parent.deriveWithChanges("child", builder -> {});
        
        assertEquals(parent, child.getParent());
        assertEquals(Behavior.DEFAULT, parent.getParent()); // Parent's parent is DEFAULT
        assertNull(Behavior.DEFAULT.getParent()); // DEFAULT has no parent
    }
    
    @Test
    public void testGetChildren() {
        // getChildren should return all children
        Behavior parent = Behavior.DEFAULT.deriveWithChanges("parent", builder -> {});
        Behavior child1 = parent.deriveWithChanges("child1", builder -> builder.on(Selectors.all(), x -> x.maximumNumberOfCallAttempts(3)));
        Behavior child2 = parent.deriveWithChanges("child2", builder -> builder.on(Selectors.all(), x -> x.maximumNumberOfCallAttempts(3)));
        
        List<Behavior> children = parent.getChildren();
        assertEquals(2, children.size());
        assertTrue(children.contains(child1));
        assertTrue(children.contains(child2));
    }
    
    // ========================================
    // Policy Conversion Tests
    // ========================================
    
    @Test
    public void testAsWritePolicy() {
        // Settings.asWritePolicy should convert correctly
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes().retryable().point().ap(), ops -> ops
                        .abandonCallAfter(Duration.ofSeconds(10))
                        .commitLevel(CommitLevel.COMMIT_MASTER)
                        .useCompression(true)
                        .useDurableDelete(true)
                        .maximumNumberOfCallAttempts(5)
                        .sendKey(false)
                )
        );
        
        Settings settings = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
        var policy = settings.asWritePolicy();
        
        assertNotNull(policy);
        assertEquals(CommitLevel.COMMIT_MASTER, policy.commitLevel);
        assertTrue(policy.compress);
        assertTrue(policy.durableDelete);
        assertEquals("maxRetries", Integer.valueOf(4), Integer.valueOf(policy.maxRetries)); // maxRetries = attempts - 1
        assertFalse(policy.sendKey);
        assertEquals("totalTimeout", Integer.valueOf(10000), Integer.valueOf(policy.totalTimeout)); // seconds to millis
    }
    
    @Test
    public void testAsBatchPolicy() {
        // Settings.asBatchPolicy should convert correctly
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().batch().ap(), ops -> ops
                        .abandonCallAfter(Duration.ofSeconds(15))
                        .maxConcurrentNodes(8)
                        .allowInlineMemoryAccess(true)
                        .allowInlineSsdAccess(false)
                        .readMode(ReadModeAP.ALL)
                )
        );
        
        Settings settings = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
        var policy = settings.asBatchPolicy();
        
        assertNotNull(policy);
        assertEquals("maxConcurrentThreads", Integer.valueOf(8), Integer.valueOf(policy.maxConcurrentThreads));
        assertTrue(policy.allowInline);
        assertFalse(policy.allowInlineSSD);
        assertEquals(ReadModeAP.ALL, policy.readModeAP);
        assertEquals("totalTimeout", Integer.valueOf(15000), Integer.valueOf(policy.totalTimeout));
    }
    
    @Test
    public void testAsQueryPolicy() {
        // Settings.asQueryPolicy should convert correctly
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().query().cp(), ops -> ops
                        .recordQueueSize(10000)
                        .consistency(ReadModeSC.LINEARIZE)
                        .maximumNumberOfCallAttempts(10)
                )
        );
        
        Settings settings = behavior.getSettings(OpKind.READ, OpShape.QUERY, Mode.CP);
        var policy = settings.asQueryPolicy();
        
        assertNotNull(policy);
        assertEquals("recordQueueSize", Integer.valueOf(10000), Integer.valueOf(policy.recordQueueSize));
        assertEquals(ReadModeSC.LINEARIZE, policy.readModeSC);
        assertEquals("maxRetries", Integer.valueOf(9), Integer.valueOf(policy.maxRetries));
    }
    
    @Test
    public void testAsReadPolicy() {
        // Settings.asReadPolicy should convert correctly
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().get().ap(), ops -> ops
                        .readMode(ReadModeAP.ONE)
                        .resetTtlOnReadAtPercent(50)
                        .useCompression(true)
                )
        );
        
        Settings settings = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
        var policy = settings.asReadPolicy();
        
        assertNotNull(policy);
        assertEquals(ReadModeAP.ONE, policy.readModeAP);
        assertEquals("readTouchTtlPercent", Integer.valueOf(50), Integer.valueOf(policy.readTouchTtlPercent));
        assertTrue(policy.compress);
    }
    
    // ========================================
    // Edge Cases and Error Handling
    // ========================================
    
    @Test
    public void testEmptyBehavior() {
        // Empty derived behavior should inherit DEFAULT settings
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("empty", builder -> {});
        
        Settings settings = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
        assertNotNull("Should inherit from DEFAULT", settings);
        // Verify it has DEFAULT settings
        assertEquals(Duration.ofSeconds(1), settings.abandonCallAfter);
        assertEquals(Integer.valueOf(3), settings.maximumNumberOfCallAttempts);
    }
    
    @Test
    public void testGetName() {
        // getName should return correct name
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("testName", builder -> {});
        assertEquals("testName", behavior.getName());
    }
    
    @Test
    public void testDefaultName() {
        // DEFAULT should have name 'DEFAULT'
        assertEquals("DEFAULT", Behavior.DEFAULT.getName());
    }
    
    @Test
    public void testClearCache() {
        // clearCache should recompute settings
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.all(), ops -> ops
                        .maximumNumberOfCallAttempts(5)
                )
        );
        
        Settings before = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(5), before.maximumNumberOfCallAttempts);
        
        // Clear cache and verify settings still work
        behavior.clearCache();
        
        Settings after = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(5), after.maximumNumberOfCallAttempts);
    }
    
    @Test
    public void testExplain() {
        // explain should return non-empty string
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
    
    // ========================================
    // Comprehensive Attribute Coverage Tests
    // ========================================
    
    @Test
    public void testAllCommonAttributes() {
        // All common attributes should be settable and retrievable
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.all(), ops -> ops
                        .abandonCallAfter(Duration.ofSeconds(99))
                        .delayBetweenRetries(Duration.ofMillis(999))
                        .maximumNumberOfCallAttempts(99)
                        .replicaOrder(Replica.MASTER_PROLES)
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
        assertEquals(Integer.valueOf(99), settings.maximumNumberOfCallAttempts);
        assertEquals(Replica.MASTER_PROLES, settings.replicaOrder);
        assertTrue(settings.sendKey);
        assertTrue(settings.useCompression);
        assertEquals(Duration.ofSeconds(88), settings.waitForCallToComplete);
        assertEquals(Duration.ofSeconds(77), settings.waitForConnectionToComplete);
        assertEquals(Duration.ofSeconds(66), settings.waitForSocketResponseAfterCallFails);
    }
    
    @Test
    public void testAllBatchAttributes() {
        // All batch attributes should be settable and retrievable
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().batch(), ops -> ops
                        .maxConcurrentNodes(99)
                        .allowInlineMemoryAccess(true)
                        .allowInlineSsdAccess(true)
                )
        );
        
        Settings settings = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
        assertEquals(Integer.valueOf(99), settings.maxConcurrentNodes);
        assertTrue(settings.allowInlineMemoryAccess);
        assertTrue(settings.allowInlineSsdAccess);
    }
    
    @Test
    public void testAllQueryAttributes() {
        // All query attributes should be settable and retrievable
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().query(), ops -> ops
                        .recordQueueSize(99999)
                )
        );
        
        Settings settings = behavior.getSettings(OpKind.READ, OpShape.QUERY, Mode.AP);
        assertEquals(Integer.valueOf(99999), settings.recordQueueSize);
    }
    
    @Test
    public void testAllReadAttributes() {
        // All read attributes should be settable and retrievable
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads(), ops -> ops
                        .resetTtlOnReadAtPercent(99)
                )
        );
        
        Settings settings = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
        assertEquals(Integer.valueOf(99), settings.resetTtlOnReadAtPercent);
    }
    
    @Test
    public void testAllReadApAttributes() {
        // All read AP attributes should be settable and retrievable
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().ap(), ops -> ops
                        .readMode(ReadModeAP.ONE)
                        .resetTtlOnReadAtPercent(88)
                )
        );
        
        Settings settings = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);
        assertEquals(ReadModeAP.ONE, settings.readModeAP);
        assertEquals(Integer.valueOf(88), settings.resetTtlOnReadAtPercent);
    }
    
    @Test
    public void testAllReadCpAttributes() {
        // All read CP attributes should be settable and retrievable
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.reads().cp(), ops -> ops
                        .consistency(ReadModeSC.ALLOW_REPLICA)
                        .resetTtlOnReadAtPercent(77)
                )
        );
        
        Settings settings = behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.CP);
        assertEquals(ReadModeSC.ALLOW_REPLICA, settings.readModeSC);
        assertEquals(Integer.valueOf(77), settings.resetTtlOnReadAtPercent);
    }
    
    @Test
    public void testAllWriteAttributes() {
        // All write attributes should be settable and retrievable
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
    public void testAllWriteApAttributes() {
        // All write AP attributes should be settable and retrievable
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("test", builder -> builder
                .on(Selectors.writes().ap(), ops -> ops
                        .commitLevel(CommitLevel.COMMIT_MASTER)
                )
        );
        
        Settings settings = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
        assertEquals(CommitLevel.COMMIT_MASTER, settings.commitLevel);
    }
}
