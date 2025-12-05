package com.aerospike.client.fluent.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.SystemSettings;
import com.aerospike.client.fluent.SystemSettingsRegistry;
import com.aerospike.client.fluent.policy.Behavior.Mode;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;

/**
 * Tests for system-level Behavior configuration including txnVerify, txnRoll,
 * connections, circuitBreaker, and refresh settings.
 */
public class SystemBehaviorTest {

    @Test
    public void testTxnVerifyDefaults() {
        // Create a behavior with default system settings
        Behavior behavior = Behavior.DEFAULT;

        // Get settings for txnVerify
        Settings settings = behavior.getSettings(OpKind.SYSTEM_TXN_VERIFY, OpShape.SYSTEM, Mode.ANY);

        // Verify defaults as specified in the plan
        assertEquals(6, settings.getMaximumNumberOfCallAttempts(), "Default maximumNumberOfCallAttempts should be 6");
        assertEquals(Duration.ofSeconds(3), Duration.ofMillis(settings.getWaitForCallToCompleteMs()), "Default waitForCallToComplete should be 3 seconds");
        assertEquals(Duration.ofSeconds(10), Duration.ofMillis(settings.getAbandonCallAfterMs()), "Default abandonCallAfter should be 10 seconds");
        assertEquals(Duration.ofSeconds(1), Duration.ofMillis(settings.getDelayBetweenRetriesMs()), "Default delayBetweenRetries should be 1 second");
    }

    @Test
    public void testTxnVerifyCustomSettings() {
        // Create a behavior with custom txnVerify settings
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("customTxnVerify", builder -> builder
            .on(Behavior.Selectors.transaction().txnVerify(), ops -> ops
                .consistency(ReadModeSC.LINEARIZE)
                .replicaOrder(Replica.MASTER)
                .maximumNumberOfCallAttempts(10)
                .waitForCallToComplete(Duration.ofSeconds(5))
                .abandonCallAfter(Duration.ofSeconds(15))
                .delayBetweenRetries(Duration.ofSeconds(2))
            )
        );

        Settings settings = behavior.getSettings(OpKind.SYSTEM_TXN_VERIFY, OpShape.SYSTEM, Mode.ANY);

        assertEquals(ReadModeSC.LINEARIZE, settings.getReadModeSC());
        assertEquals(Replica.MASTER, settings.getReplicaOrder());
        assertEquals(10, settings.getMaximumNumberOfCallAttempts());
        assertEquals(5000, settings.getWaitForCallToCompleteMs());
        assertEquals(15000, settings.getAbandonCallAfterMs());
        assertEquals(2000, settings.getDelayBetweenRetriesMs());
    }

    @Test
    public void testTxnRollDefaults() {
        Behavior behavior = Behavior.DEFAULT;
        Settings settings = behavior.getSettings(OpKind.SYSTEM_TXN_ROLL, OpShape.SYSTEM, Mode.ANY);

        // Verify defaults as specified in the plan
        assertEquals(6, settings.getMaximumNumberOfCallAttempts());
        assertEquals(Duration.ofSeconds(3), Duration.ofMillis(settings.getWaitForCallToCompleteMs()));
        assertEquals(Duration.ofSeconds(10), Duration.ofMillis(settings.getAbandonCallAfterMs()));
        assertEquals(Duration.ofSeconds(1), Duration.ofMillis(settings.getDelayBetweenRetriesMs()));
    }

    @Test
    public void testTxnRollCustomSettings() {
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("customTxnRoll", builder -> builder
            .on(Behavior.Selectors.transaction().txnRoll(), ops -> ops
                .replicaOrder(Replica.MASTER)
                .maximumNumberOfCallAttempts(8)
                .waitForCallToComplete(Duration.ofSeconds(4))
                .abandonCallAfter(Duration.ofSeconds(12))
                .delayBetweenRetries(Duration.ofMillis(500))
            )
        );

        Settings settings = behavior.getSettings(OpKind.SYSTEM_TXN_ROLL, OpShape.SYSTEM, Mode.ANY);

        assertEquals(Replica.MASTER, settings.getReplicaOrder());
        assertEquals(8, settings.getMaximumNumberOfCallAttempts());
        assertEquals(4000, settings.getWaitForCallToCompleteMs());
        assertEquals(12000, settings.getAbandonCallAfterMs());
        assertEquals(500, settings.getDelayBetweenRetriesMs());
    }

    @Test
    public void testConnectionsDefaults() {
        SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();
        SystemSettings defaults = registry.getDefaultSettings();

        ClusterDefinition def = new ClusterDefinition("host", 3000)
            .withSystemSettings(defaults);

        // Verify defaults as specified in the plan
        assertEquals(0, def.getMinimumConnectionsPerNode());
        assertEquals(100, def.getMaximumConnectionsPerNode());
        assertEquals(Duration.ofSeconds(55), def.getMaximumSocketIdleTime());
    }

    @Test
    public void testConnectionsCustomSettings() {
        SystemSettings settings = SystemSettings.builder()
        	.connections(ops -> {
                ops.minimumConnectionsPerNode(10);
                ops.maximumConnectionsPerNode(200);
                ops.maximumSocketIdleTime(Duration.ofSeconds(120));
        	})
        	.build();

        assertEquals(10, settings.getMinimumConnectionsPerNode());
        assertEquals(200, settings.getMaximumConnectionsPerNode());
        assertEquals(Duration.ofSeconds(120), settings.getMaximumSocketIdleTime());
    }

    @Test
    public void testCircuitBreakerDefaults() {
        SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();
        SystemSettings defaults = registry.getDefaultSettings();

        ClusterDefinition def = new ClusterDefinition("host", 3000)
        	.withSystemSettings(defaults);

        // Verify defaults as specified in the plan
        assertEquals(1, def.getNumTendIntervalsInErrorWindow());
        assertEquals(100, def.getMaximumErrorsInErrorWindow());
    }

    @Test
    public void testCircuitBreakerCustomSettings() {
        SystemSettings settings = SystemSettings.builder()
        	.circuitBreaker(ops -> {
                ops.numTendIntervalsInErrorWindow(5);
                ops.maximumErrorsInErrorWindow(50);
        	})
        	.build();

        assertEquals(5, settings.getNumTendIntervalsInErrorWindow());
        assertEquals(50, settings.getMaximumErrorsInErrorWindow());
    }

    @Test
    public void testRefreshDefaults() {
        SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();
        SystemSettings defaults = registry.getDefaultSettings();

        ClusterDefinition def = new ClusterDefinition("host", 3000)
            .withSystemSettings(defaults);

        // Verify defaults as specified in the plan
        assertEquals(Duration.ofMillis(1000), def.getTendInterval());
    }

    @Test
    public void testRefreshCustomSettings() {
        SystemSettings settings = SystemSettings.builder()
        	.refresh(ops -> {
                ops.tendInterval(Duration.ofMillis(500));
        	})
        	.build();

        assertEquals(Duration.ofMillis(500), settings.getTendInterval());
    }

    @Test
    public void testSelectorChaining() {
        // Verify that selector chaining works correctly
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("chainTest", builder -> builder
            .on(Behavior.Selectors.transaction().txnVerify(), ops -> ops
                .maximumNumberOfCallAttempts(12)
            )
        );

        Settings settings = behavior.getSettings(OpKind.SYSTEM_TXN_VERIFY, OpShape.SYSTEM, Mode.ANY);
        assertEquals(12, settings.getMaximumNumberOfCallAttempts());
    }

    @Test
    public void testSystemSettingsDoNotInterfereWithReadWrite() {
        // Configure system settings and verify they don't affect read/write operations
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("isolated", builder -> builder
            .on(Behavior.Selectors.reads(), ops -> ops
                .maximumNumberOfCallAttempts(5)
            )
            .on(Behavior.Selectors.writes(), ops -> ops
                .maximumNumberOfCallAttempts(7)
            )
        );

        // Verify read settings
        Settings readSettings = behavior.getSettings(OpKind.READ, OpShape.BATCH, Mode.ANY);
        assertEquals(5, readSettings.getMaximumNumberOfCallAttempts());

        // Verify write settings
        Settings writeSettings = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Mode.ANY);
        assertEquals(7, writeSettings.getMaximumNumberOfCallAttempts());
    }

    @Test
    public void testMultipleSystemCategories() {
        // Configure multiple system categories at once
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("multiSystem", builder -> builder
            .on(Behavior.Selectors.transaction().txnVerify(), ops -> ops
                .consistency(ReadModeSC.LINEARIZE)
                .maximumNumberOfCallAttempts(10)
            )
        );

        // Verify txnVerify
        Settings txnVerifySettings = behavior.getSettings(OpKind.SYSTEM_TXN_VERIFY, OpShape.SYSTEM, Mode.ANY);
        assertEquals(ReadModeSC.LINEARIZE, txnVerifySettings.getReadModeSC());
        assertEquals(10, txnVerifySettings.getMaximumNumberOfCallAttempts());
    }
}

