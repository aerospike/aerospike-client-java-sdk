package com.aerospike.client.fluent.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

import org.junit.jupiter.api.Test;

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
            .on(Behavior.Selectors.system().txnVerify(), ops -> ops
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
            .on(Behavior.Selectors.system().txnRoll(), ops -> ops
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
        Behavior behavior = Behavior.DEFAULT;
        Settings settings = behavior.getSettings(OpKind.SYSTEM_CONNECTIONS, OpShape.SYSTEM, Mode.ANY);
        
        // Verify defaults as specified in the plan
        assertEquals(0, settings.getMinimumConnectionsPerNode());
        assertEquals(100, settings.getMaximumConnectionsPerNode());
        assertEquals(Duration.ofSeconds(55), settings.getMaximumSocketIdleTime());
    }

    @Test
    public void testConnectionsCustomSettings() {
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("customConnections", builder -> builder
            .on(Behavior.Selectors.system().connections(), ops -> ops
                .minimumConnectionsPerNode(10)
                .maximumConnectionsPerNode(200)
                .maximumSocketIdleTime(Duration.ofSeconds(120))
            )
        );
        
        Settings settings = behavior.getSettings(OpKind.SYSTEM_CONNECTIONS, OpShape.SYSTEM, Mode.ANY);
        
        assertEquals(10, settings.getMinimumConnectionsPerNode());
        assertEquals(200, settings.getMaximumConnectionsPerNode());
        assertEquals(Duration.ofSeconds(120), settings.getMaximumSocketIdleTime());
    }

    @Test
    public void testCircuitBreakerDefaults() {
        Behavior behavior = Behavior.DEFAULT;
        Settings settings = behavior.getSettings(OpKind.SYSTEM_CIRCUIT_BREAKER, OpShape.SYSTEM, Mode.ANY);
        
        // Verify defaults as specified in the plan
        assertEquals(1, settings.getNumTendIntervalsInErrorWindow());
        assertEquals(100, settings.getMaximumErrorsInErrorWindow());
    }

    @Test
    public void testCircuitBreakerCustomSettings() {
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("customCircuitBreaker", builder -> builder
            .on(Behavior.Selectors.system().circuitBreaker(), ops -> ops
                .numTendIntervalsInErrorWindow(5)
                .maximumErrorsInErrorWindow(50)
            )
        );
        
        Settings settings = behavior.getSettings(OpKind.SYSTEM_CIRCUIT_BREAKER, OpShape.SYSTEM, Mode.ANY);
        
        assertEquals(5, settings.getNumTendIntervalsInErrorWindow());
        assertEquals(50, settings.getMaximumErrorsInErrorWindow());
    }

    @Test
    public void testRefreshDefaults() {
        Behavior behavior = Behavior.DEFAULT;
        Settings settings = behavior.getSettings(OpKind.SYSTEM_REFRESH, OpShape.SYSTEM, Mode.ANY);
        
        // Verify defaults as specified in the plan
        assertEquals(Duration.ofSeconds(1), settings.getTendInterval());
    }

    @Test
    public void testRefreshCustomSettings() {
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("customRefresh", builder -> builder
            .on(Behavior.Selectors.system().refresh(), ops -> ops
                .tendInterval(Duration.ofMillis(500))
            )
        );
        
        Settings settings = behavior.getSettings(OpKind.SYSTEM_REFRESH, OpShape.SYSTEM, Mode.ANY);
        
        assertEquals(Duration.ofMillis(500), settings.getTendInterval());
    }

    @Test
    public void testCascadingBehaviors() {
        // Create parent behavior with connections settings
        Behavior parent = Behavior.DEFAULT.deriveWithChanges("parent", builder -> builder
            .on(Behavior.Selectors.system().connections(), ops -> ops
                .maximumConnectionsPerNode(100)
            )
            .on(Behavior.Selectors.system().circuitBreaker(), ops -> ops
                .maximumErrorsInErrorWindow(75)
            )
        );
        
        // Create child behavior that overrides connections and adds refresh
        Behavior child = parent.deriveWithChanges("child", builder -> builder
            .on(Behavior.Selectors.system().connections(), ops -> ops
                .maximumConnectionsPerNode(250) // Override parent
            )
            .on(Behavior.Selectors.system().refresh(), ops -> ops
                .tendInterval(Duration.ofSeconds(2)) // Add new setting
            )
        );
        
        // Verify parent settings
        Settings parentSettings = parent.getSettings(OpKind.SYSTEM_CONNECTIONS, OpShape.SYSTEM, Mode.ANY);
        assertEquals(100, parentSettings.getMaximumConnectionsPerNode());
        
        Settings parentCircuitBreaker = parent.getSettings(OpKind.SYSTEM_CIRCUIT_BREAKER, OpShape.SYSTEM, Mode.ANY);
        assertEquals(75, parentCircuitBreaker.getMaximumErrorsInErrorWindow());
        
        // Verify child overrides parent's connections setting
        Settings childSettings = child.getSettings(OpKind.SYSTEM_CONNECTIONS, OpShape.SYSTEM, Mode.ANY);
        assertEquals(250, childSettings.getMaximumConnectionsPerNode());
        
        // Verify child inherits parent's circuit breaker setting
        Settings childCircuitBreaker = child.getSettings(OpKind.SYSTEM_CIRCUIT_BREAKER, OpShape.SYSTEM, Mode.ANY);
        assertEquals(75, childCircuitBreaker.getMaximumErrorsInErrorWindow());
        
        // Verify child has new refresh setting
        Settings childRefresh = child.getSettings(OpKind.SYSTEM_REFRESH, OpShape.SYSTEM, Mode.ANY);
        assertEquals(Duration.ofSeconds(2), childRefresh.getTendInterval());
    }

    @Test
    public void testSelectorChaining() {
        // Verify that selector chaining works correctly
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("chainTest", builder -> builder
            .on(Behavior.Selectors.system().txnVerify(), ops -> ops
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
            .on(Behavior.Selectors.system().connections(), ops -> ops
                .maximumConnectionsPerNode(250)
            )
            .on(Behavior.Selectors.reads(), ops -> ops
                .maximumNumberOfCallAttempts(5)
            )
            .on(Behavior.Selectors.writes(), ops -> ops
                .maximumNumberOfCallAttempts(7)
            )
        );
        
        // Verify system settings
        Settings systemSettings = behavior.getSettings(OpKind.SYSTEM_CONNECTIONS, OpShape.SYSTEM, Mode.ANY);
        assertEquals(250, systemSettings.getMaximumConnectionsPerNode());
        
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
            .on(Behavior.Selectors.system().txnVerify(), ops -> ops
                .consistency(ReadModeSC.LINEARIZE)
                .maximumNumberOfCallAttempts(10)
            )
            .on(Behavior.Selectors.system().connections(), ops -> ops
                .minimumConnectionsPerNode(5)
                .maximumConnectionsPerNode(150)
            )
            .on(Behavior.Selectors.system().circuitBreaker(), ops -> ops
                .maximumErrorsInErrorWindow(75)
            )
            .on(Behavior.Selectors.system().refresh(), ops -> ops
                .tendInterval(Duration.ofMillis(500))
            )
        );
        
        // Verify txnVerify
        Settings txnVerifySettings = behavior.getSettings(OpKind.SYSTEM_TXN_VERIFY, OpShape.SYSTEM, Mode.ANY);
        assertEquals(ReadModeSC.LINEARIZE, txnVerifySettings.getReadModeSC());
        assertEquals(10, txnVerifySettings.getMaximumNumberOfCallAttempts());
        
        // Verify connections
        Settings connectionsSettings = behavior.getSettings(OpKind.SYSTEM_CONNECTIONS, OpShape.SYSTEM, Mode.ANY);
        assertEquals(5, connectionsSettings.getMinimumConnectionsPerNode());
        assertEquals(150, connectionsSettings.getMaximumConnectionsPerNode());
        
        // Verify circuit breaker
        Settings circuitBreakerSettings = behavior.getSettings(OpKind.SYSTEM_CIRCUIT_BREAKER, OpShape.SYSTEM, Mode.ANY);
        assertEquals(75, circuitBreakerSettings.getMaximumErrorsInErrorWindow());
        
        // Verify refresh
        Settings refreshSettings = behavior.getSettings(OpKind.SYSTEM_REFRESH, OpShape.SYSTEM, Mode.ANY);
        assertEquals(Duration.ofMillis(500), refreshSettings.getTendInterval());
    }
}

