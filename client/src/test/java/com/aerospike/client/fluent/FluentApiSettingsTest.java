package com.aerospike.client.fluent;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.policy.CommitLevel;
import com.aerospike.client.fluent.policy.ReadModeAP;
import com.aerospike.client.fluent.policy.ReadModeSC;
import com.aerospike.client.fluent.policy.RecordExistsAction;
import com.aerospike.client.fluent.policy.Replica;
import com.aerospike.client.fluent.policy.WritePolicy;

/**
 * Tests for fluent API methods that set operation settings.
 */
public class FluentApiSettingsTest {

    /**
     * Stub Session for testing - returns null transaction.
     */
    private static class StubSession extends Session {
        public StubSession() {
            super(null, null);
        }

        @Override
        public Txn getCurrentTransaction() {
            return null;
        }
    }

    /**
     * Test builder - exposes protected fields for verification.
     */
    private static class TestOperationBuilder extends AbstractSessionOperationBuilder<TestOperationBuilder> {
        public TestOperationBuilder() {
            super(new StubSession(), OpType.UPSERT);
        }

        @Override
        protected TestOperationBuilder self() {
            return this;
        }

        // Expose fields for testing - New fluent API methods
        public Integer getConnectTimeout() { return connectTimeout; }
        public Integer getSocketTimeout() { return socketTimeout; }
        public Integer getTotalTimeout() { return totalTimeout; }
        public Integer getTimeoutDelay() { return timeoutDelay; }
        public Integer getMaxRetries() { return maxRetries; }
        public Integer getSleepBetweenRetries() { return sleepBetweenRetries; }
        public ReadModeAP getReadModeAP() { return readModeAP; }
        public ReadModeSC getReadModeSC() { return readModeSC; }
        public Replica getReplica() { return replica; }
        public Integer getReadTouchTtlPercent() { return readTouchTtlPercent; }
        public RecordExistsAction getRecordExistsAction() { return recordExistsAction; }
        public CommitLevel getCommitLevel() { return commitLevel; }
        public Boolean getDurableDelete() { return durableDelete; }
        public Boolean getRespondAllOps() { return respondAllOps; }
        public Boolean getOnLockingOnly() { return onLockingOnly; }
        public Boolean getXdr() { return xdr; }
        public Boolean getSendKey() { return sendKey; }
        public Boolean getCompress() { return compress; }

        // Expose fields for testing - Pre-existing fluent API methods
        public long getExpirationInSeconds() { return expirationInSeconds; }
        public int getGeneration() { return generation; }
        public Txn getTxnToUse() { return txnToUse; }
        public boolean isFailOnFilteredOut() { return failOnFilteredOut; }
        public boolean isRespondAllKeys() { return respondAllKeys; }

        // Implement filtering methods for testing (from FilterableOperation interface)
        public TestOperationBuilder failOnFilteredOut() {
            this.failOnFilteredOut = true;
            return self();
        }

        public TestOperationBuilder respondAllKeys() {
            this.respondAllKeys = true;
            return self();
        }
    }

    // ========================================================================
    // Timeout Settings Tests
    // ========================================================================

    @Test
    public void testWithConnectTimeout_int() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withConnectTimeout(5000);
        assertEquals(5000, builder.getConnectTimeout());
    }

    @Test
    public void testWithConnectTimeout_Duration() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withConnectTimeout(Duration.ofSeconds(5));
        assertEquals(5000, builder.getConnectTimeout());
    }

    @Test
    public void testWithSocketTimeout_int() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withSocketTimeout(3000);
        assertEquals(3000, builder.getSocketTimeout());
    }

    @Test
    public void testWithSocketTimeout_Duration() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withSocketTimeout(Duration.ofSeconds(3));
        assertEquals(3000, builder.getSocketTimeout());
    }

    @Test
    public void testWithTotalTimeout_int() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withTotalTimeout(10000);
        assertEquals(10000, builder.getTotalTimeout());
    }

    @Test
    public void testWithTotalTimeout_Duration() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withTotalTimeout(Duration.ofSeconds(10));
        assertEquals(10000, builder.getTotalTimeout());
    }

    @Test
    public void testWithTimeoutDelay_int() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withTimeoutDelay(500);
        assertEquals(500, builder.getTimeoutDelay());
    }

    @Test
    public void testWithTimeoutDelay_Duration() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withTimeoutDelay(Duration.ofMillis(500));
        assertEquals(500, builder.getTimeoutDelay());
    }

    @Test
    public void testWithTimeout_setsAllTimeouts_int() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withTimeout(5000);
        assertEquals(5000, builder.getConnectTimeout());
        assertEquals(5000, builder.getSocketTimeout());
        assertEquals(5000, builder.getTotalTimeout());
    }

    @Test
    public void testWithTimeout_setsAllTimeouts_Duration() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withTimeout(Duration.ofSeconds(5));
        assertEquals(5000, builder.getConnectTimeout());
        assertEquals(5000, builder.getSocketTimeout());
        assertEquals(5000, builder.getTotalTimeout());
    }

    // ========================================================================
    // Retry Settings Tests
    // ========================================================================

    @Test
    public void testWithMaxRetries() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withMaxRetries(5);
        assertEquals(5, builder.getMaxRetries());
    }

    @Test
    public void testWithSleepBetweenRetries_int() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withSleepBetweenRetries(200);
        assertEquals(200, builder.getSleepBetweenRetries());
    }

    @Test
    public void testWithSleepBetweenRetries_Duration() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withSleepBetweenRetries(Duration.ofMillis(200));
        assertEquals(200, builder.getSleepBetweenRetries());
    }

    @Test
    public void testWithoutRetries() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withoutRetries();
        assertEquals(0, builder.getMaxRetries());
    }

    // ========================================================================
    // Read Mode Settings Tests
    // ========================================================================

    @Test
    public void testWithReadModeAP() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withReadModeAP(ReadModeAP.ALL);
        assertEquals(ReadModeAP.ALL, builder.getReadModeAP());
    }

    @Test
    public void testWithReadModeSC() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withReadModeSC(ReadModeSC.LINEARIZE);
        assertEquals(ReadModeSC.LINEARIZE, builder.getReadModeSC());
    }

    @Test
    public void testWithReplica() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withReplica(Replica.MASTER_PROLES);
        assertEquals(Replica.MASTER_PROLES, builder.getReplica());
    }

    @Test
    public void testWithReadTouchTtlPercent() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withReadTouchTtlPercent(80);
        assertEquals(80, builder.getReadTouchTtlPercent());
    }

    @Test
    public void testWithoutReadTouchTtl() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withoutReadTouchTtl();
        assertEquals(-1, builder.getReadTouchTtlPercent());
    }

    // ========================================================================
    // Write Behavior Settings Tests
    // ========================================================================

    @Test
    public void testWithRecordExistsAction() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withRecordExistsAction(RecordExistsAction.UPDATE_ONLY);
        assertEquals(RecordExistsAction.UPDATE_ONLY, builder.getRecordExistsAction());
    }

    @Test
    public void testCreateOnly() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.createOnly();
        assertEquals(RecordExistsAction.CREATE_ONLY, builder.getRecordExistsAction());
    }

    @Test
    public void testUpdateOnly() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.updateOnly();
        assertEquals(RecordExistsAction.UPDATE_ONLY, builder.getRecordExistsAction());
    }

    @Test
    public void testReplaceRecord() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.replaceRecord();
        assertEquals(RecordExistsAction.REPLACE, builder.getRecordExistsAction());
    }

    @Test
    public void testReplaceOnly() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.replaceOnly();
        assertEquals(RecordExistsAction.REPLACE_ONLY, builder.getRecordExistsAction());
    }

    @Test
    public void testWithCommitLevel() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withCommitLevel(CommitLevel.COMMIT_MASTER);
        assertEquals(CommitLevel.COMMIT_MASTER, builder.getCommitLevel());
    }

    @Test
    public void testCommitAll() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.commitAll();
        assertEquals(CommitLevel.COMMIT_ALL, builder.getCommitLevel());
    }

    @Test
    public void testCommitMasterOnly() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.commitMasterOnly();
        assertEquals(CommitLevel.COMMIT_MASTER, builder.getCommitLevel());
    }

    @Test
    public void testWithDurableDelete() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withDurableDelete();
        assertTrue(builder.getDurableDelete());
    }

    @Test
    public void testWithoutDurableDelete() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withoutDurableDelete();
        assertFalse(builder.getDurableDelete());
    }

    @Test
    public void testRespondAllOps() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.respondAllOps();
        assertTrue(builder.getRespondAllOps());
    }

    @Test
    public void testOnlyIfNotLocked() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.onlyIfNotLocked();
        assertTrue(builder.getOnLockingOnly());
    }

    @Test
    public void testAsXdrWrite() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.asXdrWrite();
        assertTrue(builder.getXdr());
    }

    // ========================================================================
    // Other Settings Tests
    // ========================================================================

    @Test
    public void testSendKey() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.sendKey();
        assertTrue(builder.getSendKey());
    }

    @Test
    public void testWithoutSendingKey() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withoutSendingKey();
        assertFalse(builder.getSendKey());
    }

    @Test
    public void testWithCompression() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withCompression();
        assertTrue(builder.getCompress());
    }

    @Test
    public void testWithoutCompression() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withoutCompression();
        assertFalse(builder.getCompress());
    }

    // ========================================================================
    // Expiration Settings Tests (Pre-existing API)
    // ========================================================================

    @Test
    public void testExpireRecordAfter_Duration() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.expireRecordAfter(Duration.ofHours(2));
        assertEquals(7200, builder.getExpirationInSeconds());
    }

    @Test
    public void testExpireRecordAfterSeconds() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.expireRecordAfterSeconds(3600);
        assertEquals(3600, builder.getExpirationInSeconds());
    }

    @Test
    public void testWithNoChangeInExpiration() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withNoChangeInExpiration();
        assertEquals(-2, builder.getExpirationInSeconds());
    }

    @Test
    public void testNeverExpire() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.neverExpire();
        assertEquals(-1, builder.getExpirationInSeconds());
    }

    @Test
    public void testExpiryFromServerDefault() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.expiryFromServerDefault();
        assertEquals(0, builder.getExpirationInSeconds());
    }

    // ========================================================================
    // Generation Tests (Pre-existing API)
    // ========================================================================

    @Test
    public void testEnsureGenerationIs() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.ensureGenerationIs(5);
        assertEquals(5, builder.getGeneration());
    }

    @Test
    public void testEnsureGenerationIs_throwsOnZero() {
        TestOperationBuilder builder = new TestOperationBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.ensureGenerationIs(0));
    }

    @Test
    public void testEnsureGenerationIs_throwsOnNegative() {
        TestOperationBuilder builder = new TestOperationBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.ensureGenerationIs(-1));
    }

    // ========================================================================
    // Transaction Tests (Pre-existing API)
    // ========================================================================

    @Test
    public void testNotInAnyTransaction() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.notInAnyTransaction();
        assertNull(builder.getTxnToUse());
    }

    // ========================================================================
    // Filtering Tests (Pre-existing API)
    // ========================================================================

    @Test
    public void testFailOnFilteredOut() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.failOnFilteredOut();
        assertTrue(builder.isFailOnFilteredOut());
    }

    @Test
    public void testRespondAllKeys() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.respondAllKeys();
        assertTrue(builder.isRespondAllKeys());
    }

    // ========================================================================
    // Method Chaining Tests
    // ========================================================================

    @Test
    public void testMethodChaining() {
        TestOperationBuilder builder = new TestOperationBuilder()
            .withTotalTimeout(5000)
            .withMaxRetries(3)
            .withReadModeAP(ReadModeAP.ALL)
            .updateOnly()
            .commitAll()
            .withDurableDelete()
            .sendKey()
            .withCompression();

        assertEquals(5000, builder.getTotalTimeout());
        assertEquals(3, builder.getMaxRetries());
        assertEquals(ReadModeAP.ALL, builder.getReadModeAP());
        assertEquals(RecordExistsAction.UPDATE_ONLY, builder.getRecordExistsAction());
        assertEquals(CommitLevel.COMMIT_ALL, builder.getCommitLevel());
        assertTrue(builder.getDurableDelete());
        assertTrue(builder.getSendKey());
        assertTrue(builder.getCompress());
    }

    // ========================================================================
    // Helper Method Tests
    // ========================================================================

    @Test
    public void testApplyFluentApiSettings_appliesNonNullFields() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withTotalTimeout(5000)
               .withMaxRetries(3)
               .withReadModeAP(ReadModeAP.ALL)
               .sendKey();

        WritePolicy policy = new WritePolicy();
        policy.totalTimeout = 1000; // Default
        policy.maxRetries = 0; // Default
        policy.sendKey = false; // Default

        builder.applyFluentApiWriteSettings(policy);

        assertEquals(5000, policy.totalTimeout);
        assertEquals(3, policy.maxRetries);
        assertEquals(ReadModeAP.ALL, policy.readModeAP);
        assertTrue(policy.sendKey);
    }

    @Test
    public void testApplyFluentApiSettings_ignoresNullFields() {
        TestOperationBuilder builder = new TestOperationBuilder();
        // Don't set any fluent API fields

        WritePolicy policy = new WritePolicy();
        policy.totalTimeout = 1000;
        policy.maxRetries = 2;
        policy.sendKey = true;

        builder.applyFluentApiWriteSettings(policy);

        // Policy defaults should remain unchanged
        assertEquals(1000, policy.totalTimeout);
        assertEquals(2, policy.maxRetries);
        assertTrue(policy.sendKey);
    }

    @Test
    public void testApplyFluentApiWriteSettings_includesWriteFields() {
        TestOperationBuilder builder = new TestOperationBuilder();
        builder.withRecordExistsAction(RecordExistsAction.UPDATE_ONLY)
               .withCommitLevel(CommitLevel.COMMIT_MASTER)
               .withDurableDelete()
               .respondAllOps();

        WritePolicy policy = new WritePolicy();
        builder.applyFluentApiWriteSettings(policy);

        assertEquals(RecordExistsAction.UPDATE_ONLY, policy.recordExistsAction);
        assertEquals(CommitLevel.COMMIT_MASTER, policy.commitLevel);
        assertTrue(policy.durableDelete);
        assertTrue(policy.respondAllOps);
    }
}
