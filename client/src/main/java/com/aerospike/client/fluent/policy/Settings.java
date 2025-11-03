package com.aerospike.client.fluent.policy;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

public final class Settings {
    Duration abandonCallAfter;
    Duration delayBetweenRetries;
    Integer maximumNumberOfCallAttempts;
    Replica replicaOrder;
    Boolean sendKey;
    Boolean stackTraceOnException;
    Boolean useCompression;
    Duration waitForCallToComplete;
    Duration waitForConnectionToComplete;
    Duration waitForSocketResponseAfterCallFails;

    // Query-only
    Integer recordQueueSize;

    // Batch-only
    Integer maxConcurrentNodes;
    Boolean allowInlineMemoryAccess;
    Boolean allowInlineSsdAccess;

    // Write-mode-specific
    Boolean useDurableDelete;
    Boolean simulateXdrWrite;

    // Write-mode-ap-specific
    CommitLevel commitLevel;

    // Read-mode-specific
    ReadModeAP readModeAP;           // AP
    ReadModeSC readModeSC; // CP
    Integer resetTtlOnReadAtPercent;

    Settings() {}

    public Settings(Settings orig) {
        this.abandonCallAfter = orig.abandonCallAfter;
        this.delayBetweenRetries = orig.delayBetweenRetries;
        this.maximumNumberOfCallAttempts = orig.maximumNumberOfCallAttempts;
        this.replicaOrder = orig.replicaOrder;
        this.sendKey = orig.sendKey;
        this.useCompression = orig.useCompression;
        this.waitForCallToComplete = orig.waitForCallToComplete;
        this.waitForConnectionToComplete = orig.waitForConnectionToComplete;
        this.waitForSocketResponseAfterCallFails = orig.waitForSocketResponseAfterCallFails;
        this.recordQueueSize = orig.recordQueueSize;
        this.maxConcurrentNodes = orig.maxConcurrentNodes;
        this.allowInlineMemoryAccess = orig.allowInlineMemoryAccess;
        this.allowInlineSsdAccess = orig.allowInlineSsdAccess;
        this.useDurableDelete = orig.useDurableDelete;
        this.simulateXdrWrite = orig.simulateXdrWrite;
        this.commitLevel = orig.commitLevel;
        this.readModeAP = orig.readModeAP;
        this.readModeSC = orig.readModeSC;
        this.resetTtlOnReadAtPercent = orig.resetTtlOnReadAtPercent;
		this.stackTraceOnException = orig.stackTraceOnException;
    }

    @Override public String toString() {
        Map<String, Object> m = new LinkedHashMap<>();
        if (abandonCallAfter != null) {
			m.put("abandonCallAfter", abandonCallAfter);
		}
        if (delayBetweenRetries != null) {
			m.put("delayBetweenRetries", delayBetweenRetries);
		}
        if (maximumNumberOfCallAttempts != null) {
			m.put("maximumNumberOfCallAttempts", maximumNumberOfCallAttempts);
		}
        if (replicaOrder != null) {
			m.put("replicaOrder", replicaOrder);
		}
        if (sendKey != null) {
			m.put("sendKey", sendKey);
		}
        if (useCompression != null) {
			m.put("useCompression", useCompression);
		}
        if (waitForCallToComplete != null) {
			m.put("waitForCallToComplete", waitForCallToComplete);
		}
        if (waitForConnectionToComplete != null) {
			m.put("waitForConnectionToComplete", waitForConnectionToComplete);
		}
        if (waitForSocketResponseAfterCallFails != null) {
			m.put("waitForSocketResponseAfterCallFails", waitForSocketResponseAfterCallFails);
		}

        if (recordQueueSize != null) {
			m.put("recordQueueSize", recordQueueSize);
		}

        if (maxConcurrentNodes != null) {
			m.put("maxConcurrentNodes", maxConcurrentNodes);
		}
        if (allowInlineMemoryAccess != null) {
			m.put("allowInlineMemoryAccess", allowInlineMemoryAccess);
		}
        if (allowInlineSsdAccess != null) {
			m.put("allowInlineSsdAccess", allowInlineSsdAccess);
		}

        if (useDurableDelete != null) {
			m.put("useDurableDelete", useDurableDelete);
		}
        if (simulateXdrWrite != null) {
			m.put("simulateXdrWrite", simulateXdrWrite);
		}

        if (commitLevel != null) {
			m.put("commitLevel", commitLevel);
		}

        if (readModeAP != null) {
			m.put("readModeAP", readModeAP);
		}
        if (readModeSC != null) {
			m.put("readModeSC", readModeSC);
		}
        if (resetTtlOnReadAtPercent != null) {
			m.put("resetTtlOnReadAtPercent", resetTtlOnReadAtPercent);
		}
        if (stackTraceOnException != null) {
		    m.put("stackTraceOnException", stackTraceOnException);
		}

        return m.toString();
    }

    public int getAbandonCallAfterMs() {
        return (int)abandonCallAfter.toMillis();
    }

    public int getDelayBetweenRetriesMs() {
        return (int)delayBetweenRetries.toMillis();
    }

    public int getMaximumNumberOfCallAttempts() {
        return maximumNumberOfCallAttempts;
    }

    public Replica getReplicaOrder() {
        return replicaOrder;
    }

    public boolean getSendKey() {
        return sendKey;
    }

    public boolean getUseCompression() {
        return useCompression;
    }

    public int getWaitForCallToCompleteMs() {
        return (int)waitForCallToComplete.toMillis();
    }

    public int getWaitForConnectionToCompleteMs() {
        return (int) waitForConnectionToComplete.toMillis();
    }

    public int getWaitForSocketResponseAfterCallFailsMs() {
        return (int) waitForSocketResponseAfterCallFails.toMillis();
    }

    public int getRecordQueueSize() {
        return recordQueueSize;
    }

    public int getMaxConcurrentNodes() {
        return maxConcurrentNodes;
    }

    public boolean getAllowInlineMemoryAccess() {
        return allowInlineMemoryAccess;
    }

    public boolean getAllowInlineSsdAccess() {
        return allowInlineSsdAccess;
    }

    public boolean getUseDurableDelete() {
        return useDurableDelete;
    }

    public boolean getSimulateXdrWrite() {
        return simulateXdrWrite;
    }

    public CommitLevel getCommitLevel() {
        return commitLevel;
    }

    public ReadModeAP getReadModeAP() {
        return readModeAP;
    }

    public ReadModeSC getReadModeSC() {
        return readModeSC;
    }

    public int getResetTtlOnReadAtPercent() {
        return resetTtlOnReadAtPercent;
    }
    public boolean getStackTraceOnException() {
        return stackTraceOnException;
    }
    
    public WritePolicy asWritePolicy() {
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.commitLevel = this.commitLevel;
        writePolicy.compress = this.useCompression;
        writePolicy.connectTimeout = (int)this.waitForConnectionToComplete.toMillis();
        writePolicy.durableDelete = this.useDurableDelete;
        writePolicy.maxRetries = this.maximumNumberOfCallAttempts - 1;
        writePolicy.readModeAP = this.readModeAP == null ? ReadModeAP.ALL : this.readModeAP;
        writePolicy.readModeSC = this.readModeSC == null ? ReadModeSC.SESSION : this.readModeSC;
        writePolicy.readTouchTtlPercent = this.resetTtlOnReadAtPercent;
        writePolicy.replica = Replica.SEQUENCE; // TODO
        writePolicy.sendKey = this.sendKey;
        writePolicy.sleepBetweenRetries = (int)this.delayBetweenRetries.toMillis();
        writePolicy.socketTimeout = (int)this.waitForCallToComplete.toMillis();
        writePolicy.totalTimeout = (int)this.abandonCallAfter.toMillis();
        writePolicy.timeoutDelay = (int)this.waitForSocketResponseAfterCallFails.toMillis();

        return writePolicy;
    }

    @Deprecated(forRemoval = true)
    public BatchPolicy asBatchPolicy() {
        BatchPolicy batchPolicy = new BatchPolicy();
        batchPolicy.compress = this.useCompression;
        batchPolicy.connectTimeout = (int)this.waitForConnectionToComplete.toMillis();
        batchPolicy.maxRetries = this.maximumNumberOfCallAttempts - 1;
        batchPolicy.readModeAP = this.readModeAP == null ? ReadModeAP.ALL : this.readModeAP;
        batchPolicy.readModeSC = this.readModeSC == null ? ReadModeSC.SESSION : this.readModeSC;
        batchPolicy.readTouchTtlPercent = this.resetTtlOnReadAtPercent;
        batchPolicy.replica = Replica.SEQUENCE; // TODO
        batchPolicy.sendKey = this.sendKey;
        batchPolicy.sleepBetweenRetries = (int)this.delayBetweenRetries.toMillis();
        batchPolicy.socketTimeout = (int)this.waitForCallToComplete.toMillis();
        batchPolicy.totalTimeout = (int)this.abandonCallAfter.toMillis();
        batchPolicy.timeoutDelay = (int)this.waitForSocketResponseAfterCallFails.toMillis();

        batchPolicy.allowInline = this.allowInlineMemoryAccess;
        batchPolicy.allowInlineSSD = this.allowInlineSsdAccess;
        batchPolicy.maxConcurrentThreads = this.maxConcurrentNodes;
        return batchPolicy;
    }

    @Deprecated(forRemoval = true)
    public QueryPolicy asQueryPolicy() {
        QueryPolicy queryPolicy = new QueryPolicy(this);
        queryPolicy.compress = this.useCompression;
        queryPolicy.connectTimeout = (int)this.waitForConnectionToComplete.toMillis();
        queryPolicy.maxRetries = this.maximumNumberOfCallAttempts - 1;
        queryPolicy.readModeAP = this.readModeAP == null ? ReadModeAP.ALL : this.readModeAP;
        queryPolicy.readModeSC = this.readModeSC == null ? ReadModeSC.SESSION : this.readModeSC;
        queryPolicy.readTouchTtlPercent = this.resetTtlOnReadAtPercent;
        queryPolicy.replica = Replica.SEQUENCE; // TODO
        queryPolicy.sendKey = this.sendKey;
        queryPolicy.sleepBetweenRetries = (int)this.delayBetweenRetries.toMillis();
        queryPolicy.socketTimeout = (int)this.waitForCallToComplete.toMillis();
        queryPolicy.totalTimeout = (int)this.abandonCallAfter.toMillis();
        queryPolicy.timeoutDelay = (int)this.waitForSocketResponseAfterCallFails.toMillis();

        // TODO
//        queryPolicy.expectedDuration = QueryDuration.SHORT; // TODO
//        queryPolicy.infoTimeout = 1000; // TODO
//        if (this.maxConcurrentNodes != null) {
//            queryPolicy.maxConcurrentNodes = this.maxConcurrentNodes;
//        }
//        queryPolicy.recordQueueSize = this.recordQueueSize;
        return queryPolicy;
    }
    @Deprecated(forRemoval = true)
    public Policy asReadPolicy() {
        Policy readPolicy = new Policy();
        readPolicy.compress = this.useCompression;
        readPolicy.connectTimeout = (int)this.waitForConnectionToComplete.toMillis();
        readPolicy.maxRetries = this.maximumNumberOfCallAttempts - 1;
        readPolicy.readModeAP = this.readModeAP == null ? ReadModeAP.ALL : this.readModeAP;
        readPolicy.readModeSC = this.readModeSC == null ? ReadModeSC.SESSION : this.readModeSC;
        readPolicy.readTouchTtlPercent = this.resetTtlOnReadAtPercent;
        readPolicy.replica = Replica.SEQUENCE; // TODO
        readPolicy.sendKey = this.sendKey;
        readPolicy.sleepBetweenRetries = (int)this.delayBetweenRetries.toMillis();
        readPolicy.socketTimeout = (int)this.waitForCallToComplete.toMillis();
        readPolicy.totalTimeout = (int)this.abandonCallAfter.toMillis();
        readPolicy.timeoutDelay = (int)this.waitForSocketResponseAfterCallFails.toMillis();
        return readPolicy;
    }


}