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

import java.util.LinkedHashMap;
import java.util.Map;

public final class ResolvedSettings {
    private final CommitLevel commitLevel;
    private final Replica replicaOrder;
    private final ReadModeAP readModeAP;
    private final ReadModeSC readModeSC;
    private final int abandonCallAfterMs;
    private final int delayBetweenRetriesMs;
    private final int waitForCallToCompleteMs;
    private final int waitForConnectionToCompleteMs;
    private final int waitForSocketResponseAfterCallFailsMs;
    private final int maximumNumberOfCallAttempts;
    private final int recordQueueSize;
    private final int maxConcurrentNodes;
    private final int resetTtlOnReadAtPercent;
    private final boolean sendKey;
    private final boolean stackTraceOnException;
    private final boolean useCompression;
    private final boolean allowInlineMemoryAccess;
    private final boolean allowInlineSsdAccess;
    private final boolean useDurableDelete;
    private final boolean simulateXdrWrite;

    public ResolvedSettings(Settings src) {
        commitLevel = (src.commitLevel != null) ? src.commitLevel : CommitLevel.COMMIT_ALL;
        replicaOrder = (src.replicaOrder != null) ? src.replicaOrder : Replica.SEQUENCE;
        readModeAP = (src.readModeAP != null) ? src.readModeAP : ReadModeAP.ONE;
        readModeSC = (src.readModeSC != null) ? src.readModeSC : ReadModeSC.SESSION;

        abandonCallAfterMs = (src.abandonCallAfter != null) ?
            (int)src.abandonCallAfter.toMillis() : 1000;

        delayBetweenRetriesMs = (src.delayBetweenRetries != null) ?
            (int)src.delayBetweenRetries.toMillis() : 0;

        waitForCallToCompleteMs = (src.waitForCallToComplete != null) ?
            (int)src.waitForCallToComplete.toMillis() : 30000;

        waitForConnectionToCompleteMs = (src.waitForConnectionToComplete != null) ?
            (int)src.waitForConnectionToComplete.toMillis() : 0;

        waitForSocketResponseAfterCallFailsMs = (src.waitForSocketResponseAfterCallFails != null) ?
            (int)src.waitForSocketResponseAfterCallFails.toMillis() : 30000;

        maximumNumberOfCallAttempts = (src.maximumNumberOfCallAttempts != null) ?
            src.maximumNumberOfCallAttempts : 3;

        recordQueueSize = (src.recordQueueSize != null) ?
            src.recordQueueSize : 5000;

        maxConcurrentNodes = (src.maxConcurrentNodes != null) ?
            src.maxConcurrentNodes : 1;

        resetTtlOnReadAtPercent = (src.resetTtlOnReadAtPercent != null) ?
            src.resetTtlOnReadAtPercent : 0;

        sendKey = (src.sendKey != null) ?
            src.sendKey : false;

        stackTraceOnException = (src.stackTraceOnException != null) ?
            src.stackTraceOnException : true;

        useCompression = (src.useCompression != null) ?
            src.useCompression : false;

        allowInlineMemoryAccess = (src.allowInlineMemoryAccess != null) ?
            src.allowInlineMemoryAccess : true;

        allowInlineSsdAccess = (src.allowInlineSsdAccess != null) ?
            src.allowInlineSsdAccess : false;

        useDurableDelete = (src.useDurableDelete != null) ?
            src.useDurableDelete : false;

        simulateXdrWrite = (src.simulateXdrWrite != null) ?
            src.simulateXdrWrite : false;
     }

    public ResolvedSettings(ResolvedSettings res, Settings src) {
        commitLevel = (src.commitLevel != null) ? src.commitLevel : res.commitLevel;
        replicaOrder = (src.replicaOrder != null) ? src.replicaOrder : res.replicaOrder;
        readModeAP = (src.readModeAP != null) ? src.readModeAP : res.readModeAP;
        readModeSC = (src.readModeSC != null) ? src.readModeSC : res.readModeSC;

        abandonCallAfterMs = (src.abandonCallAfter != null) ?
            (int)src.abandonCallAfter.toMillis() : res.abandonCallAfterMs;

        delayBetweenRetriesMs = (src.delayBetweenRetries != null) ?
            (int)src.delayBetweenRetries.toMillis() : res.delayBetweenRetriesMs;

        waitForCallToCompleteMs = (src.waitForCallToComplete != null) ?
            (int)src.waitForCallToComplete.toMillis() : res.waitForCallToCompleteMs;

        waitForConnectionToCompleteMs = (src.waitForConnectionToComplete != null) ?
            (int)src.waitForConnectionToComplete.toMillis() : res.waitForConnectionToCompleteMs;

        waitForSocketResponseAfterCallFailsMs = (src.waitForSocketResponseAfterCallFails != null) ?
            (int)src.waitForSocketResponseAfterCallFails.toMillis() :
            res.waitForSocketResponseAfterCallFailsMs;

        maximumNumberOfCallAttempts = (src.maximumNumberOfCallAttempts != null) ?
            src.maximumNumberOfCallAttempts : res.maximumNumberOfCallAttempts;

        recordQueueSize = (src.recordQueueSize != null) ?
            src.recordQueueSize : res.recordQueueSize;

        maxConcurrentNodes = (src.maxConcurrentNodes != null) ?
            src.maxConcurrentNodes : res.maxConcurrentNodes;

        resetTtlOnReadAtPercent = (src.resetTtlOnReadAtPercent != null) ?
            src.resetTtlOnReadAtPercent : res.resetTtlOnReadAtPercent;

        sendKey = (src.sendKey != null) ?
            src.sendKey : res.sendKey;

        stackTraceOnException = (src.stackTraceOnException != null) ?
            src.stackTraceOnException : res.stackTraceOnException;

        useCompression = (src.useCompression != null) ?
            src.useCompression : res.useCompression;

        allowInlineMemoryAccess = (src.allowInlineMemoryAccess != null) ?
            src.allowInlineMemoryAccess : res.allowInlineMemoryAccess;

        allowInlineSsdAccess = (src.allowInlineSsdAccess != null) ?
            src.allowInlineSsdAccess : res.allowInlineSsdAccess;

        useDurableDelete = (src.useDurableDelete != null) ?
            src.useDurableDelete : res.useDurableDelete;

        simulateXdrWrite = (src.simulateXdrWrite != null) ?
            src.simulateXdrWrite : res.simulateXdrWrite;
    }

    @Override public String toString() {
        Map<String, Object> m = new LinkedHashMap<>();

        m.put("commitLevel", commitLevel);
        m.put("replicaOrder", replicaOrder);
        m.put("readModeAP", readModeAP);
        m.put("readModeSC", readModeSC);
        m.put("abandonCallAfterMs", abandonCallAfterMs);
        m.put("delayBetweenRetriesMs", delayBetweenRetriesMs);
        m.put("waitForCallToCompleteMs", waitForCallToCompleteMs);
        m.put("waitForConnectionToCompleteMs", waitForConnectionToCompleteMs);
        m.put("waitForSocketResponseAfterCallFailsMs", waitForSocketResponseAfterCallFailsMs);
        m.put("maximumNumberOfCallAttempts", maximumNumberOfCallAttempts);
        m.put("recordQueueSize", recordQueueSize);
        m.put("maxConcurrentNodes", maxConcurrentNodes);
        m.put("resetTtlOnReadAtPercent", resetTtlOnReadAtPercent);
        m.put("sendKey", sendKey);
        m.put("stackTraceOnException", stackTraceOnException);
        m.put("useCompression", useCompression);
        m.put("allowInlineMemoryAccess", allowInlineMemoryAccess);
        m.put("allowInlineSsdAccess", allowInlineSsdAccess);
        m.put("useDurableDelete", useDurableDelete);
        m.put("simulateXdrWrite", simulateXdrWrite);

        return m.toString();
    }

    public CommitLevel getCommitLevel() {
        return commitLevel;
    }

    public Replica getReplicaOrder() {
        return replicaOrder;
    }

    public ReadModeAP getReadModeAP() {
        return readModeAP;
    }

    public ReadModeSC getReadModeSC() {
        return readModeSC;
    }

    public int getAbandonCallAfterMs() {
        return abandonCallAfterMs;
    }

    public int getDelayBetweenRetriesMs() {
        return delayBetweenRetriesMs;
    }

    public int getWaitForCallToCompleteMs() {
        return waitForCallToCompleteMs;
    }

    public int getWaitForConnectionToCompleteMs() {
        return waitForConnectionToCompleteMs;
    }

    public int getWaitForSocketResponseAfterCallFailsMs() {
        return waitForSocketResponseAfterCallFailsMs;
    }

    public int getMaximumNumberOfCallAttempts() {
        return maximumNumberOfCallAttempts;
    }

    public int getRecordQueueSize() {
        return recordQueueSize;
    }

    public int getMaxConcurrentNodes() {
        return maxConcurrentNodes;
    }

    public int getResetTtlOnReadAtPercent() {
        return resetTtlOnReadAtPercent;
    }

    public boolean getSendKey() {
        return sendKey;
    }

    public boolean getStackTraceOnException() {
        return stackTraceOnException;
    }

    public boolean getUseCompression() {
        return useCompression;
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
}