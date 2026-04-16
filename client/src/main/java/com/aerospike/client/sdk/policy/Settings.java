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

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

public final class Settings {
    Duration abandonCallAfter;
    Duration delayBetweenRetries;
    Integer maximumNumberOfCallAttempts;
    Replica replicaOrder;
    Boolean sendKey;
    // Exception handling
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

    /**
     * Copy with {@code useDurableDelete} replaced — used when overriding behavior-matrix defaults
     * (e.g. SC + record delete inside operate) without mutating cached {@link Behavior} settings.
     */
    public Settings withUseDurableDelete(boolean useDurableDelete) {
        Settings s = new Settings(this);
        s.useDurableDelete = useDurableDelete;
        return s;
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
}