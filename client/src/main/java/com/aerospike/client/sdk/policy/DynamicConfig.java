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
import java.util.Map;

public class DynamicConfig {

    private Map<String, Config> behaviors;
    private Map<String, SystemSettings> system;

    // Getters and setters
    public Map<String, Config> getBehaviors() { return behaviors; }
    public void setBehaviors(Map<String, Config> behaviors) { this.behaviors = behaviors; }

    public Map<String, SystemSettings> getSystem() { return system; }
    public void setSystem(Map<String, SystemSettings> system) { this.system = system; }

    // Individual behavior configuration (name is the map key)
    public static class Config {
		private String parent;
        private Boolean sendKey;
        private Boolean useCompression;
        private Base allOperations;
        private ConsistencyModeRead readModeSC;
        private AvailabilityModeRead readModeAP;
        private Write retryableWrites;
        private Write nonRetryableWrites;
        private Batch batchReads;
        private Batch batchWrites;
        private Query query;
        private Info info;
        private SystemTxnVerifyConfig systemTxnVerify;
        private SystemTxnRoll systemTxnRoll;
        private SystemConnections systemConnections;
        private SystemCircuitBreaker systemCircuitBreaker;
        private SystemRefresh systemRefresh;
        private SystemTransactions systemTransactions;

        // Getters and setters
        public String getParent() { return parent; }
        public void setParent(String parent) { this.parent = parent; }

        public Boolean getSendKey() { return sendKey; }
        public void setSendKey(Boolean sendKey) { this.sendKey = sendKey; }

        public Boolean getUseCompression() { return useCompression; }
        public void setUseCompression(Boolean useCompression) { this.useCompression = useCompression; }

        public Base getAllOperations() { return allOperations; }
        public void setAllOperations(Base allOperations) { this.allOperations = allOperations; }

        public ConsistencyModeRead getConsistencyModeReads() { return readModeSC; }
        public void setConsistencyModeReads(ConsistencyModeRead consistencyModeReads) { this.readModeSC = consistencyModeReads; }

        public AvailabilityModeRead getAvailabilityModeReads() { return readModeAP; }
        public void setAvailabilityModeReads(AvailabilityModeRead availabilityModeReads) { this.readModeAP = availabilityModeReads; }

        public Write getRetryableWrites() { return retryableWrites; }
        public void setRetryableWrites(Write retryableWrites) { this.retryableWrites = retryableWrites; }

        public Write getNonRetryableWrites() { return nonRetryableWrites; }
        public void setNonRetryableWrites(Write nonRetryableWrites) { this.nonRetryableWrites = nonRetryableWrites; }

        public Batch getBatchReads() { return batchReads; }
        public void setBatchReads(Batch batchReads) { this.batchReads = batchReads; }

        public Batch getBatchWrites() { return batchWrites; }
        public void setBatchWrites(Batch batchWrites) { this.batchWrites = batchWrites; }

        public Query getQuery() { return query; }
        public void setQuery(Query query) { this.query = query; }

        public Info getInfo() { return info; }
        public void setInfo(Info info) { this.info = info; }

        public SystemTxnVerifyConfig getSystemTxnVerify() { return systemTxnVerify; }
        public void setSystemTxnVerify(SystemTxnVerifyConfig systemTxnVerify) { this.systemTxnVerify = systemTxnVerify; }

        public SystemTxnRoll getSystemTxnRoll() { return systemTxnRoll; }
        public void setSystemTxnRoll(SystemTxnRoll systemTxnRoll) { this.systemTxnRoll = systemTxnRoll; }

        public SystemConnections getSystemConnections() { return systemConnections; }
        public void setSystemConnections(SystemConnections systemConnections) { this.systemConnections = systemConnections; }

        public SystemCircuitBreaker getSystemCircuitBreaker() { return systemCircuitBreaker; }
        public void setSystemCircuitBreaker(SystemCircuitBreaker systemCircuitBreaker) { this.systemCircuitBreaker = systemCircuitBreaker; }

        public SystemRefresh getSystemRefresh() { return systemRefresh; }
        public void setSystemRefresh(SystemRefresh systemRefresh) { this.systemRefresh = systemRefresh; }

        public SystemTransactions getSystemTransactions() {
			return systemTransactions;
		}
		public void setSystemTransactions(SystemTransactions systemTransactions) {
			this.systemTransactions = systemTransactions;
		}
    }

    // Base policy configuration
    public static class Base {
        private Duration abandonCallAfter;
        private Duration delayBetweenRetries;
        private Integer maximumNumberOfCallAttempts;
        private Replica replicaOrder;
        private Integer resetTtlOnReadAtPercent;
        private Boolean sendKey;
        private Boolean useCompression;
        private Duration waitForCallToComplete;
        private Duration waitForConnectionToComplete;
        private Duration waitForSocketResponseAfterCallFails;

        // Getters and setters
        public Duration getAbandonCallAfter() { return abandonCallAfter; }
        public void setAbandonCallAfter(Duration abandonCallAfter) { this.abandonCallAfter = abandonCallAfter; }

        public Duration getDelayBetweenRetries() { return delayBetweenRetries; }
        public void setDelayBetweenRetries(Duration delayBetweenRetries) { this.delayBetweenRetries = delayBetweenRetries; }

        public Integer getMaximumNumberOfCallAttempts() { return maximumNumberOfCallAttempts; }
        public void setMaximumNumberOfCallAttempts(Integer maximumNumberOfCallAttempts) { this.maximumNumberOfCallAttempts = maximumNumberOfCallAttempts; }

        public Replica getReplicaOrder() { return replicaOrder; }
        public void setReplicaOrder(Replica replicaOrder) { this.replicaOrder = replicaOrder; }

        public Integer getResetTtlOnReadAtPercent() { return resetTtlOnReadAtPercent; }
        public void setResetTtlOnReadAtPercent(Integer resetTtlOnReadAtPercent) { this.resetTtlOnReadAtPercent = resetTtlOnReadAtPercent; }

        public Boolean getSendKey() { return sendKey; }
        public void setSendKey(Boolean sendKey) { this.sendKey = sendKey; }

        public Boolean getUseCompression() { return useCompression; }
        public void setUseCompression(Boolean useCompression) { this.useCompression = useCompression; }

        public Duration getWaitForCallToComplete() { return waitForCallToComplete; }
        public void setWaitForCallToComplete(Duration waitForCallToComplete) { this.waitForCallToComplete = waitForCallToComplete; }

        public Duration getWaitForConnectionToComplete() { return waitForConnectionToComplete; }
        public void setWaitForConnectionToComplete(Duration waitForConnectionToComplete) { this.waitForConnectionToComplete = waitForConnectionToComplete; }

        public Duration getWaitForSocketResponseAfterCallFails() { return waitForSocketResponseAfterCallFails; }
        public void setWaitForSocketResponseAfterCallFails(Duration waitForSocketResponseAfterCallFails) { this.waitForSocketResponseAfterCallFails = waitForSocketResponseAfterCallFails; }
    }

    // Consistency mode read configuration
    public static class ConsistencyModeRead extends Base {
        private ReadModeSC readModeSC;

        public ReadModeSC getReadConsistency() { return readModeSC; }
        public void setReadConsistency(ReadModeSC readConsistency) { this.readModeSC = readConsistency; }
    }

    // Availability mode read configuration
    public static class AvailabilityModeRead extends Base {
        private ReadModeAP readModeAP;

        public ReadModeAP getMigrationReadConsistency() { return readModeAP; }
        public void setMigrationReadConsistency(ReadModeAP migrationReadConsistency) { this.readModeAP = migrationReadConsistency; }
    }

    // Write configuration
    public static class Write extends Base {
        private Boolean useDurableDelete;

        public Boolean getUseDurableDelete() { return useDurableDelete; }
        public void setUseDurableDelete(Boolean useDurableDelete) { this.useDurableDelete = useDurableDelete; }
    }

    // Batch configuration
    public static class Batch extends Base {
        private Integer maxConcurrentServers;
        private Boolean allowInlineMemoryAccess;
        private Boolean allowInlineSsdAccess;

        public Integer getMaxConcurrentServers() { return maxConcurrentServers; }
        public void setMaxConcurrentServers(Integer maxConcurrentServers) { this.maxConcurrentServers = maxConcurrentServers; }

        public Boolean getAllowInlineMemoryAccess() { return allowInlineMemoryAccess; }
        public void setAllowInlineMemoryAccess(Boolean allowInlineMemoryAccess) { this.allowInlineMemoryAccess = allowInlineMemoryAccess; }

        public Boolean getAllowInlineSsdAccess() { return allowInlineSsdAccess; }
        public void setAllowInlineSsdAccess(Boolean allowInlineSsdAccess) { this.allowInlineSsdAccess = allowInlineSsdAccess; }
    }

    // Query configuration
    public static class Query extends Base {
        private Integer recordQueueSize;
        private Integer maxConcurrentServers;

        public Integer getRecordQueueSize() { return recordQueueSize; }
        public void setRecordQueueSize(Integer recordQueueSize) { this.recordQueueSize = recordQueueSize; }

        public Integer getMaxConcurrentServers() { return maxConcurrentServers; }
        public void setMaxConcurrentServers(Integer maxConcurrentServers) { this.maxConcurrentServers = maxConcurrentServers; }
    }

    // Info configuration
    public static class Info extends Base {
        // Info only has abandonCallAfter from the base PolicyConfig
    }

    // System - Transaction Verify configuration (read-like settings)
    public static class SystemTxnVerifyConfig extends Base {
        private ReadModeSC consistency;

        public ReadModeSC getConsistency() { return consistency; }
        public void setConsistency(ReadModeSC consistency) { this.consistency = consistency; }
    }

    // System - Transaction Roll configuration (write-like settings)
    public static class SystemTxnRoll extends Base {
        // Uses base PolicyConfig fields: abandonCallAfter, delayBetweenRetries, maximumNumberOfCallAttempts,
        // replicaOrder, waitForCallToComplete, waitForConnectionToComplete, waitForSocketResponseAfterCallFails
    }

    // System - Connections configuration
    public static class SystemConnections {
        private Integer minimumConnectionsPerNode;
        private Integer maximumConnectionsPerNode;
        private Duration maximumSocketIdleTime;

        public Integer getMinimumConnectionsPerNode() { return minimumConnectionsPerNode; }
        public void setMinimumConnectionsPerNode(Integer minimumConnectionsPerNode) { this.minimumConnectionsPerNode = minimumConnectionsPerNode; }

        public Integer getMaximumConnectionsPerNode() { return maximumConnectionsPerNode; }
        public void setMaximumConnectionsPerNode(Integer maximumConnectionsPerNode) { this.maximumConnectionsPerNode = maximumConnectionsPerNode; }

        public Duration getMaximumSocketIdleTime() { return maximumSocketIdleTime; }
        public void setMaximumSocketIdleTime(Duration maximumSocketIdleTime) { this.maximumSocketIdleTime = maximumSocketIdleTime; }
    }

    // System - Circuit Breaker configuration
    public static class SystemCircuitBreaker {
        private Integer numTendIntervalsInErrorWindow;
        private Integer maximumErrorsInErrorWindow;

        public Integer getNumTendIntervalsInErrorWindow() { return numTendIntervalsInErrorWindow; }
        public void setNumTendIntervalsInErrorWindow(Integer numTendIntervalsInErrorWindow) { this.numTendIntervalsInErrorWindow = numTendIntervalsInErrorWindow; }

        public Integer getMaximumErrorsInErrorWindow() { return maximumErrorsInErrorWindow; }
        public void setMaximumErrorsInErrorWindow(Integer maximumErrorsInErrorWindow) { this.maximumErrorsInErrorWindow = maximumErrorsInErrorWindow; }
    }

    // System - Refresh configuration
    public static class SystemRefresh {
        private Duration tendInterval;

        public Duration getTendInterval() { return tendInterval; }
        public void setTendInterval(Duration tendInterval) { this.tendInterval = tendInterval; }
    }

    // System - Transactions configuration
    public static class SystemTransactions {
		private Boolean implicitBatchWriteTransactions;
        private Duration sleepBetweenAttempts;
        private Integer numberOfAttempts;

        public Boolean getImplicitBatchWriteTransactions() {
			return implicitBatchWriteTransactions;
		}
		public void setImplicitBatchWriteTransactions(Boolean implicitBatchWriteTransactions) {
			this.implicitBatchWriteTransactions = implicitBatchWriteTransactions;
		}

		public Duration getSleepBetweenAttempts() {
			return sleepBetweenAttempts;
		}
		public void setSleepBetweenAttempts(Duration sleepBetweenAttempts) {
			this.sleepBetweenAttempts = sleepBetweenAttempts;
		}

		public Integer getNumberOfAttempts() {
			return numberOfAttempts;
		}
		public void setNumberOfAttempts(Integer numberOfAttempts) {
			this.numberOfAttempts = numberOfAttempts;
		}
    }

    // System settings configuration (for cluster-level settings)
    public static class SystemSettings {
        private SystemConnections connections;
        private SystemCircuitBreaker circuitBreaker;
        private SystemRefresh refresh;
        private SystemTransactions transactions;

        public SystemConnections getConnections() { return connections; }
        public void setConnections(SystemConnections connections) { this.connections = connections; }

        public SystemCircuitBreaker getCircuitBreaker() { return circuitBreaker; }
        public void setCircuitBreaker(SystemCircuitBreaker circuitBreaker) { this.circuitBreaker = circuitBreaker; }

        public SystemRefresh getRefresh() { return refresh; }
        public void setRefresh(SystemRefresh refresh) { this.refresh = refresh; }

        public SystemTransactions getTransactions() { return transactions; }
        public void setTransactions(SystemTransactions transactions) { this.transactions = transactions; }
   }
}