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

import java.time.Duration;
import java.util.List;

public class BehaviorYamlConfig {

    private List<BehaviorConfig> behaviors;
    private SystemConfig system;

    // Getters and setters
    public List<BehaviorConfig> getBehaviors() { return behaviors; }
    public void setBehaviors(List<BehaviorConfig> behaviors) { this.behaviors = behaviors; }

    public SystemConfig getSystem() { return system; }
    public void setSystem(SystemConfig system) { this.system = system; }

    // Individual behavior configuration
    public static class BehaviorConfig {
        private String name;
        private String parent;
        private Boolean sendKey;
        private Boolean useCompression;
        private PolicyConfig allOperations;
        private ConsistencyModeReadConfig readModeSC;
        private AvailabilityModeReadConfig readModeAP;
        private WriteConfig retryableWrites;
        private WriteConfig nonRetryableWrites;
        private BatchConfig batchReads;
        private BatchConfig batchWrites;
        private QueryConfig query;
        private InfoConfig info;
        private SystemTxnVerifyConfig systemTxnVerify;
        private SystemTxnRollConfig systemTxnRoll;

        // Getters and setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getParent() { return parent; }
        public void setParent(String parent) { this.parent = parent; }

        public Boolean getSendKey() { return sendKey; }
        public void setSendKey(Boolean sendKey) { this.sendKey = sendKey; }

        public Boolean getUseCompression() { return useCompression; }
        public void setUseCompression(Boolean useCompression) { this.useCompression = useCompression; }

        public PolicyConfig getAllOperations() { return allOperations; }
        public void setAllOperations(PolicyConfig allOperations) { this.allOperations = allOperations; }

        public ConsistencyModeReadConfig getConsistencyModeReads() { return readModeSC; }
        public void setConsistencyModeReads(ConsistencyModeReadConfig consistencyModeReads) { this.readModeSC = consistencyModeReads; }

        public AvailabilityModeReadConfig getAvailabilityModeReads() { return readModeAP; }
        public void setAvailabilityModeReads(AvailabilityModeReadConfig availabilityModeReads) { this.readModeAP = availabilityModeReads; }

        public WriteConfig getRetryableWrites() { return retryableWrites; }
        public void setRetryableWrites(WriteConfig retryableWrites) { this.retryableWrites = retryableWrites; }

        public WriteConfig getNonRetryableWrites() { return nonRetryableWrites; }
        public void setNonRetryableWrites(WriteConfig nonRetryableWrites) { this.nonRetryableWrites = nonRetryableWrites; }

        public BatchConfig getBatchReads() { return batchReads; }
        public void setBatchReads(BatchConfig batchReads) { this.batchReads = batchReads; }

        public BatchConfig getBatchWrites() { return batchWrites; }
        public void setBatchWrites(BatchConfig batchWrites) { this.batchWrites = batchWrites; }

        public QueryConfig getQuery() { return query; }
        public void setQuery(QueryConfig query) { this.query = query; }

        public InfoConfig getInfo() { return info; }
        public void setInfo(InfoConfig info) { this.info = info; }

        public SystemTxnVerifyConfig getSystemTxnVerify() { return systemTxnVerify; }
        public void setSystemTxnVerify(SystemTxnVerifyConfig systemTxnVerify) { this.systemTxnVerify = systemTxnVerify; }

        public SystemTxnRollConfig getSystemTxnRoll() { return systemTxnRoll; }
        public void setSystemTxnRoll(SystemTxnRollConfig systemTxnRoll) { this.systemTxnRoll = systemTxnRoll; }
    }

    // Base policy configuration
    public static class PolicyConfig {
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
    public static class ConsistencyModeReadConfig extends PolicyConfig {
        private ReadModeSC readModeSC;

        public ReadModeSC getReadConsistency() { return readModeSC; }
        public void setReadConsistency(ReadModeSC readConsistency) { this.readModeSC = readConsistency; }
    }

    // Availability mode read configuration
    public static class AvailabilityModeReadConfig extends PolicyConfig {
        private ReadModeAP readModeAP;

        public ReadModeAP getMigrationReadConsistency() { return readModeAP; }
        public void setMigrationReadConsistency(ReadModeAP migrationReadConsistency) { this.readModeAP = migrationReadConsistency; }
    }

    // Write configuration
    public static class WriteConfig extends PolicyConfig {
        private Boolean useDurableDelete;

        public Boolean getUseDurableDelete() { return useDurableDelete; }
        public void setUseDurableDelete(Boolean useDurableDelete) { this.useDurableDelete = useDurableDelete; }
    }

    // Batch configuration
    public static class BatchConfig extends PolicyConfig {
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
    public static class QueryConfig extends PolicyConfig {
        private Integer recordQueueSize;
        private Integer maxConcurrentServers;

        public Integer getRecordQueueSize() { return recordQueueSize; }
        public void setRecordQueueSize(Integer recordQueueSize) { this.recordQueueSize = recordQueueSize; }

        public Integer getMaxConcurrentServers() { return maxConcurrentServers; }
        public void setMaxConcurrentServers(Integer maxConcurrentServers) { this.maxConcurrentServers = maxConcurrentServers; }
    }

    // Info configuration
    public static class InfoConfig extends PolicyConfig {
        // Info only has abandonCallAfter from the base PolicyConfig
    }

    // System - Transaction Verify configuration (read-like settings)
    public static class SystemTxnVerifyConfig extends PolicyConfig {
        private ReadModeSC consistency;

        public ReadModeSC getConsistency() { return consistency; }
        public void setConsistency(ReadModeSC consistency) { this.consistency = consistency; }
    }

    // System - Transaction Roll configuration (write-like settings)
    public static class SystemTxnRollConfig extends PolicyConfig {
        // Uses base PolicyConfig fields: abandonCallAfter, delayBetweenRetries, maximumNumberOfCallAttempts,
        // replicaOrder, waitForCallToComplete, waitForConnectionToComplete, waitForSocketResponseAfterCallFails
    }

    // -----------------------------------------------------------------------------------
    // System Settings Configuration (for unified YAML)
    // -----------------------------------------------------------------------------------

    /**
     * Top-level system settings configuration containing default and cluster-specific settings.
     */
    public static class SystemConfig {
        private SystemSettingsConfig defaultSettings;
        private java.util.Map<String, SystemSettingsConfig> clusters;

        public SystemSettingsConfig getDefaultSettings() { return defaultSettings; }
        public void setDefaultSettings(SystemSettingsConfig defaultSettings) {
            this.defaultSettings = defaultSettings;
        }

        public java.util.Map<String, SystemSettingsConfig> getClusters() { return clusters; }
        public void setClusters(java.util.Map<String, SystemSettingsConfig> clusters) {
            this.clusters = clusters;
        }
    }

    /**
     * System settings for a single cluster or default settings.
     */
    public static class SystemSettingsConfig {
        private ConnectionsConfig connections;
        private CircuitBreakerConfig circuitBreaker;
        private RefreshConfig refresh;

        public ConnectionsConfig getConnections() { return connections; }
        public void setConnections(ConnectionsConfig connections) { this.connections = connections; }

        public CircuitBreakerConfig getCircuitBreaker() { return circuitBreaker; }
        public void setCircuitBreaker(CircuitBreakerConfig circuitBreaker) { this.circuitBreaker = circuitBreaker; }

        public RefreshConfig getRefresh() { return refresh; }
        public void setRefresh(RefreshConfig refresh) { this.refresh = refresh; }
    }

    /**
     * Connection pool configuration.
     */
    public static class ConnectionsConfig {
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

    /**
     * Circuit breaker configuration.
     */
    public static class CircuitBreakerConfig {
        private Integer numTendIntervalsInErrorWindow;
        private Integer maximumErrorsInErrorWindow;

        public Integer getNumTendIntervalsInErrorWindow() { return numTendIntervalsInErrorWindow; }
        public void setNumTendIntervalsInErrorWindow(Integer numTendIntervalsInErrorWindow) { this.numTendIntervalsInErrorWindow = numTendIntervalsInErrorWindow; }

        public Integer getMaximumErrorsInErrorWindow() { return maximumErrorsInErrorWindow; }
        public void setMaximumErrorsInErrorWindow(Integer maximumErrorsInErrorWindow) { this.maximumErrorsInErrorWindow = maximumErrorsInErrorWindow; }
    }

    /**
     * Cluster refresh configuration.
     */
    public static class RefreshConfig {
        private Duration tendInterval;

        public Duration getTendInterval() { return tendInterval; }
        public void setTendInterval(Duration tendInterval) { this.tendInterval = tendInterval; }
    }
}