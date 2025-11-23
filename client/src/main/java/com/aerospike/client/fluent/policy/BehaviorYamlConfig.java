package com.aerospike.client.fluent.policy;

import java.time.Duration;
import java.util.List;

public class BehaviorYamlConfig {
    
    private List<BehaviorConfig> behaviors;
    
    // Getters and setters
    public List<BehaviorConfig> getBehaviors() { return behaviors; }
    public void setBehaviors(List<BehaviorConfig> behaviors) { this.behaviors = behaviors; }
    
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
        private SystemConnectionsConfig systemConnections;
        private SystemCircuitBreakerConfig systemCircuitBreaker;
        private SystemRefreshConfig systemRefresh;
        
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
        
        public SystemConnectionsConfig getSystemConnections() { return systemConnections; }
        public void setSystemConnections(SystemConnectionsConfig systemConnections) { this.systemConnections = systemConnections; }
        
        public SystemCircuitBreakerConfig getSystemCircuitBreaker() { return systemCircuitBreaker; }
        public void setSystemCircuitBreaker(SystemCircuitBreakerConfig systemCircuitBreaker) { this.systemCircuitBreaker = systemCircuitBreaker; }
        
        public SystemRefreshConfig getSystemRefresh() { return systemRefresh; }
        public void setSystemRefresh(SystemRefreshConfig systemRefresh) { this.systemRefresh = systemRefresh; }
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
    
    // System - Connections configuration
    public static class SystemConnectionsConfig {
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
    public static class SystemCircuitBreakerConfig {
        private Integer numTendIntervalsInErrorWindow;
        private Integer maximumErrorsInErrorWindow;
        
        public Integer getNumTendIntervalsInErrorWindow() { return numTendIntervalsInErrorWindow; }
        public void setNumTendIntervalsInErrorWindow(Integer numTendIntervalsInErrorWindow) { this.numTendIntervalsInErrorWindow = numTendIntervalsInErrorWindow; }
        
        public Integer getMaximumErrorsInErrorWindow() { return maximumErrorsInErrorWindow; }
        public void setMaximumErrorsInErrorWindow(Integer maximumErrorsInErrorWindow) { this.maximumErrorsInErrorWindow = maximumErrorsInErrorWindow; }
    }
    
    // System - Refresh configuration
    public static class SystemRefreshConfig {
        private Duration tendInterval;
        
        public Duration getTendInterval() { return tendInterval; }
        public void setTendInterval(Duration tendInterval) { this.tendInterval = tendInterval; }
    }
} 