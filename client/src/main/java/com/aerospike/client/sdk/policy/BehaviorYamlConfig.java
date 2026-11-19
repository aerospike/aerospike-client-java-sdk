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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.sdk.discovery.SeedMergePolicy;
import com.aerospike.client.sdk.discovery.TranslationMissMode;
import com.aerospike.client.sdk.discovery.AwsEc2TagSeedCandidateProvider.AddressField;
import com.aerospike.client.sdk.discovery.DnsSeedCandidateProvider.RecordType;

public class BehaviorYamlConfig {

    private Map<String, BehaviorConfig> behaviors;
    private Map<String, SystemSettingsConfig> system;

    // Getters and setters
    public Map<String, BehaviorConfig> getBehaviors() { return behaviors; }
    public void setBehaviors(Map<String, BehaviorConfig> behaviors) { this.behaviors = behaviors; }

    public Map<String, SystemSettingsConfig> getSystem() { return system; }
    public void setSystem(Map<String, SystemSettingsConfig> system) { this.system = system; }

    // Individual behavior configuration (name is the map key)
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
        private Boolean allowSingleNodeBatchReads;

        public Integer getMaxConcurrentServers() { return maxConcurrentServers; }
        public void setMaxConcurrentServers(Integer maxConcurrentServers) { this.maxConcurrentServers = maxConcurrentServers; }

        public Boolean getAllowInlineMemoryAccess() { return allowInlineMemoryAccess; }
        public void setAllowInlineMemoryAccess(Boolean allowInlineMemoryAccess) { this.allowInlineMemoryAccess = allowInlineMemoryAccess; }

        public Boolean getAllowInlineSsdAccess() { return allowInlineSsdAccess; }
        public void setAllowInlineSsdAccess(Boolean allowInlineSsdAccess) { this.allowInlineSsdAccess = allowInlineSsdAccess; }

        public Boolean getAllowSingleNodeBatchReads() { return allowSingleNodeBatchReads; }
        public void setAllowSingleNodeBatchReads(Boolean allowSingleNodeBatchReads) { this.allowSingleNodeBatchReads = allowSingleNodeBatchReads; }
    }

    // Query configuration
    public static class QueryConfig extends PolicyConfig {
        private Integer recordQueueSize;
        private Integer maxConcurrentServers;
        private Boolean allowScansWithWhere;

        public Integer getRecordQueueSize() { return recordQueueSize; }
        public void setRecordQueueSize(Integer recordQueueSize) { this.recordQueueSize = recordQueueSize; }

        public Integer getMaxConcurrentServers() { return maxConcurrentServers; }
        public void setMaxConcurrentServers(Integer maxConcurrentServers) { this.maxConcurrentServers = maxConcurrentServers; }

        public Boolean getAllowScansWithWhere() { return allowScansWithWhere; }
        public void setAllowScansWithWhere(Boolean allowScansWithWhere) { this.allowScansWithWhere = allowScansWithWhere; }
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
        private Duration maximumSocketIdleTimeOnCheckout;

        public Integer getMinimumConnectionsPerNode() { return minimumConnectionsPerNode; }
        public void setMinimumConnectionsPerNode(Integer minimumConnectionsPerNode) { this.minimumConnectionsPerNode = minimumConnectionsPerNode; }

        public Integer getMaximumConnectionsPerNode() { return maximumConnectionsPerNode; }
        public void setMaximumConnectionsPerNode(Integer maximumConnectionsPerNode) { this.maximumConnectionsPerNode = maximumConnectionsPerNode; }

        public Duration getMaximumSocketIdleTime() { return maximumSocketIdleTime; }
        public void setMaximumSocketIdleTime(Duration maximumSocketIdleTime) { this.maximumSocketIdleTime = maximumSocketIdleTime; }

        public Duration getMaximumSocketIdleTimeOnCheckout() { return maximumSocketIdleTimeOnCheckout; }
        public void setMaximumSocketIdleTimeOnCheckout(Duration maximumSocketIdleTimeOnCheckout) { this.maximumSocketIdleTimeOnCheckout = maximumSocketIdleTimeOnCheckout; }
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

    // System - Transactions configuration
    public static class SystemTransactionsConfig {
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
    public static class SystemSettingsConfig {
        private SystemConnectionsConfig connections;
        private SystemCircuitBreakerConfig circuitBreaker;
        private SystemRefreshConfig refresh;
        private SystemTransactionsConfig transactions;
        private SystemMetricsConfig metrics;
        private SystemDiscoveryConfig discovery;
        private SystemTranslationConfig translation;

        public SystemConnectionsConfig getConnections() { return connections; }
        public void setConnections(SystemConnectionsConfig connections) { this.connections = connections; }

        public SystemCircuitBreakerConfig getCircuitBreaker() { return circuitBreaker; }
        public void setCircuitBreaker(SystemCircuitBreakerConfig circuitBreaker) { this.circuitBreaker = circuitBreaker; }

        public SystemRefreshConfig getRefresh() { return refresh; }
        public void setRefresh(SystemRefreshConfig refresh) { this.refresh = refresh; }

        public SystemTransactionsConfig getTransactions() { return transactions; }
        public void setTransactions(SystemTransactionsConfig transactions) { this.transactions = transactions; }

        public SystemMetricsConfig getMetrics() { return metrics; }
        public void setMetrics(SystemMetricsConfig metrics) { this.metrics = metrics; }

        public SystemDiscoveryConfig getDiscovery() {
            return discovery;
        }
        public void setDiscovery(SystemDiscoveryConfig discovery) {
            this.discovery = discovery;
        }

        public SystemTranslationConfig getTranslation() {
            return translation;
        }
        public void setTranslation(SystemTranslationConfig translation) {
            this.translation = translation;
        }
    }

    public static class SystemMetricsConfig {
        private String exporter;
        private Map<String,String> labels;
        private String reportDir;
        private String reportSizeLimit;
        private Double exportSampleRate;
        private Duration exportInterval;
        private Boolean enabled;
        private MetricsExtendedConfig extended;

        public Map<String, String> getLabels() {
            return labels;
        }
        public void setLabels(Map<String, String> labels) {
            this.labels = labels;
        }

        public String getExporter() {
            return exporter;
        }
        public void setExporter(String exporter) {
            this.exporter = exporter;
        }

        public String getReportDir() {
            return reportDir;
        }
        public void setReportDir(String reportDir) {
            this.reportDir = reportDir;
        }

        public String getReportSizeLimit() {
            return reportSizeLimit;
        }
        public void setReportSizeLimit(String reportSizeLimit) {
            this.reportSizeLimit = reportSizeLimit;
        }

        public Double getExportSampleRate() {
            return exportSampleRate;
        }
        public void setExportSampleRate(Double exportSampleRate) {
            this.exportSampleRate = exportSampleRate;
        }

        public Duration getExportInterval() {
            return exportInterval;
        }
        public void setExportInterval(Duration interval) {
            this.exportInterval = interval;
        }

        public Boolean getEnabled() {
            return enabled;
        }
        public void setEnabled(Boolean enabled) {
            this.enabled = enabled;
        }

        public MetricsExtendedConfig getExtended() {
            return extended;
        }
        public void setExtended(MetricsExtendedConfig extended) {
            this.extended = extended;
        }
    }

    public static class MetricsExtendedConfig {
        private MetricsOperationalConfig operational;
        private MetricsUsageConfig usage;

        public MetricsOperationalConfig getOperational() {
            return operational;
        }
        public void setOperational(MetricsOperationalConfig operational) {
            this.operational = operational;
        }

        public MetricsUsageConfig getUsage() {
            return usage;
        }
        public void setUsage(MetricsUsageConfig usage) {
            this.usage = usage;
        }
    }

    public static class MetricsOperationalConfig {
        private TimeUnit latencyUnit;
        private Integer latencyColumns;
        private Integer latencyShift;
        private Boolean enabled;
        private MetricsSamplerConfig sampler;

        public TimeUnit getLatencyUnit() {
            return latencyUnit;
        }
        public void setLatencyUnit(TimeUnit unit) {
            this.latencyUnit = unit;
        }

        public Integer getLatencyColumns() {
            return latencyColumns;
        }
        public void setLatencyColumns(Integer latencyColumns) {
            this.latencyColumns = latencyColumns;
        }

        public Integer getLatencyShift() {
            return latencyShift;
        }
        public void setLatencyShift(Integer latencyShift) {
            this.latencyShift = latencyShift;
        }

        public Boolean getEnabled() {
            return enabled;
        }
        public void setEnabled(Boolean enabled) {
            this.enabled = enabled;
        }

        public MetricsSamplerConfig getSampler() {
            return sampler;
        }
        public void setSampler(MetricsSamplerConfig sampler) {
            this.sampler = sampler;
        }
    }

    public static class MetricsSamplerConfig {
        private Integer range;
        private Integer threshold;

        public Integer getRange() {
            return range;
        }
        public void setRange(Integer range) {
            this.range = range;
        }

        public Integer getThreshold() {
            return threshold;
        }
        public void setThreshold(Integer threshold) {
            this.threshold = threshold;
        }
    }

    public static class MetricsUsageConfig {
        private Boolean enabled;

        public Boolean getEnabled() {
            return enabled;
        }
        public void setEnabled(Boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class SystemDiscoveryConfig {
        private SeedMergePolicy mergePolicy;
        private Duration refreshInterval;
        private Duration refreshTimeout;
        private Duration tendRefreshDeadline;
        private DnsConfig dns;
        private AwsConfig aws;

        public SeedMergePolicy getMergePolicy() {
            return mergePolicy;
        }
        public void setMergePolicy(SeedMergePolicy mergePolicy) {
            this.mergePolicy = mergePolicy;
        }

        public Duration getRefreshInterval() {
            return refreshInterval;
        }
        public void setRefreshInterval(Duration refreshInterval) {
            this.refreshInterval = refreshInterval;
        }

        public Duration getRefreshTimeout() {
            return refreshTimeout;
        }
        public void setRefreshTimeout(Duration refreshTimeout) {
            this.refreshTimeout = refreshTimeout;
        }

        public Duration getTendRefreshDeadline() {
            return tendRefreshDeadline;
        }
        public void setTendRefreshDeadline(Duration tendRefreshDeadline) {
            this.tendRefreshDeadline = tendRefreshDeadline;
        }

        public DnsConfig getDns() {
            return dns;
        }
        public void setDns(DnsConfig dns) {
            this.dns = dns;
        }

        public AwsConfig getAws() {
            return aws;
        }
        public void setAws(AwsConfig aws) {
            this.aws = aws;
        }
    }

    public static class DnsConfig {
        private List<String> names;
        private List<String> servers;
        private RecordType recordType;
        private String tlsName;
        private Integer port;
        private Duration timeout;

        public List<String> getNames() {
            return names;
        }
        public void setNames(List<String> names) {
            this.names = names;
        }

        public List<String> getServers() {
            return servers;
        }
        public void setServers(List<String> servers) {
            this.servers = servers;
        }

        public RecordType getRecordType() {
            return recordType;
        }
        public void setRecordType(RecordType recordType) {
            this.recordType = recordType;
        }

        public String getTlsName() {
            return tlsName;
        }
        public void setTlsName(String tlsName) {
            this.tlsName = tlsName;
        }

        public Integer getPort() {
            return port;
        }
        public void setPort(Integer port) {
            this.port = port;
        }

        public Duration getTimeout() {
            return timeout;
        }
        public void setTimeout(Duration timeout) {
            this.timeout = timeout;
        }
    }

    public static class AwsConfig {
        private String region;
        private Map<String,List<String>> filters;
        private AddressField addressField;
        private Integer port;
        private String tlsName;
        private Duration timeout;

        public String getRegion() {
            return region;
        }
        public void setRegion(String region) {
            this.region = region;
        }

        public Map<String, List<String>> getFilters() {
            return filters;
        }
        public void setFilters(Map<String, List<String>> filters) {
            this.filters = filters;
        }

        public AddressField getAddressField() {
            return addressField;
        }
        public void setAddressField(AddressField addressField) {
            this.addressField = addressField;
        }

        public Integer getPort() {
            return port;
        }
        public void setPort(Integer port) {
            this.port = port;
        }

        public String getTlsName() {
            return tlsName;
        }
        public void setTlsName(String tlsName) {
            this.tlsName = tlsName;
        }

        public Duration getTimeout() {
            return timeout;
        }
        public void setTimeout(Duration timeout) {
            this.timeout = timeout;
        }
    }

    public static class SystemTranslationConfig {
        private TranslationMissMode onMiss;
        private Map<String,String> staticMap;
        private Map<String,String> portMap;
        private Boolean useServicesAlternate;

        public TranslationMissMode getOnMiss() {
            return onMiss;
        }
        public void setOnMiss(TranslationMissMode onMiss) {
            this.onMiss = onMiss;
        }

        public Map<String, String> getStaticMap() {
            return staticMap;
        }
        public void setStaticMap(Map<String, String> staticMap) {
            this.staticMap = staticMap;
        }

        public Map<String, String> getPortMap() {
            return portMap;
        }
        public void setPortMap(Map<String, String> portMap) {
            this.portMap = portMap;
        }

        public Boolean getUseServicesAlternate() {
            return useServicesAlternate;
        }
        public void setUseServicesAlternate(Boolean useServicesAlternate) {
            this.useServicesAlternate = useServicesAlternate;
        }
    }
}
