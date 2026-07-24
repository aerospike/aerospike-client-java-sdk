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
package com.aerospike.client.sdk;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * System-level settings that apply to an entire Cluster instance.
 *
 * <p>These settings are cluster-wide and cannot vary per Behavior. They include
 * connection pool settings, circuit breaker configuration, and cluster refresh intervals.</p>
 *
 * <p><b>Priority Hierarchy</b> (highest to lowest):</p>
 * <ol>
 *   <li>YAML cluster-specific settings (matching cluster name)</li>
 *   <li>YAML default settings</li>
 *   <li>Code-provided settings (via {@link ClusterDefinition})</li>
 *   <li>Hard-coded defaults ({@link #DEFAULT})</li>
 * </ol>
 *
 * <p><b>Example Usage:</b></p>
 * <pre>{@code
 * // Lambda-based configuration (inline):
 * new ClusterDefinition("localhost", 3000)
 *     .withSystemSettings(builder -> builder
 *         .connections(ops -> ops
 *             .minimumConnectionsPerNode(100)
 *             .maximumConnectionsPerNode(400)
 *         )
 *         .circuitBreaker(ops -> ops
 *             .maximumErrorsInErrorWindow(50)
 *         )
 *     )
 *     .connect();
 *
 * // Or explicit builder for complex configurations:
 * SystemSettings settings = SystemSettings.builder()
 *     .connections(ops -> ops
 *         .minimumConnectionsPerNode(100)
 *         .maximumConnectionsPerNode(400)
 *     )
 *     .circuitBreaker(ops -> ops
 *         .maximumErrorsInErrorWindow(50)
 *     )
 *     .build();
 *
 * new ClusterDefinition("localhost", 3000)
 *     .withSystemSettings(settings)
 *     .connect();
 * }</pre>
 *
 * @see ClusterDefinition#withSystemSettings(SystemSettings)
 * @see SystemSettingsRegistry
 */
public class SystemSettings {

    // ===== Connections Settings =====
    private final Integer minimumConnectionsPerNode;
    private final Integer maximumConnectionsPerNode;
    private final Duration maximumSocketIdleTime;

    // ===== Circuit Breaker Settings =====
    private final Integer numTendIntervalsInErrorWindow;
    private final Integer maximumErrorsInErrorWindow;

    // ===== Refresh Settings =====
    private final Duration tendInterval;

    // ===== Transactions Settings =====
    private final Boolean implicitBatchWriteTransactions;
    private final Duration sleepBetweenAttempts;
    private final Integer numberOfAttempts;

    // ===== Metrics Settings =====
    private final Map<String,String> labels;
    private final Duration latencyWarn;
    private final Duration connectCreateWarn;
    private final String reportDir;
    private final Long reportSizeLimit;
    private final Integer interval;
    private final Integer latencyColumns;
    private final Integer latencyShift;
    private final Integer batchSizeWarn;
    private final Integer shortQueryRecordsMax;
    private final Integer longQueryRecordsMin;
    private final Boolean enabled;

    /**
     * Hard-coded default system settings.
     * These are the lowest priority and serve as the base for all other settings.
     */
    public static final SystemSettings DEFAULT = builder()
        .connections(ops -> ops
            .minimumConnectionsPerNode(0)
            .maximumConnectionsPerNode(100)
            .maximumSocketIdleTime(Duration.ofSeconds(55))
        )
        .circuitBreaker(ops -> ops
            .numTendIntervalsInErrorWindow(1)
            .maximumErrorsInErrorWindow(100)
        )
        .refresh(ops -> ops
            .tendInterval(Duration.ofSeconds(1))
        )
        .transactions(ops -> ops
            .implicitBatchWriteTransactions(true)
            .sleepBetweenAttempts(Duration.ofMillis(1000))
            .numberOfAttempts(5)
        )
        .metrics(ops -> ops
            .labels(new HashMap<>())
            .latencyWarn(Duration.ofMillis(50))
            .connectCreateWarn(Duration.ofMillis(500))
            .reportDir(".")
            .reportSizeLimit(0L)
            .interval(30)
            .latencyColumns(7)
            .latencyShift(1)
            .batchSizeWarn(500)
            .shortQueryRecordsMax(100)
            .longQueryRecordsMin(10)
            .enabled(false)
        )
        .build();

    private SystemSettings(Builder builder) {
        this.minimumConnectionsPerNode = builder.minimumConnectionsPerNode;
        this.maximumConnectionsPerNode = builder.maximumConnectionsPerNode;
        this.maximumSocketIdleTime = builder.maximumSocketIdleTime;
        this.numTendIntervalsInErrorWindow = builder.numTendIntervalsInErrorWindow;
        this.maximumErrorsInErrorWindow = builder.maximumErrorsInErrorWindow;
        this.tendInterval = builder.tendInterval;
        this.implicitBatchWriteTransactions = builder.implicitBatchWriteTransactions;
        this.sleepBetweenAttempts = builder.sleepBetweenAttempts;
        this.numberOfAttempts = builder.numberOfAttempts;
        this.labels = builder.labels;
        this.latencyWarn = builder.latencyWarn;
        this.connectCreateWarn = builder.connectCreateWarn;
        this.reportDir = builder.reportDir;
        this.reportSizeLimit = builder.reportSizeLimit;
        this.interval = builder.interval;
        this.latencyColumns = builder.latencyColumns;
        this.latencyShift = builder.latencyShift;
        this.batchSizeWarn = builder.batchSizeWarn;
        this.shortQueryRecordsMax = builder.shortQueryRecordsMax;
        this.longQueryRecordsMin = builder.longQueryRecordsMin;
        this.enabled = builder.enabled;
    }

    /**
     * Creates a new builder for SystemSettings.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Merges this settings instance with a base, using base values for any null fields.
     * This enables the 4-level priority hierarchy.
     *
     * @param base the base settings to use for null fields
     * @return a new SystemSettings with merged values
     */
    public SystemSettings mergeWith(SystemSettings base) {
        if (base == null) {
            return this;
        }

        Builder merged = builder();

        // Connections
        merged.minimumConnectionsPerNode = this.minimumConnectionsPerNode != null
            ? this.minimumConnectionsPerNode : base.minimumConnectionsPerNode;
        merged.maximumConnectionsPerNode = this.maximumConnectionsPerNode != null
            ? this.maximumConnectionsPerNode : base.maximumConnectionsPerNode;
        merged.maximumSocketIdleTime = this.maximumSocketIdleTime != null
            ? this.maximumSocketIdleTime : base.maximumSocketIdleTime;

        // Circuit Breaker
        merged.numTendIntervalsInErrorWindow = this.numTendIntervalsInErrorWindow != null
            ? this.numTendIntervalsInErrorWindow : base.numTendIntervalsInErrorWindow;
        merged.maximumErrorsInErrorWindow = this.maximumErrorsInErrorWindow != null
            ? this.maximumErrorsInErrorWindow : base.maximumErrorsInErrorWindow;

        // Refresh
        merged.tendInterval = this.tendInterval != null
            ? this.tendInterval : base.tendInterval;

        // Transactions
        merged.implicitBatchWriteTransactions = this.implicitBatchWriteTransactions != null
            ? this.implicitBatchWriteTransactions : base.implicitBatchWriteTransactions;
        merged.sleepBetweenAttempts = this.sleepBetweenAttempts != null
            ? this.sleepBetweenAttempts : base.sleepBetweenAttempts;
        merged.numberOfAttempts = this.numberOfAttempts != null
            ? this.numberOfAttempts : base.numberOfAttempts;

        // Metrics
        merged.labels = this.labels != null
            ? this.labels : base.labels;
        merged.latencyWarn = this.latencyWarn != null
            ? this.latencyWarn : base.latencyWarn;
        merged.connectCreateWarn = this.connectCreateWarn != null
            ? this.connectCreateWarn : base.connectCreateWarn;
        merged.reportDir = this.reportDir != null
            ? this.reportDir : base.reportDir;
        merged.reportSizeLimit = this.reportSizeLimit != null
            ? this.reportSizeLimit : base.reportSizeLimit;
        merged.interval = this.interval != null
            ? this.interval : base.interval;
        merged.latencyColumns = this.latencyColumns != null
            ? this.latencyColumns : base.latencyColumns;
        merged.latencyShift = this.latencyShift != null
            ? this.latencyShift : base.latencyShift;
        merged.batchSizeWarn = this.batchSizeWarn != null
            ? this.batchSizeWarn : base.batchSizeWarn;
        merged.shortQueryRecordsMax = this.shortQueryRecordsMax != null
            ? this.shortQueryRecordsMax : base.shortQueryRecordsMax;
        merged.longQueryRecordsMin = this.longQueryRecordsMin != null
            ? this.longQueryRecordsMin : base.longQueryRecordsMin;
        merged.enabled = this.enabled != null
            ? this.enabled : base.enabled;

        return merged.build();
    }

    // Getters
    public Integer getMinimumConnectionsPerNode() { return minimumConnectionsPerNode; }
    public Integer getMaximumConnectionsPerNode() { return maximumConnectionsPerNode; }
    public Duration getMaximumSocketIdleTime() { return maximumSocketIdleTime; }
    public Integer getNumTendIntervalsInErrorWindow() { return numTendIntervalsInErrorWindow; }
    public Integer getMaximumErrorsInErrorWindow() { return maximumErrorsInErrorWindow; }
    public Duration getTendInterval() { return tendInterval; }
    public Boolean getImplicitBatchWriteTransactions() { return implicitBatchWriteTransactions; }
    public Duration getSleepBetweenAttempts() { return sleepBetweenAttempts; }
    public Integer getNumberOfAttempts() { return numberOfAttempts; }
    public Map<String,String> getLabels() { return labels; }
    public Duration getLatencyWarn() { return latencyWarn; }
    public Duration getConnectCreateWarn() { return connectCreateWarn; }
    public String getReportDir() { return reportDir; }
    public Long getReportSizeLimit() { return reportSizeLimit; }
    public Integer getInterval() { return interval; }
    public Integer getLatencyColumns() { return latencyColumns; }
    public Integer getLatencyShift() { return latencyShift; }
    public Integer getBatchSizeWarn() { return batchSizeWarn; }
    public Integer getShortQueryRecordsMax() { return shortQueryRecordsMax; }
    public Integer getLongQueryRecordsMin() { return longQueryRecordsMin; }
    public Boolean getEnabled() { return enabled; }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SystemSettings that = (SystemSettings) o;
        return Objects.equals(minimumConnectionsPerNode, that.minimumConnectionsPerNode) &&
               Objects.equals(maximumConnectionsPerNode, that.maximumConnectionsPerNode) &&
               Objects.equals(maximumSocketIdleTime, that.maximumSocketIdleTime) &&
               Objects.equals(numTendIntervalsInErrorWindow, that.numTendIntervalsInErrorWindow) &&
               Objects.equals(maximumErrorsInErrorWindow, that.maximumErrorsInErrorWindow) &&
               Objects.equals(tendInterval, that.tendInterval) &&
               Objects.equals(implicitBatchWriteTransactions, that.implicitBatchWriteTransactions) &&
               Objects.equals(sleepBetweenAttempts, that.sleepBetweenAttempts) &&
               Objects.equals(numberOfAttempts, that.numberOfAttempts) &&
               Objects.equals(labels, that.labels) &&
               Objects.equals(latencyWarn, that.latencyWarn) &&
               Objects.equals(connectCreateWarn, that.connectCreateWarn) &&
               Objects.equals(reportDir, that.reportDir) &&
               Objects.equals(reportSizeLimit, that.reportSizeLimit) &&
               Objects.equals(interval, that.interval) &&
               Objects.equals(latencyColumns, that.latencyColumns) &&
               Objects.equals(latencyShift, that.latencyShift) &&
               Objects.equals(batchSizeWarn, that.batchSizeWarn) &&
               Objects.equals(shortQueryRecordsMax, that.shortQueryRecordsMax) &&
               Objects.equals(longQueryRecordsMin, that.longQueryRecordsMin) &&
               Objects.equals(enabled, that.enabled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(minimumConnectionsPerNode, maximumConnectionsPerNode, maximumSocketIdleTime,
                           numTendIntervalsInErrorWindow, maximumErrorsInErrorWindow, tendInterval,
                           implicitBatchWriteTransactions, sleepBetweenAttempts, numberOfAttempts,
                           labels, latencyWarn, connectCreateWarn, reportDir, reportSizeLimit,
                           interval, latencyColumns, latencyShift, batchSizeWarn, shortQueryRecordsMax,
                           longQueryRecordsMin, enabled);
    }

    @Override
    public String toString() {
        return "SystemSettings{" +
               "minConns=" + minimumConnectionsPerNode +
               ", maxConns=" + maximumConnectionsPerNode +
               ", socketIdleTime=" + maximumSocketIdleTime +
               ", errorWindow=" + numTendIntervalsInErrorWindow +
               ", maxErrors=" + maximumErrorsInErrorWindow +
               ", tendInterval=" + tendInterval +
               ", implicitBatchWriteTransactions=" + implicitBatchWriteTransactions +
               ", sleepBetweenAttempts=" + sleepBetweenAttempts +
               ", numberOfAttempts=" + numberOfAttempts +
               ", labels=" + labels +
               ", latencyWarn=" + latencyWarn.toMillis() + "ms" +
               ", connectCreateWarn=" + connectCreateWarn.toMillis() + "ms" +
               ", reportDir=" + reportDir +
               ", reportSizeLimit=" + reportSizeLimit +
               ", interval=" + interval +
               ", latencyColumns=" + latencyColumns +
               ", latencyShift=" + latencyShift +
               ", batchSizeWarn=" + batchSizeWarn +
               ", shortQueryRecordsMax=" + shortQueryRecordsMax +
               ", longQueryRecordsMin=" + longQueryRecordsMin +
               ", enabled=" + enabled +
               '}';
    }

    /**
     * Builder for SystemSettings with lambda-based configuration.
     */
    public static class Builder {
        private Integer minimumConnectionsPerNode;
        private Integer maximumConnectionsPerNode;
        private Duration maximumSocketIdleTime;
        private Integer numTendIntervalsInErrorWindow;
        private Integer maximumErrorsInErrorWindow;
        private Duration tendInterval;
        private Boolean implicitBatchWriteTransactions;
        private Duration sleepBetweenAttempts;
        private Integer numberOfAttempts;

        // Metrics
        private Map<String,String> labels;
        private Duration latencyWarn;
        private Duration connectCreateWarn;
        private String reportDir;
        private Long reportSizeLimit;
        private Integer interval;
        private Integer latencyColumns;
        private Integer latencyShift;
        private Integer batchSizeWarn;
        private Integer shortQueryRecordsMax;
        private Integer longQueryRecordsMin;
        private Boolean enabled;

        /**
         * Configure connection settings using a lambda.
         *
         * <p>Example:</p>
         * <pre>{@code
         * builder.connections(ops -> ops
         *     .minimumConnectionsPerNode(100)
         *     .maximumConnectionsPerNode(400)
         * )
         * }</pre>
         *
         * @param configurator lambda to configure connection settings
         * @return this builder for method chaining
         */
        public Builder connections(Consumer<ConnectionsTweaks> configurator) {
            configurator.accept(new ConnectionsTweaksImpl(this));
            return this;
        }

        /**
         * Configure circuit breaker settings using a lambda.
         *
         * <p>Example:</p>
         * <pre>{@code
         * builder.circuitBreaker(ops -> ops
         *     .numTendIntervalsInErrorWindow(2)
         *     .maximumErrorsInErrorWindow(50)
         * )
         * }</pre>
         *
         * @param configurator lambda to configure circuit breaker settings
         * @return this builder for method chaining
         */
        public Builder circuitBreaker(Consumer<CircuitBreakerTweaks> configurator) {
            configurator.accept(new CircuitBreakerTweaksImpl(this));
            return this;
        }

        /**
         * Configure cluster refresh settings using a lambda.
         *
         * <p>Example:</p>
         * <pre>{@code
         * builder.refresh(ops -> ops
         *     .tendInterval(Duration.ofSeconds(2))
         * )
         * }</pre>
         *
         * @param configurator lambda to configure refresh settings
         * @return this builder for method chaining
         */
        public Builder refresh(Consumer<RefreshTweaks> configurator) {
            configurator.accept(new RefreshTweaksImpl(this));
            return this;
        }

        /**
         * Configure transaction settings using a lambda.
         *
         * <p>These settings control transaction retry behavior and batch write transaction handling
         * at the system level. They apply to all transactions executed on the cluster.</p>
         *
         * <p>Example:</p>
         * <pre>{@code
         * builder.transactions(ops -> ops
         *     .implicitBatchWriteTransactions(true)
         *     .numberOfAttempts(5)
         *     .sleepBetweenAttempts(Duration.ofMillis(500))
         * )
         * }</pre>
         *
         * @param configurator lambda to configure transaction settings
         * @return this builder for method chaining
         */
        public Builder transactions(Consumer<TransactionsTweaks> configurator) {
            configurator.accept(new TransactionsTweaksImpl(this));
            return this;
        }

        /**
         * Configure metrics settings using a lambda.
         *
         * <p>Example:</p>
         * <pre>{@code
         * builder.metrics(ops -> ops
         *     .enabled(true)
         * )
         * }</pre>
         *
         * @param configurator lambda to configure transaction settings
         * @return this builder for method chaining
         */
        public Builder metrics(Consumer<MetricsTweaks> configurator) {
            configurator.accept(new MetricsTweaksImpl(this));
            return this;
        }

        /**
         * Builds the SystemSettings instance.
         */
        public SystemSettings build() {
            return new SystemSettings(this);
        }
    }

    // -----------------------------------------------------------------------------------
    // Tweaks interfaces for lambda-based configuration
    // -----------------------------------------------------------------------------------

    /**
     * Interface for configuring connection-related settings.
     */
    public interface ConnectionsTweaks {
        /**
         * Sets the minimum number of synchronous connections per server node.
         *
         * @param n minimum connections (0 or greater)
         * @return this tweaks instance for method chaining
         */
        ConnectionsTweaks minimumConnectionsPerNode(int n);

        /**
         * Sets the maximum number of synchronous connections per server node.
         *
         * @param n maximum connections (must be greater than minimum)
         * @return this tweaks instance for method chaining
         */
        ConnectionsTweaks maximumConnectionsPerNode(int n);

        /**
         * Sets the maximum socket idle time before connection closed.
         *
         * @param duration maximum idle time
         * @return this tweaks instance for method chaining
         */
        ConnectionsTweaks maximumSocketIdleTime(Duration duration);
    }

    /**
     * Interface for configuring circuit breaker settings.
     */
    public interface CircuitBreakerTweaks {
        /**
         * Sets the number of tend intervals to track for error rate calculation.
         *
         * @param n number of intervals
         * @return this tweaks instance for method chaining
         */
        CircuitBreakerTweaks numTendIntervalsInErrorWindow(int n);

        /**
         * Sets the maximum number of errors allowed in the error window before
         * triggering the circuit breaker.
         *
         * @param n maximum errors
         * @return this tweaks instance for method chaining
         */
        CircuitBreakerTweaks maximumErrorsInErrorWindow(int n);
    }

    /**
     * Interface for configuring cluster refresh settings.
     */
    public interface RefreshTweaks {
        /**
         * Sets the interval between cluster tend operations.
         * Tend operations refresh the cluster topology and node health status.
         *
         * @param interval tend interval
         * @return this tweaks instance for method chaining
         */
        RefreshTweaks tendInterval(Duration interval);
    }

    /**
     * Interface for configuring transaction settings.
     *
     * <p>These settings control transaction retry behavior and batch write transaction handling
     * at the system level. They apply to all transactions executed on the cluster.</p>
     *
     * <p><b>Example Usage:</b></p>
     * <pre>{@code
     * SystemSettings settings = SystemSettings.builder()
     *     .transactions(ops -> ops
     *         .implicitBatchWriteTransactions(true)
     *         .numberOfAttempts(5)
     *         .sleepBetweenAttempts(Duration.ofMillis(500))
     *     )
     *     .build();
     * }</pre>
     */
    public interface TransactionsTweaks {
        /**
         * Enables or disables implicit batch write transactions.
         *
         * <p>When enabled, batch write operations will automatically use transactions
         * to ensure atomicity across all records in the batch. When disabled, batch
         * writes are performed without transaction guarantees.</p>
         *
         * <p>This setting affects all batch write operations at the system level.
         * Individual behaviors can override this setting per-operation.</p>
         *
         * @param b {@code true} to enable implicit batch write transactions,
         *          {@code false} to disable
         * @return this tweaks instance for method chaining
         */
        TransactionsTweaks implicitBatchWriteTransactions(boolean b);

        /**
         * Sets the sleep duration between transaction retry attempts.
         *
         * <p>When a transaction fails with a retryable error (such as MRT_BLOCKED,
         * MRT_VERSION_MISMATCH, or TXN_FAILED), the client will wait this duration
         * before retrying the transaction.</p>
         *
         * @param duration sleep duration between retry attempts
         * @return this tweaks instance for method chaining
         */
        TransactionsTweaks sleepBetweenAttempts(Duration duration);

        /**
         * Sets the maximum number of transaction retry attempts.
         *
         * <p>When a transaction fails with a retryable error, the client will retry
         * up to this many times before giving up and throwing an exception.</p>
         *
         * <p>A value of 1 means no retries (only the initial attempt). Higher values
         * allow for more retry attempts, which can help handle transient failures
         * but may increase latency.</p>
         *
         * @param n maximum number of retry attempts (1 or greater)
         * @return this tweaks instance for method chaining
         */
        TransactionsTweaks numberOfAttempts(int n);
    }

    /**
     * Interface for configuring transaction settings.
     *
     * <p>These settings control transaction retry behavior and batch write transaction handling
     * at the system level. They apply to all transactions executed on the cluster.</p>
     *
     * <p><b>Example Usage:</b></p>
     * <pre>{@code
     * SystemSettings settings = SystemSettings.builder()
     *     .transactions(ops -> ops
     *         .implicitBatchWriteTransactions(true)
     *         .numberOfAttempts(5)
     *         .sleepBetweenAttempts(Duration.ofMillis(500))
     *     )
     *     .build();
     * }</pre>
     */
    public interface MetricsTweaks {
        MetricsTweaks labels(Map<String,String> labels);
        MetricsTweaks latencyWarn(Duration duration);
        MetricsTweaks connectCreateWarn(Duration duration);
        MetricsTweaks reportDir(String dir);
        MetricsTweaks reportSizeLimit(Long limit);
        MetricsTweaks interval(Integer limit);
        MetricsTweaks latencyColumns(Integer limit);
        MetricsTweaks latencyShift(Integer limit);
        MetricsTweaks batchSizeWarn(Integer n);
        MetricsTweaks shortQueryRecordsMax(Integer n);
        MetricsTweaks longQueryRecordsMin(Integer n);
        MetricsTweaks enabled(Boolean b);
    }

    // -----------------------------------------------------------------------------------
    // Internal implementations of tweaks interfaces
    // -----------------------------------------------------------------------------------

    private static class ConnectionsTweaksImpl implements ConnectionsTweaks {
        private final Builder builder;

        ConnectionsTweaksImpl(Builder builder) {
            this.builder = builder;
        }

        @Override
        public ConnectionsTweaks minimumConnectionsPerNode(int n) {
            builder.minimumConnectionsPerNode = n;
            return this;
        }

        @Override
        public ConnectionsTweaks maximumConnectionsPerNode(int n) {
            builder.maximumConnectionsPerNode = n;
            return this;
        }

        @Override
        public ConnectionsTweaks maximumSocketIdleTime(Duration duration) {
            builder.maximumSocketIdleTime = duration;
            return this;
        }
    }

    private static class CircuitBreakerTweaksImpl implements CircuitBreakerTweaks {
        private final Builder builder;

        CircuitBreakerTweaksImpl(Builder builder) {
            this.builder = builder;
        }

        @Override
        public CircuitBreakerTweaks numTendIntervalsInErrorWindow(int n) {
            builder.numTendIntervalsInErrorWindow = n;
            return this;
        }

        @Override
        public CircuitBreakerTweaks maximumErrorsInErrorWindow(int n) {
            builder.maximumErrorsInErrorWindow = n;
            return this;
        }
    }

    private static class RefreshTweaksImpl implements RefreshTweaks {
        private final Builder builder;

        RefreshTweaksImpl(Builder builder) {
            this.builder = builder;
        }

        @Override
        public RefreshTweaks tendInterval(Duration interval) {
            builder.tendInterval = interval;
            return this;
        }
    }

    private static class TransactionsTweaksImpl implements TransactionsTweaks {
        private final Builder builder;

        TransactionsTweaksImpl(Builder builder) {
            this.builder = builder;
        }

        @Override
        public TransactionsTweaks implicitBatchWriteTransactions(boolean b) {
            builder.implicitBatchWriteTransactions = b;
            return this;
        }

        @Override
        public TransactionsTweaks sleepBetweenAttempts(Duration duration) {
            builder.sleepBetweenAttempts = duration;
            return this;
        }

        @Override
        public TransactionsTweaks numberOfAttempts(int n) {
            builder.numberOfAttempts = n;
            return this;
        }
    }

    private static class MetricsTweaksImpl implements MetricsTweaks {
        private final Builder builder;

        MetricsTweaksImpl(Builder builder) {
            this.builder = builder;
        }

        @Override
        public MetricsTweaks labels(Map<String,String> labels) {
            builder.labels = labels;
            return this;
        }

        @Override
        public MetricsTweaks latencyWarn(Duration duration) {
            builder.latencyWarn = duration;
            return this;
        }

        @Override
        public MetricsTweaks connectCreateWarn(Duration duration) {
            builder.connectCreateWarn = duration;
            return this;
        }

        @Override
        public MetricsTweaks reportDir(String dir) {
            builder.reportDir = dir;
            return this;
        }

        @Override
        public MetricsTweaks reportSizeLimit(Long limit) {
            builder.reportSizeLimit = limit;
            return this;
        }

        @Override
        public MetricsTweaks interval(Integer interval) {
            builder.interval = interval;
            return this;
        }

        @Override
        public MetricsTweaks latencyColumns(Integer n) {
            builder.latencyColumns = n;
            return this;
        }

        @Override
        public MetricsTweaks latencyShift(Integer n) {
            builder.latencyShift = n;
            return this;
        }

        @Override
        public MetricsTweaks batchSizeWarn(Integer n) {
            builder.batchSizeWarn = n;
            return this;
        }

        @Override
        public MetricsTweaks shortQueryRecordsMax(Integer n) {
            builder.shortQueryRecordsMax = n;
            return this;
        }

        @Override
        public MetricsTweaks longQueryRecordsMin(Integer n) {
            builder.longQueryRecordsMin = n;
            return this;
        }

        @Override
        public MetricsTweaks enabled(Boolean b) {
            builder.enabled = b;
            return this;
        }
    }
}

