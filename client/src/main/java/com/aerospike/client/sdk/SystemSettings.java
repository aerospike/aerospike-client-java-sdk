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
		       Objects.equals(numberOfAttempts, that.numberOfAttempts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(minimumConnectionsPerNode, maximumConnectionsPerNode, maximumSocketIdleTime,
                           numTendIntervalsInErrorWindow, maximumErrorsInErrorWindow, tendInterval,
                           implicitBatchWriteTransactions, sleepBetweenAttempts, numberOfAttempts);
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
}

