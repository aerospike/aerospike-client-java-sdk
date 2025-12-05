package com.aerospike.client.fluent;

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
        .build();

    private SystemSettings(Builder builder) {
        this.minimumConnectionsPerNode = builder.minimumConnectionsPerNode;
        this.maximumConnectionsPerNode = builder.maximumConnectionsPerNode;
        this.maximumSocketIdleTime = builder.maximumSocketIdleTime;
        this.numTendIntervalsInErrorWindow = builder.numTendIntervalsInErrorWindow;
        this.maximumErrorsInErrorWindow = builder.maximumErrorsInErrorWindow;
        this.tendInterval = builder.tendInterval;
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

        return merged.build();
    }
    
    // Getters
    public Integer getMinimumConnectionsPerNode() { return minimumConnectionsPerNode; }
    public Integer getMaximumConnectionsPerNode() { return maximumConnectionsPerNode; }
    public Duration getMaximumSocketIdleTime() { return maximumSocketIdleTime; }
    public Integer getNumTendIntervalsInErrorWindow() { return numTendIntervalsInErrorWindow; }
    public Integer getMaximumErrorsInErrorWindow() { return maximumErrorsInErrorWindow; }
    public Duration getTendInterval() { return tendInterval; }

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
               Objects.equals(tendInterval, that.tendInterval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(minimumConnectionsPerNode, maximumConnectionsPerNode, maximumSocketIdleTime,
                           numTendIntervalsInErrorWindow, maximumErrorsInErrorWindow, tendInterval);
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
}

