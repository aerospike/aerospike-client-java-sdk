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
 * Metrics settings that apply to an entire Cluster instance.
 *
 * <p><b>Example Usage:</b></p>
 * <pre>{@code
 * // Lambda-based configuration (inline):
 * new ClusterDefinition("localhost", 3000)
 *     .withMetricsSettings(builder -> builder
 *         .signals(ops -> ops
 *             .enabled(true)
 *         )
 *     )
 *     .connect();
 *
 * // Or explicit builder for complex configurations:
 * MetricsSettings settings = MetricsSettings.builder()
 *     .signals(ops -> ops
 *          .enabled(true)
 *     )
 *     .build();
 *
 * new ClusterDefinition("localhost", 3000)
 *     .withMetricsSettings(settings)
 *     .connect();
 * }</pre>
 *
 * @see ClusterDefinition#withMetricsSettings(MetricsSettings)
 */
public class MetricsSettings {
    private final Duration latencyWarn;
    private final Duration connectCreateWarn;
    private final Integer batchSizeWarn;
    private final Integer shortQueryRecordsMax;
    private final Integer longQueryRecordsMin;
    private final Boolean enabled;

    /**
     * Default metrics settings.
     * These are the lowest priority and serve as the base for all other settings.
     */
    public static final MetricsSettings DEFAULT = builder()
        .signals(ops -> ops
            .latencyWarn(Duration.ofMillis(50))
            .connectCreateWarn(Duration.ofMillis(500))
            .batchSizeWarn(500)
            .shortQueryRecordsMax(100)
            .longQueryRecordsMin(10)
            .enabled(false)
        )
        .build();

    private MetricsSettings(Builder builder) {
        this.latencyWarn = builder.latencyWarn;
        this.connectCreateWarn = builder.connectCreateWarn;
        this.batchSizeWarn = builder.batchSizeWarn;
        this.shortQueryRecordsMax = builder.shortQueryRecordsMax;
        this.longQueryRecordsMin = builder.longQueryRecordsMin;
        this.enabled = builder.enabled;
   }

    /**
     * Creates a new builder for MetricsSettings.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Merges this settings instance with a base, using base values for any null fields.
     * This enables the 4-level priority hierarchy.
     *
     * @param base the base settings to use for null fields
     * @return a new MetricsSettings with merged values
     */
    public MetricsSettings mergeWith(MetricsSettings base) {
        if (base == null) {
            return this;
        }

        Builder merged = builder();

        merged.latencyWarn = this.latencyWarn != null
            ? this.latencyWarn : base.latencyWarn;
        merged.connectCreateWarn = this.connectCreateWarn != null
            ? this.connectCreateWarn : base.connectCreateWarn;
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
    public Duration getLatencyWarn() { return latencyWarn; }
    public Duration getConnectCreateWarn() { return connectCreateWarn; }
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
        MetricsSettings that = (MetricsSettings) o;
        return Objects.equals(latencyWarn, that.latencyWarn) &&
               Objects.equals(connectCreateWarn, that.connectCreateWarn) &&
               Objects.equals(batchSizeWarn, that.batchSizeWarn) &&
               Objects.equals(shortQueryRecordsMax, that.shortQueryRecordsMax) &&
               Objects.equals(longQueryRecordsMin, that.longQueryRecordsMin) &&
               Objects.equals(enabled, that.enabled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(latencyWarn, connectCreateWarn, batchSizeWarn,
                           shortQueryRecordsMax, longQueryRecordsMin, enabled);
    }

    @Override
    public String toString() {
        return "MetricsSettings{" +
               "latencyWarn=" + latencyWarn.toMillis() + "ms" +
               ", connectCreateWarn=" + connectCreateWarn.toMillis() + "ms" +
               ", batchSizeWarn=" + batchSizeWarn +
               ", shortQueryRecordsMax=" + shortQueryRecordsMax +
               ", longQueryRecordsMin=" + longQueryRecordsMin +
               ", enabled=" + enabled +
               '}';
    }

    /**
     * Builder for metrics settings with lambda-based configuration.
     */
    public static class Builder {
        private Duration latencyWarn;
        private Duration connectCreateWarn;
        private Integer batchSizeWarn;
        private Integer shortQueryRecordsMax;
        private Integer longQueryRecordsMin;
        private Boolean enabled;

        /**
         * Configure metrics signals using a lambda.
         */
        public Builder signals(Consumer<SignalsTweaks> configurator) {
            configurator.accept(new SignalsTweaksImpl(this));
            return this;
        }

        /**
         * Builds the MetricsSettings instance.
         */
        public MetricsSettings build() {
            return new MetricsSettings(this);
        }
    }

    // -----------------------------------------------------------------------------------
    // Tweaks interfaces for lambda-based configuration
    // -----------------------------------------------------------------------------------

    /**
     * Interface for configuring metrics signal related settings.
     */
    public interface SignalsTweaks {
        SignalsTweaks latencyWarn(Duration duration);
        SignalsTweaks connectCreateWarn(Duration duration);
        SignalsTweaks batchSizeWarn(Integer n);
        SignalsTweaks shortQueryRecordsMax(Integer n);
        SignalsTweaks longQueryRecordsMin(Integer n);
        SignalsTweaks enabled(Boolean b);
    }

    // -----------------------------------------------------------------------------------
    // Internal implementations of tweaks interfaces
    // -----------------------------------------------------------------------------------

    private static class SignalsTweaksImpl implements SignalsTweaks {
        private final Builder builder;

        SignalsTweaksImpl(Builder builder) {
            this.builder = builder;
        }

        @Override
        public SignalsTweaks latencyWarn(Duration duration) {
            builder.latencyWarn = duration;
            return this;
        }

        @Override
        public SignalsTweaks connectCreateWarn(Duration duration) {
            builder.connectCreateWarn = duration;
            return this;
        }

        @Override
        public SignalsTweaks batchSizeWarn(Integer n) {
            builder.batchSizeWarn = n;
            return this;
        }

        @Override
        public SignalsTweaks shortQueryRecordsMax(Integer n) {
            builder.shortQueryRecordsMax = n;
            return this;
        }

        @Override
        public SignalsTweaks longQueryRecordsMin(Integer n) {
            builder.longQueryRecordsMin = n;
            return this;
        }

        @Override
        public SignalsTweaks enabled(Boolean b) {
            builder.enabled = b;
            return this;
        }
    }
}
