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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Metrics extended operational settings.
 */
public class MetricsOperational {
    private final TimeUnit latencyUnit;
    private final Integer latencyColumns;
    private final Integer latencyShift;
    private final Integer samplerRange;
    private final Integer samplerThreshold;
    private final Boolean enabled;

    MetricsOperational(Builder builder) {
        this.latencyUnit = builder.latencyUnit;
        this.latencyColumns = builder.latencyColumns;
        this.latencyShift = builder.latencyShift;
        this.samplerRange = builder.samplerRange;
        this.samplerThreshold = builder.samplerThreshold;
        this.enabled = builder.enabled;
    }

    /**
     * Create a new builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Merges this settings instance with a base, using base values for any null fields.
     * This enables the 4-level priority hierarchy.
     */
    Builder mergeWith(MetricsOperational base) {
        Builder merged = builder();

        merged.latencyUnit = this.latencyUnit != null
            ? this.latencyUnit : base.latencyUnit;
        merged.latencyColumns = this.latencyColumns != null
            ? this.latencyColumns : base.latencyColumns;
        merged.latencyShift = this.latencyShift != null
            ? this.latencyShift : base.latencyShift;
        merged.samplerRange = this.samplerRange != null
            ? this.samplerRange : base.samplerRange;
        merged.samplerThreshold = this.samplerThreshold != null
            ? this.samplerThreshold : base.samplerThreshold;
        merged.enabled = this.enabled != null
            ? this.enabled : base.enabled;

        return merged;
    }

    // Getters
    public TimeUnit getLatencyUnit() { return latencyUnit; }
    public Integer getLatencyColumns() { return latencyColumns; }
    public Integer getLatencyShift() { return latencyShift; }
    public Integer getSamplerRange() { return samplerRange; }
    public Integer getSamplerThreshold() { return samplerThreshold; }
    public Boolean getEnabled() { return enabled; }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MetricsOperational that = (MetricsOperational) o;
        return
            Objects.equals(latencyUnit, that.latencyUnit) &&
            Objects.equals(latencyColumns, that.latencyColumns) &&
            Objects.equals(latencyShift, that.latencyShift) &&
            Objects.equals(samplerRange, that.samplerRange) &&
            Objects.equals(samplerThreshold, that.samplerThreshold) &&
            Objects.equals(enabled, that.enabled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(latencyUnit, latencyColumns, latencyShift, samplerRange, samplerThreshold, enabled);
    }

    @Override
    public String toString() {
        return "MetricsOperational{" +
            "latencyUnit=" + latencyUnit+
            ", latencyColumns=" + latencyColumns +
            ", latencyShift=" + latencyShift +
            ", samplerRange=" + samplerRange +
            ", samplerThreshold=" + samplerThreshold +
            ", enabled=" + enabled +
            '}';
    }

    /**
     * Builder for metrics settings with lambda-based configuration.
     */
    public static class Builder {
        private TimeUnit latencyUnit;
        private Integer latencyColumns;
        private Integer latencyShift;
        private Integer samplerRange;
        private Integer samplerThreshold;
        private Boolean enabled;

        public MetricsOperational build() {
            return new MetricsOperational(this);
        }
    }

    // -----------------------------------------------------------------------------------
    // Tweaks interfaces for lambda-based configuration
    // -----------------------------------------------------------------------------------

    /**
     * Interface for configuring metrics signal related settings.
     */
    public interface MetricsOperationalTweaks {
        MetricsOperationalTweaks latencyUnit(TimeUnit unit);
        MetricsOperationalTweaks latencyColumns(Integer limit);
        MetricsOperationalTweaks latencyShift(Integer limit);
        MetricsOperationalTweaks samplerRange(Integer range);
        MetricsOperationalTweaks samplerThreshold(Integer threshold);
        MetricsOperationalTweaks enabled(Boolean b);
    }

    // -----------------------------------------------------------------------------------
    // Internal implementations of tweaks interfaces
    // -----------------------------------------------------------------------------------

    static class MetricsOperationalTweaksImpl implements MetricsOperationalTweaks {
        private final Builder builder;

        MetricsOperationalTweaksImpl(Builder builder) {
            this.builder = builder;
        }

        @Override
        public MetricsOperationalTweaks latencyUnit(TimeUnit unit) {
            builder.latencyUnit = unit;
            return this;
        }

        @Override
        public MetricsOperationalTweaks latencyColumns(Integer n) {
            builder.latencyColumns = n;
            return this;
        }

        @Override
        public MetricsOperationalTweaks latencyShift(Integer n) {
            builder.latencyShift = n;
            return this;
        }

        @Override
        public MetricsOperationalTweaks samplerRange(Integer n) {
            builder.samplerRange = n;
            return this;
        }

        @Override
        public MetricsOperationalTweaks samplerThreshold(Integer n) {
            builder.samplerThreshold = n;
            return this;
        }

        @Override
        public MetricsOperationalTweaks enabled(Boolean b) {
            builder.enabled = b;
            return this;
        }
    }
}
