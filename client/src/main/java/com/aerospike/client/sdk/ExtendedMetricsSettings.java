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
 * Extended metrics settings.
 */
public class ExtendedMetricsSettings {
    private final TimeUnit latencyUnit;
    private final Integer latencyColumns;
    private final Integer latencyShift;
    private final Boolean enabled;

    ExtendedMetricsSettings(Builder builder) {
        this.latencyUnit = builder.latencyUnit;
        this.latencyColumns = builder.latencyColumns;
        this.latencyShift = builder.latencyShift;
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
     */
    Builder mergeWith(ExtendedMetricsSettings base) {
        Builder merged = builder();

        merged.latencyUnit = this.latencyUnit != null
            ? this.latencyUnit : base.latencyUnit;
        merged.latencyColumns = this.latencyColumns != null
            ? this.latencyColumns : base.latencyColumns;
        merged.latencyShift = this.latencyShift != null
            ? this.latencyShift : base.latencyShift;
        merged.enabled = this.enabled != null
            ? this.enabled : base.enabled;

        return merged;
    }

    // Getters
    public TimeUnit getLatencyUnit() { return latencyUnit; }
    public Integer getLatencyColumns() { return latencyColumns; }
    public Integer getLatencyShift() { return latencyShift; }
    public Boolean getEnabled() { return enabled; }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExtendedMetricsSettings that = (ExtendedMetricsSettings) o;
        return
            Objects.equals(latencyUnit, that.latencyUnit) &&
            Objects.equals(latencyColumns, that.latencyColumns) &&
            Objects.equals(latencyShift, that.latencyShift) &&
            Objects.equals(enabled, that.enabled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(latencyUnit, latencyColumns, latencyShift, enabled);
    }

    @Override
    public String toString() {
        return "ExtendedMetricsSettings{" +
            "latencyUnit=" + latencyUnit+
            ", latencyColumns=" + latencyColumns +
            ", latencyShift=" + latencyShift +
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
        private Boolean enabled;

        public ExtendedMetricsSettings build() {
            return new ExtendedMetricsSettings(this);
        }
    }

    // -----------------------------------------------------------------------------------
    // Tweaks interfaces for lambda-based configuration
    // -----------------------------------------------------------------------------------

    /**
     * Interface for configuring metrics signal related settings.
     */
    public interface ExtendedMetricsTweaks {
        ExtendedMetricsTweaks latencyUnit(TimeUnit unit);
        ExtendedMetricsTweaks latencyColumns(Integer limit);
        ExtendedMetricsTweaks latencyShift(Integer limit);
        ExtendedMetricsTweaks enabled(Boolean b);
    }

    // -----------------------------------------------------------------------------------
    // Internal implementations of tweaks interfaces
    // -----------------------------------------------------------------------------------

    static class ExtendedMetricsTweaksImpl implements ExtendedMetricsTweaks {
        private final Builder builder;

        ExtendedMetricsTweaksImpl(Builder builder) {
            this.builder = builder;
        }

        @Override
        public ExtendedMetricsTweaks latencyUnit(TimeUnit unit) {
            builder.latencyUnit = unit;
            return this;
        }

        @Override
        public ExtendedMetricsTweaks latencyColumns(Integer n) {
            builder.latencyColumns = n;
            return this;
        }

        @Override
        public ExtendedMetricsTweaks latencyShift(Integer n) {
            builder.latencyShift = n;
            return this;
        }

        @Override
        public ExtendedMetricsTweaks enabled(Boolean b) {
            builder.enabled = b;
            return this;
        }
    }
}
