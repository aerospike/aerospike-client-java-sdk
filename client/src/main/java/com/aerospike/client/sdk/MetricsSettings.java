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

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import com.aerospike.client.sdk.MetricsExtended.MetricsExtendedTweaks;
import com.aerospike.client.sdk.metrics.MetricsListener;

/**
 * Metrics settings that apply to an entire Cluster instance.
 */
public class MetricsSettings {
    private final MetricsListener listener;
    private final Map<String,String> labels;
    private final String reportDir;
    private final Long reportSizeLimit;
    private final Double exportSampleRate;
    private final Integer exportInterval;
    private final Boolean enabled;
    private final MetricsExtended extended;

    MetricsSettings(Builder builder) {
        this.listener = builder.listener;
        this.labels = builder.labels;
        this.reportDir = builder.reportDir;
        this.reportSizeLimit = builder.reportSizeLimit;
        this.exportSampleRate = builder.exportSampleRate;
        this.exportInterval = builder.exportInterval;
        this.enabled = builder.enabled;
        this.extended = new MetricsExtended(builder.extended);
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
    Builder mergeWith(MetricsSettings base) {
        Builder merged = builder();

        merged.listener = this.listener != null
            ? this.listener : base.listener;
        merged.labels = this.labels != null
            ? this.labels : base.labels;
        merged.reportDir = this.reportDir != null
            ? this.reportDir : base.reportDir;
        merged.reportSizeLimit = this.reportSizeLimit != null
            ? this.reportSizeLimit : base.reportSizeLimit;
        merged.exportSampleRate = this.exportSampleRate != null
            ? this.exportSampleRate : base.exportSampleRate;
        merged.exportInterval = this.exportInterval != null
            ? this.exportInterval : base.exportInterval;
        merged.enabled = this.enabled != null
            ? this.enabled : base.enabled;

        merged.extended = this.extended.mergeWith(base.extended);

        return merged;
    }

    // Getters
    public MetricsListener getListener() { return listener; }
    public Map<String,String> getLabels() { return labels; }
    public String getReportDir() { return reportDir; }
    public Long getReportSizeLimit() { return reportSizeLimit; }
    public Double getExportSampleRate() { return exportSampleRate; }
    public Integer getExportInterval() { return exportInterval; }
    public Boolean getEnabled() { return enabled; }
    public MetricsExtended getExtended() { return extended; }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MetricsSettings that = (MetricsSettings) o;
        return
            Objects.equals(listener, that.listener) &&
            Objects.equals(labels, that.labels) &&
            Objects.equals(reportDir, that.reportDir) &&
            Objects.equals(reportSizeLimit, that.reportSizeLimit) &&
            Objects.equals(exportSampleRate, that.exportSampleRate) &&
            Objects.equals(exportInterval, that.exportInterval) &&
            Objects.equals(enabled, that.enabled) &&
            Objects.equals(extended, that.extended);
    }

    @Override
    public int hashCode() {
        return Objects.hash(listener, labels, reportDir, reportSizeLimit, exportSampleRate,
            exportInterval, enabled, extended);
    }

    @Override
    public String toString() {
        return "MetricsSettings{" +
            "listener=" + ((listener != null)? listener.getClass().getName() : "null") +
            ", labels=" + labels +
            ", reportDir=" + reportDir +
            ", reportSizeLimit=" + reportSizeLimit +
            ", exportSampleRate=" + exportSampleRate +
            ", exportInterval=" + exportInterval +
            ", enabled=" + enabled +
            ", extended=" + extended +
            '}';
    }

    /**
     * Builder for metrics settings with lambda-based configuration.
     */
    public static class Builder {
        private MetricsListener listener;
        private Map<String,String> labels;
        private String reportDir;
        private Long reportSizeLimit;
        private Double exportSampleRate;
        private Integer exportInterval;
        private Boolean enabled;
        private MetricsExtended.Builder extended = MetricsExtended.builder();

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
    public interface MetricsTweaks {
        MetricsTweaks listener(MetricsListener listener);
        MetricsTweaks labels(Map<String,String> labels);
        MetricsTweaks reportDir(String dir);
        MetricsTweaks reportSizeLimit(Long limit);
        MetricsTweaks exportSampleRate(Double rate);
        MetricsTweaks exportInterval(Integer limit);
        MetricsTweaks enabled(Boolean b);
        MetricsTweaks extended(Consumer<MetricsExtendedTweaks> configurator);
    }

    // -----------------------------------------------------------------------------------
    // Internal implementations of tweaks interfaces
    // -----------------------------------------------------------------------------------

    static class MetricsTweaksImpl implements MetricsTweaks {
        private final Builder builder;

        MetricsTweaksImpl(Builder builder) {
            this.builder = builder;
        }

        @Override
        public MetricsTweaks listener(MetricsListener listener) {
            builder.listener = listener;
            return this;
        }

        @Override
        public MetricsTweaks labels(Map<String,String> labels) {
            builder.labels = labels;
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
        public MetricsTweaks exportSampleRate(Double rate) {
            builder.exportSampleRate = rate;
            return this;
        }

        @Override
        public MetricsTweaks exportInterval(Integer interval) {
            builder.exportInterval = interval;
            return this;
        }

        @Override
        public MetricsTweaks enabled(Boolean b) {
            builder.enabled = b;
            return this;
        }

        @Override
        public MetricsTweaks extended(Consumer<MetricsExtendedTweaks> configurator) {
            configurator.accept(new MetricsExtended.MetricsExtendedTweaksImpl(builder.extended));
            return this;
        }
    }
}
