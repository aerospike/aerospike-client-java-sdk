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
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import com.aerospike.client.sdk.ExtendedMetricsSettings.ExtendedMetricsTweaks;
import com.aerospike.client.sdk.metrics.MetricsListener;
import com.aerospike.client.sdk.metrics.MetricsTier;

/**
 * Metrics settings that apply to an entire Cluster instance.
 */
public class MetricsSettings {
    private final MetricsListener listener;
    private final Map<String,String> labels;
    private final Duration latencyWarn;
    private final Duration connectCreateWarn;
    private final String reportDir;
    private final Long reportSizeLimit;
    private final Double exportSampleRate;
    private final Integer interval;
    private final Integer batchSizeWarn;
    private final Integer shortQueryRecordsMax;
    private final Integer longQueryRecordsMin;
    private final Boolean enabled;
    private final ExtendedMetricsSettings extended;
    private final MetricsTier tier;

    MetricsSettings(Builder builder) {
        this.listener = builder.listener;
        this.labels = builder.labels;
        this.latencyWarn = builder.latencyWarn;
        this.connectCreateWarn = builder.connectCreateWarn;
        this.reportDir = builder.reportDir;
        this.reportSizeLimit = builder.reportSizeLimit;
        this.exportSampleRate = builder.exportSampleRate;
        this.interval = builder.interval;
        this.batchSizeWarn = builder.batchSizeWarn;
        this.shortQueryRecordsMax = builder.shortQueryRecordsMax;
        this.longQueryRecordsMin = builder.longQueryRecordsMin;
        this.enabled = builder.enabled;
        this.extended = new ExtendedMetricsSettings(builder.extended);

        if (this.enabled != null && this.enabled) {
            this.tier = (extended.getEnabled() != null && extended.getEnabled())?
                MetricsTier.EXTENDED : MetricsTier.STANDARD;
        }
        else {
            this.tier = MetricsTier.NONE;
        }
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
        merged.latencyWarn = this.latencyWarn != null
            ? this.latencyWarn : base.latencyWarn;
        merged.connectCreateWarn = this.connectCreateWarn != null
            ? this.connectCreateWarn : base.connectCreateWarn;
        merged.reportDir = this.reportDir != null
            ? this.reportDir : base.reportDir;
        merged.reportSizeLimit = this.reportSizeLimit != null
            ? this.reportSizeLimit : base.reportSizeLimit;
        merged.exportSampleRate = this.exportSampleRate != null
            ? this.exportSampleRate : base.exportSampleRate;
        merged.interval = this.interval != null
            ? this.interval : base.interval;
        merged.batchSizeWarn = this.batchSizeWarn != null
            ? this.batchSizeWarn : base.batchSizeWarn;
        merged.shortQueryRecordsMax = this.shortQueryRecordsMax != null
            ? this.shortQueryRecordsMax : base.shortQueryRecordsMax;
        merged.longQueryRecordsMin = this.longQueryRecordsMin != null
            ? this.longQueryRecordsMin : base.longQueryRecordsMin;
        merged.enabled = this.enabled != null
            ? this.enabled : base.enabled;

        merged.extended = this.extended.mergeWith(base.extended);

        return merged;
    }

    // Getters
    public MetricsListener getListener() { return listener; }
    public Map<String,String> getLabels() { return labels; }
    public Duration getLatencyWarn() { return latencyWarn; }
    public Duration getConnectCreateWarn() { return connectCreateWarn; }
    public String getReportDir() { return reportDir; }
    public Long getReportSizeLimit() { return reportSizeLimit; }
    public Double getExportSampleRate() { return exportSampleRate; }
    public Integer getInterval() { return interval; }
    public Integer getBatchSizeWarn() { return batchSizeWarn; }
    public Integer getShortQueryRecordsMax() { return shortQueryRecordsMax; }
    public Integer getLongQueryRecordsMin() { return longQueryRecordsMin; }
    public Boolean getEnabled() { return enabled; }
    public ExtendedMetricsSettings getExtended() { return extended; }
    public MetricsTier getTier() { return tier; }

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
            Objects.equals(latencyWarn, that.latencyWarn) &&
            Objects.equals(connectCreateWarn, that.connectCreateWarn) &&
            Objects.equals(reportDir, that.reportDir) &&
            Objects.equals(reportSizeLimit, that.reportSizeLimit) &&
            Objects.equals(exportSampleRate, that.exportSampleRate) &&
            Objects.equals(interval, that.interval) &&
            Objects.equals(batchSizeWarn, that.batchSizeWarn) &&
            Objects.equals(shortQueryRecordsMax, that.shortQueryRecordsMax) &&
            Objects.equals(longQueryRecordsMin, that.longQueryRecordsMin) &&
            Objects.equals(enabled, that.enabled) &&
            Objects.equals(extended, that.extended);
    }

    @Override
    public int hashCode() {
        return Objects.hash(listener, labels, latencyWarn, connectCreateWarn, reportDir,
            reportSizeLimit, exportSampleRate, interval, batchSizeWarn,
            shortQueryRecordsMax, longQueryRecordsMin, enabled, extended);
    }

    @Override
    public String toString() {
        return "MetricsSettings{" +
            "listener=" + ((listener != null)? listener.getClass().getName() : "null") +
            ", labels=" + labels +
            ", latencyWarn=" + latencyWarn.toMillis() + "ms" +
            ", connectCreateWarn=" + connectCreateWarn.toMillis() + "ms" +
            ", reportDir=" + reportDir +
            ", reportSizeLimit=" + reportSizeLimit +
            ", exportSampleRate=" + exportSampleRate +
            ", interval=" + interval +
            ", batchSizeWarn=" + batchSizeWarn +
            ", shortQueryRecordsMax=" + shortQueryRecordsMax +
            ", longQueryRecordsMin=" + longQueryRecordsMin +
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
        private Duration latencyWarn;
        private Duration connectCreateWarn;
        private String reportDir;
        private Long reportSizeLimit;
        private Double exportSampleRate;
        private Integer interval;
        private Integer batchSizeWarn;
        private Integer shortQueryRecordsMax;
        private Integer longQueryRecordsMin;
        private Boolean enabled;
        private ExtendedMetricsSettings.Builder extended = ExtendedMetricsSettings.builder();

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
        MetricsTweaks latencyWarn(Duration duration);
        MetricsTweaks connectCreateWarn(Duration duration);
        MetricsTweaks reportDir(String dir);
        MetricsTweaks reportSizeLimit(Long limit);
        MetricsTweaks exportSampleRate(Double rate);
        MetricsTweaks interval(Integer limit);
        MetricsTweaks batchSizeWarn(Integer n);
        MetricsTweaks shortQueryRecordsMax(Integer n);
        MetricsTweaks longQueryRecordsMin(Integer n);
        MetricsTweaks enabled(Boolean b);
        MetricsTweaks extended(Consumer<ExtendedMetricsTweaks> configurator);
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
        public MetricsTweaks exportSampleRate(Double rate) {
            builder.exportSampleRate = rate;
            return this;
        }

        @Override
        public MetricsTweaks interval(Integer interval) {
            builder.interval = interval;
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

        @Override
        public MetricsTweaks extended(Consumer<ExtendedMetricsTweaks> configurator) {
            configurator.accept(new ExtendedMetricsSettings.ExtendedMetricsTweaksImpl(builder.extended));
            return this;
        }
    }
}
