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

/**
 * Metrics extended operational settings.
 */
public class MetricsSampler {
    private final Integer range;
    private final Integer threshold;

    MetricsSampler(Builder builder) {
        this.range = builder.range;
        this.threshold = builder.threshold;
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
    Builder mergeWith(MetricsSampler base) {
        Builder merged = builder();

        merged.range = this.range != null
            ? this.range : base.range;
        merged.threshold = this.threshold != null
            ? this.threshold : base.threshold;

        return merged;
    }

    // Getters
    public Integer getRange() { return range; }
    public Integer getThreshold() { return threshold; }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MetricsSampler that = (MetricsSampler) o;
        return
            Objects.equals(range, that.range) &&
            Objects.equals(threshold, that.threshold);
    }

    @Override
    public int hashCode() {
        return Objects.hash(range, threshold);
    }

    @Override
    public String toString() {
        return "MetricsSampler{" +
            "range=" + range +
            ", threshold=" + threshold +
            '}';
    }

    /**
     * Builder for metrics settings with lambda-based configuration.
     */
    public static class Builder {
        private Integer range;
        private Integer threshold;

        public MetricsSampler build() {
            return new MetricsSampler(this);
        }
    }

    // -----------------------------------------------------------------------------------
    // Tweaks interfaces for lambda-based configuration
    // -----------------------------------------------------------------------------------

    /**
     * Interface for configuring metrics signal related settings.
     */
    public interface MetricsSamplerTweaks {
        MetricsSamplerTweaks range(Integer range);
        MetricsSamplerTweaks threshold(Integer threshold);
    }

    // -----------------------------------------------------------------------------------
    // Internal implementations of tweaks interfaces
    // -----------------------------------------------------------------------------------

    static class MetricsSamplerTweaksImpl implements MetricsSamplerTweaks {
        private final Builder builder;

        MetricsSamplerTweaksImpl(Builder builder) {
            this.builder = builder;
        }

        @Override
        public MetricsSamplerTweaks range(Integer range) {
            builder.range = range;
            return this;
        }

        @Override
        public MetricsSamplerTweaks threshold(Integer threshold) {
            builder.threshold = threshold;
            return this;
        }
    }
}
