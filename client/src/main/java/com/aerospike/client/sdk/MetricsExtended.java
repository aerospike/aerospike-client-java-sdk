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
import java.util.function.Consumer;

import com.aerospike.client.sdk.MetricsOperational.MetricsOperationalTweaks;
import com.aerospike.client.sdk.MetricsUsage.MetricsUsageTweaks;

/**
 * Metrics extended settings.
 */
public class MetricsExtended {
    private final MetricsOperational operational;
    private final MetricsUsage usage;

    MetricsExtended(Builder builder) {
        this.operational = new MetricsOperational(builder.operational);
        this.usage = new MetricsUsage(builder.usage);
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
    Builder mergeWith(MetricsExtended base) {
        Builder merged = builder();

        merged.operational = this.operational.mergeWith(base.operational);
        merged.usage = this.usage.mergeWith(base.usage);

        return merged;
    }

    // Getters
    public MetricsOperational getOperational() {
        return operational;
    }

    public MetricsUsage getUsage() {
        return usage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MetricsExtended that = (MetricsExtended) o;
        return
            Objects.equals(operational, that.operational) &&
            Objects.equals(usage, that.usage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operational, usage);
    }

    @Override
    public String toString() {
        return "MetricsExtended{" +
            "operational=" + operational+
            ", usage=" + usage +
            '}';
    }

    /**
     * Builder for metrics settings with lambda-based configuration.
     */
    public static class Builder {
        private MetricsOperational.Builder operational = MetricsOperational.builder();
        private MetricsUsage.Builder usage = MetricsUsage.builder();

        public MetricsExtended build() {
            return new MetricsExtended(this);
        }
    }

    // -----------------------------------------------------------------------------------
    // Tweaks interfaces for lambda-based configuration
    // -----------------------------------------------------------------------------------

    /**
     * Interface for configuring metrics signal related settings.
     */
    public interface MetricsExtendedTweaks {
        MetricsExtendedTweaks operational(Consumer<MetricsOperationalTweaks> configurator);
        MetricsExtendedTweaks usage(Consumer<MetricsUsageTweaks> configurator);
    }

    // -----------------------------------------------------------------------------------
    // Internal implementations of tweaks interfaces
    // -----------------------------------------------------------------------------------

    static class MetricsExtendedTweaksImpl implements MetricsExtendedTweaks {
        private final Builder builder;

        MetricsExtendedTweaksImpl(Builder builder) {
            this.builder = builder;
        }

        @Override
        public MetricsExtendedTweaks operational(Consumer<MetricsOperationalTweaks> configurator) {
            configurator.accept(new MetricsOperational.MetricsOperationalTweaksImpl(builder.operational));
            return this;
        }

        @Override
        public MetricsExtendedTweaks usage(Consumer<MetricsUsageTweaks> configurator) {
            configurator.accept(new MetricsUsage.MetricsUsageTweaksImpl(builder.usage));
            return this;
        }
    }
}
