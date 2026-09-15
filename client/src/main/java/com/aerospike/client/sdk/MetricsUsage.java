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
 * Metrics extended usage settings.
 */
public class MetricsUsage {
    private final Boolean enabled;

    MetricsUsage(Builder builder) {
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
    Builder mergeWith(MetricsUsage base) {
        Builder merged = builder();

        merged.enabled = this.enabled != null
            ? this.enabled : base.enabled;

        return merged;
    }

    // Getters
    public Boolean getEnabled() { return enabled; }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MetricsUsage that = (MetricsUsage) o;
        return
            Objects.equals(enabled, that.enabled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled);
    }

    @Override
    public String toString() {
        return "MetricsUsage{" +
            "enabled=" + enabled +
            '}';
    }

    /**
     * Builder for metrics settings with lambda-based configuration.
     */
    public static class Builder {
        private Boolean enabled;

        public MetricsUsage build() {
            return new MetricsUsage(this);
        }
    }

    // -----------------------------------------------------------------------------------
    // Tweaks interfaces for lambda-based configuration
    // -----------------------------------------------------------------------------------

    /**
     * Interface for configuring metrics related settings.
     */
    public interface MetricsUsageTweaks {
        MetricsUsageTweaks enabled(Boolean b);
    }

    // -----------------------------------------------------------------------------------
    // Internal implementations of tweaks interfaces
    // -----------------------------------------------------------------------------------

    static class MetricsUsageTweaksImpl implements MetricsUsageTweaks {
        private final Builder builder;

        MetricsUsageTweaksImpl(Builder builder) {
            this.builder = builder;
        }

        @Override
        public MetricsUsageTweaks enabled(Boolean b) {
            builder.enabled = b;
            return this;
        }
    }
}
