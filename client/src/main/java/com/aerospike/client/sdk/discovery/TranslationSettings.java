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
package com.aerospike.client.sdk.discovery;

import java.util.Map;
import java.util.Objects;

/**
 * Network translation settings that apply to an entire Cluster instance.
 */
public class TranslationSettings {
    private final String provider;
    private final Map<String,Object> options;
    private final TranslationMissMode onMiss;
    private final Map<String,String> staticMap;
    private final Map<String,String> portMap;
    private final Boolean useServicesAlternate;

    public TranslationSettings(Builder builder) {
        this.provider = builder.provider;
        this.options = builder.options;
        this.onMiss = builder.onMiss;
        this.staticMap = builder.staticMap;
        this.portMap = builder.portMap;
        this.useServicesAlternate = builder.useServicesAlternate;
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
    public Builder mergeWith(TranslationSettings base) {
        Builder merged = builder();

        merged.provider = this.provider != null
            ? this.provider : base.provider;
        merged.options = this.options != null
            ? this.options : base.options;
        merged.onMiss = this.onMiss != null
            ? this.onMiss : base.onMiss;
        merged.staticMap = this.staticMap != null
            ? this.staticMap : base.staticMap;
        merged.portMap = this.portMap != null
            ? this.portMap : base.portMap;
        merged.useServicesAlternate = this.useServicesAlternate != null
            ? this.useServicesAlternate : base.useServicesAlternate;

        return merged;
    }

    // Getters
    public String getProvider() {
        return provider;
    }

    public Map<String, Object> getOptions() {
        return options;
    }

    public TranslationMissMode getOnMiss() {
        return onMiss;
    }

    public Map<String, String> getStaticMap() {
        return staticMap;
    }

    public Map<String, String> getPortMap() {
        return portMap;
    }

    public Boolean getUseServicesAlternate() {
        return useServicesAlternate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TranslationSettings that = (TranslationSettings) o;
        return
            Objects.equals(provider, that.provider) &&
            Objects.equals(options, that.options) &&
            Objects.equals(onMiss, that.onMiss) &&
            Objects.equals(staticMap, that.staticMap) &&
            Objects.equals(portMap, that.portMap) &&
            Objects.equals(useServicesAlternate, that.useServicesAlternate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(provider, options, onMiss, staticMap, portMap, useServicesAlternate);
    }

    @Override
    public String toString() {
        return "MetricsSettings{" +
            "provider=" + provider +
            ", options=" + options +
            ", onMiss=" + onMiss +
            ", staticMap=" + staticMap +
            ", portMap=" + portMap +
            ", useServicesAlternate=" + useServicesAlternate +
            '}';
    }

    /**
     * Builder for metrics settings with lambda-based configuration.
     */
    public static class Builder {
        private String provider;
        private Map<String,Object> options;
        private TranslationMissMode onMiss;
        private Map<String,String> staticMap;
        private Map<String,String> portMap;
        private Boolean useServicesAlternate;

        /**
         * Builds the MetricsSettings instance.
         */
        public TranslationSettings build() {
            return new TranslationSettings(this);
        }
    }

    // -----------------------------------------------------------------------------------
    // Tweaks interfaces for lambda-based configuration
    // -----------------------------------------------------------------------------------

    /**
     * Interface for configuring network translation.
     */
    public interface TranslationTweaks {
        TranslationTweaks provider(String provider);
        TranslationTweaks options(Map<String,Object> options);
        TranslationTweaks onMiss(TranslationMissMode onMiss);
        TranslationTweaks staticMap(Map<String,String> staticMap);
        TranslationTweaks portMap(Map<String,String> portMap);
        TranslationTweaks useServicesAlternate(Boolean useServicesAlternate);
    }

    // -----------------------------------------------------------------------------------
    // Internal implementations of tweaks interfaces
    // -----------------------------------------------------------------------------------

    public static class TranslationTweaksImpl implements TranslationTweaks {
        private final Builder builder;

        public TranslationTweaksImpl(Builder builder) {
            this.builder = builder;
        }

        @Override
        public TranslationTweaks provider(String provider) {
            builder.provider = provider;
            return this;
        }

        @Override
        public TranslationTweaks options(Map<String,Object> options) {
            builder.options = options;
            return this;
        }

        @Override
        public TranslationTweaks onMiss(TranslationMissMode onMiss) {
            builder.onMiss = onMiss;
            return this;
        }

        @Override
        public TranslationTweaks staticMap(Map<String,String> staticMap) {
            builder.staticMap = staticMap;
            return this;
        }

        @Override
        public TranslationTweaks portMap(Map<String,String> portMap) {
            builder.portMap = portMap;
            return this;
        }

        @Override
        public TranslationTweaks useServicesAlternate(Boolean useServicesAlternate) {
            builder.useServicesAlternate = useServicesAlternate;
            return this;
        }
    }
}
