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

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import com.aerospike.client.sdk.AerospikeException;

/**
 * Metrics settings that apply to an entire Cluster instance.
 */
public class DiscoverySettings {
    private final String provider;
    private final Map<String,Object> options;
    private final SeedMergePolicy mergePolicy;
    private final DiscoveryExecutionMode executionMode;
    private final Duration refreshInterval;
    private final Duration refreshTimeout;
    private final Duration tendRefreshDeadline;
    private final Map<String,Function<Map<String,Object>,SeedCandidateProvider>> providerFactories;
    private final DnsSettings dns;
    private final AwsSettings aws;

    public DiscoverySettings(Builder builder) {
        this.provider = builder.provider;
        this.options = builder.options;
        this.mergePolicy = builder.mergePolicy;
        this.executionMode = builder.executionMode;
        this.refreshInterval = builder.refreshInterval;
        this.refreshTimeout = builder.refreshTimeout;
        this.tendRefreshDeadline = builder.tendRefreshDeadline;
        this.providerFactories = builder.providerFactories;
        this.dns = new DnsSettings(builder.dns);
        this.aws = new AwsSettings(builder.aws);
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
    public Builder mergeWith(DiscoverySettings base) {
        Builder merged = builder();

        merged.provider = this.provider != null
            ? this.provider : base.provider;
        merged.options = this.options != null
            ? this.options : base.options;
        merged.mergePolicy = this.mergePolicy != null
            ? this.mergePolicy : base.mergePolicy;
        merged.executionMode = this.executionMode != null
            ? this.executionMode : base.executionMode;
        merged.refreshInterval = this.refreshInterval != null
            ? this.refreshInterval : base.refreshInterval;
        merged.refreshTimeout = this.refreshTimeout != null
            ? this.refreshTimeout : base.refreshTimeout;
        merged.tendRefreshDeadline = this.tendRefreshDeadline != null
            ? this.tendRefreshDeadline : base.tendRefreshDeadline;
        merged.tendRefreshDeadline = this.tendRefreshDeadline != null
            ? this.tendRefreshDeadline : base.tendRefreshDeadline;
        merged.providerFactories = this.providerFactories != null
            ? this.providerFactories : base.providerFactories;

        merged.dns = this.dns.mergeWith(base.dns);
        merged.aws = this.aws.mergeWith(base.aws);

        return merged;
    }

    // Getters
    public String getProvider() { return provider; }
    public Map<String,Object> getOptions() { return options; }
    public SeedMergePolicy getMergePolicy() { return mergePolicy; }
    public DiscoveryExecutionMode getExecutionMode() { return executionMode; }
    public Duration getRefreshInterval() { return refreshInterval; }
    public Duration getRefreshTimeout() { return refreshTimeout; }
    public Duration getTendRefreshDeadline() { return tendRefreshDeadline; }

    public Map<String, Function<Map<String, Object>, SeedCandidateProvider>> getProviderFactories() {
        return providerFactories;
    }

    public DnsSettings getDns() { return dns; }
    public AwsSettings getAws() { return aws; }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DiscoverySettings that = (DiscoverySettings) o;
        return
            Objects.equals(provider, that.provider) &&
            Objects.equals(options, that.options) &&
            Objects.equals(mergePolicy, that.mergePolicy) &&
            Objects.equals(executionMode, that.executionMode) &&
            Objects.equals(refreshInterval, that.refreshInterval) &&
            Objects.equals(refreshTimeout, that.refreshTimeout) &&
            Objects.equals(tendRefreshDeadline, that.tendRefreshDeadline) &&
            Objects.equals(providerFactories, that.providerFactories) &&
            Objects.equals(dns, that.dns) &&
            Objects.equals(aws, that.aws);
    }

    @Override
    public int hashCode() {
        return Objects.hash(provider, options, mergePolicy, executionMode, refreshInterval,
            refreshTimeout, tendRefreshDeadline, providerFactories, dns, aws);
    }

    @Override
    public String toString() {
        return "MetricsSettings{" +
            "provider=" + provider +
            ", options=" + options +
            ", mergePolicy=" + mergePolicy +
            ", executionMode=" + executionMode +
            ", refreshIntervalMs=" + refreshInterval +
            ", refreshTimeoutMs=" + refreshTimeout +
            ", tendRefreshDeadlineMs=" + tendRefreshDeadline +
            ", providerFactories=" + providerFactories +
            ", dns=" + dns +
            ", aws=" + aws +
            '}';
    }

    /**
     * Builder for metrics settings with lambda-based configuration.
     */
    public static class Builder {
        private String provider;
        private Map<String,Object> options;
        private SeedMergePolicy mergePolicy;
        private DiscoveryExecutionMode executionMode;
        private Duration refreshInterval;
        private Duration refreshTimeout;
        private Duration tendRefreshDeadline;
        private Map<String,Function<Map<String,Object>,SeedCandidateProvider>> providerFactories;
        private DnsSettings.Builder dns = DnsSettings.builder();
        private AwsSettings.Builder aws = AwsSettings.builder();

        /**
         * Builds the MetricsSettings instance.
         */
        public DiscoverySettings build() {
            return new DiscoverySettings(this);
        }
    }

    // -----------------------------------------------------------------------------------
    // Tweaks interfaces for lambda-based configuration
    // -----------------------------------------------------------------------------------

    /**
     * Interface for configuring metrics signal related settings.
     */
    public interface DiscoveryTweaks {
        DiscoveryTweaks provider(String provider);
        DiscoveryTweaks options(Map<String,Object> options);
        DiscoveryTweaks mergePolicy(SeedMergePolicy mergePolicy);
        DiscoveryTweaks executionMode(DiscoveryExecutionMode executionMode);
        DiscoveryTweaks refreshInterval(Duration refreshInterval);
        DiscoveryTweaks refreshTimeout(Duration refreshTimeout);
        DiscoveryTweaks tendRefreshDeadline(Duration tendRefreshDeadline);
        DiscoveryTweaks addProviderFactory(String name, Function<Map<String,Object>,SeedCandidateProvider> factory);
        DiscoveryTweaks dns(Consumer<DnsSettings.DnsTweaks> configurator);
        DiscoveryTweaks aws(Consumer<AwsSettings.AwsTweaks> configurator);
    }

    // -----------------------------------------------------------------------------------
    // Internal implementations of tweaks interfaces
    // -----------------------------------------------------------------------------------

    public static class DiscoveryTweaksImpl implements DiscoveryTweaks {
        private final Builder builder;

        public DiscoveryTweaksImpl(Builder builder) {
            this.builder = builder;
        }

        @Override
        public DiscoveryTweaks provider(String provider) {
            builder.provider = provider;
            return this;
        }

        @Override
        public DiscoveryTweaks options(Map<String,Object> options) {
            builder.options = options;
            return this;
        }

        @Override
        public DiscoveryTweaks mergePolicy(SeedMergePolicy mergePolicy) {
            builder.mergePolicy = mergePolicy;
            return this;
        }

        @Override
        public DiscoveryTweaks executionMode(DiscoveryExecutionMode executionMode) {
            builder.executionMode = executionMode;
            return this;
        }

        @Override
        public DiscoveryTweaks refreshInterval(Duration refreshInterval) {
            builder.refreshInterval = refreshInterval;
            return this;
        }

        @Override
        public DiscoveryTweaks refreshTimeout(Duration refreshTimeout) {
            builder.refreshTimeout = refreshTimeout;
            return this;
        }

        @Override
        public DiscoveryTweaks tendRefreshDeadline(Duration tendRefreshDeadline) {
            builder.tendRefreshDeadline = tendRefreshDeadline;
            return this;
        }

        @Override
        public DiscoveryTweaks addProviderFactory(
            String name, Function<Map<String,Object>,SeedCandidateProvider> factory
        ) {
            builder.providerFactories = register("seed candidate provider", name, factory,
                StaticDiscoveryConfig.PROVIDERS, builder.providerFactories);
            return this;
        }

        @Override
        public DiscoveryTweaks dns(Consumer<DnsSettings.DnsTweaks> configurator) {
            configurator.accept(new DnsSettings.DnsTweaksImpl(builder.dns));
            return this;
        }

        @Override
        public DiscoveryTweaks aws(Consumer<AwsSettings.AwsTweaks> configurator) {
            configurator.accept(new AwsSettings.AwsTweaksImpl(builder.aws));
            return this;
        }

        private static <T> Map<String,Function<Map<String,Object>,T>> register(
            String type,
            String name,
            Function<Map<String,Object>,T> factory,
            List<String> builtIn,
            Map<String,Function<Map<String,Object>,T>> factories
        ) {
            if (name == null || name.isEmpty()) {
                throw new AerospikeException("Invalid " + type + " name: " + name);
            }

            if (builtIn.contains(name)) {
                throw new AerospikeException("Invalid " + type + " name '" + name + "': built-in name");
            }

            if (factories != null && factories.containsKey(name)) {
                throw new AerospikeException("Invalid " + type + " name '" + name + "': already registered");
            }

            if (factory == null) {
                throw new AerospikeException("Invalid " + type + " factory for '" + name + "': null");
            }

            if (factories == null) {
                factories = new LinkedHashMap<>();
            }
            factories.put(name, factory);
            return factories;
        }
    }
}
