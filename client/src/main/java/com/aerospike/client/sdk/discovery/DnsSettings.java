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
import java.util.List;
import java.util.Objects;

import com.aerospike.client.sdk.discovery.DnsSeedCandidateProvider.RecordType;

/**
 * Network translation settings that apply to an entire Cluster instance.
 */
public class DnsSettings {
    private final List<String> names;
    private final RecordType recordType;
    private final Integer port;
    private final String tlsName;
    private final Duration timeout;
    private final List<String> servers;
    private final String providerUrl;
    private final Integer serverCount;

    public DnsSettings(Builder builder) {
        this.names = builder.names;
        this.recordType = builder.recordType;
        this.port = builder.port;
        this.tlsName = builder.tlsName;
        this.timeout = builder.timeout;
        this.servers = builder.servers;
        this.providerUrl = builder.providerUrl;
        this.serverCount = builder.serverCount;
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
    public Builder mergeWith(DnsSettings base) {
        Builder merged = builder();

        merged.names = this.names != null
            ? this.names : base.names;
        merged.recordType = this.recordType != null
            ? this.recordType : base.recordType;
        merged.port = this.port != null
            ? this.port : base.port;
        merged.tlsName = this.tlsName != null
            ? this.tlsName : base.tlsName;
        merged.timeout = this.timeout != null
            ? this.timeout : base.timeout;
        merged.servers = this.servers != null
            ? this.servers : base.servers;
        merged.providerUrl = this.providerUrl != null
            ? this.providerUrl : base.providerUrl;
        merged.serverCount = this.serverCount != null
            ? this.serverCount : base.serverCount;

        return merged;
    }

    // Getters
    public List<String> getNames() {
        return names;
    }

    public RecordType getRecordType() {
        return recordType;
    }

    public Integer getPort() {
        return port;
    }

    public String getTlsName() {
        return tlsName;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public List<String> getServers() {
        return servers;
    }

    public String getProviderUrl() {
        return providerUrl;
    }

    public Integer getServerCount() {
        return serverCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DnsSettings that = (DnsSettings) o;
        return
            Objects.equals(names, that.names) &&
            Objects.equals(recordType, that.recordType) &&
            Objects.equals(port, that.port) &&
            Objects.equals(tlsName, that.tlsName) &&
            Objects.equals(timeout, that.timeout) &&
            Objects.equals(servers, that.servers) &&
            Objects.equals(providerUrl, that.providerUrl) &&
            Objects.equals(serverCount, that.serverCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            names,
            recordType,
            port,
            tlsName,
            timeout,
            servers,
            providerUrl,
            serverCount);
    }

    @Override
    public String toString() {
        return "MetricsSettings{" +
            "names=" + names +
            ", recordType=" + recordType +
            ", port=" + port +
            ", tlsName=" + tlsName +
            ", timeout=" + timeout +
            ", servers=" + servers +
            ", providerUrl=" + providerUrl +
            ", serverCount=" + serverCount +
            '}';
    }

    /**
     * Builder for metrics settings with lambda-based configuration.
     */
    public static class Builder {
        private List<String> names;
        private RecordType recordType;
        private Integer port;
        private String tlsName;
        private Duration timeout;
        private List<String> servers;
        private String providerUrl;
        private Integer serverCount;

        /**
         * Builds the MetricsSettings instance.
         */
        public DnsSettings build() {
            return new DnsSettings(this);
        }
    }

    // -----------------------------------------------------------------------------------
    // Tweaks interfaces for lambda-based configuration
    // -----------------------------------------------------------------------------------

    /**
     * Interface for configuring network translation.
     */
    public interface DnsTweaks {
        DnsTweaks names(List<String> names);
        DnsTweaks recordType(RecordType recordType);
        DnsTweaks port(Integer port);
        DnsTweaks tlsName(String tlsName);
        DnsTweaks timeout(Duration timeout);
        DnsTweaks servers(List<String> servers);
        DnsTweaks providerUrl(String providerUrl);
        DnsTweaks serverCount(Integer serverCount);
    }

    // -----------------------------------------------------------------------------------
    // Internal implementations of tweaks interfaces
    // -----------------------------------------------------------------------------------

    public static class DnsTweaksImpl implements DnsTweaks {
        private final Builder builder;

        public DnsTweaksImpl(Builder builder) {
            this.builder = builder;
        }

        @Override
        public DnsTweaks names(List<String> names) {
            builder.names = names;
            return this;
        }

        @Override
        public DnsTweaks recordType(RecordType recordType) {
            builder.recordType = recordType;
            return this;
        }

        @Override
        public DnsTweaks port(Integer port) {
            builder.port = port;
            return this;
        }

        @Override
        public DnsTweaks tlsName(String tlsName) {
            builder.tlsName = tlsName;
            return this;
        }

        @Override
        public DnsTweaks timeout(Duration timeout) {
            builder.timeout = timeout;
            return this;
        }

        @Override
        public DnsTweaks servers(List<String> servers) {
            builder.servers = servers;
            return this;
        }

        @Override
        public DnsTweaks providerUrl(String providerUrl) {
            builder.providerUrl = providerUrl;
            return this;
        }

        @Override
        public DnsTweaks serverCount(Integer serverCount) {
            builder.serverCount = serverCount;
            return this;
        }
    }
}
