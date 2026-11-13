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

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.aerospike.client.sdk.discovery.AwsEc2TagSeedCandidateProvider.AddressField;
import com.aerospike.client.sdk.discovery.AwsEc2TagSeedCandidateProvider.Credentials;
import com.aerospike.client.sdk.discovery.AwsEc2TagSeedCandidateProvider.CredentialsHook;

/**
 * Network translation settings that apply to an entire Cluster instance.
 */
public class AwsSettings {
    private final String region;
    private final Map<String,List<String>> filters;
    private final AddressField addressField;
    private final Integer port;
    private final String tlsName;
    private final Duration timeout;
    private final Duration initTimeout;
    private final String endpoint;
    private final URI uri;
    private final String hostHeader;
    private final String path;
    private final String query;
    private final AwsEnvironment env;
    private final AwsXml xml;
    private final Credentials credentials;
    private final CredentialsHook credentialsHook;

    public AwsSettings(Builder builder) {
        this.region = builder.region;
        this.filters = builder.filters;
        this.addressField = builder.addressField;
        this.port = builder.port;
        this.tlsName = builder.tlsName;
        this.timeout = builder.timeout;
        this.initTimeout = builder.initTimeout;
        this.endpoint = builder.endpoint;
        this.uri = builder.uri;
        this.hostHeader = builder.hostHeader;
        this.path = builder.path;
        this.query = builder.query;
        this.env = builder.env;
        this.xml = builder.xml;
        this.credentials = builder.credentials;
        this.credentialsHook = builder.credentialsHook;
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
    public Builder mergeWith(AwsSettings base) {
        Builder merged = builder();

        merged.region = this.region != null
            ? this.region : base.region;
        merged.filters = this.filters != null
            ? this.filters : base.filters;
        merged.addressField = this.addressField != null
            ? this.addressField : base.addressField;
        merged.port = this.port != null
            ? this.port : base.port;
        merged.tlsName = this.tlsName != null
            ? this.tlsName : base.tlsName;
        merged.timeout = this.timeout != null
            ? this.timeout : base.timeout;
        merged.initTimeout = this.initTimeout != null
            ? this.initTimeout : base.initTimeout;
        merged.endpoint = this.endpoint != null
            ? this.endpoint : base.endpoint;
        merged.uri = this.uri != null
            ? this.uri : base.uri;
        merged.hostHeader = this.hostHeader != null
            ? this.hostHeader : base.hostHeader;
        merged.path = this.path != null
            ? this.path : base.path;
        merged.query = this.query != null
            ? this.query : base.query;
        merged.env = this.env != null
            ? this.env : base.env;
        merged.xml = this.xml != null
            ? this.xml : base.xml;
        merged.credentials = this.credentials != null
            ? this.credentials : base.credentials;
        merged.credentialsHook = this.credentialsHook != null
            ? this.credentialsHook : base.credentialsHook;

        return merged;
    }

    // Getters
    public String getRegion() {
        return region;
    }

    public Map<String,List<String>> getFilters() {
        return filters;
    }

    public AddressField getAddressField() {
        return addressField;
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

    public Duration getInitTimeout() {
        return initTimeout;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public URI getUri() {
        return uri;
    }

    public String getHostHeader() {
        return hostHeader;
    }

    public String getPath() {
        return path;
    }

    public String getQuery() {
        return query;
    }

    public AwsEnvironment getEnv() {
        return env;
    }

    public AwsXml getXml() {
        return xml;
    }

    public Credentials getCredentials() {
        return credentials;
    }

    public CredentialsHook getCredentialsHook() {
        return credentialsHook;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AwsSettings that = (AwsSettings) o;
        return
            Objects.equals(region, that.region) &&
            Objects.equals(filters, that.filters) &&
            Objects.equals(addressField, that.addressField) &&
            Objects.equals(port, that.port) &&
            Objects.equals(tlsName, that.tlsName) &&
            Objects.equals(timeout, that.timeout) &&
            Objects.equals(initTimeout, that.initTimeout) &&
            Objects.equals(endpoint, that.endpoint) &&
            Objects.equals(uri, that.uri) &&
            Objects.equals(hostHeader, that.hostHeader) &&
            Objects.equals(path, that.path) &&
            Objects.equals(query, that.query) &&
            Objects.equals(env, that.env) &&
            Objects.equals(xml, that.xml) &&
            Objects.equals(credentials, that.credentials) &&
            Objects.equals(credentialsHook, that.credentialsHook);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            region,
            filters,
            addressField,
            port,
            tlsName,
            timeout,
            initTimeout,
            endpoint,
            uri,
            hostHeader,
            path,
            query,
            env,
            xml,
            credentials,
            credentialsHook);
    }

    @Override
    public String toString() {
        return "AwsSettings{" +
            "region=" + region +
            ", filters=" + filters +
            ", addressField=" + addressField +
            ", port=" + port +
            ", tlsName=" + tlsName +
            ", timeout=" + timeout +
            ", initTimeout=" + initTimeout +
            ", endpoint=" + endpoint +
            ", uri=" + uri +
            ", hostHeader=" + hostHeader +
            ", path=" + path +
            ", query=" + query +
            ", env=" + env +
            ", xml=" + xml +
            ", credentials=" + credentials +
            ", credentialsHook=" + credentialsHook +
            '}';
    }

    /**
     * Builder for metrics settings with lambda-based configuration.
     */
    public static class Builder {
        private String region;
        private Map<String,List<String>> filters;
        private AddressField addressField;
        private Integer port;
        private String tlsName;
        private Duration timeout;
        private Duration initTimeout;
        private String endpoint;
        private URI uri;
        private String hostHeader;
        private String path;
        private String query;
        private AwsEnvironment env;
        private AwsXml xml;
        private Credentials credentials;
        private CredentialsHook credentialsHook;

        /**
         * Builds the MetricsSettings instance.
         */
        public AwsSettings build() {
            return new AwsSettings(this);
        }
    }

    // -----------------------------------------------------------------------------------
    // Tweaks interfaces for lambda-based configuration
    // -----------------------------------------------------------------------------------

    /**
     * Interface for configuring network translation.
     */
    public interface AwsTweaks {
        AwsTweaks region(String region);
        AwsTweaks filters(Map<String,List<String>> filters);
        AwsTweaks addressField(AddressField addressField);
        AwsTweaks port(Integer port);
        AwsTweaks tlsName(String tlsName);
        AwsTweaks timeout(Duration timeout);
        AwsTweaks initTimeout(Duration initTimeout);
        AwsTweaks endpoint(String endpoint);
        AwsTweaks uri(URI uri);
        AwsTweaks hostHeader(String hostHeader);
        AwsTweaks path(String path);
        AwsTweaks query(String query);
        AwsTweaks env(AwsEnvironment env);
        AwsTweaks xml(AwsXml xml);
        AwsTweaks credentials(String accessKeyId, String secretAccessKey, String sessionToken);
        AwsTweaks credentialsHook(CredentialsHook credentialsHook);
    }

    // -----------------------------------------------------------------------------------
    // Internal implementations of tweaks interfaces
    // -----------------------------------------------------------------------------------

    public static class AwsTweaksImpl implements AwsTweaks {
        private final Builder builder;

        public AwsTweaksImpl(Builder builder) {
            this.builder = builder;
        }

        @Override
        public AwsTweaks region(String region) {
            builder.region = region;
            return this;
        }

        @Override
        public AwsTweaks filters(Map<String,List<String>> filters) {
            builder.filters = filters;
            return this;
        }

        @Override
        public AwsTweaks addressField(AddressField addressField) {
            builder.addressField = addressField;
            return this;
        }

        @Override
        public AwsTweaks port(Integer port) {
            builder.port = port;
            return this;
        }

        @Override
        public AwsTweaks tlsName(String tlsName) {
            builder.tlsName = tlsName;
            return this;
        }

        @Override
        public AwsTweaks timeout(Duration timeout) {
            builder.timeout = timeout;
            return this;
        }

        @Override
        public AwsTweaks initTimeout(Duration initTimeout) {
            builder.initTimeout = initTimeout;
            return this;
        }

        @Override
        public AwsTweaks endpoint(String endpoint) {
            builder.endpoint = endpoint;
            return this;
        }

        @Override
        public AwsTweaks uri(URI uri) {
            builder.uri = uri;
            return this;
        }

        @Override
        public AwsTweaks hostHeader(String hostHeader) {
            builder.hostHeader = hostHeader;
            return this;
        }

        @Override
        public AwsTweaks path(String path) {
            builder.path = path;
            return this;
        }

        @Override
        public AwsTweaks query(String query) {
            builder.query = query;
            return this;
        }

        @Override
        public AwsTweaks env(AwsEnvironment env) {
            builder.env = env;
            return this;
        }

        @Override
        public AwsTweaks xml(AwsXml xml) {
            builder.xml = xml;
            return this;
        }

        @Override
        public AwsTweaks credentials(String accessKeyId, String secretAccessKey, String sessionToken) {
            builder.credentials = new Credentials(accessKeyId, secretAccessKey, sessionToken);
            return this;
        }

        @Override
        public AwsTweaks credentialsHook(CredentialsHook credentialsHook) {
            builder.credentialsHook = credentialsHook;
            return this;
        }
    }
}
