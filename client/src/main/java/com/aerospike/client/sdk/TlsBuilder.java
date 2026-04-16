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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/**
 * Builder class for configuring TLS settings for Aerospike cluster connections.
 *
 * <p>This class provides an API for configuring TLS parameters such as
 * TLS name, CA file, protocols, ciphers, and other TLS-specific options.</p>
 *
 * <p>Two certificate formats are supported:</p>
 * <ul>
 *   <li><strong>PEM files</strong> - Using {@link #caFile}, {@link #clientCertFile}, {@link #clientKeyFile}</li>
 *   <li><strong>Java KeyStore (JKS/PKCS12)</strong> - Using {@link #trustStore}, {@link #keyStore}</li>
 * </ul>
 * <p><strong>Note:</strong> You must use one format or the other, not both.</p>
 *
 * <h3>Private Key Format Requirements (PEM)</h3>
 * <p>When using PEM files for client authentication, the private key file must be in
 * <strong>PKCS#8 format</strong>. This class supports:</p>
 * <ul>
 *   <li>{@code -----BEGIN PRIVATE KEY-----} (PKCS#8 unencrypted)</li>
 *   <li>{@code -----BEGIN ENCRYPTED PRIVATE KEY-----} (PKCS#8 encrypted)</li>
 * </ul>
 *
 * <p><strong>PKCS#1 keys are NOT supported.</strong> If your private key file begins with
 * {@code -----BEGIN RSA PRIVATE KEY-----} or {@code -----BEGIN EC PRIVATE KEY-----},
 * you must convert it to PKCS#8 format first.</p>
 *
 * <h4>Converting PKCS#1 to PKCS#8</h4>
 * <p>Use OpenSSL to convert your key:</p>
 * <pre>
 * # Convert unencrypted PKCS#1 RSA key to unencrypted PKCS#8:
 * openssl pkcs8 -topk8 -nocrypt -in rsa-key-pkcs1.pem -out rsa-key-pkcs8.pem
 *
 * # Convert unencrypted PKCS#1 RSA key to encrypted PKCS#8:
 * openssl pkcs8 -topk8 -in rsa-key-pkcs1.pem -out rsa-key-pkcs8-encrypted.pem
 *
 * # Convert EC key (SEC1 format) to PKCS#8:
 * openssl pkcs8 -topk8 -nocrypt -in ec-key.pem -out ec-key-pkcs8.pem
 * </pre>
 *
 * <p>Alternatively, you can use Java KeyStore format which handles key format conversion
 * automatically when importing keys.</p>
 *
 * <h3>PEM Example (Simple)</h3>
 * <pre>{@code
 * ClusterDefinition cluster = new ClusterDefinition("localhost", 3100)
 *     .withTlsConfig(tls -> tls
 *         .tlsName("myTlsName")
 *         .caFile("/path/to/ca.pem")
 *     )
 *     .withNativeCredentials("myUser", "password");
 * }</pre>
 *
 * <h3>PEM Example (Mutual TLS)</h3>
 * <pre>{@code
 * ClusterDefinition cluster = new ClusterDefinition("localhost", 3100)
 *     .withTlsConfig(tls -> tls
 *         .tlsName("myTlsName")
 *         .caFile("/path/to/ca.pem")
 *         .clientCertFile("/path/to/client-cert.pem")
 *         .clientKeyFile("/path/to/client-key-pkcs8.pem")  // Must be PKCS#8 format
 *         .protocols("TLSv1.2", "TLSv1.3")
 *     )
 *     .withNativeCredentials("myUser", "password");
 * }</pre>
 *
 * <h3>Java KeyStore Example</h3>
 * <pre>{@code
 * ClusterDefinition cluster = new ClusterDefinition("localhost", 3100)
 *     .withTlsConfig(tls -> tls
 *         .tlsName("myTlsName")
 *         .trustStore("/path/to/truststore.jks", "truststorePassword", "JKS")
 *         .keyStore("/path/to/keystore.jks", "keystorePassword", "JKS")
 *     )
 *     .withNativeCredentials("myUser", "password");
 * }</pre>
 *
 * <h3>PKCS12 KeyStore Example</h3>
 * <pre>{@code
 * ClusterDefinition cluster = new ClusterDefinition("localhost", 3100)
 *     .withTlsConfig(tls -> tls
 *         .tlsName("myTlsName")
 *         .trustStore("/path/to/truststore.p12", "password", "PKCS12")
 *         .keyStore("/path/to/keystore.p12", "password", "PKCS12")
 *     );
 * }</pre>
 */
public class TlsBuilder {
    private String tlsName;
    private String caFile;
    private String[] protocols;
    private String[] ciphers;
    private boolean forLoginOnly = false;
    private boolean revokeCertificatesForLoginOnly = false;
    private java.math.BigInteger[] revokeCertificates;
    private String clientCertFile;
    private String clientKeyFile;
    private String clientKeyPassword;
    private String trustStorePath;
    private String trustStorePassword;
    private String trustStoreType = "JKS";
    private String keyStorePath;
    private String keyStorePassword;
    private String keyStoreType = "JKS";
    private SSLContext customSslContext;
    private boolean enableTrustAllCertificates = false;

    /**
     * Creates a new TlsBuilder with default settings.
     */
    public TlsBuilder() {
    }

    /**
     * Sets the TLS name for server certificate validation and hostname verification.
     *
     * <p>This TLS name will be applied to all Host objects that don't already have
     * a TLS name set. The TLS name is used for:</p>
     * <ul>
     *   <li><strong>Certificate validation:</strong> Verifies the server certificate matches this name</li>
     *   <li><strong>SNI (Server Name Indication):</strong> Tells the server which certificate to present</li>
     *   <li><strong>Hostname override:</strong> Allows validation against a different name than the connection address</li>
     * </ul>
     *
     * <p><strong>Example use cases:</strong></p>
     * <ul>
     *   <li>Connecting by IP address but validating against a domain certificate</li>
     *   <li>Load balancer scenarios where backend server has different certificate</li>
     *   <li>Development environments with self-signed certificates</li>
     * </ul>
     *
     * @param tlsName the TLS name for certificate validation and hostname verification
     * @return this TlsBuilder for method chaining
     */
    public TlsBuilder tlsName(String tlsName) {
        this.tlsName = tlsName;
        return this;
    }

    /**
     * Sets the path to the Certificate Authority (CA) PEM file.
     *
     * <p>The CA file contains the certificates used to verify the server's identity.
     * This method supports PEM-formatted certificate files for easy certificate management.</p>
     *
     * @param caFile the path to the CA certificate PEM file
     * @return this TlsBuilder for method chaining
     */
    public TlsBuilder caFile(String caFile) {
        this.caFile = caFile;
        return this;
    }

    /**
     * Sets the trust store path and password for Java KeyStore format.
     *
     * <p>Alternative to PEM CA files, this allows using Java KeyStore (JKS)
     * or PKCS12 format trust stores for certificate authority verification.</p>
     *
     * @param trustStorePath the path to the trust store file
     * @param password the trust store password
     * @param storeType the store type ("JKS", "PKCS12", etc.)
     * @return this TlsBuilder for method chaining
     */
    public TlsBuilder trustStore(String trustStorePath, String password, String storeType) {
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = password;
        this.trustStoreType = storeType != null ? storeType : "JKS";
        return this;
    }

    /**
     * Sets the key store path and password for Java KeyStore format.
     *
     * <p>Alternative to PEM client certificate files, this allows using Java KeyStore (JKS)
     * or PKCS12 format key stores for client authentication.</p>
     *
     * @param keyStorePath the path to the key store file
     * @param password the key store password
     * @param storeType the store type ("JKS", "PKCS12", etc.)
     * @return this TlsBuilder for method chaining
     */
    public TlsBuilder keyStore(String keyStorePath, String password, String storeType) {
        this.keyStorePath = keyStorePath;
        this.keyStorePassword = password;
        this.keyStoreType = storeType != null ? storeType : "JKS";
        return this;
    }

    /**
     * Sets the allowable TLS protocols for the connection.
     *
     * <p>Specifies which TLS protocol versions are acceptable for the connection.
     * Common values include "TLSv1.2", "TLSv1.3".</p>
     *
     * @param protocols the allowed TLS protocol versions
     * @return this TlsBuilder for method chaining
     */
    public TlsBuilder protocols(String... protocols) {
        this.protocols = protocols;
        return this;
    }

    /**
     * Sets the allowable TLS cipher suites for the connection.
     *
     * <p>Specifies which cipher suites are acceptable for the TLS connection.
     * This controls the encryption algorithms used for the secure connection.</p>
     *
     * @param ciphers the allowed cipher suites
     * @return this TlsBuilder for method chaining
     */
    public TlsBuilder ciphers(String... ciphers) {
        this.ciphers = ciphers;
        return this;
    }

    /**
     * Configures whether TLS should be used only for login authentication.
     *
     * <p>When set to true, TLS is only used during the initial authentication phase.
     * After authentication, the connection switches to non-encrypted communication.
     * This can improve performance while still securing the authentication process.</p>
     *
     * @param forLoginOnly true to use TLS only for login, false for full TLS
     * @return this TlsBuilder for method chaining
     */
    public TlsBuilder forLoginOnly(boolean forLoginOnly) {
        this.forLoginOnly = forLoginOnly;
        return this;
    }

    /**
     * Sets the client certificate PEM file for mutual TLS authentication.
     *
     * <p>This certificate is presented to the server to authenticate the client.
     * Used in mutual TLS (mTLS) scenarios. Supports PEM-formatted certificate files.</p>
     *
     * @param clientCertFile the path to the client certificate PEM file
     * @return this TlsBuilder for method chaining
     */
    public TlsBuilder clientCertFile(String clientCertFile) {
        this.clientCertFile = clientCertFile;
        return this;
    }

    /**
     * Sets the client private key PEM file for mutual TLS authentication.
     *
     * <p>This private key corresponds to the client certificate and is used
     * to prove ownership of the certificate during mutual TLS authentication.</p>
     *
     * <p><strong>Important:</strong> The private key must be in PKCS#8 format
     * (headers: {@code -----BEGIN PRIVATE KEY-----} or {@code -----BEGIN ENCRYPTED PRIVATE KEY-----}).
     * PKCS#1 format keys ({@code -----BEGIN RSA PRIVATE KEY-----}) are not supported.
     * See class-level documentation for conversion instructions.</p>
     *
     * @param clientKeyFile the path to the client private key PEM file (PKCS#8 format)
     * @return this TlsBuilder for method chaining
     * @see #clientKeyPassword(String) for encrypted private keys
     */
    public TlsBuilder clientKeyFile(String clientKeyFile) {
        this.clientKeyFile = clientKeyFile;
        return this;
    }

    /**
     * Sets the password for the client private key file.
     *
     * <p>If the client private key file is password-protected, this password
     * will be used to decrypt the key for use in mutual TLS authentication.</p>
     *
     * @param clientKeyPassword the password for the client private key
     * @return this TlsBuilder for method chaining
     */
    public TlsBuilder clientKeyPassword(String clientKeyPassword) {
        this.clientKeyPassword = clientKeyPassword;
        return this;
    }

    /**
     * Sets the certificate revocation list.
     *
     * <p>Specifies certificates that should not be trusted, even if they are
     * otherwise valid. This is used for enhanced security by blocking known
     * compromised certificates.</p>
     *
     * @param revokeCertificates array of revoked certificate identifiers
     * @return this TlsBuilder for method chaining
     */
    public TlsBuilder revokeCertificates(java.math.BigInteger... revokeCertificates) {
        this.revokeCertificates = revokeCertificates;
        return this;
    }

    /**
     * Configures whether certificate revocation should apply only to login.
     *
     * <p>When true, certificate revocation checking is only performed during
     * the login phase. When false, it applies to all TLS communications.</p>
     *
     * @param revokeCertificatesForLoginOnly true to limit revocation checking to login only
     * @return this TlsBuilder for method chaining
     */
    public TlsBuilder revokeCertificatesForLoginOnly(boolean revokeCertificatesForLoginOnly) {
        this.revokeCertificatesForLoginOnly = revokeCertificatesForLoginOnly;
        return this;
    }

    /**
     * Sets a custom SSLContext for advanced TLS configuration.
     *
     * <p>When a custom SSLContext is provided, it takes precedence over
     * other TLS configuration methods. This allows for complete control
     * over the SSL/TLS setup for advanced use cases.</p>
     *
     * @param sslContext the custom SSLContext to use
     * @return this TlsBuilder for method chaining
     */
    public TlsBuilder customSslContext(SSLContext sslContext) {
        this.customSslContext = sslContext;
        return this;
    }

    /**
     * Enables trusting all certificates (insecure - for testing only).
     *
     * <p><strong>WARNING:</strong> This disables certificate validation
     * and should NEVER be used in production environments. It makes
     * the connection vulnerable to man-in-the-middle attacks.</p>
     *
     * @param trustAll true to trust all certificates (insecure)
     * @return this TlsBuilder for method chaining
     */
    public TlsBuilder trustAllCertificates(boolean trustAll) {
        this.enableTrustAllCertificates = trustAll;
        return this;
    }

    /**
     * Creates an SSLContext based on the current TLS configuration.
     *
     * <p>This method uses the sslcontext-kickstart library to create an SSLContext
     * from the configured PEM files, key stores, or other SSL materials.</p>
     *
     * @return the configured SSLContext, or null if TLS is not enabled
     * @throws RuntimeException if SSL configuration fails
     */
    public SSLContext createSslContext() {
        if (customSslContext != null) {
            return customSslContext;
        }

        if (!isTlsEnabled()) {
            return null;
        }

        // If caFile is defined, create SslContext that references certificate files directly.
        //
        // ClusterDefinition def = new ClusterDefinition(host, port)
        //     .withTlsConfig(tls -> tls
        //         .caFile(caFile)
        //         .clientCertFile(clientCertFile)
        //         .clientKeyFile(clientKeyFile)
        //         .tlsName(tlsName)
        //     );
        //
        // If caFile is not defined, use jvm default SslContext which needs truststore/keystore
        // to be defined on the command line:
        //
        // ClusterDefinition def = new ClusterDefinition(host, port)
        //     .withTlsConfig(tls -> tls
        //         .tlsName(tlsName)
        //     );
        //
        // Server authentication:
        // java -Djavax.net.ssl.trustStore=<path> -Djavax.net.ssl.trustStorePassword=<pass> -jar <app jar> <args>
        //
        // Mutual authentication:
        // java -Djavax.net.ssl.trustStore=<path> -Djavax.net.ssl.trustStorePassword=<pass>
        //      -Djavax.net.ssl.keyStore=<path> -Djavax.net.ssl.keyStorePassword=<pass>
        //      -jar <app jar> <args>
        if (caFile != null) {
            customSslContext = toSSLContext();
        }
        return customSslContext;
    }


    /**
     * Creates an SSLFactory based on the current TLS configuration.
     *
     * <p>This method builds an SSLFactory using either PEM files or Java KeyStore (JKS/PKCS12) files,
     * but not both. The method validates that only one certificate format is configured.</p>
     *
     * <h3>PEM-based Configuration</h3>
     * <p>When using PEM files, configure with:</p>
     * <ul>
     *   <li>{@link #caFile(String)} - CA certificate for server verification</li>
     *   <li>{@link #clientCertFile(String)} - Client certificate for mutual TLS</li>
     *   <li>{@link #clientKeyFile(String)} - Client private key for mutual TLS</li>
     *   <li>{@link #clientKeyPassword(String)} - Password for encrypted private key (optional)</li>
     * </ul>
     *
     * <h3>KeyStore-based Configuration</h3>
     * <p>When using Java KeyStore files, configure with:</p>
     * <ul>
     *   <li>{@link #trustStore(String, String, String)} - Trust store containing CA certificates</li>
     *   <li>{@link #keyStore(String, String, String)} - Key store containing client certificate and key</li>
     * </ul>
     *
     * <h3>Trust All Certificates</h3>
     * <p>For testing only, use {@link #trustAllCertificates(boolean)} to disable certificate validation.
     * <strong>WARNING:</strong> This is insecure and should never be used in production.</p>
     *
     * @return the configured SSLFactory
     * @throws AerospikeException if certificate files are not found, or if both PEM and KeyStore
     *         configurations are specified
     * @throws RuntimeException if SSL configuration fails
     *
     * @see #createSslContext()
     */
    public SSLContext toSSLContext() {
        // Validate that only one certificate format is used
        boolean hasPemConfig = caFile != null || clientCertFile != null || clientKeyFile != null;
        boolean hasKeyStoreConfig = trustStorePath != null || keyStorePath != null;

        if (hasPemConfig && hasKeyStoreConfig) {
            throw new AerospikeException(
                "Cannot mix PEM files (caFile, clientCertFile, clientKeyFile) with KeyStore files " +
                "(trustStore, keyStore). Please use only one certificate format.");
        }

        // Handle trust-all-certificates mode (for testing only)
        if (enableTrustAllCertificates) {
            if (Log.warnEnabled()) {
                Log.warn("TLS configured to trust all certificates - THIS IS INSECURE and should only be used for testing!");
            }
            return buildTrustAllContext();
        }

        // Use KeyStore-based configuration
        if (hasKeyStoreConfig) {
            return buildFromKeyStore();
        }

        // Use PEM-based configuration (default)
     //   return buildFromPem();
        return buildSslContextFromPem();
    }

    /**
     * Builds an SSLContext from Java KeyStore (JKS/PKCS12) files using native JDK.
     */
    private SSLContext buildFromKeyStore() {
        try {
            TrustManager[] trustManagers = null;
            KeyManager[] keyManagers = null;

            // Configure trust store (CA certificates)
            if (trustStorePath != null) {
                if (Log.debugEnabled()) {
                    Log.debug("Using trust store: " + trustStorePath + " (type: " + trustStoreType + ")");
                }
                Path trustPath = java.nio.file.Paths.get(trustStorePath);
                if (!Files.exists(trustPath)) {
                    throw new AerospikeException(String.format("Trust store file '%s' not found", trustStorePath));
                }

                KeyStore trustStore = KeyStore.getInstance(trustStoreType);
                try (InputStream is = Files.newInputStream(trustPath)) {
                    trustStore.load(is, trustStorePassword != null ? trustStorePassword.toCharArray() : new char[0]);
                }

                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(trustStore);
                trustManagers = tmf.getTrustManagers();
            }

            // Configure key store (client certificate and key for mutual TLS)
            if (keyStorePath != null) {
                if (Log.debugEnabled()) {
                    Log.debug("Using key store: " + keyStorePath + " (type: " + keyStoreType + ")");
                }
                Path keyPath = java.nio.file.Paths.get(keyStorePath);
                if (!Files.exists(keyPath)) {
                    throw new AerospikeException(String.format("Key store file '%s' not found", keyStorePath));
                }

                char[] keyPassword = keyStorePassword != null ? keyStorePassword.toCharArray() : new char[0];
                KeyStore keyStore = KeyStore.getInstance(keyStoreType);
                try (InputStream is = Files.newInputStream(keyPath)) {
                    keyStore.load(is, keyPassword);
                }

                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keyStore, keyPassword);
                keyManagers = kmf.getKeyManagers();
            }

            String tlsProtocol = chooseTlsProtocol(this.protocols);
            SSLContext ctx = SSLContext.getInstance(tlsProtocol != null ? tlsProtocol : "TLS");
            ctx.init(keyManagers, trustManagers, null);
            return ctx;
        } catch (GeneralSecurityException | IOException ex) {
            throw new AerospikeException(ex);
        }
    }

    /**
     * Builds an SSLContext that trusts all certificates (INSECURE - for testing only).
     */
    private SSLContext buildTrustAllContext() {
        try {
            TrustManager[] trustAllManagers = new TrustManager[] {
                new javax.net.ssl.X509TrustManager() {
                    @Override
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return new java.security.cert.X509Certificate[0];
                    }
                    @Override
                    public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
                        // Trust all
                    }
                    @Override
                    public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
                        // Trust all
                    }
                }
            };

            KeyManager[] keyManagers = null;
            // Still allow identity material for client authentication
            if (keyStorePath != null) {
                char[] keyPassword = keyStorePassword != null ? keyStorePassword.toCharArray() : new char[0];
                KeyStore keyStore = KeyStore.getInstance(keyStoreType);
                try (InputStream is = Files.newInputStream(java.nio.file.Paths.get(keyStorePath))) {
                    keyStore.load(is, keyPassword);
                }
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keyStore, keyPassword);
                keyManagers = kmf.getKeyManagers();
            }

            String tlsProtocol = chooseTlsProtocol(this.protocols);
            SSLContext ctx = SSLContext.getInstance(tlsProtocol != null ? tlsProtocol : "TLS");
            ctx.init(keyManagers, trustAllManagers, null);
            return ctx;
        } catch (GeneralSecurityException | IOException ex) {
            throw new AerospikeException(ex);
        }
    }

    // Public Getters
    public String getTlsName() { return tlsName; }
    public String getCaFile() { return caFile; }
    public String[] getProtocols() { return protocols; }
    public String[] getCiphers() { return ciphers; }
    public boolean isForLoginOnly() { return forLoginOnly; }
    public boolean isRevokeCertificatesForLoginOnly() { return revokeCertificatesForLoginOnly; }
    public java.math.BigInteger[] getRevokeCertificates() { return revokeCertificates; }
    public String getClientCertFile() { return clientCertFile; }
    public String getClientKeyFile() { return clientKeyFile; }
    public String getClientKeyPassword() { return clientKeyPassword; }
    public String getTrustStorePath() { return trustStorePath; }
    public String getTrustStorePassword() { return trustStorePassword; }
    public String getTrustStoreType() { return trustStoreType; }
    public String getKeyStorePath() { return keyStorePath; }
    public String getKeyStorePassword() { return keyStorePassword; }
    public String getKeyStoreType() { return keyStoreType; }
    public SSLContext getCustomSslContext() { return customSslContext; }
    public boolean isTrustAllCertificates() { return enableTrustAllCertificates; }

    /**
     * Returns true if any TLS configuration has been set, indicating TLS should be enabled.
     */
    boolean isTlsEnabled() {
        return tlsName != null || caFile != null || protocols != null || ciphers != null ||
               forLoginOnly || revokeCertificatesForLoginOnly || revokeCertificates != null ||
               clientCertFile != null || clientKeyFile != null || clientKeyPassword != null ||
               trustStorePath != null || keyStorePath != null || customSslContext != null ||
               enableTrustAllCertificates;
    }

    /** Regex pattern for matching PEM block headers and content. */
    private static final Pattern PEM_BLOCK = Pattern.compile(
            "-----BEGIN ([A-Z0-9 ]+)-----([^-]+)-----END \\1-----",
            Pattern.DOTALL);

    /**
     * Selects the best TLS protocol version from the requested protocols that is supported by the JVM.
     *
     * <p>Protocols are sorted by preference (TLSv1.3, TLSv1.2, etc.) and the first
     * supported protocol is returned. If no requested protocols are supported,
     * returns the first requested protocol anyway.</p>
     *
     * @param requested array of requested TLS protocol versions (e.g., "TLSv1.2", "TLSv1.3")
     * @return the selected protocol, or null if no protocols were requested
     * @throws GeneralSecurityException if there's an error probing the JVM's SSL capabilities
     */
    private String chooseTlsProtocol(String[] requested) throws GeneralSecurityException {
        // If nothing provided, let JVM decide (often best in FIPS mode).
        if (requested == null || requested.length == 0) {
            return null;
        }

        // What this JVM/provider actually supports.
        SSLContext probe = SSLContext.getInstance("TLS");
        probe.init(null, null, null);
        SSLParameters supportedParams = probe.getSupportedSSLParameters();
        Set<String> supported = new HashSet<>(Arrays.asList(supportedParams.getProtocols()));

        // Prefer newer if caller gave an unordered list.
        List<String> preference = List.of("TLSv1.3", "TLSv1.2", "TLSv1.1", "TLSv1", "TLS");
        List<String> req = new ArrayList<>(Arrays.asList(requested));

        // If the user's array is already in preference order, you can skip sorting.
        // This version sorts by our preference list but keeps unknowns at the end.
        req.sort(Comparator.comparingInt(p -> {
            int idx = preference.indexOf(p);
            return idx >= 0 ? idx : Integer.MAX_VALUE;
        }));

        for (String p : req) {
            if (p == null) {
                continue;
            }
            String proto = p.trim();
            if (supported.contains(proto)) {
                return proto;
            }
        }

        return requested[0];
    }
    /**
     * Builds an SSLContext from PEM certificate files using native JDK APIs.
     *
     * <p>This method provides FIPS-compliant SSL context creation without requiring
     * external libraries like BouncyCastle. It supports:</p>
     * <ul>
     *   <li>CA certificates for server verification (from {@link #caFile})</li>
     *   <li>Client certificate chains for mutual TLS (from {@link #clientCertFile})</li>
     *   <li>Private keys in PKCS#8 format, encrypted or unencrypted (from {@link #clientKeyFile})</li>
     * </ul>
     *
     * <p><strong>Note:</strong> Private keys must be in PKCS#8 format. PKCS#1 format
     * keys (e.g., {@code -----BEGIN RSA PRIVATE KEY-----}) are not supported.
     * See class-level documentation for conversion instructions.</p>
     *
     * @return the configured SSLContext
     * @throws AerospikeException if certificate files cannot be read or parsed
     */
    public SSLContext buildSslContextFromPem() {

        InputStream certChainPem = null;
        InputStream privateKeyPem = null;
        InputStream caPem = null;
        try {
            certChainPem = streamFromFile(this.clientCertFile, "certChain");
            privateKeyPem = streamFromFile(this.clientKeyFile, "clientKeyFile");
            caPem = streamFromFile(this.caFile, "caCertChain");

            TrustManager[] trustManagers = null;
            if (caPem != null) {
                KeyStore trustStore = newInMemoryKeyStore();
                int i = 1;
                for (X509Certificate cert : readCertificatesFromPem(caPem)) {
                    trustStore.setCertificateEntry("ca-" + (i++), cert);
                }
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(trustStore);
                trustManagers = tmf.getTrustManagers();
            }

            KeyManager[] keyManagers = null;
            if (certChainPem != null || privateKeyPem != null) {
                if (certChainPem == null || privateKeyPem == null) {
                    throw new KeyStoreException("Both cert chain PEM and private key PEM must be provided for mTLS identity.");
                }

                char[] kmPassword = (clientKeyPassword != null) ? clientKeyPassword.toCharArray() : new char[0];

                List<X509Certificate> chain = readCertificatesFromPem(certChainPem);
                PrivateKey privateKey = readPrivateKeyFromPem(privateKeyPem, kmPassword);

                KeyStore identityStore = newInMemoryKeyStore();
                // password required by KeyManagerFactory even if key isn't encrypted
                identityStore.setKeyEntry("key", privateKey, kmPassword, chain.toArray(new Certificate[0]));

                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(identityStore, kmPassword);
                keyManagers = kmf.getKeyManagers();
            }

            String tlsProtocol = chooseTlsProtocol(this.protocols);
            SSLContext ctx = SSLContext.getInstance(tlsProtocol != null ? tlsProtocol : "TLS");
            ctx.init(keyManagers, trustManagers, null);
            return ctx;
        }
        catch (GeneralSecurityException|IOException ex) {
            throw new AerospikeException(ex);
        }
        finally {
            close(certChainPem);
            close(privateKeyPem);
            close(caPem);

        }
    }

    /**
     * Creates a new empty in-memory KeyStore using PKCS12 format.
     *
     * <p>PKCS12 is preferred over JKS as it's more widely supported in FIPS environments.</p>
     *
     * @return an empty, initialized KeyStore
     * @throws GeneralSecurityException if the KeyStore cannot be created
     * @throws IOException if the KeyStore cannot be initialized
     */
    private static KeyStore newInMemoryKeyStore() throws GeneralSecurityException, IOException {
        // "PKCS12" is generally the safest default in FIPS contexts vs "JKS"
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        return ks;
    }

    /**
     * Reads X.509 certificates from a PEM-formatted input stream.
     *
     * <p>Parses PEM blocks with {@code -----BEGIN CERTIFICATE-----} headers
     * and returns all certificates found in the stream.</p>
     *
     * @param pem input stream containing PEM-formatted certificates
     * @return list of X.509 certificates parsed from the PEM data
     * @throws IOException if the stream cannot be read
     * @throws CertificateException if no valid certificates are found or parsing fails
     */
    private static List<X509Certificate> readCertificatesFromPem(InputStream pem) throws IOException, CertificateException {
        byte[] bytes = pem.readAllBytes();
        String text = new String(bytes, StandardCharsets.US_ASCII);

        List<X509Certificate> certs = new ArrayList<>();
        CertificateFactory cf = CertificateFactory.getInstance("X.509");

        Matcher m = PEM_BLOCK.matcher(text);
        while (m.find()) {
            String type = m.group(1);
            if (!"CERTIFICATE".equals(type)) {
                continue;
            }

            byte[] der = Base64.getMimeDecoder().decode(m.group(2));
            try (InputStream in = new ByteArrayInputStream(der)) {
                certs.add((X509Certificate) cf.generateCertificate(in));
            }
        }

        if (certs.isEmpty()) {
            throw new CertificateException("No CERTIFICATE blocks found in PEM input.");
        }
        return certs;
    }

    /**
     * Reads a private key from a PEM-formatted input stream.
     *
     * <p>Supports PKCS#8 format keys:</p>
     * <ul>
     *   <li>{@code -----BEGIN PRIVATE KEY-----} (unencrypted PKCS#8)</li>
     *   <li>{@code -----BEGIN ENCRYPTED PRIVATE KEY-----} (encrypted PKCS#8)</li>
     * </ul>
     *
     * <p><strong>Note:</strong> PKCS#1 format keys ({@code -----BEGIN RSA PRIVATE KEY-----})
     * are not supported. Convert them to PKCS#8 using OpenSSL.</p>
     *
     * @param pem input stream containing PEM-formatted private key
     * @param password password for decrypting encrypted keys, or empty array for unencrypted keys
     * @return the parsed PrivateKey
     * @throws IOException if the stream cannot be read
     * @throws GeneralSecurityException if no valid private key is found or decryption fails
     */
    private static PrivateKey readPrivateKeyFromPem(InputStream pem, char[] password)
            throws IOException, GeneralSecurityException {

        String text = new String(pem.readAllBytes(), StandardCharsets.US_ASCII);
        Matcher m = PEM_BLOCK.matcher(text);

        while (m.find()) {
            String type = m.group(1);
            byte[] der = Base64.getMimeDecoder().decode(m.group(2));

            if ("PRIVATE KEY".equals(type)) {
                return decodePkcs8PrivateKey(der);
            }
            if ("ENCRYPTED PRIVATE KEY".equals(type)) {
                if (password == null) {
                    throw new KeyStoreException("Encrypted private key provided but no password supplied.");
                }
                byte[] decrypted = decryptEncryptedPkcs8(der, password);
                return decodePkcs8PrivateKey(decrypted);
            }
        }

        throw new KeyStoreException("No PRIVATE KEY / ENCRYPTED PRIVATE KEY blocks found in PEM input.");
    }

    /**
     * Decrypts an encrypted PKCS#8 private key.
     *
     * <p>Uses the password-based encryption (PBE) algorithm specified in the
     * encrypted key's metadata to decrypt and return the raw PKCS#8 key bytes.</p>
     *
     * @param encryptedPkcs8Der DER-encoded encrypted PKCS#8 private key
     * @param password password for decryption
     * @return decrypted PKCS#8 private key bytes
     * @throws GeneralSecurityException if decryption fails (wrong password, unsupported algorithm, etc.)
     * @throws IOException if the encrypted key structure cannot be parsed
     */
    private static byte[] decryptEncryptedPkcs8(byte[] encryptedPkcs8Der, char[] password)
            throws GeneralSecurityException, IOException {

        EncryptedPrivateKeyInfo epki = new EncryptedPrivateKeyInfo(encryptedPkcs8Der);

        Cipher cipher = Cipher.getInstance(epki.getAlgName()); // provider must support this (FIPS mode may restrict)
        PBEKeySpec pbeKeySpec = new PBEKeySpec(password);
        SecretKeyFactory skf = SecretKeyFactory.getInstance(epki.getAlgName());
        SecretKey pbeKey = skf.generateSecret(pbeKeySpec);

        cipher.init(Cipher.DECRYPT_MODE, pbeKey, epki.getAlgParameters());
        PKCS8EncodedKeySpec keySpec = epki.getKeySpec(cipher);
        return keySpec.getEncoded();
    }

    /**
     * Decodes a PKCS#8-encoded private key into a PrivateKey object.
     *
     * <p>Attempts to decode the key using common algorithm types (RSA, EC, DSA)
     * until one succeeds. This allows the method to work with different key
     * types without requiring the caller to specify the algorithm.</p>
     *
     * @param pkcs8Der DER-encoded PKCS#8 private key bytes
     * @return the decoded PrivateKey
     * @throws GeneralSecurityException if the key cannot be decoded with any supported algorithm
     */
    private static PrivateKey decodePkcs8PrivateKey(byte[] pkcs8Der) throws GeneralSecurityException {
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(pkcs8Der);

        // Try common key types; expand if you need more.
        for (String alg : List.of("RSA", "EC", "DSA")) {
            try {
                return KeyFactory.getInstance(alg).generatePrivate(spec);
            } catch (InvalidKeySpecException ignored) {
                // try next
            }
        }
        throw new InvalidKeySpecException("Unsupported private key algorithm or provider does not allow it.");
    }

    /**
     * Opens a file as an InputStream with debug logging.
     *
     * @param filename the path to the file, or null
     * @param logPrefix prefix for debug log messages (e.g., "certChain", "clientKeyFile")
     * @return an InputStream for the file, or null if filename is null
     * @throws AerospikeException if the file does not exist
     */
    private InputStream streamFromFile(String filename, String logPrefix) {
        if (filename != null) {
            try {
                if (Log.debugEnabled()) {
                    Log.debug("Using %s file: %s".formatted(logPrefix, filename));
                }
                return new FileInputStream(new File(filename));
            } catch (FileNotFoundException e) {
                throw new AerospikeException(String.format("%s file '%s' not found", logPrefix, filename));
            }
        }
        return null;
    }

    /**
     * Builds an SSLContext from PEM certificate files. This uses an external library (ssl-context-kickstarter-for-pem) which might
     * not be useful in a FIPS environment as it uses non-FIPS BouncyCastle
     */
    /*
     * This method requires the following (Non-FIPS compliant) import:
     *      <dependency>
     *        <groupId>io.github.hakky54</groupId>
     *        <artifactId>sslcontext-kickstart-for-pem</artifactId>
     *        <version>8.1.1</version>
     *    </dependency>
     *
    private SSLContext buildFromPem() {
        InputStream certFile = null;
        InputStream keyFile = null;

        InputStream caFileStream = null;
        try {
            certFile = streamFromFile(this.clientCertFile, "certChain");
            keyFile = streamFromFile(this.clientKeyFile, "clientKeyFile");
            caFileStream = streamFromFile(this.caFile, "caCertChain");

            SSLFactory sslFactory;
            X509ExtendedTrustManager trustManager = PemUtils.loadTrustMaterial(caFileStream);
            if (certFile != null || keyFile != null) {
                X509ExtendedKeyManager keyManager = PemUtils.loadIdentityMaterial(
                        certFile, keyFile,
                        clientKeyPassword == null ? null : clientKeyPassword.toCharArray());
                sslFactory = SSLFactory.builder()
                        .withIdentityMaterial(keyManager)
                        .withTrustMaterial(trustManager)
                        .build();
            }
            else {
                sslFactory = SSLFactory.builder()
                        .withTrustMaterial(trustManager)
                        .build();
            }

            return sslFactory.getSslContext();
        }
        finally {
            close(certFile);
            close(keyFile);
            close(caFileStream);
        }
    }
    */

    /**
     * Safely closes an InputStream, ignoring any IOException.
     *
     * @param is the InputStream to close, may be null
     */
    private void close(InputStream is) {
        if (is != null) {
            try {
                is.close();
            }
            catch (IOException ioe) {}
        }
    }

}
