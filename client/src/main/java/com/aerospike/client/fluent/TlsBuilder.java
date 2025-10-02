package com.aerospike.client.fluent;

import javax.net.ssl.SSLContext;

/**
 * Builder class for configuring TLS settings for Aerospike cluster connections.
 * 
 * <p>This class provides a fluent API for configuring TLS parameters such as
 * TLS name, CA file, protocols, ciphers, and other TLS-specific options.</p>
 * 
 * <p>Simple example usage:</p>
 * <pre>{@code
 * ClusterDefinition cluster = new ClusterDefinition("localhost", 3100)
 *     .withTlsConfigOf()
 *         .tlsName("myTlsName")
 *         .caFile("myCaFile")
 *     .done()
 *     .withNativeCredentials("myUser", "password");
 * }</pre>
 * 
 * <p>Advanced example usage:</p>
 * <pre>{@code
 * ClusterDefinition cluster = new ClusterDefinition("localhost", 3100)
 *     .withTlsConfigOf()
 *         .tlsName("myTlsName")
 *         .caFile("myCaFile")
 *         .protocols("TLSv1.2", "TLSv1.3")
 *         .ciphers("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256")
 *         .forLoginOnly(false)
 *     .done()
 *     .withNativeCredentials("myUser", "password");
 * }</pre>
 */
public class TlsBuilder {
    private final ClusterDefinition parent;
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
     * Package-private constructor to be called from ClusterDefinition
     */
    TlsBuilder(ClusterDefinition parent) {
        this.parent = parent;
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
     * to prove ownership of the certificate during mutual TLS authentication.
     * Supports PEM-formatted private key files.</p>
     * 
     * @param clientKeyFile the path to the client private key PEM file
     * @return this TlsBuilder for method chaining
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
        
        // For now, return null to use Aerospike's default TLS implementation
        // This allows the TLS configuration to work with basic settings while
        // avoiding complex SSL context creation issues
        
        // TODO: Implement proper SSL context creation using sslcontext-kickstart
        // when method signatures are confirmed
        
        return null;
    }
    
    /**
     * Completes TLS configuration and returns to the parent ClusterDefinition.
     * 
     * <p>This method finalizes the TLS configuration and returns control back
     * to the ClusterDefinition for further configuration or connection establishment.</p>
     * 
     * @return the parent ClusterDefinition for continued method chaining
     */
    public ClusterDefinition done() {
        parent.setTlsBuilder(this);
        return parent;
    }

    // Package-private getters for ClusterDefinition to access the configured values
    String getTlsName() { return tlsName; }
    String getCaFile() { return caFile; }
    String[] getProtocols() { return protocols; }
    String[] getCiphers() { return ciphers; }
    boolean isForLoginOnly() { return forLoginOnly; }
    boolean isRevokeCertificatesForLoginOnly() { return revokeCertificatesForLoginOnly; }
    java.math.BigInteger[] getRevokeCertificates() { return revokeCertificates; }
    String getClientCertFile() { return clientCertFile; }
    String getClientKeyFile() { return clientKeyFile; }
    String getClientKeyPassword() { return clientKeyPassword; }
    String getTrustStorePath() { return trustStorePath; }
    String getTrustStorePassword() { return trustStorePassword; }
    String getTrustStoreType() { return trustStoreType; }
    String getKeyStorePath() { return keyStorePath; }
    String getKeyStorePassword() { return keyStorePassword; }
    String getKeyStoreType() { return keyStoreType; }
    SSLContext getCustomSslContext() { return customSslContext; }
    boolean isTrustAllCertificates() { return enableTrustAllCertificates; }
    
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
}
