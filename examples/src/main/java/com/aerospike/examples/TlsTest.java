package com.aerospike.examples;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.Log.Level;

public class TlsTest {
    public static void main(String[] args) {
        String certHome = System.getenv("CERT_HOME");
        if (certHome == null) {
            certHome = "";
        } else if (!certHome.endsWith("/")) {
            certHome = certHome + "/";
        }
        ClusterDefinition definition = new ClusterDefinition("localhost", 3101)
                .withTlsConfigOf()
                    .caFile(certHome + "cacert.pem")
                    .clientCertFile(certHome + "cert.pem")
                    .clientKeyFile(certHome + "key.pem")
                    .tlsName("tls1")
                .done()
                .withLogLevel(Level.DEBUG)
                .withNativeCredentials("admin", "admin");
        
        Cluster cluster = definition.connect();
        System.out.println("connected!");
        cluster.close();
    }
}
