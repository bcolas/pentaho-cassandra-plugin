package org.pentaho.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.cache.LoadingCache;

import java.io.*;
import java.net.InetSocketAddress;
import java.security.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.security.cert.CertificateException;

import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;

public class CassandraConnection {
	private static final Logger logger = LoggerFactory.getLogger(CassandraConnection.class);

    private final Map<String, String> config;

    private final LoadingCache<Map<String, String>, CassandraConnection> owningCache;

    private final AtomicInteger references = new AtomicInteger();
    private final Session session;

    CassandraConnection(Map<String, String> config, LoadingCache<Map<String, String>, CassandraConnection> owningCache) throws CassandraConnectionException {
        this.config = config;
        this.owningCache = owningCache;
        this.session = createSession(this.config);
    }

    public Session getSession() {
        return this.session;
    }

    public void release() {
        int ref;
        int newRef;
        do {
            ref = this.references.get();
            newRef = ref == 1 ? -1 : ref - 1;
        } while (!this.references.compareAndSet(ref, newRef));

        if (newRef == -1) {
            logger.debug("Released last reference to {}, closing Session", this.config);
            dispose();
        } else {
            logger.debug("Released reference to {}, new count = {}", this.config, newRef);
        }
    }

    boolean acquire() {
        int ref;

        do {
            ref = this.references.get();
            if (ref < 0) {
                logger.debug("Failed to acquire reference to {}", this.config);
                return false;
            }
        } while (!this.references.compareAndSet(ref, ref + 1));

        logger.debug("Acquired reference to {}, new count = {}", this.config, ref + 1);
        return true;
    }

    private void dispose() {
        this.session.getCluster().close();
        this.owningCache.invalidate(this.config);
    }

    private Session createSession(Map<String, String> config) throws CassandraConnectionException {
        String nodes = config.get("nodes");
        String port_s = config.get("port");
        String username = config.get("username");
        String password = config.get("password");
        String keyspace = config.get("keyspace");
        boolean withSSL = Boolean.valueOf(config.get("SslEnabled"));
        String truststoreFilePath = config.get("truststoreFilePath");
        String truststorePass = config.get("truststorePass");
        ConnectionCompression compression = ConnectionCompression.fromString(config.get("compression"));

        Collection<InetSocketAddress> addresses = new ArrayList<>();
        int port;

        if ((nodes != null) && (nodes.trim().length() > 0)) {
            String[] arrayOfString;
            int j = (arrayOfString = nodes.split("\\|")).length;

            for (int i = 0; i < j; i++) {
                String parts = arrayOfString[i];
                String host = parts.trim();
                port = Integer.parseInt(port_s);
                addresses.add(new InetSocketAddress(host, port));
            }
        } else {
            throw new CassandraConnectionException("No host(s) for cassandra provided");
        }

        Cluster.Builder builder = Cluster.builder().addContactPointsWithPorts(addresses);
        //Cluster.Builder builder = Cluster.builder().addContactPoint(nodes).withPort(Integer.parseInt(port_s));
        
        builder.withProtocolVersion(ProtocolVersion.V3);
        

        if ((username != null) && (password != null) && (username.trim().length() + password.trim().length() > 0)) {
            builder = builder.withCredentials(username, password);
        }

        if ((withSSL) && ((truststoreFilePath == null) || (truststoreFilePath.isEmpty()))) {
            builder = builder.withSSL();
        } else if (withSSL) {
            builder = builder.withSSL(getSSLOptions(truststoreFilePath, truststorePass));
        }

        switch (compression) {
            case SNAPPY:
                builder.withCompression(ProtocolOptions.Compression.SNAPPY);
                break;
            case NONE:
                builder.withCompression(ProtocolOptions.Compression.NONE);
                break;
            default:
                logger.warn("Compression " + compression.toString() + " is not implemented, ignored");
        }

        builder.withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()));

        builder.withPoolingOptions(new PoolingOptions()
                .setCoreConnectionsPerHost(HostDistance.REMOTE, 1)
                .setCoreConnectionsPerHost(HostDistance.LOCAL, 1)
                .setMaxConnectionsPerHost(HostDistance.REMOTE, 1)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 1)
                .setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
                .setMaxRequestsPerConnection(HostDistance.REMOTE, 32768)
                .setNewConnectionThreshold(HostDistance.REMOTE, 128));

        try {
            Cluster cluster = builder.build();
            Session session = cluster.connect(keyspace);
            logger.info("Cluster initialised and Session connected for keyspace '{}'", keyspace);
            return session;
        } catch (UnauthorizedException e) {
            String msg = "Fatal error. Cassandra user '" + username + "' is not authorized for keyspace '" + keyspace + "' : " + e.getMessage();
            logger.error(msg, e);
            throw new CassandraConnectionException(msg, e);
        } catch (AuthenticationException e) {
            InetSocketAddress errorHost = e.getAddress();
            String msg = "Fatal error. Unable to authenticate Cassandra user '" + username + "' for keyspace '" + keyspace +
                    "' on Cassandra host '" + errorHost.toString() + "': " + e.getMessage();
            logger.error(msg, e);
            throw new CassandraConnectionException(msg, e);
        } catch (NoHostAvailableException e) {
            Map<InetSocketAddress, Throwable> errors = e.getErrors();
            StringBuilder errorMesages = new StringBuilder("Unable to establish a client connection to any Cassandra node: " + e.getMessage() + ". Tried: ");
            if ((errors != null) && (errors.size() > 0) && (errors.keySet() != null)) {
                for (InetSocketAddress address : errors.keySet()) {
                    Throwable throwable = errors.get(address);
                    String stackTrace = stackTraceToString(throwable);
                    errorMesages.append("\n\t").append(address.toString()).append(" : ").append(throwable.getMessage());
                    errorMesages.append("\n\t").append(stackTrace);
                }
            }
            logger.error(errorMesages.toString(), e);
            throw new CassandraConnectionException(errorMesages.toString(), e);
        } catch (DriverException e) {
            String msg = "Unexpected DriverException encountered: " + e.getMessage();
            logger.error(msg, e);
            throw new CassandraConnectionException(msg, e);
        }
    }

    private static String stackTraceToString(Throwable t) {
        StringWriter errors = new StringWriter();
        t.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }

    private static SSLOptions getSSLOptions(String truststoreFilePath, String truststorePass) throws CassandraConnectionException {
        File truststoreFile = new File(truststoreFilePath);
        if (truststoreFile.canRead() && truststoreFilePath.trim().length() > 0) {
            SSLContext context;
            try {
                context = CassandraConnection.getSSLContext(truststoreFile, truststorePass);
            }
            catch (Exception e) {
                throw new CassandraConnectionException("Can not read trust store file: " + truststoreFilePath + " ; " + e.getMessage());
            }

            return JdkSSLOptions.builder().withSSLContext(context).build();
        }
        return null;
    }

    private static SSLContext getSSLContext(File truststoreFile, String truststorePass) throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, UnrecoverableKeyException, KeyManagementException {
        SSLContext ctx;
        FileInputStream tsf = null;
        ctx = null;
        try {
            try {
                ctx = SSLContext.getInstance("SSL");
                tsf = new FileInputStream(truststoreFile);
                KeyStore ts = KeyStore.getInstance("JKS");
                ts.load(tsf, truststorePass.toCharArray());
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(ts);
                ctx.init(null, tmf.getTrustManagers(), new SecureRandom());
            }
            catch (Exception e) {
                logger.error("Problem encountered getting SSL Context " + e.getMessage(), e);
                if (tsf != null) {
                    tsf.close();
                }
            }
        }
        finally {
            if (tsf != null) {
                tsf.close();
            }
        }
        return ctx;
    }
}
