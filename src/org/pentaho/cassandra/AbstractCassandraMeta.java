package org.pentaho.cassandra;

import org.pentaho.cassandra.ConnectionCompression;

import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public abstract class AbstractCassandraMeta  extends BaseStepMeta implements StepMetaInterface {
	protected static final String CASSANDRA_NODES = "cassandra_nodes";
    protected static final String CASSANDRA_PORT = "cassandra_port";
    protected static final String USERNAME = "username";
    protected static final String PASSWORD = "password";
    protected static final String CASSANDRA_KEYSPACE = "cassandra_keyspace";
    protected static final String CASSANDRA_WITH_SSL = "cassandra_withSSL";
    protected static final String CASSANDRA_TRUSTSTORE_FILE_PATH = "cassandra_trustStoreFilePath";
    protected static final String CASSANDRA_TRUSTSTORE_PASS = "cassandra_trustStorePass";
    protected static final String COMPRESSION = "compression";
    protected static final String CASSANDRA_COLUMN_FAMILY = "cassandra_columnFamily";

    protected static final String FOUR_SPACES_FOR_XML = "    ";

    protected String cassandraNodes;
    protected String cassandraPort;
    protected String username;
    protected String password;
    protected String keyspace;
    protected String columnfamily;
    protected Boolean SslEnabled;
    protected String trustStoreFilePath;
    protected String trustStorePass;
    protected ConnectionCompression compression;

    @Override
    public void setDefault() {
        this.cassandraNodes = "localhost";
        this.cassandraPort = "9042";
        this.username = "";
        this.password = "";
        this.keyspace = "";
        this.columnfamily = "";
        this.SslEnabled = Boolean.FALSE;
        this.trustStoreFilePath = "";
        this.trustStorePass = "";
        this.compression = ConnectionCompression.SNAPPY;
    }

    protected void checkNulls() {
        if (this.cassandraNodes == null) {
            this.cassandraNodes = "";
        }
        if (this.cassandraPort == null) {
            this.cassandraPort = "";
        }
        if (this.username == null) {
            this.username = "";
        }
        if (this.password == null) {
            this.password = "";
        }
        if (this.keyspace == null) {
            this.keyspace = "";
        }
        if (this.columnfamily == null) {
            this.columnfamily = "";
        }
        if (this.SslEnabled == null) {
            this.SslEnabled = Boolean.FALSE;
        }
        if (this.trustStoreFilePath == null) {
            this.trustStoreFilePath = "";
        }
        if (this.trustStorePass == null) {
            this.trustStorePass = "";
        }
    }
    
    public String getCassandraNodes() {
    	return this.cassandraNodes;
    }
    
    public void setCassandraNodes(String cassandraNodes) {
    	this.cassandraNodes = cassandraNodes;
    }
    
    public String getCassandraPort() {
    	return this.cassandraPort;
    }
    
    public void setCassandraPort(String cassandraPort) {
    	this.cassandraPort = cassandraPort;
    }
    
    public String getUsername() {
    	return this.username;
    }
    
    public void setUsername(String username) {
    	this.username = username;
    }
    
    public String getPassword() {
    	return this.password;
    }
    
    public void setPassword(String password) {
    	this.password = password;
    }
    
    public String getKeyspace() {
    	return this.keyspace;
    }
    
    public void setKeyspace(String keyspace) {
    	this.keyspace = keyspace;
    }
    
    public Boolean getSslEnabled() {
    	return this.SslEnabled;
    }
    
    public void setSslEnabled(Boolean SslEnabled) {
    	this.SslEnabled = SslEnabled;
    }
    
    public String getTrustStoreFilePath() {
    	return this.trustStoreFilePath;
    }
    
    public void setTrustStoreFilePath(String trustStoreFilePath) {
    	this.trustStoreFilePath = trustStoreFilePath;
    }
    
    public String getTrustStorePass() {
    	return this.trustStorePass;
    }
    
    public void setTrustStorePass(String trustStorePass) {
    	this.trustStorePass = trustStorePass;
    }
    
    public ConnectionCompression getCompression() {
    	return this.compression;
    }
    
    public void setCompression(ConnectionCompression compression) {
    	this.compression = compression;
    }
}
