package org.pentaho.cassandra;

public class CassandraConnectionException extends Exception {
	private static final long serialVersionUID = -8251681391139907927L;

    public CassandraConnectionException() {}

    public CassandraConnectionException(String message) {
        super(message);
    }

    public CassandraConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
