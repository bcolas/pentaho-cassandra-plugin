package org.pentaho.cassandra;

import com.datastax.driver.core.DataType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;

import org.pentaho.di.core.row.ValueMetaInterface;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Utils {
	private static final LoadingCache<Map<String, String>, CassandraConnection> cache = CacheBuilder.newBuilder()
            .build(new CacheLoader<Map<String, String>, CassandraConnection>() {
                @Override
                public CassandraConnection load(Map<String, String> config) throws Exception {
                    return new CassandraConnection(config, Utils.cache);
                }
            });

    public static CassandraConnection connect(String nodes, String port, String username, String password, String keyspace, boolean withSSL, String truststoreFilePath, String truststorePass, ConnectionCompression compression)
            throws CassandraConnectionException {
        Map<String, String> config = buildConfig(nodes, port, username, password, keyspace, withSSL, truststoreFilePath, truststorePass, compression);
        try {
            CassandraConnection connection;
            do {
                connection = cache.get(config);
            }
            while (!connection.acquire());
            return connection;
        } catch (ExecutionException e) {
            throw new CassandraConnectionException("Failed to get a connection", e);
        }
    }

    private static Map<String, String> buildConfig(String nodes, String port, String username, String password, String keyspace, boolean withSSL, String truststoreFilePath, String truststorePass, ConnectionCompression compression) {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
                .put("nodes", nodes)
                .put("port", port)
                .put("SslEnabled", Boolean.toString(withSSL));
        if (username != null) builder.put("username", username);
        if (password != null) builder.put("password", password);
        if (keyspace != null) builder.put("keyspace", keyspace);
        if (truststoreFilePath != null) builder.put("truststoreFilePath", truststoreFilePath);
        if (truststorePass != null) builder.put("truststorePass", truststorePass);
        if (compression != null) builder.put("compression", compression.toString());
        return builder.build();
    }

    public static int convertDataType(DataType type) {
        switch (type.getName()) {
        	case SMALLINT: 
            case INT:
            case BIGINT:
            case COUNTER:
            case TIME:
            	return ValueMetaInterface.TYPE_INTEGER; // 5 > java.lang.Long
            case ASCII:
            case TEXT:
            case VARCHAR:
            case UUID:
            case TIMEUUID:            	
                return ValueMetaInterface.TYPE_STRING; // 2 > java.lang.String
            case INET:
            	return ValueMetaInterface.TYPE_INET; // 10 > 
            case BOOLEAN:
                return ValueMetaInterface.TYPE_BOOLEAN; // 4 > java.lang.Boolean
            case DECIMAL:
            case FLOAT:
            case DOUBLE:
                return ValueMetaInterface.TYPE_NUMBER; // 1 > java.lang.Double
            case VARINT:
                return ValueMetaInterface.TYPE_BIGNUMBER; // 6 > java.math.BigDecimal
            case TIMESTAMP:
                return ValueMetaInterface.TYPE_DATE; // 3 > java.util.Date
            case BLOB:
            	return ValueMetaInterface.TYPE_BINARY; // 8 > java.lang.byte[]
            case LIST:
            case MAP:
            case SET:
            	return ValueMetaInterface.TYPE_SERIALIZABLE; // 0
            default:
            	return ValueMetaInterface.TYPE_STRING;
        }
    }
}
