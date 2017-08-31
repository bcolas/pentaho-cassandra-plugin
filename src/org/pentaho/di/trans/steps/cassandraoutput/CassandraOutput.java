/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.cassandraoutput;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import org.pentaho.cassandra.CassandraConnection;
import org.pentaho.cassandra.ConnectionCompression;
import org.pentaho.cassandra.Utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;


public class CassandraOutput extends BaseStep implements StepInterface {
    private static final int CONNECTION_RETRIES = 3;
    private static final long CONNECTION_RETRY_TIMEOUT = 10000L;

    private CassandraConnection connection;
    private CassandraOutputMeta meta;
    private CassandraOutputData data;
    private HashMap<String, Integer> fieldMapping;

    private HashMap<String, Boolean> fieldTypeIsTextMapping;
    private HashMap<String, Boolean> fieldTypeIsTimestampMapping;
    private boolean syncModeEnabled = false;

    private List<ResultSetFuture> openFutures = new LinkedList<>();
    private int maxOpenQueue = 1000;

    private String nodes;
    private String port;
    private String username;
    private String password;
    private String keyspace;
    private String columnfamily;
    private boolean SslEnabled;
    private String truststoreFilePath;
    private String truststorepass;
    private ConnectionCompression compression;

    public CassandraOutput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        if (super.init(smi, sdi)) {
            this.meta = ((CassandraOutputMeta) smi);

            this.nodes = environmentSubstitute(this.meta.getCassandraNodes());
            this.port = environmentSubstitute(this.meta.getCassandraPort());
            this.username = environmentSubstitute(this.meta.getUsername());
            this.password = environmentSubstitute(this.meta.getPassword());
            this.keyspace = environmentSubstitute(this.meta.getKeyspace());
            this.columnfamily = environmentSubstitute(this.meta.getColumnfamily());
            this.SslEnabled = this.meta.getSslEnabled();
            this.truststoreFilePath = environmentSubstitute(this.meta.getTrustStoreFilePath());
            this.truststorepass = environmentSubstitute(this.meta.getTrustStorePass());
            this.compression = this.meta.getCompression();
            this.syncModeEnabled = this.meta.isSyncMode();
            this.maxOpenQueue = (this.syncModeEnabled ? 0 : this.meta.getBatchSize());

            try {
                if (Const.isEmpty(this.columnfamily)) {
                    throw new RuntimeException(BaseMessages.getString(CassandraOutputMeta.PKG,
                            "CassandraOutput.Error.NoColumnFamilySpecified", new String[0]));
                }

                if ((this.meta.isSpecifyFields()) && (this.meta.getCassandraFields().length != this.meta.getStreamFields().length)) {
                    throw new RuntimeException(BaseMessages.getString(CassandraOutputMeta.PKG,
                            "CassandraOutput.Error.InitializationColumnProblem", new String[0]));
                }

                this.connection = Utils.connect(this.nodes, this.port, this.username, this.password,
                        this.keyspace, this.SslEnabled, this.truststoreFilePath, this.truststorepass, this.compression);

                return true;
            } catch (Exception ex) {
                logError(BaseMessages.getString(CassandraOutputMeta.PKG,"CassandraOutput.Error.InitializationProblem"), ex);
            }
        }
        return false;
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        Object[] r = getRow();

        if ((r == null) || (isStopped())) {

            if ((this.connection != null) && (!this.syncModeEnabled)) {
                this.connection.getSession().closeAsync();
                processOpenFutures(0);
            }

            if (this.connection != null) {
                this.connection.release();
            }

            setOutputDone();
            return false;
        }

        if (this.first) {
            initialize(smi, sdi);
        }

        StringBuilder finalInsert = new StringBuilder("INSERT INTO "+this.keyspace+"."+this.columnfamily+" (");

        int y = 1;
        for (String columnRow : this.fieldMapping.keySet()) {
        	Object field = r[this.fieldMapping.get(columnRow)];
        	if (field != null) {
        		if (y>1)
            		finalInsert.append(',');
        		finalInsert.append(columnRow);
        		y++;
        	}
        }

        finalInsert.append(") VALUES (");

        int i = 1;
        for (String columnRow : this.fieldMapping.keySet()) {
            Object field = r[this.fieldMapping.get(columnRow)];
            if (field != null) {
            	if (i>1)
                	finalInsert.append(',');
            	if (this.fieldTypeIsTextMapping.get(columnRow)) {
                    finalInsert.append('\'');
                    if ((field instanceof String)) {
                        finalInsert.append(((String) field).replaceAll("'", "''"));
                    } else {
                        finalInsert.append(field);
                    }
                    finalInsert.append('\'');
                } else if (this.fieldTypeIsTimestampMapping.get(columnRow)) {
                	finalInsert.append('\'');
                    if ((field instanceof String)) {
                        finalInsert.append(((String) field).replaceAll("'", "''"));
                    } else {
                        finalInsert.append(field);
                    }
                    finalInsert.append('\'');
                } else {
                    finalInsert.append(field);
                }
                i++;
            }
        }

        int m_ttl = this.meta.getTtl();
        String cql = finalInsert.append(')').append((this.meta.getTtl() > 0 ? " USING ttl " + m_ttl : "")).append(';').toString();

        ResultSetFuture lastFuture;
        try {
            logRowlevel(cql);
            if (this.syncModeEnabled) {
                this.connection.getSession().execute(cql);
                incrementLinesOutput();
            } else {
                lastFuture = this.connection.getSession().executeAsync(cql);
                addFuture(lastFuture);
            }
            return true;
        } catch (InvalidQueryException e) {
            throw new KettleStepException("The data type of a field does not match the data type in cassandra: SQL: " + cql + " :" + e.getMessage());
        } catch (NoHostAvailableException e) {
            int retryCount = 0;

            while ((!isStopped()) && (retryCount++ < CONNECTION_RETRIES)) {
                logError("No Host Available Exception when doing insert, trying again in 10 seconds: " + e.getMessage());

                try {
                    Thread.sleep(CONNECTION_RETRY_TIMEOUT);
                    this.connection = Utils.connect(this.nodes, this.port, this.username, this.password,
                            this.keyspace, this.SslEnabled, this.truststoreFilePath, this.truststorepass, this.compression);

                    if (this.syncModeEnabled) {
                        this.connection.getSession().execute(cql);
                    } else {
                        lastFuture = this.connection.getSession().executeAsync(cql);
                    }

                    return true;
                } catch (InterruptedException e1) {
                    logError("Interrupted sleep while retrying query after No Host Available Exception: " + e1.getMessage());
                } catch (NoHostAvailableException e1) {
                    logError("No Host Available Exception while inserting, even after " + CONNECTION_RETRY_TIMEOUT / 1000 + " seconds delayed retry: " + e1.getMessage());
                } catch (InvalidQueryException e1) {
                    throw new KettleStepException(
                            "The data type of a field does not match the data type in cassandra: SQL: " + cql + " :" + e1.getMessage());
                } catch (Exception e1) {
                    throw new KettleException("Unknown error occured for CQL:\n" + cql, e1);
                }
            }

            if (retryCount == CONNECTION_RETRIES) {
                throw new KettleException("No hosts available after " + retryCount + " retries, aborting");
            }
        } catch (Exception e) {
            throw new KettleException("Unknown error occured for CQL:\n" + cql, e);
        }

        return false;
    }

    @Override
    public void setStopped(boolean stopped) {
        if ((isStopped()) && (stopped)) {
            return;
        }
        super.setStopped(stopped);
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        if (this.connection != null)
            this.connection.release();
        super.dispose(smi, sdi);
    }

    protected void initialize(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        this.first = false;

        this.meta = ((CassandraOutputMeta) smi);
        this.data = ((CassandraOutputData) sdi);

        if ((data == null) || (this.meta == null)) {
            return;
        }

        data.setOutputRowMeta(getInputRowMeta().clone());
        
        Map<String, String> inputFields = new HashMap<>();
        if (this.meta.isSpecifyFields()) {
            for (int i = 0; i < this.meta.getCassandraFields().length; i++) {
                inputFields.put(this.meta.getCassandraFields()[i], this.meta.getStreamFields()[i]);
            }
        } else {
            String[] arrayOfString;
            int j = (arrayOfString = data.getOutputRowMeta().getFieldNames()).length;
            for (int i = 0; i < j; i++) {
                String s = arrayOfString[i];
                inputFields.put(s, s);
            }
        }

        //int m_ttl = this.meta.getTtl();
        try {
            Cluster cluster = this.connection.getSession().getCluster();

            try {
                this.fieldMapping = new LinkedHashMap<>();
                this.fieldTypeIsTextMapping = new HashMap<>();
                this.fieldTypeIsTimestampMapping = new HashMap<>();

                List<ColumnMetadata> columns = cluster.getMetadata().getKeyspace(this.keyspace).getTable(this.columnfamily).getColumns();
                Object iter = columns.iterator();

                while (((Iterator) iter).hasNext()) {
                    ColumnMetadata columnRow = (ColumnMetadata) ((Iterator) iter).next();

                    this.fieldTypeIsTextMapping.put(columnRow.getName(), "TEXT".equals(columnRow.getType().getName().name()));
                    this.fieldTypeIsTimestampMapping.put(columnRow.getName(), "TIMESTAMP".equals(columnRow.getType().getName().name()));

                    if (inputFields.containsKey(columnRow.getName())) {
                        this.fieldMapping.put(columnRow.getName(), data.getOutputRowMeta().indexOfValue(inputFields.get(columnRow.getName())));

                        if (!this.fieldTypeIsTextMapping.get(columnRow.getName())) {
                            int index = data.getOutputRowMeta().indexOfValue(inputFields.get(columnRow.getName()));

                            if (("INT".equals(columnRow.getType().getName().name())) && (data.getOutputRowMeta().getValueMeta(index).getType() != 5)) {
                                throw new KettleStepException("Column " + columnRow.getName() +
                                        " type ( INT ) in Cassandra does not match field type ( " +
                                        data.getOutputRowMeta().getValueMeta(index).getType() + " ) in Pentaho ");
                            }

                            if (("BIGINT".equals(columnRow.getType().getName().name())) && (data.getOutputRowMeta().getValueMeta(index).getType() != 6) &&
                                    (data.getOutputRowMeta().getValueMeta(index).getType() != 5)) {
                                throw new KettleStepException("Column " + columnRow.getName() +
                                        " type ( BIGINT ) in Cassandra does not match field type ( " +
                                        data.getOutputRowMeta().getValueMeta(index).getType() + " ) in Pentaho ");
                            }
                        }
                    }
                }

                try {
                    List<ColumnMetadata> keyColumn = cluster.getMetadata().getKeyspace(this.keyspace).getTable(this.columnfamily).getPrimaryKey();
                    Object keyIter = keyColumn.iterator();

                    while (((Iterator) keyIter).hasNext()) {
                        ColumnMetadata column = (ColumnMetadata) ((Iterator) keyIter).next();

                        if (!inputFields.containsKey(column.getName())) {
                            throw new KettleStepException(BaseMessages.getString(CassandraOutputMeta.PKG,
                                    "CassandraOutput.Error.CantFindKeyField", new String[]{column.getName()}));
                        }
                    }
                } catch (Exception e) {
                    logError(BaseMessages.getString(CassandraOutputMeta.PKG,
                            "CassandraOutput.Error.InitializationColumnProblem"), e);
                    throw e;
                }

            } catch (Exception e) {
                logError(BaseMessages.getString(CassandraOutputMeta.PKG,"CassandraOutput.Error.InitializationColumnProblem"), e);
                throw e;
            }
        } catch (Exception ex) {
            logError(BaseMessages.getString(CassandraOutputMeta.PKG,"CassandraOutput.Error.InitializationProblem"), ex);
        }
    }

    private void addFuture(ResultSetFuture future) {
        this.openFutures.add(future);
        processOpenFutures(this.maxOpenQueue);
    }

    private void processOpenFutures(int maxOpen) {
        while ((!isStopped()) && (hasTooManyOpenFutures(maxOpen))) {
            logDebug("waiting for max " + maxOpen + " open futures");
            try {
                Thread.sleep(128L);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private boolean hasTooManyOpenFutures(int maxOpen) {
        if (this.openFutures.size() > maxOpen) {
            Iterator<ResultSetFuture> iter = this.openFutures.iterator();

            while (iter.hasNext()) {
                ResultSetFuture f = iter.next();
                if (f.isCancelled()) {
                    incrementLinesRejected();
                    iter.remove();
                } else if (f.isDone()) {
                    incrementLinesOutput();
                    iter.remove();
                }
            }
            return this.openFutures.size() > maxOpen;
        }
        return false;
    }
}