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

package org.pentaho.di.trans.steps.cassandrainput;

import java.math.BigDecimal;
import java.util.Date;

import org.apache.cassandra.db.marshal.AbstractType;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ColumnDefinitions;

import org.pentaho.cassandra.CassandraConnection;
import org.pentaho.cassandra.CassandraConnectionException;
import org.pentaho.cassandra.ConnectionCompression;
import org.pentaho.cassandra.Utils;
import org.pentaho.di.trans.steps.cassandrainput.CassandraInput;
import org.pentaho.di.trans.steps.cassandrainput.CassandraInputData;
import org.pentaho.di.trans.steps.cassandrainput.CassandraInputMeta;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public class CassandraInput extends BaseStep implements StepInterface {
    private static Class<?> PKG = CassandraInput.class;

    private CassandraInputMeta meta;
    private CassandraInputData data;
    private CassandraConnection connection;

    private String[] rowColumns;

    private Boolean executeForEachInputRow = Boolean.FALSE;
    private int rowLimit;

    private String cqlStatement;

    public CassandraInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        this.meta = ((CassandraInputMeta) smi);
        this.data = ((CassandraInputData) sdi);
        ResultSet rs = null;

        Object[] r = getRow();
        if ((this.executeForEachInputRow) && (r == null)) {
            setOutputDone();
            return false;
        }

        if (this.first) {
            this.first = false;

            this.data.outputRowMeta = new RowMeta();
            rs = this.connection.getSession().execute(this.cqlStatement);
            this.meta.createOutputRowMeta(this.data.outputRowMeta, rs);

            this.rowColumns = this.data.outputRowMeta.getFieldNames();
        }

        try {
            int rowCount = 0;
            do {
                Object[] outputRow = new Object[this.rowColumns.length];
                Row row = rs.one();

                if (rs.isExhausted()) {
                    logDetailed("ResultSet exhausted, no rows returned.");
                    break;
                }

                incrementLinesInput();
                rowCount++;

                String[] arrayOfString;
                int j = (arrayOfString = this.rowColumns).length;

                ColumnDefinitions cd = row.getColumnDefinitions();

                for (int i = 0; i < j; i++) {
                    String col = arrayOfString[i];
                    DataType type = cd.getType(col);
                    logDetailed("Getting "+col+" ("+type+"-"+type.getName()+")");
                    
                    Object o = null;
                    if (!row.isNull(col))
                    {
                    	switch (type.getName()) {
                			case SMALLINT: 
                				o = (long) row.getShort(col);
                				break;
                			case INT:
                				o = (long) row.getInt(col);
                				break;
                			case BIGINT:
                			case COUNTER:
                				o = row.getLong(col);
                				break;
                			case TIME:
                				o = row.getTime(col);
                				break;
                			case ASCII:
                			case TEXT:
                			case VARCHAR:
                				o = row.getString(col);
                				break;
                			case UUID:
                			case TIMEUUID:
                				o = row.getUUID(col).toString();
                				break;
                			case INET:
                				o = row.getInet(col).getHostName();
                				break;
                			case BOOLEAN:
                				o = (Boolean) row.getBool(col);
                				break;
                			case DECIMAL:
                				o = (Double) row.getDecimal(col).doubleValue();
                				break;
                			case FLOAT:
                				o = Double.valueOf(row.getFloat(col));
                				break;
                			case DOUBLE:
                				o = (Double) row.getDouble(col);
                				break;
                			case VARINT:
                				o = new BigDecimal(row.getVarint(col));
                				break;
                			case TIMESTAMP:
                				o = row.getTimestamp(col);
                				break;
                			case BLOB:
                				o = row.getBytes(col).array();
                				break;
                			case LIST:
                			case MAP:
                			case SET:
                				o = row.getObject(col);
                				break;
                			default:
                				logDetailed("Getting Custom type "+type.asFunctionParameterString());
                				try {
                					o = ((AbstractType)Class.forName(type.asFunctionParameterString()).getField("instance").get(null)).getString(row.getBytes(col));
                				}
                				catch (ClassNotFoundException cnfe) { 
                					logError( "Class Not Found Exception : " + cnfe.getMessage());
                					logError( "Stack trace: " + Const.CR + Const.getStackTracker( cnfe ) );
                				}
                				catch (NoSuchFieldException nsfe) { 
                					logError( "No Such Field Exception" + nsfe.getMessage()); 
                					logError( "Stack trace: " + Const.CR + Const.getStackTracker( nsfe ) );
                				}
                				catch (IllegalAccessException iae) { 
                					logError( "Illegal Access Exception " + iae.getMessage()); 
                					logError( "Stack trace: " + Const.CR + Const.getStackTracker( iae ) );
                				}
                				break;
                    	}
                    }

                    /*if ((o instanceof Integer)) {
                        o = (long) (Integer) o;
                    }

                    if (o instanceof Float) {
                        o = Double.valueOf((Float) o);
                    }*/

                    outputRow[i] = o;
                }

                putRow(this.data.outputRowMeta, outputRow);

                if ((isStopped()) || (rs.isExhausted())) break;

            } while ((this.rowLimit == 0) || (rowCount < this.rowLimit));
        } catch (KettleException e) {
            logError(BaseMessages.getString(PKG, "CassandraInputDialog.Error.StepCanNotContinueForErrors", e.getMessage()));
            logError(Const.getStackTracker(e));
            setErrors(1L);
            stopAll();
            setOutputDone();
            return false;
        }

        setOutputDone();
        return false;
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        this.meta = ((CassandraInputMeta) smi);
        this.data = ((CassandraInputData) sdi);
        String nodes = environmentSubstitute(this.meta.getCassandraNodes());
        String port = environmentSubstitute(this.meta.getCassandraPort());
        String username = environmentSubstitute(this.meta.getUsername());
        String password = environmentSubstitute(this.meta.getPassword());
        String keyspace = environmentSubstitute(this.meta.getKeyspace());
        Boolean withSSL = this.meta.getSslEnabled();
        String trustStoreFilePath = environmentSubstitute(this.meta.getTrustStoreFilePath());
        String trustStorePass = environmentSubstitute(this.meta.getTrustStorePass());
        String compression = environmentSubstitute(this.meta.getCompression().name());
        this.executeForEachInputRow = this.meta.getExecuteForEachInputRow();
        this.rowLimit = this.meta.getRowLimit();
        this.cqlStatement = environmentSubstitute(this.meta.getCqlStatement());
        try {
            this.connection = Utils.connect(nodes, port, username, password, keyspace, withSSL,
                    trustStoreFilePath, trustStorePass, ConnectionCompression.fromString(compression));
        } catch (CassandraConnectionException e) {
            logError("Could not initialize step: " + e.getMessage());
        }
        logDetailed("Init connection OK : "+this.connection);
        return super.init(this.meta, this.data);
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        if (this.connection != null) {
            this.connection.release();
        }
        super.dispose(this.meta, this.data);
    }
}
