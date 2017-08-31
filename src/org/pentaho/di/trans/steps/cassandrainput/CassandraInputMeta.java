/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import org.pentaho.cassandra.*;

import java.util.List;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@Step( id = "CassandraInput", image = "DatastaxCassandraInput.svg", name = "Cassandra Input",
description = "Read from a Cassandra table", categoryDescription = "Big Data" )
public class CassandraInputMeta extends AbstractCassandraMeta {
    private static final String CQL = "cql";
    private static final String EXECUTE_FOR_EACH_INPUT = "execute_for_each_input";
    private static final String ROW_LIMIT = "rowLimit";

    private String cqlStatement;
    private int rowLimit = 0;
    private Boolean executeForEachInputRow;

    @Override
    public String getXML() {
        StringBuilder retval = new StringBuilder();

        if (!Const.isEmpty(this.cassandraNodes)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_NODES, this.cassandraNodes));
        }

        if (!Const.isEmpty(this.cassandraPort)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_PORT, this.cassandraPort));
        }

        if (!Const.isEmpty(this.username)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(USERNAME, this.username));
        }

        if (!Const.isEmpty(this.password)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(PASSWORD, Encr.encryptPasswordIfNotUsingVariables(this.password)));
        }

        if (!Const.isEmpty(this.keyspace)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_KEYSPACE, this.keyspace));
        }

        if (this.SslEnabled != null) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_WITH_SSL, this.SslEnabled ? "Y" : "N"));
        }

        if (!Const.isEmpty(this.trustStoreFilePath)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_TRUSTSTORE_FILE_PATH, this.trustStoreFilePath));
        }

        if (!Const.isEmpty(this.trustStorePass)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_TRUSTSTORE_PASS, Encr.encryptPasswordIfNotUsingVariables(this.trustStorePass)));
        }

        if (this.compression != null) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(COMPRESSION, this.compression.toString()));
        }

        if (!Const.isEmpty(this.cqlStatement)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CQL, this.cqlStatement));
        }

        if (this.executeForEachInputRow != null) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(EXECUTE_FOR_EACH_INPUT, this.executeForEachInputRow));
        }

        if (this.rowLimit > 0) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(ROW_LIMIT, this.rowLimit));
        }

        return retval.toString();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metastore) throws KettleXMLException {
        this.cassandraNodes = XMLHandler.getTagValue(stepnode, CASSANDRA_NODES);
        this.cassandraPort = XMLHandler.getTagValue(stepnode, CASSANDRA_PORT);
        this.username = XMLHandler.getTagValue(stepnode, USERNAME);
        this.password = XMLHandler.getTagValue(stepnode, PASSWORD);
        if (!Const.isEmpty(this.password)) {
            this.password = Encr.decryptPasswordOptionallyEncrypted(this.password);
        }
        this.keyspace = XMLHandler.getTagValue(stepnode, CASSANDRA_KEYSPACE);
        this.SslEnabled = "Y".equals(XMLHandler.getTagValue(stepnode, CASSANDRA_WITH_SSL));
        this.trustStoreFilePath = XMLHandler.getTagValue(stepnode, CASSANDRA_TRUSTSTORE_FILE_PATH);
        this.trustStorePass = XMLHandler.getTagValue(stepnode, CASSANDRA_TRUSTSTORE_PASS);
        String sCompression = XMLHandler.getTagValue(stepnode, COMPRESSION);
        this.cqlStatement = XMLHandler.getTagValue(stepnode, CQL);
        this.executeForEachInputRow = "Y".equals(XMLHandler.getTagValue(stepnode, EXECUTE_FOR_EACH_INPUT));
        String sRowLimit = XMLHandler.getTagValue(stepnode, ROW_LIMIT);

        checkNulls();

        if (this.cqlStatement == null) {
            this.cqlStatement = "";
        }
        if (!Const.isEmpty(sRowLimit)) {
            this.rowLimit = Integer.parseInt(sRowLimit);
        }
        this.compression = (Const.isEmpty(sCompression) ? ConnectionCompression.SNAPPY : ConnectionCompression.fromString(sCompression));
    }

    @Override
    public void readRep(Repository rep, IMetaStore metastore, ObjectId id_step, List<DatabaseMeta> databases)
            throws KettleException {
        this.cassandraNodes = rep.getStepAttributeString(id_step, 0, CASSANDRA_NODES);
        this.cassandraPort = rep.getStepAttributeString(id_step, 0, CASSANDRA_PORT);
        this.username = rep.getStepAttributeString(id_step, 0, USERNAME);
        this.password = rep.getStepAttributeString(id_step, 0, PASSWORD);
        if (!Const.isEmpty(this.password)) {
            this.password = Encr.decryptPasswordOptionallyEncrypted(this.password);
        }
        this.keyspace = rep.getStepAttributeString(id_step, 0, CASSANDRA_KEYSPACE);
        this.SslEnabled = rep.getStepAttributeBoolean(id_step, CASSANDRA_WITH_SSL);
        this.trustStoreFilePath = rep.getStepAttributeString(id_step, 0, CASSANDRA_TRUSTSTORE_FILE_PATH);
        this.trustStorePass = rep.getStepAttributeString(id_step, 0, CASSANDRA_TRUSTSTORE_PASS);
        String sCompression = rep.getStepAttributeString(id_step, 0, COMPRESSION);
        this.cqlStatement = rep.getStepAttributeString(id_step, 0, CQL);
        this.executeForEachInputRow = rep.getStepAttributeBoolean(id_step, EXECUTE_FOR_EACH_INPUT);
        this.rowLimit = ((int) rep.getStepAttributeInteger(id_step, ROW_LIMIT));

        if (!Const.isEmpty(this.trustStorePass)) {
            this.trustStorePass = Encr.decryptPasswordOptionallyEncrypted(this.trustStorePass);
        }

        checkNulls();

        if (this.cqlStatement == null) {
            this.cqlStatement = "";
        }
        this.compression = (Const.isEmpty(sCompression) ? ConnectionCompression.SNAPPY : ConnectionCompression.fromString(sCompression));
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metastore, ObjectId id_transformation, ObjectId id_step) throws KettleException {
        if (!Const.isEmpty(this.cassandraNodes)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_NODES, this.cassandraNodes);
        }

        if (!Const.isEmpty(this.cassandraPort)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_PORT, this.cassandraPort);
        }

        if (!Const.isEmpty(this.username)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, USERNAME, this.username);
        }

        if (!Const.isEmpty(this.password)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, PASSWORD, Encr.encryptPasswordIfNotUsingVariables(this.password));
        }

        if (!Const.isEmpty(this.keyspace)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_KEYSPACE, this.keyspace);
        }

        if (this.SslEnabled != null) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_WITH_SSL, this.SslEnabled);
        }

        if (!Const.isEmpty(this.trustStoreFilePath)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_TRUSTSTORE_FILE_PATH, this.trustStoreFilePath);
        }

        if (!Const.isEmpty(this.trustStorePass)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_TRUSTSTORE_PASS,
                    Encr.encryptPasswordIfNotUsingVariables(this.trustStorePass));
        }

        if (this.compression != null) {
            rep.saveStepAttribute(id_transformation, id_step, 0, COMPRESSION, this.compression.toString());
        }

        if (!Const.isEmpty(this.cqlStatement)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CQL, this.cqlStatement);
        }

        if (this.executeForEachInputRow != null) {
            rep.saveStepAttribute(id_transformation, id_step, 0, EXECUTE_FOR_EACH_INPUT, this.executeForEachInputRow);
        }

        rep.saveStepAttribute(id_transformation, id_step, 0, ROW_LIMIT, this.rowLimit);
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        return new CassandraInput(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new CassandraInputData();
    }

    @Override
    public String getDialogClassName() {
        return CassandraInputDialog.class.getName();
    }

    public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
        return new CassandraInputDialog(shell, meta, transMeta, name);
    }

    @Override
    public void setDefault() {
        super.setDefault();

        this.executeForEachInputRow = Boolean.FALSE;
        this.cqlStatement = "select <fields> from <column family> where <condition>;";
        this.rowLimit = 0;
    }

    @Override
    public void getFields(RowMetaInterface row, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore)
            throws KettleStepException {
        CassandraConnection connection;
        String nodes = space.environmentSubstitute(this.cassandraNodes);
        String port = space.environmentSubstitute(this.cassandraPort);
        String keyspace = space.environmentSubstitute(this.keyspace);
        String cql = space.environmentSubstitute(this.cqlStatement);

        if ((Const.isEmpty(nodes)) || (Const.isEmpty(port)) || (Const.isEmpty(keyspace)) || (Const.isEmpty(cql))) {
            return;
        }

        String user = space.environmentSubstitute(this.username);
        String pass = space.environmentSubstitute(this.password);
        String trustfile = space.environmentSubstitute(this.trustStoreFilePath);
        String trustpass = space.environmentSubstitute(this.trustStorePass);


        logDebug("meta: opening connection");
        try {
            connection = Utils.connect(nodes, port, user, pass, keyspace, this.SslEnabled, trustfile, trustpass, this.compression);
        } catch (CassandraConnectionException e) {
            throw new KettleStepException("Failed to create connection", e);
        }

        Session session = connection.getSession();

        logDebug("meta: parsing cql '" + cql + "'");
        ResultSet rs = session.execute(cql);
        createOutputRowMeta(row, rs);

        connection.release();
    }

    void createOutputRowMeta(RowMetaInterface row, ResultSet rs) {
        row.clear();

        for (ColumnDefinitions.Definition d : rs.getColumnDefinitions()) {
            logDebug(d.getName() + ',' + d.getType().getName() + ',' + d.getType().asFunctionParameterString());

            ValueMetaBase valueMeta = new ValueMetaBase(d.getName(), Utils.convertDataType(d.getType()));
            valueMeta.setTrimType(0);
            row.addValueMeta(valueMeta);
        }
    }
    
    public Boolean getExecuteForEachInputRow() {
    	return this.executeForEachInputRow;
    }
    
    public void setExecuteForEachInputRow(Boolean executeForEachInputRow) {
    	this.executeForEachInputRow = executeForEachInputRow;
    }
    
    public int getRowLimit() {
    	return this.rowLimit;
    }
    
    public void setRowLimit(int rowLimit) {
    	this.rowLimit = rowLimit;
    }
    
    public String getCqlStatement() {
    	return this.cqlStatement;
    }
    
    public void setCqlStatement(String cqlStatement) {
    	this.cqlStatement = cqlStatement;
    }
}