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

import org.pentaho.cassandra.CommonDialog;
import org.pentaho.cassandra.ConnectionCompression;

import java.util.List;

import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.dialog.EnterNumberDialog;
import org.pentaho.di.ui.core.dialog.EnterTextDialog;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.core.widget.StyledTextComp;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.spoon.job.JobGraph;
import org.pentaho.di.ui.trans.dialog.TransPreviewProgressDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.steps.tableinput.SQLValuesHighlight;

public class CassandraInputDialog extends BaseStepDialog implements StepDialogInterface {
    private static final Class<?> PKG = CassandraInputMeta.class;

    private StyledTextComp wtCql;
    private CCombo wcDatafrom;
    private TextVar wLimit;
    private Label wlEachRow;
    private Button wEachRow;
    private CassandraInputMeta meta;
    private Label wlPosition;
    private TextVar wHost;
    private TextVar wPort;
    private TextVar wUsername;
    private TextVar wPassword;
    private Button wWithSSL;
    private TextVar wTruststorefile;
    private CCombo wCompression;
    private TextVar wTruststorepass;
    private TextVar wKeyspace;
    
    protected boolean changedInDialog;

    public CassandraInputDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta) in, transMeta, sname);
        this.meta = ((CassandraInputMeta) in);
    }

    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        this.shell = new Shell(parent, 3312);
        this.props.setLook(this.shell);
        setShellImage(this.shell, this.meta);

        /*ModifyListener lsMod = e -> {
            CassandraInputDialog.this.changedInDialog = false;
            CassandraInputDialog.this.meta.setChanged();
            if ((e.widget instanceof TextVar)) {
                TextVar t = (TextVar) e.widget;
                t.setToolTipText(CassandraInputDialog.this.transMeta.environmentSubstitute(t.getText()));
            }
        };*/
        
        ModifyListener lsMod = new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				changedInDialog = false;
				meta.setChanged();
				if ((e.widget instanceof TextVar)) {
	                TextVar t = (TextVar) e.widget;
	                t.setToolTipText(CassandraInputDialog.this.transMeta.environmentSubstitute(t.getText()));
	            }
			}
		};

        this.changed = this.meta.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = 5;
        formLayout.marginHeight = 5;

        this.shell.setLayout(formLayout);
        this.shell.setText(BaseMessages.getString(PKG, "AutSoftCassandraInput.StepName"));

        int middle = this.props.getMiddlePct();

        this.wlStepname = new Label(this.shell, 131072);
        this.wlStepname.setText(BaseMessages.getString(PKG, "System.Label.StepName"));
        this.props.setLook(this.wlStepname);
        FormData fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(middle, -4);
        fd.top = new FormAttachment(0, 4);
        this.wlStepname.setLayoutData(fd);
        this.wStepname = new Text(this.shell, 18436);
        this.wStepname.setText(this.stepname);
        this.props.setLook(this.wStepname);
        this.wStepname.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(0, 4);
        fd.right = new FormAttachment(100, 0);
        this.wStepname.setLayoutData(fd);

        Label wlHost = new Label(this.shell, 131072);
        wlHost.setText(BaseMessages.getString(PKG, "AutSoftCassandraInputDialog.Hostname.Label"));
        this.props.setLook(wlHost);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(middle, -4);
        fd.top = new FormAttachment(this.wStepname, 4);
        wlHost.setLayoutData(fd);

        this.wHost = new TextVar(this.transMeta, this.shell, 18436);
        this.wHost.setText(this.meta.getCassandraNodes());
        this.props.setLook(this.wHost);
        this.wHost.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.wStepname, 4);
        fd.right = new FormAttachment(100, 0);
        this.wHost.setLayoutData(fd);

        Label wlPort = new Label(this.shell, 131072);
        wlPort.setText(BaseMessages.getString(PKG, "AutSoftCassandraInputDialog.Port.Label"));
        this.props.setLook(wlPort);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(middle, -4);
        fd.top = new FormAttachment(this.wHost, 4);
        wlPort.setLayoutData(fd);

        this.wPort = new TextVar(this.transMeta, this.shell, 18436);
        this.wPort.setText(this.meta.getCassandraPort());
        this.props.setLook(this.wPort);
        this.wPort.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.wHost, 4);
        fd.right = new FormAttachment(100, 0);
        this.wPort.setLayoutData(fd);

        Label wlUsername = new Label(this.shell, 131072);
        wlUsername.setText(BaseMessages.getString(PKG, "AutSoftCassandraInputDialog.Username.Label"));
        this.props.setLook(wlUsername);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(middle, -4);
        fd.top = new FormAttachment(this.wPort, 4);
        wlUsername.setLayoutData(fd);

        this.wUsername = new TextVar(this.transMeta, this.shell, 18436);
        this.wUsername.setText(this.meta.getUsername());
        this.props.setLook(this.wUsername);
        this.wUsername.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.wPort, 4);
        fd.right = new FormAttachment(100, 0);
        this.wUsername.setLayoutData(fd);

        Label wlPassword = new Label(this.shell, 131072);
        wlPassword.setText(BaseMessages.getString(PKG, "AutSoftCassandraInputDialog.Password.Label"));
        this.props.setLook(wlPassword);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(middle, -4);
        fd.top = new FormAttachment(this.wUsername, 4);
        wlPassword.setLayoutData(fd);

        this.wPassword = new TextVar(this.transMeta, this.shell, 4212740);
        this.wPassword.setText(this.meta.getPassword());
        this.props.setLook(this.wPassword);
        this.wPassword.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.wUsername, 4);
        fd.right = new FormAttachment(100, 0);
        this.wPassword.setLayoutData(fd);

        Label wlWithSSL = new Label(this.shell, 131072);
        wlWithSSL.setText(BaseMessages.getString(PKG, "AutSoftCassandraInputDialog.WithSSL.Label"));
        this.props.setLook(wlWithSSL);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(middle, -4);
        fd.top = new FormAttachment(this.wPassword, 4);
        wlWithSSL.setLayoutData(fd);

        this.wWithSSL = new Button(this.shell, 32);
        this.wWithSSL.setSelection(this.meta.getSslEnabled());
        this.props.setLook(this.wWithSSL);
        fd = new FormData();
        fd.right = new FormAttachment(100, 0);
        fd.top = new FormAttachment(this.wPassword, 4);
        fd.left = new FormAttachment(middle, 0);
        this.wWithSSL.setLayoutData(fd);


        Label wlTruststorefile = new Label(this.shell, 131072);
        this.props.setLook(wlTruststorefile);
        wlTruststorefile.setText(BaseMessages.getString(PKG, "AutSoftCassandraInputDialog.TrustStoreFile.Label"));
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.wWithSSL, 4);
        fd.right = new FormAttachment(middle, -4);
        wlTruststorefile.setLayoutData(fd);

        this.wTruststorefile = new TextVar(this.transMeta, this.shell, 18436);
        this.wTruststorefile.setText(this.meta.getTrustStoreFilePath());
        this.props.setLook(this.wTruststorefile);
        this.wTruststorefile.addModifyListener(lsMod);
        fd = new FormData();
        fd.right = new FormAttachment(100, 0);
        fd.top = new FormAttachment(this.wWithSSL, 4);
        fd.left = new FormAttachment(middle, 0);
        this.wTruststorefile.setLayoutData(fd);


        Label wlTruststorepass = new Label(this.shell, 4325376);
        this.props.setLook(wlTruststorepass);
        wlTruststorepass.setText(BaseMessages.getString(PKG, "AutSoftCassandraInputDialog.TrustStorePassword.Label"));
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.wTruststorefile, 4);
        fd.right = new FormAttachment(middle, -4);
        wlTruststorepass.setLayoutData(fd);

        this.wTruststorepass = new TextVar(this.transMeta, this.shell, 18436);
        this.props.setLook(this.wTruststorepass);
        this.wTruststorepass.setText(this.meta.getTrustStorePass());
        this.wTruststorepass.addModifyListener(lsMod);

        fd = new FormData();
        fd.right = new FormAttachment(100, 0);
        fd.top = new FormAttachment(this.wTruststorefile, 4);
        fd.left = new FormAttachment(middle, 0);
        this.wTruststorepass.setLayoutData(fd);


        Label wlCompression = new Label(this.shell, 131072);
        wlCompression.setText(BaseMessages.getString(PKG, "AutSoftCassandraInputDialog.Compression.Label"));
        wlCompression.setToolTipText(BaseMessages.getString(PKG, "AutSoftCassandraInputDialog.Compression.TipText"));
        this.props.setLook(wlCompression);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.wTruststorepass, 4);
        fd.right = new FormAttachment(middle, -4);
        wlCompression.setLayoutData(fd);

        this.wCompression = new CCombo(this.shell, 2052);

        this.wCompression.add(ConnectionCompression.NONE.getText());
        this.wCompression.add(ConnectionCompression.SNAPPY.getText());
        this.wCompression.add(ConnectionCompression.PIEDPIPER.getText() + " (Coming soon)");
        this.wCompression.setEditable(false);
        this.wCompression.addModifyListener(lsMod);
        this.props.setLook(this.wCompression);
        this.wCompression.addModifyListener(new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				CommonDialog.setCompressionTooltips(wCompression, CassandraInputDialog.PKG);
			}
        });
        this.wCompression.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.wTruststorepass, 4);

        this.wCompression.setLayoutData(fd);

        Label wlKeyspace = new Label(this.shell, 131072);
        wlKeyspace.setText(BaseMessages.getString(PKG, "AutSoftCassandraInputDialog.SourceKeyspace.Label"));
        this.props.setLook(wlKeyspace);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(middle, -4);
        fd.top = new FormAttachment(this.wCompression, 4);
        wlKeyspace.setLayoutData(fd);

        this.wKeyspace = new TextVar(this.transMeta, this.shell, 18436);
        this.wKeyspace.setText(this.meta.getKeyspace());
        this.props.setLook(this.wKeyspace);
        this.wKeyspace.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.wCompression, 4);
        fd.right = new FormAttachment(100, 0);
        this.wKeyspace.setLayoutData(fd);

        this.wOK = new Button(this.shell, 8);
        this.wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
        this.wPreview = new Button(this.shell, 8);
        this.wPreview.setText(BaseMessages.getString(PKG, "System.Button.Preview"));
        this.wCancel = new Button(this.shell, 8);
        this.wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

        setButtonPositions(new Button[]{this.wOK, this.wPreview, this.wCancel}, 4, null);

        Label wlLimit = new Label(this.shell, 131072);
        wlLimit.setText(BaseMessages.getString(PKG, "AutSoftCassandraInputDialog.LimitSize"));
        this.props.setLook(wlLimit);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(middle, -4);
        fd.bottom = new FormAttachment(this.wOK, -8);
        wlLimit.setLayoutData(fd);

        this.wLimit = new TextVar(this.transMeta, this.shell, 18436);
        this.props.setLook(this.wLimit);
        this.wLimit.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.right = new FormAttachment(100, 0);
        fd.bottom = new FormAttachment(this.wOK, -8);
        this.wLimit.setLayoutData(fd);

        this.wlEachRow = new Label(this.shell, 131072);
        this.wlEachRow.setText(BaseMessages.getString(PKG, "AutSoftCassandraInputDialog.ExecuteForEachRow"));
        this.props.setLook(this.wlEachRow);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(middle, -4);
        fd.bottom = new FormAttachment(this.wLimit, -4);
        this.wlEachRow.setLayoutData(fd);

        this.wEachRow = new Button(this.shell, 32);
        this.props.setLook(this.wEachRow);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.right = new FormAttachment(100, 0);
        fd.bottom = new FormAttachment(this.wLimit, -4);
        this.wEachRow.setLayoutData(fd);
        SelectionAdapter lsSelMod = new SelectionAdapter() {
            public void widgetSelected(SelectionEvent arg0) {
                CassandraInputDialog.this.meta.setChanged();
            }
        };
        this.wEachRow.addSelectionListener(lsSelMod);

        this.wEachRow.setEnabled(false);

        Label wlDatafrom = new Label(this.shell, 131072);
        wlDatafrom.setText(BaseMessages.getString(PKG, "AutSoftCassandraInputDialog.InsertDataFromStep"));
        this.props.setLook(wlDatafrom);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(middle, -4);
        fd.bottom = new FormAttachment(this.wEachRow, -4);
        wlDatafrom.setLayoutData(fd);
        this.wcDatafrom = new CCombo(this.shell, 2048);
        this.props.setLook(this.wcDatafrom);

        List<StepMeta> previousSteps = this.transMeta.findPreviousSteps(this.transMeta.findStep(this.stepname));
        for (StepMeta stepMeta : previousSteps) {
            this.wcDatafrom.add(stepMeta.getName());
        }

        this.wcDatafrom.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.right = new FormAttachment(100, 0);
        fd.bottom = new FormAttachment(this.wEachRow, -4);
        this.wcDatafrom.setLayoutData(fd);
        this.wcDatafrom.setEnabled(false);

        this.wlPosition = new Label(this.shell, 0);
        this.props.setLook(this.wlPosition);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(100, 0);
        fd.bottom = new FormAttachment(wlDatafrom, -4);
        this.wlPosition.setLayoutData(fd);

        Label wlCql = new Label(this.shell, 0);
        wlCql.setText(BaseMessages.getString(PKG, "AutSoftCassandraInputDialog.CQL"));
        this.props.setLook(wlCql);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.wKeyspace, 8);
        wlCql.setLayoutData(fd);

        this.wtCql = new StyledTextComp(this.transMeta, this.shell, 19202, "");
        this.props.setLook(this.wtCql, 1);
        this.wtCql.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(wlCql, 4);
        fd.right = new FormAttachment(100, -8);
        fd.bottom = new FormAttachment(this.wlPosition, -4);
        this.wtCql.setLayoutData(fd);
        this.wtCql.addModifyListener(new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				CassandraInputDialog.this.setCQLToolTip();
				CassandraInputDialog.this.setPosition();
			}
        });
        this.wtCql.addKeyListener(new KeyAdapter() {
            public void keyPressed(KeyEvent e) {
                CassandraInputDialog.this.setPosition();
            }

            public void keyReleased(KeyEvent e) {
                CassandraInputDialog.this.setPosition();
            }
        });
        this.wtCql.addFocusListener(new FocusAdapter() {
            public void focusGained(FocusEvent e) {
                CassandraInputDialog.this.setPosition();
            }

            public void focusLost(FocusEvent e) {
                CassandraInputDialog.this.setPosition();
            }
        });
        this.wtCql.addMouseListener(new MouseAdapter() {
            public void mouseDoubleClick(MouseEvent e) {
                CassandraInputDialog.this.setPosition();
            }

            public void mouseDown(MouseEvent e) {
                CassandraInputDialog.this.setPosition();
            }

            public void mouseUp(MouseEvent e) {
                CassandraInputDialog.this.setPosition();
            }
        });
        this.wtCql.addLineStyleListener(new SQLValuesHighlight());

        this.lsCancel = new Listener() {
			public void handleEvent(Event e) {
				cancel();
			}
        };
        this.lsPreview = new Listener() {
			public void handleEvent(Event e) {
				preview();
			}
        };
        this.lsOK = new Listener() {
			public void handleEvent(Event e) {
				ok();
			}
        };
        Listener lsDatafrom = new Listener() {
			public void handleEvent(Event e) {
				setFlags();
			}
        };
        this.wCancel.addListener(13, this.lsCancel);
        this.wPreview.addListener(13, this.lsPreview);
        this.wOK.addListener(13, this.lsOK);
        this.wcDatafrom.addListener(13, lsDatafrom);
        this.wcDatafrom.addListener(16, lsDatafrom);

        this.lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }

        };

        this.wStepname.addSelectionListener(this.lsDef);
        this.wLimit.addSelectionListener(this.lsDef);

        this.shell.addShellListener(new ShellAdapter() {
            public void shellClosed(ShellEvent e) {
                checkCancel(e);
            }
        });

        getData();
        this.changedInDialog = false;
        this.meta.setChanged(this.changed);

        setSize();

        this.shell.open();
        while (!this.shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
        return this.stepname;
    }

    private void setPosition() {
        String scr = this.wtCql.getText();
        int linenr = this.wtCql.getLineAtOffset(this.wtCql.getCaretOffset()) + 1;
        int posnr = this.wtCql.getCaretOffset();


        int colnr = 0;
        while ((posnr > 0) && (scr.charAt(posnr - 1) != '\n') && (scr.charAt(posnr - 1) != '\r')) {
            posnr--;
            colnr++;
        }
        this.wlPosition.setText(BaseMessages.getString(PKG, "AutSoftCassandraInputDialog.Position.Label", String.valueOf(linenr), String.valueOf(colnr)));
    }

    private void setCQLToolTip() {
        this.wtCql.setToolTipText(this.transMeta.environmentSubstitute(this.wtCql.getText()));
    }


    private void getData() {
        this.wCompression.setText(this.meta.getCompression().toString());

        if (!Const.isEmpty(this.meta.getCqlStatement())) {
            this.wtCql.setText(this.meta.getCqlStatement());
        }
        this.wLimit.setText(String.valueOf(this.meta.getRowLimit()));

        boolean hasInfo = (this.meta.getStepIOMeta().getInfoStreams() != null) && (!this.meta.getStepIOMeta().getInfoStreams().isEmpty());
        this.wEachRow.setEnabled(hasInfo);
        this.wlEachRow.setEnabled(hasInfo);

        setCQLToolTip();
        setFlags();

        this.wStepname.selectAll();
        this.wStepname.setFocus();
    }

    private void checkCancel(ShellEvent e) {
        if (this.changedInDialog) {
            int save = JobGraph.showChangedWarning(this.shell, this.wStepname.getText());
            if (save == 256) {
                e.doit = false;
            } else if (save == 64) {
                ok();
            } else {
                cancel();
            }
        } else {
            cancel();
        }
    }

    private void cancel() {
        this.stepname = null;
        this.meta.setChanged(this.changed);
        dispose();
    }

    private void getInfo(CassandraInputMeta meta, boolean preview) {
        meta.setCassandraNodes(this.wHost.getText());
        meta.setCassandraPort(this.wPort.getText());
        meta.setUsername(this.wUsername.getText());
        meta.setPassword(this.wPassword.getText());
        meta.setKeyspace(this.wKeyspace.getText());
        meta.setSslEnabled(this.wWithSSL.getSelection());
        meta.setTrustStoreFilePath(this.wTruststorefile.getText());
        meta.setTrustStorePass(this.wTruststorepass.getText());
        meta.setCompression(ConnectionCompression.fromString(this.wCompression.getText()));
        meta.setCqlStatement((preview) && (!Const.isEmpty(this.wtCql.getSelectionText())) ? this.wtCql.getSelectionText() : this.wtCql.getText());
        meta.setRowLimit(Const.isEmpty(this.wLimit.getText()) ? 0 : Integer.parseInt(this.wLimit.getText()));
        meta.setExecuteForEachInputRow(this.wEachRow.getSelection());
    }

    private void ok() {
        if (Const.isEmpty(this.wStepname.getText())) {
            return;
        }

        this.stepname = this.wStepname.getText();
        getInfo(this.meta, false);

        dispose();
    }

    private void setFlags() {
        if (!Const.isEmpty(this.wcDatafrom.getText())) {
            this.wEachRow.setEnabled(true);
            this.wlEachRow.setEnabled(true);

            this.wPreview.setEnabled(false);
        } else {
            this.wEachRow.setEnabled(false);
            this.wEachRow.setSelection(false);
            this.wlEachRow.setEnabled(false);

            this.wPreview.setEnabled(true);
        }
    }

    private void preview() {
        CassandraInputMeta oneMeta = new CassandraInputMeta();
        getInfo(oneMeta, true);

        TransMeta previewMeta = TransPreviewFactory.generatePreviewTransformation(this.transMeta, oneMeta, this.wStepname.getText());

        EnterNumberDialog numberDialog = new EnterNumberDialog(this.shell, this.props.getDefaultPreviewSize(),
                BaseMessages.getString(PKG, "AutSoftCassandraInputDialog.EnterPreviewSize"),
                BaseMessages.getString(PKG, "AutSoftCassandraInputDialog.NumberOfRowsToPreview"));
        int previewSize = numberDialog.open();

        if (previewSize > 0) {
            TransPreviewProgressDialog progressDialog = new TransPreviewProgressDialog(this.shell, previewMeta,
                    new String[]{this.wStepname.getText()}, new int[]{previewSize});
            progressDialog.open();

            Trans trans = progressDialog.getTrans();
            String loggingText = progressDialog.getLoggingText();

            if (!progressDialog.isCancelled()) {
                if ((trans.getResult() != null) && (trans.getResult().getNrErrors() > 0L)) {
                    EnterTextDialog etd = new EnterTextDialog(this.shell,
                            BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title"),
                            BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message"), loggingText, true);
                    etd.setReadOnly();
                    etd.open();
                } else {
                    PreviewRowsDialog prd = new PreviewRowsDialog(this.shell, this.transMeta, 0, this.wStepname.getText(),
                            progressDialog.getPreviewRowsMeta(this.wStepname.getText()),
                            progressDialog.getPreviewRows(this.wStepname.getText()), loggingText);
                    prd.open();
                }
            }
        }
    }
}
