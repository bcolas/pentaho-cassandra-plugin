package org.pentaho.cassandra;

import org.eclipse.swt.custom.CCombo;
import org.pentaho.di.i18n.BaseMessages;

public class CommonDialog {
	public static void setCompressionTooltips(CCombo wCompression, Class dialogClass)
    {
        switch (ConnectionCompression.fromString(wCompression.getText())) {
            case NONE:
                wCompression.setToolTipText(BaseMessages.getString(dialogClass, "CompressionNone.TipText"));
                break;
            case SNAPPY:
                wCompression.setToolTipText(BaseMessages.getString(dialogClass, "CompressionSnappy.TipText"));
                break;
            case PIEDPIPER:
                wCompression.setToolTipText(BaseMessages.getString(dialogClass, "CompressionPiedPiper.TipText"));
                break;
            default:
                wCompression.setToolTipText(BaseMessages.getString(dialogClass, "CompressionNotAvailable.TipText"));
        }
    }
}
