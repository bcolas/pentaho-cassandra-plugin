package org.pentaho.cassandra;

public enum ConnectionCompression {
	NONE("NONE"),
    SNAPPY("SNAPPY"),
    PIEDPIPER("PIED PIPER");

    private String text;

    ConnectionCompression(String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }

    public static ConnectionCompression fromString(String text) {
        for (ConnectionCompression b : ConnectionCompression.values()) {
            if (b.text.equalsIgnoreCase(text)) {
                return b;
            }
        }

        throw new IllegalArgumentException("Compression not supported");
    }
}
