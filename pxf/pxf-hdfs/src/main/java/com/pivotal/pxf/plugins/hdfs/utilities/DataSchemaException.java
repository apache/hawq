package com.pivotal.pxf.plugins.hdfs.utilities;

/**
 * Thrown when there is a data schema problem detected by any plugin that
 * requires a schema.
 * {@link DataSchemaException.MessageFmt#SCHEMA_NOT_ON_CLASSPATH} when the specified schema is missing from the CLASSPATH.
 * {@link DataSchemaException.MessageFmt#SCHEMA_NOT_INDICATED} when a schema was required but was not specified in the pxf uri.
 */
public class DataSchemaException extends RuntimeException {
    public static enum MessageFmt {
		SCHEMA_NOT_INDICATED("%s requires a data schema to be specified in the "+
							 "pxf uri, but none was found. Please supply it" +
							 "using the DATA-SCHEMA option "),
		SCHEMA_NOT_ON_CLASSPATH("schema resource \"%s\" is not located on the classpath");
		
        String format;

        MessageFmt(String format) {
            this.format = format;
        }

        public String getFormat() {
            return format;
        }
    }

    private MessageFmt msgFormat;

    /**
     * Constructs a DataSchemaException.
     *
     * @param msgFormat the message format
     * @param msgArgs the message arguments
     */
    public DataSchemaException(MessageFmt msgFormat, String... msgArgs) {
        super(String.format(msgFormat.getFormat(), (Object[]) msgArgs));
        this.msgFormat = msgFormat;
    }

    public MessageFmt getMsgFormat() {
        return msgFormat;
    }
}