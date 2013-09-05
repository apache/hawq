package com.pivotal.hawq.mapreduce;

@SuppressWarnings("serial")
public class HAWQException extends Exception {

    private int etype;

    public static final int DEFAULT_EXCEPTION = 0;
    public static final int WRONGFILEFORMAT_EXCEPTION = 1;

    public HAWQException() {
    }

    public HAWQException(String emsg) {
        super(emsg);
    }

    public HAWQException(String emsg, int etype) {
        super(emsg);
        this.etype = etype;
    }

    public String getMessage() {
        StringBuffer buffer = new StringBuffer();
        switch (etype) {
            case WRONGFILEFORMAT_EXCEPTION:
                buffer.append("Wrong File Format: ");
                buffer.append(super.getMessage());
                return buffer.toString();
            default:
                buffer.append(super.getMessage());
                return buffer.toString();
        }
    }
}
