package com.pivotal.hawq.mapreduce;

@SuppressWarnings("serial")
public class HAWQException extends Exception {

    private int etype;

    public static final int DEFAULT_EXCEPTION = 0;
    public static final int WRONGFILEFORMAT_EXCEPTION = 1;

    public HAWQException() {
    }

	/**
	 * Constructor
	 * 
	 * @param emsg
	 *            error message
	 */
    public HAWQException(String emsg) {
        super(emsg);
    }

	/**
	 * Constructor
	 * 
	 * @param emsg
	 *            error message
	 * @param etype
	 *            0 is default type, and 1 means wrong file format exception
	 */
    public HAWQException(String emsg, int etype) {
        super(emsg);
        this.etype = etype;
    }

    @Override
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
