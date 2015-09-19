package com.pivotal.pxf.api;

/**
 * Thrown when a problem occurs while fetching or parsing a record from the user's input data.
 */
public class BadRecordException extends Exception {
    public BadRecordException() {
    }

    /**
     * Constructs a BadRecordException.
     *
     * @param cause the cause of this exception
     */
    public BadRecordException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a BadRecordException.
     *
     * @param message the cause of this exception
     */
    public BadRecordException(String message) {
        super(message);
    }
}
