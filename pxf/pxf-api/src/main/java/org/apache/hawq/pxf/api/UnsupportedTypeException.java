package org.apache.hawq.pxf.api;

/**
 * Thrown when the resolver tries to serializes/deserializes an unsupported type.
 */
public class UnsupportedTypeException extends RuntimeException {

    /**
     * Constructs an UnsupportedTypeException
     *
     * @param cause cause of this exception
     */
    public UnsupportedTypeException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs an UnsupportedTypeException
     *
     * @param message cause of this exception
     */
    public UnsupportedTypeException(String message) {
        super(message);
    }
}
