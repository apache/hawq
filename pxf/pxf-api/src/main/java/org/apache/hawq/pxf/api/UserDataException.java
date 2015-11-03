package org.apache.hawq.pxf.api;

/**
 * Thrown when Accessor/Resolver failes to parse {@link org.apache.hawq.pxf.api.utilities.InputData#userData}.
 */
public class UserDataException extends Exception {

    /**
     * Constructs an UserDataException
     *
     * @param cause the cause of this exception
     */
    public UserDataException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs an UserDataException
     *
     * @param message the cause of this exception
     */
    public UserDataException(String message) {
        super(message);
    }
}
