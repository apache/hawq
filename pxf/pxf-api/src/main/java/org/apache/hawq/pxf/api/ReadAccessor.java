package org.apache.hawq.pxf.api;

/**
 * Interface that defines access to the source data store (e.g, a file on HDFS, a region of an HBase table, etc).
 */
public interface ReadAccessor {
    /**
     * Opens the resource for reading.
     *
     * @return true if the resource is successfully opened
     * @throws Exception if opening the resource failed
     */
    boolean openForRead() throws Exception;

    /**
     * Reads the next object.
     *
     * @return the object which was read
     * @throws Exception if reading from the resource failed
     */
    OneRow readNextObject() throws Exception;

    /**
     * Closes the resource.
     *
     * @throws Exception if closing the resource failed
     */
    void closeForRead() throws Exception;
}
