package com.pivotal.pxf.api;

/**
 * Interface that defines access to the source data store (e.g, a file on HDFS, a region of an HBase table, etc).
 */
public interface ReadAccessor {
    /**
     * Opens the resource for reading.
     *
     * @return true if the resource is successfully opened
     * @throws Exception
     */
    boolean openForRead() throws Exception;

    /**
     * Reads the next object.
     *
     * @return the object which was read
     * @throws Exception
     */
    OneRow readNextObject() throws Exception;

    /**
     * Closes the resource.
     *
     * @throws Exception
     */
    void closeForRead() throws Exception;
}
