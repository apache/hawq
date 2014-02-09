package com.pivotal.pxf.api;

/**
 * Interface for writing data into a data store (e.g. a sequence file on HDFS).
 */
public interface WriteAccessor {
    /**
     * Opens the resource for write.
     *
     * @return true if the resource is successfully opened
     * @throws Exception
     */
    boolean openForWrite() throws Exception;

    /**
     * Writes the next object.
     *
     * @param onerow the object to be written
     * @return true if the write succeeded
     * @throws Exception
     */
    boolean writeNextObject(OneRow onerow) throws Exception;

    /**
     * Closes the resource for write.
     *
     * @throws Exception
     */
    void closeForWrite() throws Exception;
}
