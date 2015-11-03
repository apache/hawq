package org.apache.hawq.pxf.api;

/**
 * Interface for writing data into a data store (e.g. a sequence file on HDFS).
 */
public interface WriteAccessor {
    /**
     * Opens the resource for write.
     *
     * @return true if the resource is successfully opened
     * @throws Exception if opening the resource failed
     */
    boolean openForWrite() throws Exception;

    /**
     * Writes the next object.
     *
     * @param onerow the object to be written
     * @return true if the write succeeded
     * @throws Exception writing to the resource failed
     */
    boolean writeNextObject(OneRow onerow) throws Exception;

    /**
     * Closes the resource for write.
     *
     * @throws Exception if closing the resource failed
     */
    void closeForWrite() throws Exception;
}
