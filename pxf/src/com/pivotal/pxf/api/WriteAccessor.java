package com.pivotal.pxf.api;

/*
 * An interface for writing data into a data store 
 * (e.g. a sequence file on HDFS).
 * All classes that implement actual access to such data sources must 
 * implement this interface.
 */
public interface WriteAccessor {
    boolean openForWrite() throws Exception;

    boolean writeNextObject(OneRow onerow) throws Exception;

    void closeForWrite() throws Exception;
}
