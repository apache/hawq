package com.pivotal.pxf.api.accessors;

import com.pivotal.pxf.api.format.OneRow;

/*
 * Internal interface that defines the access to data on the source
 * data store (e.g, a file on HDFS, a region of an HBase table, etc).
 * All classes that implement actual access to such data sources must 
 * respect this interface
 */
public interface ReadAccessor {
    boolean openForRead() throws Exception;

    OneRow readNextObject() throws Exception;

    void closeForRead() throws Exception;
}
