package org.apache.hawq.pxf.service;

import org.apache.hawq.pxf.service.io.Writable;

import java.io.DataInputStream;

/**
 * Bridge interface - defines the interface of the Bridge classes. Any Bridge
 * class acts as an iterator over Hadoop stored data, and should implement
 * getNext (for reading) or setNext (for writing) for handling accessed data.
 */
public interface Bridge {
    boolean beginIteration() throws Exception;

    Writable getNext() throws Exception;

    boolean setNext(DataInputStream inputStream) throws Exception;

    boolean isThreadSafe();
}
