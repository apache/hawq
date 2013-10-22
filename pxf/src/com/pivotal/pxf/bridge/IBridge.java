package com.pivotal.pxf.bridge;

import java.io.DataInputStream;

import org.apache.hadoop.io.Writable;

/*
 * IBridge interface - defines the interface of the Bridge classes.
 * Any Bridge class acts as an iterator over Hadoop stored data, and 
 * should implement getNext (for reading) or setNext (for writing) 
 * for handling accessed data.
 */
public interface IBridge 
{
	boolean beginIteration() throws Exception;
	Writable getNext() throws Exception;
	boolean setNext(DataInputStream inputStream) throws Exception;
}
