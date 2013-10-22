package com.pivotal.pxf.resolvers;

import java.io.DataInputStream;

import com.pivotal.pxf.format.OneRow;

/*
 * Interface that defines the serialization of data read from the DB
 * into a OneRow object.
 * Every implementation of a serialization method 
 * (e.g, Writable, Avro, ...) must implement this interface.
 */
public interface IWriteResolver
{
	public OneRow setFields(DataInputStream inputStream) throws Exception;
}
