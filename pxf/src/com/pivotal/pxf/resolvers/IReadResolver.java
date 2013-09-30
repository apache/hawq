package com.pivotal.pxf.resolvers;

import java.util.List;

import com.pivotal.pxf.format.OneField;
import com.pivotal.pxf.format.OneRow;

/*
 * Interface that defines the deserialization of one record brought from 
 * the data Accessor. Every implementation of a deserialization method 
 * (e.g, Writable, Avro, ...) must implement this interface.
 */
public interface IReadResolver 
{	
	public List<OneField> getFields(OneRow row) throws Exception;
}
