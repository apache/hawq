package com.pivotal.pxf.api.resolvers;

import com.pivotal.pxf.api.format.OneField;
import com.pivotal.pxf.api.format.OneRow;

import java.io.DataInputStream;
import java.util.List;

/*
 * Interface that defines the serialization of data read from the DB
 * into a OneRow object.
 * Every implementation of a serialization method 
 * (e.g, Writable, Avro, ...) must implement this interface.
 */
public interface WriteResolver {
    OneRow setFields(List<OneField> record) throws Exception;
}
