package com.pivotal.pxf.api;

import java.util.List;

/*
 * Interface that defines the deserialization of one record brought from 
 * the data Accessor. Every implementation of a deserialization method 
 * (e.g, Writable, Avro, ...) must implement this interface.
 */
public interface ReadResolver {
    List<OneField> getFields(OneRow row) throws Exception;
}
