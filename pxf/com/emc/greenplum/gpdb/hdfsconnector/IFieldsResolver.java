package com.emc.greenplum.gpdb.hdfsconnector;

import java.util.List;

/*
 * Interface that defines the deserializtion of one record brought from an HDFS file.
 * Every implementation of a deserialization method (Writable, Avro, BP, Thrift, ...)
 * must respect this interface
 */
public interface IFieldsResolver
{
	List<OneField> GetFields(OneRow row) throws Exception;
}
