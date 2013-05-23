package com.pivotal.pxf.utilities;

import java.io.FileNotFoundException;

import org.apache.avro.Schema;

import com.pivotal.pxf.format.OutputFormat;

/*
 * Class for passing HDFS parameters to the GPHdfsBridge
 */
public class HDFSMetaData extends HDMetaData
{
    protected String srlzSchemaName;
	protected String path;
    protected String hdfsAccessor;
    protected String hdfsResolver;
	protected String hdfsAddition1;

	// this schema object variable is special -  it is not filled from getProperty like all 
	// the others. Instead it is used by the AvroFileAccessor to pass the avro 
	// schema to the AvroResolver. In this case only the AvroFileAccessor can
	// fetch the schema because it is the only one that can read the Avro file.
	// So the AvroResolver needs to get the schema from the AvroFileAccessor, and
	// this schema variable is the way it's done.
	protected Schema avroSchema = null;
	
	/* 
     * C'tor
     */			
    public HDFSMetaData(HDMetaData meta) throws Exception
    {
		super(meta);
		InitPropertyNotFoundMessages();
		/*
		 * We don't want to fail if schema was not supplied. There are HDFS resources which do not require schema.
		 * If on the other hand the schema is required we will fail when the Resolver or Accessor will request the schema
		 * by calling function srlzSchemaName(). 
		 */
		srlzSchemaName = getOptionalProperty("X-GP-DATA-SCHEMA");
				
		/* 
		 * accessor - will throw exception from getPropery() if outputFormat is FORMAT_GPDB_WRITABLE 
		 * and the user did not supply accessor=...
		 * resolver - will throw exception from getPropery() if outputFormat is FORMAT_GPDB_WRITABLE 
		 * and the user did not supply resolver=...
		 */
		if (outputFormat() == OutputFormat.FORMAT_GPDB_WRITABLE)
		{
			hdfsAccessor = getProperty("X-GP-ACCESSOR");
			hdfsResolver = getProperty("X-GP-RESOLVER");			
		}
		
		/*
		 * TODO: leading '/' is expected. gpdb ignores it. deal more gracefully...
		 */
		path = "/" + getProperty("X-GP-DATA-DIR"); 		
    }
	
	private void InitPropertyNotFoundMessages()
	{
		propertyErrorMap.put("X-GP-ACCESSOR", "Accessor was not supplied in the CREATE EXTERNAL TABLE statement " + 
							 "Please supply accessor using option accessor ");
		propertyErrorMap.put("X-GP-RESOLVER", "Resolver was not supplied in the CREATE EXTERNAL TABLE statement " + 
							 "Please supply resolver using option resolver ");
	}
	
	/*
	 * Tests for the case schema resource is a file like avro_schema.avsc
	 * or for the case schema resource is a Java class. in which case we add <.class> suffix 
	 */
	private boolean isSchemaResourceOnClasspath(String resource)
	{
		if (this.getClass().getClassLoader().getResource(resource) != null)
			return true;
		if (this.getClass().getClassLoader().getResource(resource + ".class") != null)
			return true;
		
		return false;
	}	
	
	/* returns the path to the resource required
     * (might be a file path or a table name)
     */
    public String path()
    {
        return path;
    }

	/* returns the path of the schema used for various deserializers
	 * e.g, Avro file name, Java object file name.
	 */
    public String srlzSchemaName() throws  FileNotFoundException, IllegalArgumentException
    {
		/*
		 * Testing that the schema name was supplied by the user - schema is an optional properly.
		 */
		if (srlzSchemaName == null)
            throw new IllegalArgumentException("Schema was not supplied in the CREATE EXTERNAL TABLE statement." +
											   " Please supply the schema using option schema ");		
		/* 
		 * Testing that the schema resource exists
		 */
		if (!isSchemaResourceOnClasspath(srlzSchemaName))
			throw new FileNotFoundException("schema resource \"" + srlzSchemaName + "\" is not located on the classpath ");
		
		
        return srlzSchemaName;
    }

    /* 
	 * returns the ClassName for the java class that handles the file access
     */
    public String accessor()
    {
        return hdfsAccessor;
    }

    /*
	 * returns the ClassName for the java class that handles the record deserialization
     */
    public String resolver()
    {
        return hdfsResolver;
    }

	/*
	 * The avroSchema is set from the outside by the AvroFileAccessor
	 */
	public void SetAvroFileSchema(Schema theAvroSchema)
	{
		avroSchema = theAvroSchema;
	}

	/*
	 * The avroSchema fetched by the AvroResolver and used in case of Avro File
	 * In case of avro records inside a sequence file this variable will be null
	 * and the AvroResolver will not use it.
	 */	
	public Schema GetAvroFileSchema()
	{
		return avroSchema;
	}
}
