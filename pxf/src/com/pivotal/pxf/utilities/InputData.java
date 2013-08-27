package com.pivotal.pxf.utilities;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.pxf.format.OutputFormat;

/*
 * Common configuration of all MetaData classes
 * Provides read-only access to common parameters supplied using system properties
 */

public class InputData
{
	public static final int INVALID_SPLIT_IDX = -1;

	private   Log Log;	

	protected Map<String, String> requestParametersMap;
	protected OutputFormat outputFormat;
	protected ArrayList<ColumnDescriptor> tupleDescription;
	protected int dataFragment;
	protected int segmentId;
	protected int totalSegments;
	protected int port;
	protected byte[] userData = null;
	protected boolean filterStringValid;
	protected String filterString;
	protected String host;
	protected String srlzSchemaName;
	protected String path;
	protected String accessor;
	protected String resolver;
	protected String tableName;

	/* 
	 * The name of the recordkey column. It can appear in any location in the columns list.
	 * By specifying the recordkey column, the user declares that he is interested to receive for every record 
	 * retrieved also the the recordkey in the database. The recordkey is present in HBase table (it is called rowkey),
	 * and in sequence files. When the HDFS storage element queried will not have a recordkey and
	 * the user will still specify it in the "create external table" statement, then the values for this field will be null.
	 * This field will always be the first field in the tuple returned
	 */
	protected ColumnDescriptor recordkeyColumn;	

	/*
	 * this schema object variable is special -  it is not filled from getProperty like all 
	 * the others. Instead it is used by the AvroFileAccessor to pass the avro 
	 * schema to the AvroResolver. In this case only the AvroFileAccessor can
	 * fetch the schema because it is the only one that can read the Avro file.
	 * So the AvroResolver needs to get the schema from the AvroFileAccessor, and
	 *this schema variable is the way it's done.
	 */
	protected Schema avroSchema = null;

	/*
	 * When a property is not found we throw an exception from getProperty method(). The exception message
	 * has a generic form containing the HTTP option name. For example:
	 * --  Property "X-GP-ACCESSOR" has no value in current request  --
	 * X-GP-ACCESSOR is a PXF internal term, and it would be better not to display it to the user.
	 * With propertyErrorMap we make possible to attach a specific explanatory message to a property
	 * that will be used instead of the generic one.
	 */
	protected Map<String, String> propertyErrorMap = new HashMap<String, String>();

	/* Constructor of InputData
	 * Parses greenplum.* configuration variables
	 */
	public InputData(Map<String, String> paramsMap)
	{
		Log = LogFactory.getLog(InputData.class);
		requestParametersMap = paramsMap;
		InitPropertyNotFoundMessages();

		// Store alignment for global use as a system property
		System.setProperty("greenplum.alignment", getProperty("X-GP-ALIGNMENT"));

		segmentId = getIntProperty("X-GP-SEGMENT-ID");
		totalSegments = getIntProperty("X-GP-SEGMENT-COUNT");

		filterStringValid = getBoolProperty("X-GP-HAS-FILTER");

		if (filterStringValid)
			filterString = getProperty("X-GP-FILTER");

		parseFormat(getProperty("X-GP-FORMAT"));

		host = getProperty("X-GP-URL-HOST");
		port = getIntProperty("X-GP-URL-PORT");

		tupleDescription = new ArrayList<ColumnDescriptor>();
		recordkeyColumn = null;
		parseTupleDescription();

		dataFragment = INVALID_SPLIT_IDX;
		parseDataFragment(getOptionalProperty("X-GP-DATA-FRAGMENT"));	

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
		accessor = getProperty("X-GP-ACCESSOR");
		resolver = getProperty("X-GP-RESOLVER");			

		/* TODO: leading '/' is expected. gpdb ignores it. deal more gracefully... */
		path = "/" + getProperty("X-GP-DATA-DIR");

		/* TODO: once leading '/' is removed from the path variable, remove tableName and use path in HBase classes */
		tableName = getProperty("X-GP-DATA-DIR"); /* for HBase and Hive */

		parseUserData();
	}

	/*
	 * Expose the parameters map
	 */
	public Map<String, String> getParametersMap()
	{
		return requestParametersMap;
	}

	/* Copy constructor of InputData
	 * Used to create from an extending class
	 */
	public InputData(InputData copy)
	{
		Log = LogFactory.getLog(InputData.class);

		this.requestParametersMap = copy.requestParametersMap;
		this.propertyErrorMap     = copy.propertyErrorMap;

		this.segmentId = copy.segmentId;
		this.totalSegments = copy.totalSegments;
		this.outputFormat = copy.outputFormat;
		this.host = copy.host;
		this.port = copy.port;
		this.tupleDescription = copy.tupleDescription;
		this.dataFragment = copy.dataFragment;
		this.recordkeyColumn = copy.recordkeyColumn;
		this.filterStringValid = copy.filterStringValid;
		this.filterString = copy.filterString;
		this.srlzSchemaName = copy.srlzSchemaName;
		this.path = copy.path;
		this.accessor = copy.accessor;
		this.resolver = copy.resolver;
		this.tableName = copy.tableName;
	}

	public byte[] getFragmentUserData()
	{
		return userData;
	}

	/* 
	 * Returns a property as a string type
	 */	
	public String getProperty(String property)
	{
		String result	= requestParametersMap.get(property);
		String error	= new String("");

		if (result == null)
		{
			if (propertyErrorMap.containsKey(property))
				error = (String)propertyErrorMap.get(property);
			else 
			{
				error = "Internal server error. Property \"" + property + 
						"\" has no value in current request";
				Log.error(error);
			}
			throw new IllegalArgumentException(error);
		}

		return result;
	}

	/* 
	 * Unlike getProperty(), it will not fail if the property is not found. It will just return null instead
	 */	
	protected String getOptionalProperty(String property)
	{
		return requestParametersMap.get(property);
	}

	/* 
	 * It's meant for the plugin implementor, that in his aceessor, resolver,.. is looking for a propietary property
	 * that he introduced in the "create external table ..." statement. He is not aware that if he added
	 * property=value
	 * by the time it got here, it became
	 * X-GP-PROPERTY=value
	 * So in his plugin accessor the user will want to look for 'property', only that there is no such key
	 * in the inputData map. For this purpose we offer method
	 * getUserProperty()
	 * 
	 */	
	protected String getUserProperty(String userProp)
	{
		String prop = "X-GP-" + userProp.toUpperCase();
		return getProperty(prop);
	}

	/*
	 * Returns a property as an int type
	 */
	protected int getIntProperty(String property)
	{
		return Integer.parseInt(getProperty(property));
	}

	/*
	 * Returns a property as boolean type
	 *
	 * A boolean property is defined as an int where 0 means false 
	 * and anything else true (like C)
	 */
	protected boolean getBoolProperty(String property)
	{
		return getIntProperty(property) != 0;
	}

	/* 
	 * Returns the number of segments in GP
	 */
	public int totalSegments()
	{
		return totalSegments;
	}

	/* 
	 * Returns the current segment ID
	 */
	public int segmentId()
	{
		return segmentId;
	}

	/* 
	 * Returns the current outputFormat
	 * currently either text or gpdbwritable
	 */
	public OutputFormat outputFormat()
	{
		return outputFormat;
	}

	/* 
	 * Returns the server name providing the service
	 */
	public String serverName()
	{
		return host;
	}

	/* 
	 * Returns the server port providing the service
	 */
	public int serverPort()
	{
		return port;
	}

	/*
	 * Returns true if there is a filter string to parse
	 */
	public boolean hasFilter()
	{
		return filterStringValid;
	}

	/*
	 * The filter string
	 */
	public String filterString()
	{
		return filterString;
	}	

	/* 
	 * Returns the number of columns in Tuple Description
	 */
	public int columns()
	{
		return tupleDescription.size();
	}

	/* 
	 * Returns column index from Tuple Description
	 */
	public ColumnDescriptor getColumn(int index)
	{
		return tupleDescription.get(index);
	}

	/* 
	 * Returns a data fragment
	 */
	public int getDataFragment()
	{
		return dataFragment;
	}

	/*
	 * Returns the column descriptor of the recordkey column.
	 * If the recordkey column was not specified by the user in the create table statement,
	 * then getRecordkeyColumn will return null.
	 */
	public ColumnDescriptor getRecordkeyColumn()
	{
		return recordkeyColumn;
	}

	/* 
	 * Returns the path to the resource required
	 * (might be a file path or a table name)
	 */
	public String path()
	{
		return path;
	}

	/* 
	 * Returns the path of the schema used for various deserializers
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
	 * Returns the ClassName for the java class that handles the file access
	 */
	public String accessor()
	{
		return accessor;
	}

	/*
	 * Returns the ClassName for the java class that handles the record deserialization
	 */
	public String resolver()
	{
		return resolver;
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

	/*
	 * Returns table name
	 */	
	public String tableName()
	{
		return tableName;
	}	

	/*
	 * The avroSchema is set from the outside by the AvroFileAccessor
	 */
	public void SetAvroFileSchema(Schema theAvroSchema)
	{
		avroSchema = theAvroSchema;
	}

	/* 
	 * Sets the format type based on input string
	 */			
	protected void parseFormat(String formatString)
	{
		if (formatString.equals("TEXT"))
			outputFormat = OutputFormat.FORMAT_TEXT;
		else if (formatString.equals("GPDBWritable"))
			outputFormat = OutputFormat.FORMAT_GPDB_WRITABLE;
		else
			throw new IllegalArgumentException("Wrong value for greenplum.format " + formatString);
	}

	/* 
	 * Fills the index of allocated data fragments
	 */			
	protected void parseDataFragment(String fragment)
	{

		/* 
		 * 1. When the request made to the PXF bridge is GetFragments or Analyze, Hawq has no fragments list
		 * to send so this list will be empty.
		 */
		if (fragment == null || fragment.isEmpty())
			return;
				
		/* 
		 * 2. Convert to int 
		 * */
		dataFragment = Integer.parseInt(fragment);
		
	}

	/* 
	 * Sets the tuple description for the record
	 */		
	void parseTupleDescription()
	{		
		int columns = getIntProperty("X-GP-ATTRS");
		for (int i = 0; i < columns; ++i)
		{
			String columnName = getProperty("X-GP-ATTR-NAME" + i);
			int columnType = getIntProperty("X-GP-ATTR-TYPE" + i);
			ColumnDescriptor column = new ColumnDescriptor(columnName, columnType, i);
			tupleDescription.add(column);

			if (columnName.equalsIgnoreCase(ColumnDescriptor.recordkeyName))
				recordkeyColumn = column;						
		}		
	}

	/* 
	 * Initializes the messages map
	 */		
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

	private void parseUserData()
	{
		String encoded = getOptionalProperty("X-GP-FRAGMENT-USER-DATA");
		if (encoded == null)
			return;

		userData = Base64.decodeBase64(encoded);
		Log.debug("decoded X-GP-FRAGMENT-USER-DATA: " + new String(userData));
	}
}
