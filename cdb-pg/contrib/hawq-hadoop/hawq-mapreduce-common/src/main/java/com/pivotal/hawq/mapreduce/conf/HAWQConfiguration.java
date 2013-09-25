package com.pivotal.hawq.mapreduce.conf;

import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import org.apache.hadoop.conf.Configuration;

/**
 * A container for configuration property names for jobs with HAWQ input/output.
 * 
 * The job can be configured using the static methods in this class.
 * Alternatively, the properties can be set in the configuration with proper
 * values.
 */
public final class HAWQConfiguration
{

	/** table name */
	public static final String TABLE_NAME_PROPERTY = "mapreduce.hawq.table.name";

	/** The compress type of the table */
	public static final String TABLE_TYPE_PROPERTY = "mapreduce.hawq.table.type";

	/** Whether the checksum field is needed */
	public static final String TABLE_CHECKSUM_PROPERTY = "mapreduce.hawq.table.checksum";

	/** The schema of the table */
	public static final String TABLE_SCHEMA_PROPERTY = "mapreduce.hawq.table.schema";

	/** The compress type of the table */
	public static final String TABLE_COMPRESSTYPE_PROPERTY = "mapreduce.hawq.table.compresstype";

	/** The block size of the table */
	public static final String TABLE_BLOCKSIZE_PROPERTY = "mapreduce.hawq.table.blocksize";

	/** The file attributes of the table */
	public static final String TABLE_FILEATTRIBUTES_PROPERTY = "mapreduce.hawq.table.fileattributes";

	/** The encoding of the table */
	public static final String TABLE_ENCODING_PROPERTY = "mapreduce.hawq.table.encoding";

	/*
	 * GOSQL-1047
	 * 
	 * Save version of database
	 */
	/** The version of the database */
	public static final String DATABASE_VERSION_PROPERTY = "mapreduce.hawq.database.version";

	/**
	 * Get whether the table has a checksum from configuration
	 * 
	 * @param conf
	 *            The configuration
	 * @return true if the table has a checksum
	 */
	public static boolean getInputTableChecksum(Configuration conf)
	{
		return conf
				.getBoolean(HAWQConfiguration.TABLE_CHECKSUM_PROPERTY, false);
	}

	/**
	 * Set whether the table has a checksum into configuration
	 * 
	 * @param conf
	 *            the configuration
	 * @param hasChecksum
	 *            true if the table has a checksum
	 */
	public static void setInputTableChecksum(Configuration conf,
			boolean hasChecksum)
	{
		conf.setBoolean(HAWQConfiguration.TABLE_CHECKSUM_PROPERTY, hasChecksum);
	}

	/**
	 * Get the schema of the table from configuration
	 * 
	 * @param conf
	 *            the configuration
	 * @return schema of the table
	 */
	public static HAWQSchema getInputTableSchema(Configuration conf)
	{
		return HAWQSchema.fromString(conf.get(TABLE_SCHEMA_PROPERTY, null));
	}

	/**
	 * Set the schema of the table into configuration
	 * 
	 * @param conf
	 *            the configuration
	 * @param schema
	 *            schema of the table
	 */
	public static void setInputTableSchema(Configuration conf, HAWQSchema schema)
	{
		conf.setStrings(TABLE_SCHEMA_PROPERTY, schema.toString());
	}

	/**
	 * Get the compress type of the table from configuration
	 * 
	 * @param conf
	 *            the configuration
	 * @return compress type of the table
	 */
	public static String getInputTableCompressType(Configuration conf)
	{
		return conf.get(TABLE_COMPRESSTYPE_PROPERTY, "none");
	}

	/**
	 * Set the compress type of the table into configuration
	 * 
	 * @param conf
	 *            the configuration
	 * @param compressType
	 *            compress type of the table
	 */
	public static void setInputTableCompressType(Configuration conf,
			String compressType)
	{
		conf.set(TABLE_COMPRESSTYPE_PROPERTY, compressType);
	}

	/**
	 * Get block size of the table from configuration
	 * 
	 * @param conf
	 *            the configuration
	 * @return block size of the table
	 */
	public static int getInputTableBlockSize(Configuration conf)
	{
		// default 32 KB
		return conf.getInt(TABLE_BLOCKSIZE_PROPERTY, 32 * 1024);
	}

	/**
	 * Set block size of the table into configuration
	 * 
	 * @param conf
	 *            the configuration
	 * @param blockSize
	 *            block size of the table
	 */
	public static void setInputTableBlockSize(Configuration conf, int blockSize)
	{
		conf.setInt(TABLE_BLOCKSIZE_PROPERTY, blockSize);
	}

	/**
	 * Get file attributes of the table from configuration
	 * 
	 * @param conf
	 *            the configuration
	 * @return file attributes of the table
	 */
	public static String[] getInputTableFileAttnames(Configuration conf)
	{
		return conf.getStrings(TABLE_FILEATTRIBUTES_PROPERTY);
	}

	/**
	 * Set attribute names of the table into configuration
	 * 
	 * @param conf
	 *            the configuration
	 * @param attnames
	 *            file attributes of the table
	 */
	public static void setInputTableFileAttnames(Configuration conf,
			String[] attnames)
	{
		conf.setStrings(TABLE_FILEATTRIBUTES_PROPERTY, attnames);
	}

	/**
	 * Get the encoding of the table from configuration
	 * 
	 * @param conf
	 *            the configuration
	 * @return encoding of the table
	 */
	public static String getInputTableEncoding(Configuration conf)
	{
		return conf.get(TABLE_ENCODING_PROPERTY, "UTF8");
	}

	/**
	 * Set the encoding of the table into configuration
	 * 
	 * @param conf
	 *            the configuration
	 * @param encoding
	 *            encoding of the table
	 */
	public static void setInputTableEncoding(Configuration conf, String encoding)
	{
		conf.set(TABLE_ENCODING_PROPERTY, encoding);
	}

	/**
	 * Get the table type of the table from configuration
	 * 
	 * @param conf
	 *            the configuration
	 * @return table type of the table
	 */
	public static String getInputTableType(Configuration conf)
	{
		return conf.get(TABLE_TYPE_PROPERTY, "");
	}

	/**
	 * Set the table type of the table into configuration
	 * 
	 * @param conf
	 *            the configuration
	 * @param tableType
	 *            table type of the table
	 */
	public static void setInputTableType(Configuration conf, String tableType)
	{
		conf.set(TABLE_TYPE_PROPERTY, tableType);
	}

	/*
	 * GOSQL-1047
	 * 
	 * Two functions below supply get/set method for version of database
	 */
	/**
	 * Get version of database from configuration
	 * 
	 * @param conf
	 *            The configuration
	 * @return version of database
	 */
	public static String getDatabaseVersion(Configuration conf)
	{
		return conf.get(HAWQConfiguration.DATABASE_VERSION_PROPERTY);
	}

	/**
	 * Set version of database into configuration
	 * 
	 * @param conf
	 *            the configuration
	 * @param version
	 *            version of database
	 */
	public static void setDatabaseVersion(Configuration conf, String version)
	{
		conf.set(HAWQConfiguration.DATABASE_VERSION_PROPERTY, version);
	}

}
