package com.pivotal.hawq.mapreduce.ao.db;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.ao.file.HAWQAOFileStatus;
import com.pivotal.hawq.mapreduce.file.HAWQFileStatus;
import com.pivotal.hawq.mapreduce.schema.HAWQField;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import org.yaml.snakeyaml.Yaml;

public final class Metadata
{

	private String tableName;
	private String tableEncoding;
	private HAWQSchema schema;
	private String[] schemaStr;
	private String[] attnames;
	private HAWQFileStatus[] filestatus;
	private Database.TableType tableType;

	/**
	 * Initialize Metadata through database
	 * 
	 * @param url
	 *            database url
	 * @param username
	 *            user name
	 * @param password
	 *            password
	 * @param tableName
	 *            table name
	 * @throws Exception
	 */
	public Metadata(String url, String username, String password,
			String tableName) throws Exception
	{
		Database database = null;
		try
		{
			database = new Database();
			database.connectToDatabase(url, username, "");

			this.tableName = tableName;
			tableEncoding = database.getDatabaseEncoding();
			int tableOid = database.getTableOid(tableName);

			tableType = database.getTableType(tableOid);
			/*
			 * GPSQL-972
			 * 
			 * Make different handle for different table
			 */
			switch (tableType)
			{
			case AO_TABLE:
				filestatus = database.getAOTableFileAttributes(tableOid);
			case CO_TABLE:
			case PARQUET_TABLE:
				break;
			default:
				break;
			}

			schema = database.getTableSchema(tableOid, tableName);
			int fieldCount = schema.getFieldCount();
			schemaStr = new String[fieldCount];
			attnames = new String[fieldCount];
			/*
			 * GPSQL-925
			 * 
			 * Before bug fix: for (int i = 1; i < schema.getFieldCount(); i++)
			 * Forget this loop is started from 1, the last element in array is
			 * null
			 */
			for (int i = 1; i <= fieldCount; i++)
			{
				schemaStr[i - 1] = schema.getField(i).asPrimitive().getType()
						.toString();
				attnames[i - 1] = schema.getField(i).getName();
			}
		}
		catch (Exception e)
		{
			throw e;
		}
		finally
		{
			if (database != null)
				database.close();
		}
	}

	/**
	 * Initialize Metadata through metadata file in local filesystem.
	 * 
	 * To get metadata file, please use gpextract first
	 * 
	 * @param path
	 * @throws FileNotFoundException
	 * @throws HAWQException
	 */
	public Metadata(String path) throws FileNotFoundException, HAWQException
	{
		InputStream input = new FileInputStream(new File(path));
		Yaml yaml = new Yaml();
		Map<?, ?> metadata = (Map<?, ?>) yaml.load(input);
		String fileFormat = metadata.get("FileFormat").toString();
		tableName = metadata.get("TableName").toString();
		tableEncoding = metadata.get("Encoding").toString();
		if (fileFormat.equals("AO"))
		{
			// Process schema
			tableType = Database.TableType.AO_TABLE;
			List<?> fieldsInFile = (List<?>) metadata.get("AO_Schema");
			int schemaSize = fieldsInFile.size();
			List<HAWQField> fields = new ArrayList<HAWQField>();
			for (int i = 0; i < schemaSize; ++i)
			{
				Map<?, ?> field = (Map<?, ?>) fieldsInFile.get(i);
				String fieldStr = field.get("type").toString();
				try
				{
					if (fieldStr.startsWith("_"))
					{
						// array type
						if (fieldStr.equals("_int4")
								|| fieldStr.equals("_int8")
								|| fieldStr.equals("_int2")
								|| fieldStr.equals("_float4")
								|| fieldStr.equals("_float8")
								|| fieldStr.equals("_bool")
								|| fieldStr.equals("_time")
								|| fieldStr.equals("_date")
								|| fieldStr.equals("_interval"))
						{
							HAWQPrimitiveField.PrimitiveType type = HAWQPrimitiveField.PrimitiveType
									.valueOf(fieldStr.substring(1)
											.toUpperCase());
							fields.add(HAWQSchema.optional_field_array(type,
									field.get("name").toString()));
						}
						else
							throw new HAWQException(fieldStr
									+ " is not supported yet.");
					}
					else
					{
						HAWQPrimitiveField.PrimitiveType type = HAWQPrimitiveField.PrimitiveType
								.valueOf(fieldStr.toUpperCase());
						fields.add(HAWQSchema.optional_field(type,
								field.get("name").toString()));
					}
				}
				catch (IllegalArgumentException e)
				{
					throw new HAWQException(fieldStr + " is not supported yet.");
				}
			}
			schema = new HAWQSchema(tableName, fields);
			int fieldCount = schema.getFieldCount();
			schemaStr = new String[fieldCount];
			attnames = new String[fieldCount];
			for (int i = 1; i <= schema.getFieldCount(); i++)
			{
				schemaStr[i - 1] = schema.getField(i).asPrimitive().getType()
						.toString();
				attnames[i - 1] = schema.getField(i).getName();
			}

			// Process file attributes
			Map<?, ?> fileLocations = (Map<?, ?>) metadata
					.get("AO_FileLocations");
			List<HAWQAOFileStatus> fileStatus = loadFileAttributes(
					fileLocations, (List<?>) fileLocations.get("Partitions"));
			filestatus = fileStatus.toArray(new HAWQAOFileStatus[fileStatus
					.size()]);
		}
		else
		{
			throw new UnsupportedOperationException(
					"Only append only(row orientation) table is supported");
		}
	}

	private List<HAWQAOFileStatus> loadFileAttributes(Map<?, ?> fileAttributes,
			List<?> partitions)
	{
		int blockSize = Integer.parseInt(fileAttributes.get("Blocksize")
				.toString());
		boolean checksum = Boolean.parseBoolean(fileAttributes.get("Checksum")
				.toString());

		Object obj = fileAttributes.get("CompressionType");
		String compressType;
		if (obj == null)
			compressType = "none";
		else
			compressType = obj.toString();

		List<?> files = (List<?>) fileAttributes.get("Files");
		int fileNum = files.size();
		List<HAWQAOFileStatus> fileStatus = new ArrayList<HAWQAOFileStatus>();
		for (int i = 0; i < fileNum; ++i)
		{
			Map<?, ?> file = (Map<?, ?>) files.get(i);
			String pathStr = file.get("path").toString();
			int length = Integer.parseInt(file.get("size").toString());
			fileStatus.add(new HAWQAOFileStatus(pathStr, length, checksum,
					compressType, blockSize));
		}

		if (partitions != null)
		{
			for (int i = 0; i < partitions.size(); ++i)
				fileStatus.addAll(loadFileAttributes(
						(Map<?, ?>) partitions.get(i), null));
		}
		return fileStatus;
	}

	/**
	 * 
	 * @return type of table, for example: append only, parquet, etc.
	 */
	public Database.TableType getTableType()
	{
		return tableType;
	}

	/**
	 * 
	 * @return name of table
	 */
	public String getTableName()
	{
		return tableName;
	}

	/**
	 * 
	 * @return encoding of table
	 */
	public String getTableEncoding()
	{
		return tableEncoding;
	}

	/**
	 * 
	 * @return schema of table
	 */
	public HAWQSchema getSchema()
	{
		return schema;
	}

	/**
	 * 
	 * @return schema name of table
	 */
	public String[] getSchemaStr()
	{
		return schemaStr;
	}

	/**
	 * 
	 * @return Attributes name of table
	 */
	public String[] getAttnames()
	{
		return attnames;
	}

	/**
	 * 
	 * @return File status of table
	 */
	public HAWQFileStatus[] getFileStatus()
	{
		return filestatus;
	}

	@Override
	public String toString()
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append("TableType:\t");
		buffer.append(tableType);
		buffer.append("\nTableName:\t");
		buffer.append(tableName);
		buffer.append("\nTableEncoding:\t");
		buffer.append(tableEncoding);
		buffer.append("\nAttrNames:\t");
		for (int i = 0; i < attnames.length; ++i)
		{
			buffer.append(attnames[i]);
			if (i != attnames.length - 1)
				buffer.append(',');
		}
		buffer.append("\nSchema:\t\t");
		for (int i = 0; i < schemaStr.length; ++i)
		{
			if (schema.getField(i + 1).isArray())
				buffer.append('_');
			buffer.append(schemaStr[i]);
			if (i != schemaStr.length - 1)
				buffer.append(',');
		}
		buffer.append("\nFileAttribute:\t");
		for (int i = 0; i < filestatus.length; ++i)
		{
			buffer.append(filestatus[i].toString());
			if (i != filestatus.length - 1)
				buffer.append(',');
		}
		return buffer.toString();
	}

}
