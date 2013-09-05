package com.pivotal.hawq.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.pivotal.hawq.mapreduce.ao.HAWQAOInputFormat;
import com.pivotal.hawq.mapreduce.ao.db.Database;
import com.pivotal.hawq.mapreduce.ao.db.Metadata;
import com.pivotal.hawq.mapreduce.ao.file.HAWQAOSplit;

public class HAWQInputFormat extends FileInputFormat<Void, HAWQRecord>
{

	private HAWQAOInputFormat aoInputFormat = new HAWQAOInputFormat();
	// private HAWQParquetInputFormat parquetInputFormat = new
	// HAWQParquetInputFormat();

	private static Database.TableType tableType = null;

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException
	{
		switch (tableType)
		{
		case AO_TABLE:
			return aoInputFormat.getSplits(job);
		case PARQUET_TABLE:
			throw new UnsupportedOperationException(
					"Only append only(row orientation) table is supported");
		default:
			throw new IOException("Please call HAWQInputFormat.setInput first");
		}
	}

	@Override
	public RecordReader<Void, HAWQRecord> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException
	{
		Database.TableType type = null;
		if (split instanceof HAWQAOSplit)
			type = Database.TableType.AO_TABLE;
		else
			throw new UnsupportedOperationException(
					"Only append only(row orientation) table is supported");

		switch (type)
		{
		case AO_TABLE:
			return aoInputFormat.createRecordReader(split, context);
		case PARQUET_TABLE:
			// return parquetInputFormat.createRecordReader(split, context);
		default:
			throw new UnsupportedOperationException(
					"Only append only(row orientation) table is supported");
		}
	}

	/**
	 * Initializes the map-part of the job with the appropriate input settings
	 * through connecting to Database.
	 * 
	 * @param conf
	 *            The map-reduce job configuration
	 * @param db_url
	 *            The database URL to connect to
	 * @param username
	 *            The username for setting up a connection to the database
	 * @param password
	 *            The password for setting up a connection to the database
	 * @param tableName
	 *            The name of the table to access to
	 * @throws Exception
	 */
	public static void setInput(Configuration conf, String db_url,
			String username, String password, String tableName)
			throws Exception
	{
		Metadata metadata = new Metadata(db_url, username, password, tableName);
		setInput(conf, metadata);
	}

	/**
	 * Initializes the map-part of the job with the appropriate input settings
	 * through reading metadata file stored in local filesystem.
	 * 
	 * To get metadata file, please use gpextract first
	 * 
	 * @param conf
	 *            The map-reduce job configuration
	 * @param pathStr
	 *            The metadata file path in local filesystem. e.g.
	 *            /home/gpadmin/metadata/postgres_test
	 * @throws Exception
	 */
	public static void setInput(Configuration conf, String pathStr)
			throws Exception
	{
		Metadata metadata = new Metadata(pathStr);
		setInput(conf, metadata);
	}

	private static void setInput(Configuration conf, Metadata metadata)
			throws HAWQException
	{
		/*
		 * GPSQL-972
		 * 
		 * When the input table is not ao table, throw exception
		 */
		tableType = metadata.getTableType();
		switch (tableType)
		{
		case AO_TABLE:
			HAWQAOInputFormat.setInput(conf, metadata);
			break;
		case PARQUET_TABLE:
		default:
			throw new HAWQException(
					"Only append only(row orientation) table is supported");

		}
	}
}
