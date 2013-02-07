package com.emc.greenplum.gpdb.hdfsconnector;

import java.lang.IllegalArgumentException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.TException;

import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.MetaException; 
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

/*
 * Fragmenter class for a HIVE table
 *
 * Given a Hive table and its partitions
 * divide the data into fragments (here a data fragment is actually a HDFS file block) and return a list of them.
 * Each data fragment will contain the following information:
 * a. sourceName: full HDFS path to the data file that this data fragment is part of
 * b. hosts: a list of the datanode machines that hold a replica of this block
 * c. userData: file_input_format_name_DELIM_serde_name_DELIM_serialization_properties
 */
public class HiveDataFragmenter extends BaseDataFragmenter
{	
	private	JobConf jobConf;
	HiveClient client;
	private Log Log;
	private List<FragmentInfo> fragmentInfos;
	
	public static final String HIVE_USER_DATA_DELIM = "hive_usr_delim";
	public static final String HIVE_LINEFEED_REPLACE = "hive_linefeed_replace";
	public static final int TODO_REMOVE_THIS_CONST = 1000; 
	
	/* internal class used for parsing the qualified table name received as input to GetFragments() */
	class TblDesc
	{
		public String dbName;
		public String tableName;
	}
	
	/* 
	 * A Hive table unit - means a subset of the HIVE table, where we can say that for all files in this subset,
	 * they all have the same InputFormat and Serde.
	 * For a partitioned table the HiveTableUnit will be one partition and for an unpartitioned table, the 
	 * HiveTableUnit will be the whole table
	 */
	class HiveTablePartition
	{
		public StorageDescriptor storageDesc;
		public Properties properties;
		
		public HiveTablePartition(StorageDescriptor inStorageDesc, Properties inProperties)
		{
			storageDesc = inStorageDesc;
			properties = inProperties;
		}
	}
	
	/*
	 * C'tor
	 */
	public HiveDataFragmenter(BaseMetaData md) throws TTransportException
	{
		super(md);
		Log = LogFactory.getLog(HiveDataFragmenter.class);

		jobConf = new JobConf(new Configuration(), HiveDataFragmenter.class);
		fragmentInfos = new ArrayList<FragmentInfo>();
		client = InitHiveClient();
	}
	
	/*
	 * path is a data source URI that can appear as a file 
	 * name, a directory name  or a wildcard returns the data 
	 * fragments in json format
	 */	
	public String GetFragments(String qualifiedTableName) throws Exception
	{
		TblDesc tblDesc = parseTableQualifiedName(qualifiedTableName);
		if (tblDesc == null) 
			throw new IllegalArgumentException(qualifiedTableName + " is not a valid Hive table name. Should be either <table_name> or <db_name.table_name>");
		
		fetchTableMetaData(tblDesc);		
		
		Log.debug(FragmentInfo.listToString(fragmentInfos, qualifiedTableName));		
		return FragmentInfo.listToJSON(fragmentInfos);
	}
	
	/* Initialize the Hive client */
	private HiveClient InitHiveClient() throws TTransportException
	{
		/*
		 * TODO  - must replace 10000, the hardcoded thrift server port with a generic value
		 */
		TSocket transport = new TSocket("localhost" , 10000);
		TBinaryProtocol protocol = new TBinaryProtocol(transport);
		HiveClient client = new org.apache.hadoop.hive.service.HiveClient(protocol);
		transport.open();
		return client;
	}
	
	/*
	 * parseTableQualifiedName() extract the db_name and table_name from the qualifiedName.
	 * qualifiedName is the Hive table name that the user enters in the CREATE EXTERNAL TABLE statement. It can be
	 * either <table_name> or <db_name.table_name>.
	 */
	TblDesc parseTableQualifiedName(String qualifiedName)
	{
		TblDesc tblDesc = new TblDesc();
		
		String[] toks = qualifiedName.split("[.]");
		if (toks.length == 1)
		{
			tblDesc.dbName = "default";
			tblDesc.tableName = toks[0];
		}
		else if (toks.length == 2)
		{
			tblDesc.dbName = toks[0];
			tblDesc.tableName = toks[1];
		}
		else
			tblDesc = null;
		
		return tblDesc;
	}
	
	/*
	 * Goes over the table partitions metadata and extracts the splits and the InputFormat and Serde per split.
	 */
	private void fetchTableMetaData(TblDesc tblDesc) throws Exception
	{
		Table tbl = client.get_table(tblDesc.dbName, tblDesc.tableName);
		List<Partition> partitions = client.get_partitions(tblDesc.dbName, tblDesc.tableName, (short)TODO_REMOVE_THIS_CONST); // guessing the max partitions - will have to further research this
		StorageDescriptor descTable = tbl.getSd();
		Properties props;
		
		if (partitions.size() == 0)
		{
			props = MetaStoreUtils.getSchema(tbl);
			fetchPartitionMetaData(descTable, props);
		}
		else 
		{
			List<FieldSchema> partitionKeys = tbl.getPartitionKeys();
			
			for (Partition partition : partitions)
			{
				StorageDescriptor descPartition  = partition.getSd();
				props = MetaStoreUtils.getSchema(descPartition,
												 descTable,
												 (Map<String,String>)null, // Map<string, string> parameters - can be empty
												 tblDesc.dbName, tblDesc.tableName, // table name
												 partitionKeys);
				fetchPartitionMetaData(descPartition, props);
			}			
		}
		
	}
	
	/*
	 * Fill a table partition
	 */
	private void fetchPartitionMetaData(StorageDescriptor stdsc, Properties props) throws Exception
	{
		HiveTablePartition tablePartition = new HiveTablePartition(stdsc, props);
		FileInputFormat<?, ?> fformat = makeInputFormat(stdsc.getInputFormat(), jobConf);
		fformat.setInputPaths(jobConf, new Path(stdsc.getLocation()));
		InputSplit[] splits = fformat.getSplits(jobConf, 1);
		
		for (InputSplit split : splits)
		{	
			FileSplit fsp = (FileSplit)split;
			String filepath = fsp.getPath().toUri().getPath();
			filepath = filepath.substring(1); // TODO - remove the '/' from the beginning - will deal with this next 
			
			FragmentInfo fi = new FragmentInfo(filepath,
											   fsp.getLocations());
			fi.setUserData(makeUserData(tablePartition));
			fragmentInfos.add(fi);
		}
	}
	
	/* Create the partition InputFormat  */
	static FileInputFormat<?, ?> makeInputFormat(String inputFormatName, JobConf jobConf) throws Exception
	{		
		Class<?> c = Class.forName(inputFormatName, true, JavaUtils.getClassLoader());
		FileInputFormat<?, ?> fformat = (FileInputFormat<?, ?>)c.newInstance();
	
		if (inputFormatName.compareTo("org.apache.hadoop.mapred.TextInputFormat") == 0) // The only InputFormat that needs a special configuration
			((TextInputFormat)fformat).configure(jobConf);
		
		return fformat;
	}
	
	/*
	 * Turn a Properties class into a string
	 */
	private String serializeProperties(Properties props) throws Exception
	{
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		props.store(outStream, new String("")/* comments */);
		return outStream.toString();		
	}
	
	private String makeUserData(HiveTablePartition partData) throws Exception
	{
		String inputFormatName = partData.storageDesc.getInputFormat();
		String serdeName = partData.storageDesc.getSerdeInfo().getSerializationLib();
		String propertiesString = serializeProperties(partData.properties);
		String userData = inputFormatName + HIVE_USER_DATA_DELIM + serdeName + HIVE_USER_DATA_DELIM + propertiesString;
		
		/*
		 * we replace the LINEFEED, since Jetty will treat wrong this message once it will receive it from the GP Segment.
		 * Jetty will remove all data comming after \n. So we replace all "\n" instances here and return them back in 
		 * BridgeResource, once we passed the Jetty framework. The "\n" occurences are embedded inside the propertiesString.
		 */
		return userData.replaceAll("\n", HIVE_LINEFEED_REPLACE);
	}
	
}




