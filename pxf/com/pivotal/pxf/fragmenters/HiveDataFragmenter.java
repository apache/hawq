package com.pivotal.pxf.fragmenters;

import java.lang.IllegalArgumentException;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.service.HiveClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.pivotal.pxf.utilities.InputData;

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
public class HiveDataFragmenter extends Fragmenter
{	
	private	JobConf jobConf;
	HiveClient client;
	private Log Log;
	
	public static final String HIVE_USER_DATA_DELIM = "hive_usr_delim";
	public static final String HIVE_ONE_PARTITION_DELIM = "hive_one_partition_delim";
	public static final String HIVE_PARTITIONS_DELIM = "hive_partitions_delim";
	public static final String HIVE_TABLE_WITHOUT_PARTITIONS = "hive_table_without_partitions";
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
		public Partition partition;
		public List<FieldSchema> partitionKeys;
		
		public HiveTablePartition(StorageDescriptor inStorageDesc, Properties inProperties)
		{
			storageDesc = inStorageDesc;
			properties = inProperties;
			partition = null;
			partitionKeys = null;
		}
		
		public HiveTablePartition(StorageDescriptor inStorageDesc, 
								  Properties inProperties,
								  Partition inPartition,
								  List<FieldSchema> inPartitionKeys)
		{
			storageDesc = inStorageDesc;
			properties = inProperties;
			partition = inPartition;
			partitionKeys = inPartitionKeys;
		}
		
	}
	
	/*
	 * C'tor
	 */
	public HiveDataFragmenter(InputData md) throws TTransportException
	{
		super(md);
		Log = LogFactory.getLog(HiveDataFragmenter.class);

		jobConf = new JobConf(new Configuration(), HiveDataFragmenter.class);
		client = InitHiveClient();
	}
	
	/*
	 * path is a data source URI that can appear as a file 
	 * name, a directory name  or a wildcard returns the data 
	 * fragments in json format
	 */	
	public FragmentsOutput GetFragments() throws Exception
	{
		TblDesc tblDesc = parseTableQualifiedName(inputData.tableName());
		if (tblDesc == null) 
			throw new IllegalArgumentException(inputData.tableName() + " is not a valid Hive table name. Should be either <table_name> or <db_name.table_name>");
		
		fetchTableMetaData(tblDesc);		

		return fragments;
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
		String tblType = tbl.getTableType();
		
		Log.debug("Table: " + tblDesc.dbName + "." + tblDesc.tableName + ", type: " + tblType);
		
		if (TableType.valueOf(tblType) == TableType.VIRTUAL_VIEW)
			throw new UnsupportedOperationException("PXF doesn't support HIVE views"); 
		
		List<Partition> partitions = client.get_partitions(tblDesc.dbName, tblDesc.tableName, (short)TODO_REMOVE_THIS_CONST); // guessing the max partitions - will have to further research this
		StorageDescriptor descTable = tbl.getSd();
		Properties props;
		
		if (partitions.size() == 0)
		{
			props = MetaStoreUtils.getSchema(tbl);
			fetchMetaDataForSimpleTable(descTable, props);
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
				fetchMetaDataForPrtitionedTable(descPartition, props, partition, partitionKeys);
			}			
		}
		
	}
	
	private void fetchMetaDataForSimpleTable(StorageDescriptor stdsc, Properties props) throws Exception
	{
		HiveTablePartition tablePartition = new HiveTablePartition(stdsc, props);
		fetchMetaData(tablePartition);
	}
	
	private void fetchMetaDataForPrtitionedTable(StorageDescriptor stdsc, 
												 Properties props,
												 Partition partition,
												 List<FieldSchema> partitionKeys) throws Exception
	{
		HiveTablePartition tablePartition = new HiveTablePartition(stdsc, props, partition, partitionKeys);
		fetchMetaData(tablePartition);
	}
	
	/*
	 * Fill a table partition
	 */
	private void fetchMetaData(HiveTablePartition tablePartition) throws Exception
	{
		FileInputFormat<?, ?> fformat = makeInputFormat(tablePartition.storageDesc.getInputFormat(), jobConf);
		FileInputFormat.setInputPaths(jobConf, new Path(tablePartition.storageDesc.getLocation()));
		InputSplit[] splits = fformat.getSplits(jobConf, 1);
		
		for (InputSplit split : splits)
		{	
			FileSplit fsp = (FileSplit)split;
			String filepath = fsp.getPath().toUri().getPath();
			filepath = filepath.substring(1); // TODO - remove the '/' from the beginning - will deal with this next 
			
			fragments.addFragment(filepath, fsp.getLocations(), makeUserData(tablePartition));
		}
	}
	
	/* Create the partition InputFormat  */
	static public FileInputFormat<?, ?> makeInputFormat(String inputFormatName, JobConf jobConf) throws Exception
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
	
	/*
	 * Turn the partition keys into a string
	 */
	private String serializePartitionKeys(HiveTablePartition partData) throws Exception
	{
		if (partData.partition == null) /* this is a simple hive table - there are no partitions */
			return HIVE_TABLE_WITHOUT_PARTITIONS;
		
		String partitionKeys = new String("");
		
		ListIterator<String> valsIter = partData.partition.getValues().listIterator();
		ListIterator<FieldSchema> keysIter = partData.partitionKeys.listIterator();
		while (valsIter.hasNext() && keysIter.hasNext())
		{
			if (!partitionKeys.isEmpty())
				partitionKeys = partitionKeys + HIVE_PARTITIONS_DELIM;
				
			FieldSchema key = keysIter.next();
			String name = key.getName();
			String type = key.getType(); 
			String val = valsIter.next();
			
			String oneLevel = name + HIVE_ONE_PARTITION_DELIM + type + HIVE_ONE_PARTITION_DELIM + val;
			partitionKeys = partitionKeys + oneLevel;			
		}
		
		return partitionKeys;
	}
	
	private byte[] makeUserData(HiveTablePartition partData) throws Exception
	{
		String inputFormatName = partData.storageDesc.getInputFormat();
		String serdeName = partData.storageDesc.getSerdeInfo().getSerializationLib();
		String propertiesString = serializeProperties(partData.properties);
		String partionKeys = serializePartitionKeys(partData);
		String userData = inputFormatName + HIVE_USER_DATA_DELIM + 
						  serdeName + HIVE_USER_DATA_DELIM + 
		                  propertiesString + HIVE_USER_DATA_DELIM +
						  partionKeys;
		
		return userData.getBytes();
	}
	
}




