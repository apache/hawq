package com.pivotal.pxf.accessors;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import com.pivotal.pxf.filtering.FilterParser;
import com.pivotal.pxf.filtering.HiveFilterBuilder;
import com.pivotal.pxf.fragmenters.HiveDataFragmenter;
import com.pivotal.pxf.utilities.ColumnDescriptor;
import com.pivotal.pxf.utilities.InputData;


/*
 * Specialization of HdfsSplittableDataAccessor for sequence files
 */
public class HiveAccessor extends HdfsSplittableDataAccessor
{
	private Log Log;
	
	public class Partition
	{
		public String name;
		public String type;
		public String val;
		
		Partition(String inName, String inType, String inVal)
		{
			name = inName;
			type = inType;
			val = inVal;
		}
	}
	
	private List<Partition> partitions;
	/*
	 * C'tor
	 * Creates the InputFormat and the RecordReader object
	 */	
	public HiveAccessor(InputData input) throws Exception
	{
		/*
		 * Unfortunately, Java does not allow us to call a function before calling the base constructor,
		 * otherwise it would have been:  super(input, createInputFormat(input))
		 */
		super(input,
			  (FileInputFormat<?, ?>)null);
		fformat = createInputFormat(input);
		
		Log = LogFactory.getLog(HiveAccessor.class);
 	}
	
	/*
	 * openForRead
	 * Overriding openForRead to enable partition filtering
	 * If partition filter is set and the file currently opened by the accessor does not belong
	 * to the partition we return false and stop processing for this file
	 */	
	public boolean openForRead() throws Exception
	{
		if (!isOurDataInsideFilteredPartition())
			return false;
		
		return super.openForRead();
	}
	
	/*
	 * Override virtual method to create specialized record reader
	 */
	protected Object getReader(JobConf jobConf, InputSplit split) throws IOException
	{
		return fformat.getRecordReader(split, jobConf, Reporter.NULL);
	}
	
	private FileInputFormat<?, ?> createInputFormat(InputData input) throws Exception
	{
		String userData = new String(input.getFragmentUserData());
		String[] toks = userData.split(HiveDataFragmenter.HIVE_UD_DELIM);
		initPartitionFields(toks[3]);
		return HiveDataFragmenter.makeInputFormat(toks[0]/* inputFormat name */, jobConf);
	}
	
	/*
	 * The partition fields are initialized  one time  base on userData provided by the fragmenter
	 */
	private void initPartitionFields(String partitionKeys)
	{
		partitions	= new LinkedList<Partition>();
		if (partitionKeys.compareTo(HiveDataFragmenter.HIVE_NO_PART_TBL) == 0)
			return;
		
		String[] partitionLevels = partitionKeys.split(HiveDataFragmenter.HIVE_PARTITIONS_DELIM);
		for (String partLevel : partitionLevels)
		{
			String[] levelKey = partLevel.split(HiveDataFragmenter.HIVE_1_PART_DELIM);
			String name = levelKey[0];
			String type = levelKey[1];
			String val = levelKey[2];
			partitions.add(new Partition(name, type, val));			
		}		
	}	
	
	private boolean isOurDataInsideFilteredPartition() throws Exception
	{
		boolean returnData = true;
		if (!inputData.hasFilter())
			return returnData;
		
		String filterStr = inputData.filterString();
		HiveFilterBuilder eval = new HiveFilterBuilder(inputData);
		Object filter = eval.getFilterObject(filterStr);
		
		returnData = isFiltered(partitions, filter);
		
		String dataSource = inputData.path();
		int segmentId = inputData.segmentId();
		Log.debug("segmentId: " + segmentId  + " " + dataSource + "--" + filterStr + "returnData: " + returnData);
			
		if (filter instanceof List)
		{
			for (Object f : (List)filter)
				printOneBasicFilter(f);
		}
		else 
			printOneBasicFilter(filter);

		return returnData;
	}
	
	private boolean isFiltered(List<Partition> partitionFields, Object filter)
	{
		if (filter instanceof List)
		{
			/*
			 * We are going over each filter in the filters list and test it against all the partition fields
			 * since filters are connected only by AND operators, its enough for one filter to fail in order to
			 * deny this data.
			 */
			for (Object f : (List)filter)
			{
				if (testOneFilter(partitionFields, f, inputData) == false)
					return false;
			}
			return true;
		}
		 
		return testOneFilter(partitionFields, filter, inputData);
	}
	
	/*
	 * We are testing  one filter against all the partition fields. 
	 * The filter has the form "fieldA = valueA".
	 * The partitions have the form partitionOne=valueOne/partitionTwo=ValueTwo/partitionThree=valueThree
	 * 1. For a filter to match one of the partitions, lets say partitionA for example, we need:
	 * fieldA = partittionOne and valueA = valueOne. If this condition occurs, we return true.
	 * 2. If fieldA does not match any one of the partition fields we also return true, it means we ignore this filter
	 * because it is not on a partition field.
	 * 3. If fieldA = partittionOne and valueA != valueOne, then we return false.
	 */
	private boolean testOneFilter(List<Partition> partitionFields, Object filter, InputData input)
	{
		// Let's look first at the filter
		FilterParser.BasicFilter bFilter = (FilterParser.BasicFilter)filter;
		
		boolean isFilterOperationEqual = (bFilter.getOperation() == FilterParser.Operation.HDOP_EQ);
		if (isFilterOperationEqual == false) /* in case this is not an "equality filter" we ignore it here - in partition filtering */
			return true;
		
		int filterColumnIndex = bFilter.getColumn().index();
		String filterValue = bFilter.getConstant().constant().toString();
		ColumnDescriptor filterColumn = input.getColumn(filterColumnIndex);
		String filterColumnName = filterColumn.columnName();

		for (Partition partition : partitionFields)
		{
			if (filterColumnName.compareTo(partition.name) == 0)
			{
				if (filterValue.compareTo(partition.val) == 0)
					return true;
				else
					return false; /* the filter field matches a partition field, but the values do not match */
			}
		} 
		
		/* filter field did not match any partition field, so we ignore this filter and hence return true */
		return true;
	}
	
	private void printOneBasicFilter(Object filter)
	{
		FilterParser.BasicFilter bFilter = (FilterParser.BasicFilter)filter;
		boolean isOperationEqual = (bFilter.getOperation() == FilterParser.Operation.HDOP_EQ);
		int columnIndex = bFilter.getColumn().index();
		String value = bFilter.getConstant().constant().toString();
		Log.debug("isOperationEqual:  " + isOperationEqual + " columnIndex:  " + columnIndex + " value:  " + value);		
	}
	
}
