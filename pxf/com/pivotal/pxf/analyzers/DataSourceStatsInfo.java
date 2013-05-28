package com.pivotal.pxf.analyzers;

import java.io.IOException;
import java.lang.StringBuilder;

import org.codehaus.jackson.map.ObjectMapper;

/*
 * DataSourceStatsInfo is a public class that represents the size 
 * information of given path.
 */
public class DataSourceStatsInfo 
{

	private static final long DEFAULT_BLOCK_SIZE = 67108864L; // 64MB (in bytes)
	private static final long DEFAULT_NUMBER_OF_BLOCKS = 1L;
	private static final long DEFAULT_NUMBER_OF_TUPLES = 1000000L;
	
	private long	blockSize;		// block size (in bytes)	
	private long	numberOfBlocks;	// number of blocks
	private long    numberOfTuples; // number of tuples
	
	public DataSourceStatsInfo(long blockSize,
					     long numberOfBlocks,
						 long numberOfTuples)
	{
		this.setBlockSize(blockSize);
		this.setNumberOfBlocks(numberOfBlocks);
		this.setNumberOfTuples(numberOfTuples);
	}
		
	/*
	 * Default values
	 */
	public DataSourceStatsInfo()
	{
		this(DEFAULT_BLOCK_SIZE, DEFAULT_NUMBER_OF_BLOCKS, DEFAULT_NUMBER_OF_TUPLES);
	}
	
	/*
	 * Given a FragmentsSizeInfo, serialize it in JSON to be used as
	 * the result string for GPDB. An example result is as follows:
	 *
	 * {"PXFFilesSize":{"blockSize":67108864,"numberOfBlocks":1,"numberOfTuples":5}}
	 */
	public static String dataToJSON(DataSourceStatsInfo info) throws IOException
	{
		ObjectMapper	mapper	= new ObjectMapper();
	
		String result = new String("{\"PXFDataSourceStats\":");
		// mapper serializes all members of the class by default
		result += mapper.writeValueAsString(info);
		result += "}";
		
		return result;
	}
	
	/*
	 * Given a size info structure, convert it to be readable. Intended
	 * for debugging purposes only. 'datapath' is the data path part of 
	 * the original URI (e.g., table name, *.csv, etc). 
	 */
	public static String dataToString(DataSourceStatsInfo info, String datapath)
	{
		StringBuilder result = new StringBuilder();
		
		result.append("Statistics information for \"" + datapath + "\" ");
		
		result.append(" Block Size: " + info.blockSize + 
				      ", Number of blocks: " + info.numberOfBlocks +
				      ", Number of tuples: " + info.numberOfTuples);
				
		return result.toString();
	}
	
	public long getBlockSize() {
		return blockSize;
	}

	private void setBlockSize(long blockSize) {
		this.blockSize = blockSize;
	}

	public long getNumberOfBlocks() {
		return numberOfBlocks;
	}

	private void setNumberOfBlocks(long numberOfBlocks) {
		this.numberOfBlocks = numberOfBlocks;
	}

	public long getNumberOfTuples() {
		return numberOfTuples;
	}

	private void setNumberOfTuples(long numberOfTuples) {
		this.numberOfTuples = numberOfTuples;
	}
	
}
