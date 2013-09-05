package com.pivotal.hawq.mapreduce.ao.file;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class HAWQAOSplit extends FileSplit
{
	private boolean checksum;
	private String compressType = null;
	private int blockSize;

	public HAWQAOSplit()
	{
		super();
	}

	/**
	 * Constructs a split with host information
	 * 
	 * @param file
	 *            the file name
	 * @param start
	 *            the position of the first byte in the file to process
	 * @param length
	 *            the number of bytes in the file to process
	 * @param hosts
	 *            the list of hosts containing the block, possibly null
	 */
	public HAWQAOSplit(Path file, long start, long length, String[] hosts,
			boolean checksum, String compressType, int blockSize)
	{
		super(file, start, length, hosts);
		this.checksum = checksum;
		this.compressType = compressType;
		this.blockSize = blockSize;
	}

	@Override
	public String toString()
	{
		return super.toString() + "+" + checksum + "+" + compressType + "+"
				+ blockSize;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		super.write(out);
		out.writeBoolean(checksum);
		Text.writeString(out, compressType);
		out.writeInt(blockSize);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		super.readFields(in);
		checksum = in.readBoolean();
		compressType = Text.readString(in);
		blockSize = in.readInt();
	}

	public boolean getChecksum()
	{
		return checksum;
	}

	public String getCompressType()
	{
		return compressType;
	}

	public int getBlockSize()
	{
		return blockSize;
	}
}
