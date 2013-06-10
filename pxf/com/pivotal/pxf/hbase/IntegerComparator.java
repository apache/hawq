package com.pivotal.pxf.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * This is a Filter comparator for HBase
 * It is external to GPHBase code
 *
 * To use with HBase it must reside in the classpath of 
 * every region server
 *
 * It converts a value into Long before comparing
 * The filter is good for any integer numeric comparison
 * i.e. integer, bigint, smallint
 */
public class IntegerComparator extends WritableByteArrayComparable 
{
    private Long val;
    
	/*
	 * Used for serialization
	 */
    public IntegerComparator() 
    {
        super();
    }

    public IntegerComparator(Long inVal) 
    {
        this.val = inVal;
    }

    public byte[] getValue() 
    {
        return Bytes.toBytes(val);
    }

	/*
	 * The comparison function
	 *
	 * Currently is uses Long.parseLong
	 */
    public int compareTo(byte[] value, int offset, int length) 
    {
    	/*
    	 * Fix for HD-2610: query fails when recordkey is integer.
    	 */
    	if (length == 0) 
    		return 1; // empty line, can't compare.
    	
		// TODO optimize by parsing the bytes directly. 
		// Maybe we can even determine if it is an int or a string encoded
		String valueAsString = new String(value, offset, length);
        Long valueAsLong = Long.parseLong(valueAsString);
        return val.compareTo(valueAsLong);
    }

	/*
	 * Used for serialization
	 */
    public void readFields(DataInput in) throws IOException {
        Long inVal = in.readLong();
        this.val = inVal;
    }

	/*
	 * Used for serialization
	 */
    public void write(DataOutput out) throws IOException {
        out.writeLong(val);
    }
}
