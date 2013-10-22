package com.pivotal.pxf.accessors;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.utilities.InputData;

/*
 * Specialization of HdfsAtomicDataAccessor for \n delimited files with quoted \n
 */
public class QuotedLineBreakAccessor extends HdfsAtomicDataAccessor
{
    private BufferedReader reader;

	/*
	 * C'tor
	 * Creates the QuotedLineBreakAccessor
	 */
	public QuotedLineBreakAccessor(InputData input) throws Exception
	{
		super(input);
 	}

	public boolean openForRead() throws Exception
	{		
		if (!super.openForRead())
			return false;
		reader = new BufferedReader(new InputStreamReader(inp));
        return reader != null;
	}
	
	/*
	 * readNextObject
	 * Fetches one record (maybe partial) from the  file. The record is returned as a Java object.
	 */			
	public OneRow readNextObject() throws IOException
	{
		if (super.readNextObject() == null) /* check if working segment */
			return null;
		
        String next_line = reader.readLine();
        if (next_line == null) /* EOF */
            return null;
        
		return new OneRow(null, new Text(next_line));
	}
}
