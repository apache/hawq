package com.pivotal.pxf.resolvers;

import java.io.DataInputStream;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import com.pivotal.pxf.format.OneField;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.format.SimpleText;
import com.pivotal.pxf.hadoop.io.GPDBWritable;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Plugin;

/*
 * Class StringPassResolver handles "deserialization" and serialization of String records 
 * StringPassResolver implements IReadResolver and IWriteResolver interfaces.
 * Returns strings as-is.
 */
public class StringPassResolver extends Plugin implements IReadResolver, IWriteResolver
{
	// for write
	private byte[] buf;
	private OneRow oneRow;
	SimpleText text;
	int curLoc;
	private static final char LINE_DELIMITER = '\n';
	private static final int BUF_SIZE = 1024; 
	private static final int EOF = -1; 


	private Log Log;
	
	/*
	 * C'tor
	 */
	public StringPassResolver(InputData input) throws Exception
	{
		super(input);
		buf = new byte[BUF_SIZE];
		oneRow = new OneRow();
		text = new SimpleText();
		curLoc = 0;

		Log = LogFactory.getLog(StringPassResolver.class);
	}

	/*
	 * getFields returns a list of the fields of one record.
	 * Each record field is represented by a OneField item.
	 * OneField item contains two fields: an integer representing the field type and a Java
	 * Object representing the field value.
	 */
	public List<OneField> getFields(OneRow onerow) throws Exception
	{
		List<OneField> record =  new LinkedList<OneField>();
		String line =  ((Text)(onerow.getData())).toString();
		
		/*
		 * This call forces a whole text line into a single varchar field and replaces 
		 * the proper field separation code can be found in previous revisions. The reasons 
		 * for doing so as this point are:
		 * 1. performance
		 * 2. desire to not replicate text parsing logic from the backend into java
		 */
		
		addOneFieldToRecord(record, GPDBWritable.VARCHAR, line);
		return record;
	}

	/*
	 * Set's OneField item
	 */
	void addOneFieldToRecord(List<OneField> record, int gpdbWritableType, Object val)
	{
		OneField oneField = new OneField();
		oneField.type = gpdbWritableType;
		oneField.val = val;
		
		record.add(oneField);
	}
	
	/*
	 * Reads a row (line) from inputStream and flushes it into SimpleText object.
	 */
	public OneRow setFields(DataInputStream inputStream) throws Exception
	{	
		
		byte c = 0;
		curLoc = 0;
		text.clear();
		
		while ((c = (byte)inputStream.read()) != EOF)
		{
			buf[curLoc] = c;
			curLoc++;
			
			if (c==LINE_DELIMITER)
			{
				Log.trace("read one line, size " + curLoc);
				break;
			}	
			
			if (isBufferFull())
			{
				flushBuffer();
			}
		}

		if (!isBufferEmpty())
		{
			// the buffer doesn't end with a line break.
			if (c == EOF) 
			{
				Log.warn("Stream ended without line break");
			}
			flushBuffer();
		}

		if (text.getLength() > 0)
		{
			oneRow.setData(text);
			return oneRow;
		}
		
		// else
		return null;
	}
	
	private boolean isBufferEmpty()
	{
		return (curLoc == 0);
	}
	
	private boolean isBufferFull()
	{
		return (curLoc == BUF_SIZE);
	}
	
	private void flushBuffer()
	{
		text.append(buf, 0, curLoc);
		curLoc = 0;
	}
}
	

