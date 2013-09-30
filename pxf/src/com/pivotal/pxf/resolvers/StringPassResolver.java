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
	private OneRow onerow;
	private static final char LINE_DELIMITER = '\n';
	private static final int BUF_SIZE = 1024; 

	private Log Log;
	
	/*
	 * C'tor
	 */
	public StringPassResolver(InputData input) throws Exception
	{
		super(input);
		buf = new byte[BUF_SIZE];
		onerow = new OneRow();

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
		SimpleText text = new SimpleText();
		int expected = inputStream.available();
		int curLoc = 0;
		byte c = 0;
		
		while (expected > 0) 
		{
			c = (byte) inputStream.read();
			
			buf[curLoc] = c;
			
			curLoc++;
			expected--;	

			if (curLoc == BUF_SIZE)
			{
				// flush 
				text.append(buf, 0, BUF_SIZE);
				curLoc = 0;
			}
			
			if (c==LINE_DELIMITER)
			{
				Log.trace("read one line, size " + curLoc);
				break;
			}	
			
		}
		
		// the buffer doesn't end with a line break.
		// we assume it's the last line.
		if (c != LINE_DELIMITER)
		{
			Log.warn("Stream ended without line break");
		}
		
		if (curLoc > 0)
		{
			// flush 
			text.append(buf, 0, curLoc);
		}
		
		if (text.getLength() > 0)
		{
			onerow.setData(text);
			return onerow;
		}
		
		// else
		return null;
	}
}
	

