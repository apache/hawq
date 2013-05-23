package com.pivotal.pxf.resolvers;

import com.pivotal.pxf.hadoop.io.GPDBWritable;
import com.pivotal.pxf.format.OneField;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.utilities.HDFSMetaData;
import com.pivotal.pxf.utilities.RecordkeyAdapter;

import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.io.Text;

/*
 * Class TextResolver handles deserialization of Text records 
 * WritableResolver implements IFieldsResolver exposing one method: GetFields
 */
public class TextResolver implements IFieldsResolver
{
    private HDFSMetaData connectorConfiguration;
	private RecordkeyAdapter recordkeyAdapter = new RecordkeyAdapter();
	
	/* hardcode argument delim is a temporary solution that will be used in case
	 * we decide later on, to open the text line into it's separate fields
	private String delim = new String(",");
	*/
	
	/*
	 * C'tor
	 */
	public TextResolver(HDFSMetaData conf) throws Exception
	{
        connectorConfiguration = conf;
	}

	/*
	 * GetFields returns a list of the fields of one record.
	 * Each record field is represented by a OneField item.
	 * OneField item contains two fields: an integer representing the field type and a Java
	 * Object representing the field value.
	 */
	public List<OneField> GetFields(OneRow onerow) throws Exception
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
}
