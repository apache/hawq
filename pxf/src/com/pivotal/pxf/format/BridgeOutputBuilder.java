package com.pivotal.pxf.format;

import java.lang.reflect.Array;
import java.util.List;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.Writable;

import com.pivotal.pxf.exception.BadRecordException;
import com.pivotal.pxf.hadoop.io.GPDBWritable;
import com.pivotal.pxf.hadoop.io.GPDBWritable.TypeMismatchException;
import com.pivotal.pxf.utilities.InputData;

/*
 * Class creates the output record that is piped by the java process to the GPDB backend
 * Actually, the output record is serialized and the obtained byte string is piped to the GPDB 
 * segment. The output record will implement Writable, and the mission of BridgeOutputBuilder
 * will be to translate a list of OneField objects (obtained from the Resolver) into 
 * an output record.
 */
public class BridgeOutputBuilder
{
	private InputData inputData;
	private Writable output = null;
	private GPDBWritable errorRecord = null;
	private String delim = ",";
	private String endl = "\n";

	

	/*
	 * C'tor
	 */
	public BridgeOutputBuilder(InputData input)
	{
		inputData = input;
		makeErrorRecord();
	}
	
	/*
	 * We need a separate GPDBWritable record to represent the error record. Just setting
	 * the errorFlag on the "output" GPDBWritable variable is not good enough, since the GPDBWritable is built
	 * only after the first record is read from the file. And if we encounter an error while fetching
	 * the first record from the file, then the output member will be null. The reason we cannot count on
	 * the schema to build the GPDBWritable output variable before reading the first record, is 
	 * because the schema does not account for arrays - we cannot know from the schema the length of
	 * an array. We find out only after fetching the first record.
	 */
	void makeErrorRecord()
	{
		int [] errSchema = {GPDBWritable.TEXT};
		
		if (inputData.outputFormat() != OutputFormat.FORMAT_GPDB_WRITABLE)
			return;
		errorRecord = new GPDBWritable(errSchema);
		errorRecord.setError(true);
	}
	
	/*
	 * Returns the error record
	 */
	public Writable getErrorOutput(Exception ex) throws Exception
	{
		
		if (inputData.outputFormat() == OutputFormat.FORMAT_GPDB_WRITABLE)
			return errorRecord;
		else 
			throw ex;
	}
	
	/*
	 * Translates recFields (obtained from the Resolver) into an output record.
	 */
	public Writable makeOutput(List<OneField> recFields) throws BadRecordException
	{
		if (output == null)
			createOutputRecord(recFields);

		fillOutputRecord(recFields);

		return output;
	}

	/*
	 * Creates the output record based on the configuration output type
	 */	
	void createOutputRecord(List<OneField> recFields)
	{
		if (inputData.outputFormat() == OutputFormat.FORMAT_GPDB_WRITABLE)
			makeGPDBWritableOutput(recFields);
		else /* output is text*/
			output = new SimpleText();
	}

	/*
	 * Creates the GPDBWritable object. The object is created one time
	 * and is refilled from recFields for each record sent 
	 */
    GPDBWritable makeGPDBWritableOutput(List<OneField> recFields)
	{
		int num_actual_fields = recFields.size();
		int [] schema = new int[num_actual_fields];
		
		for (int i = 0; i < num_actual_fields; i++)
			schema[i] = recFields.get(i).type;

		output = new GPDBWritable(schema);

        return (GPDBWritable)output;
	}

	/*
	 * Fills the output record based on the fields in recFields
	 */
	void fillOutputRecord(List<OneField> recFields) throws BadRecordException
	{
		if (inputData.outputFormat() == OutputFormat.FORMAT_GPDB_WRITABLE)
			fillGPDBWritable(recFields);
		else
			fillText(recFields);
	}

	/*
	 * Fills a GPDBWritable object based on recFields
	 */
	void fillGPDBWritable(List<OneField> recFields) throws BadRecordException
	{
		int size = recFields.size();
		if (size == 0) /* size 0 means the resolver couldn't deserialize any of the record fields*/
			throw new BadRecordException("No fields in record");

		for (int i = 0; i < size; i++)
			fillOneGPDBWritableField(recFields.get(i), i);
	}

	/*
	 * Fills a Text object based on recFields
	 */	
	void fillText(List<OneField> recFields)
	{
		int size = recFields.size();
		String strline = new String();

		for (int i = 0; i < size; i++)
		{
			strline = strline.concat((recFields.get(i).val).toString());
			String tail = (i < (size - 1)) ? delim : endl;
			strline = strline.concat(tail);
		}

		((SimpleText)output).set(strline);
	}

    /*
     * Fills one GPDBWritable field
     */
    void fillOneGPDBWritableField(OneField oneField, int i) throws BadRecordException
    {
        int type = oneField.type;
        Object val = oneField.val;
        GPDBWritable GPDBoutput = (GPDBWritable) output;
        try
        {
            switch (type)
            {
                case GPDBWritable.INTEGER:
                    GPDBoutput.setInt(i, (Integer) val);
                    break;
                case GPDBWritable.FLOAT8:
                    GPDBoutput.setDouble(i, (Double) val);
                    break;
                case GPDBWritable.REAL:
                    GPDBoutput.setFloat(i, (Float) val);
                    break;
                case GPDBWritable.BIGINT:
                    GPDBoutput.setLong(i, (Long) val);
                    break;
                case GPDBWritable.SMALLINT:
                    GPDBoutput.setShort(i, (Short) val);
                    break;
                case GPDBWritable.BOOLEAN:
                    GPDBoutput.setBoolean(i, (Boolean) val);
                    break;
                case GPDBWritable.BYTEA:
                    int length = Array.getLength(val);
                    byte[] bts = new byte[length];
                    for (int j = 0; j < length; j++)
                    {
                        bts[j] = Array.getByte(val, j);
                    }
                    GPDBoutput.setBytes(i, bts);
                    break;
                case GPDBWritable.VARCHAR:
                case GPDBWritable.BPCHAR:
                case GPDBWritable.TEXT:
                case GPDBWritable.NUMERIC:
                case GPDBWritable.TIMESTAMP:
                    GPDBoutput.setString(i, ObjectUtils.toString(val, null));
                    break;
                default:
                    String valClassName = (val != null) ? val.getClass().getSimpleName() : null;
                    throw new UnsupportedOperationException(valClassName + " is not supported for gpdb conversion");
            }
        }
        catch (TypeMismatchException e)
        {
            throw new BadRecordException(e);
        }
    }
}
