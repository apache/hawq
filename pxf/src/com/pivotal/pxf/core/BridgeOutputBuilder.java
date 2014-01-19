package com.pivotal.pxf.core;

import java.lang.reflect.Array;
import java.util.List;

import com.pivotal.pxf.api.OneField;
import com.pivotal.pxf.api.OutputFormat;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.core.io.Text;
import com.pivotal.pxf.core.io.Writable;
import org.apache.commons.lang.ObjectUtils;

import com.pivotal.pxf.api.BadRecordException;
import com.pivotal.pxf.core.io.GPDBWritable;
import static com.pivotal.pxf.api.io.DataType.*;
import com.pivotal.pxf.core.io.GPDBWritable.TypeMismatchException;
import com.pivotal.pxf.api.utilities.InputData;

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
		int [] errSchema = {TEXT.getOID()};
		
		if (inputData.outputFormat() != OutputFormat.BINARY)
			return;

        errorRecord = new GPDBWritable(errSchema);
        errorRecord.setError(true);
    }

    /*
     * Returns the error record
     */
    public Writable getErrorOutput(Exception ex) throws Exception
    {
        if (inputData.outputFormat() == OutputFormat.BINARY)
        {
            errorRecord.setString(0, ex.getMessage());
            return errorRecord;
        }
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
		if (inputData.outputFormat() == OutputFormat.BINARY)
			makeGPDBWritableOutput(recFields);
		else /* output is text*/
			output = new Text();
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
		if (inputData.outputFormat() == OutputFormat.BINARY)
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
		StringBuilder strLine = new StringBuilder();

        for (int i = 0; i < size; i++) {
            strLine.append(recFields.get(i).val.toString())
                    .append(i < (size - 1) ? delim : endl);
        }

        ((Text) output).set(strLine.toString());
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
            switch (DataType.get(type))
            {
                case INTEGER:
                    GPDBoutput.setInt(i, (Integer) val);
                    break;
                case FLOAT8:
                    GPDBoutput.setDouble(i, (Double) val);
                    break;
                case REAL:
                    GPDBoutput.setFloat(i, (Float) val);
                    break;
                case BIGINT:
                    GPDBoutput.setLong(i, (Long) val);
                    break;
                case SMALLINT:
                    GPDBoutput.setShort(i, (Short) val);
                    break;
                case BOOLEAN:
                    GPDBoutput.setBoolean(i, (Boolean) val);
                    break;
                case BYTEA:
                    int length = Array.getLength(val);
                    byte[] bts = new byte[length];
                    for (int j = 0; j < length; j++)
                    {
                        bts[j] = Array.getByte(val, j);
                    }
                    GPDBoutput.setBytes(i, bts);
                    break;
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                case NUMERIC:
                case TIMESTAMP:
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
