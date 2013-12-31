import java.io.DataInputStream;
import java.util.List;
import java.util.LinkedList;

import com.pivotal.pxf.format.OneField;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Plugin;
import com.pivotal.pxf.resolvers.IReadResolver;
import com.pivotal.pxf.resolvers.IWriteResolver;
import com.pivotal.pxf.hadoop.io.GPDBWritable;


/*
 * Class that defines the deserializtion of one record brought from the external input data.
 * Every implementation of a deserialization method (Writable, Avro, BP, Thrift, ...)
 * must inherit this abstract class
 * Dummy implementation, for documentation
 */
public class DummyResolver extends Plugin implements IReadResolver, IWriteResolver
{
	private int rowNumber;
	
	public DummyResolver(InputData metaData)
	{
		super(metaData);
		rowNumber = 0;
	}
	
	public List<OneField> getFields(OneRow row) throws Exception
    {
        /* break up the row into fields */
        List<OneField> output = new LinkedList<OneField>();
        String[] fields = ((String)row.getData()).split(",");
        
        output.add(new OneField(GPDBWritable.INTEGER /* type */,Integer.parseInt(fields[0]) /* value */));
        output.add(new OneField(GPDBWritable.VARCHAR ,fields[1]));
        output.add(new OneField(GPDBWritable.INTEGER ,Integer.parseInt(fields[2])));
        
        return output;
    }

	@Override
	public OneRow setFields(DataInputStream inputStream) throws Exception {
		
		/* should read inputStream row by row */
		
		if (rowNumber > 5)
			return null;
		return new OneRow(null, new String("row number " + rowNumber++));
	}
}
