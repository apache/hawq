import com.pivotal.pxf.api.format.OneField;
import com.pivotal.pxf.api.format.OneRow;
import com.pivotal.pxf.api.resolvers.ReadResolver;
import com.pivotal.pxf.api.resolvers.WriteResolver;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

import java.util.LinkedList;
import java.util.List;

import static com.pivotal.pxf.api.io.DataType.INTEGER;
import static com.pivotal.pxf.api.io.DataType.VARCHAR;


/*
 * Class that defines the deserializtion of one record brought from the external input data.
 * Every implementation of a deserialization method (Writable, Avro, BP, Thrift, ...)
 * must inherit this abstract class
 * Dummy implementation, for documentation
 */
public class DummyResolver extends Plugin implements ReadResolver, WriteResolver {
    private int rowNumber;

    public DummyResolver(InputData metaData) {
        super(metaData);
        rowNumber = 0;
    }

    public List<OneField> getFields(OneRow row) throws Exception {
        /* break up the row into fields */
        List<OneField> output = new LinkedList<OneField>();
        String[] fields = ((String) row.getData()).split(",");

        output.add(new OneField(INTEGER.getOID() /* type */, Integer.parseInt(fields[0]) /* value */));
        output.add(new OneField(VARCHAR.getOID(), fields[1]));
        output.add(new OneField(INTEGER.getOID(), Integer.parseInt(fields[2])));

        return output;
    }

    @Override
    public OneRow setFields(List<OneField> record) throws Exception {
        /* should read inputStream row by row */
        return rowNumber > 5
                ? null
                : new OneRow(null, "row number " + rowNumber++);
    }
}
