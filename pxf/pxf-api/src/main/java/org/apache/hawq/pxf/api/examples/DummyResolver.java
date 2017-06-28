package org.apache.hawq.pxf.api.examples;

import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadResolver;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import java.util.LinkedList;
import java.util.List;
import static org.apache.hawq.pxf.api.io.DataType.INTEGER;
import static org.apache.hawq.pxf.api.io.DataType.VARCHAR;

public class DummyResolver extends Plugin implements ReadResolver {
    private int rowNumber;
    public DummyResolver(InputData metaData) {
        super(metaData);
        rowNumber = 0;
    }
    @Override
    public List<OneField> getFields(OneRow row) throws Exception {
        /* break up the row into fields */
        List<OneField> output = new LinkedList<OneField>();
        String[] fields = ((String) row.getData()).split(",");
        output.add(new OneField(INTEGER.getOID() /* type */, Integer.parseInt(fields[0]) /* value */));
        output.add(new OneField(VARCHAR.getOID(), fields[1]));
        output.add(new OneField(INTEGER.getOID(), Integer.parseInt(fields[2])));
        return output;
    }
}
