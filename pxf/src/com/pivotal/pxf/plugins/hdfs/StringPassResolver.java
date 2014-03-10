package com.pivotal.pxf.plugins.hdfs;

import com.pivotal.pxf.api.OneField;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.ReadResolver;
import com.pivotal.pxf.api.WriteResolver;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

import java.util.LinkedList;
import java.util.List;

import static com.pivotal.pxf.api.io.DataType.VARCHAR;

/**
 * StringPassResolver handles "deserialization" and serialization of 
 * String records. StringPassResolver implements IReadResolver and 
 * IWriteResolver interfaces. Returns strings as-is.
 */
public class StringPassResolver extends Plugin implements ReadResolver, WriteResolver {
    // for write
    private OneRow oneRow;

    /**
     * Constructs a StringPassResolver
     * 
     * @param inputData input all input parameters coming from the client request
     * @throws Exception
     */
    public StringPassResolver(InputData inputData) throws Exception {
        super(inputData);
        oneRow = new OneRow();
        this.inputData = inputData;
    }

    /*
     * getFields returns a list of the fields of one record.
     * Each record field is represented by a OneField item.
     * OneField item contains two fields: an integer representing the field type and a Java
     * Object representing the field value.
     */
    @Override
    public List<OneField> getFields(OneRow onerow) {
        /*
         * This call forces a whole text line into a single varchar field and replaces
		 * the proper field separation code can be found in previous revisions. The reasons 
		 * for doing so as this point are:
		 * 1. performance
		 * 2. desire to not replicate text parsing logic from the backend into java
		 */
        String line = (onerow.getData()).toString();
        List<OneField> record = new LinkedList<OneField>();
        record.add(new OneField(VARCHAR.getOID(), line));
        return record;
    }

    /*
     * Creates a OneRow object from the singleton list
     */
    @Override
    public OneRow setFields(List<OneField> record) throws Exception {
        if (((byte[]) record.get(0).val).length == 0) {
            return null;
        }

        oneRow.setData(record.get(0).val);
        return oneRow;
    }
}


