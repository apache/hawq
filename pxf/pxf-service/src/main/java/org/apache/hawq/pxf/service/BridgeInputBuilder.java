package org.apache.hawq.pxf.service;

import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OutputFormat;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.service.io.GPDBWritable;
import org.apache.hawq.pxf.service.io.Text;
import org.apache.hawq.pxf.service.utilities.ProtocolData;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInput;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class BridgeInputBuilder {
    private ProtocolData protocolData;
    private static final Log LOG = LogFactory.getLog(BridgeInputBuilder.class);

    public BridgeInputBuilder(ProtocolData protocolData) throws Exception {
        this.protocolData = protocolData;
    }

    public List<OneField> makeInput(DataInput inputStream) throws Exception {
        if (protocolData.outputFormat() == OutputFormat.TEXT) {
            Text txt = new Text();
            txt.readFields(inputStream);
            return Collections.singletonList(new OneField(DataType.BYTEA.getOID(), txt.getBytes()));
        }

        GPDBWritable gpdbWritable = new GPDBWritable();
        gpdbWritable.readFields(inputStream);

        if (gpdbWritable.isEmpty()) {
            LOG.debug("Reached end of stream");
            return null;
        }

        GPDBWritableMapper mapper = new GPDBWritableMapper(gpdbWritable);
        int[] colTypes = gpdbWritable.getColType();
        List<OneField> record = new LinkedList<OneField>();
        for (int i = 0; i < colTypes.length; i++) {
            mapper.setDataType(colTypes[i]);
            record.add(new OneField(colTypes[i], mapper.getData(i)));
        }

        return record;
    }
}
