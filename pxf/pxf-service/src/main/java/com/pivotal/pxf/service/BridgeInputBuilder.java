package com.pivotal.pxf.service;

import com.pivotal.pxf.api.OneField;
import com.pivotal.pxf.api.OutputFormat;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.service.io.GPDBWritable;
import com.pivotal.pxf.service.io.Text;
import com.pivotal.pxf.service.utilities.ProtocolData;

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
