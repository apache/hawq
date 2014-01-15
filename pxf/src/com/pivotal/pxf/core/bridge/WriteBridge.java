package com.pivotal.pxf.core.bridge;

import com.pivotal.pxf.api.accessors.WriteAccessor;
import com.pivotal.pxf.api.exception.BadRecordException;
import com.pivotal.pxf.api.format.OneField;
import com.pivotal.pxf.api.format.OneRow;
import com.pivotal.pxf.api.resolvers.WriteResolver;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;
import com.pivotal.pxf.core.format.BridgeInputBuilder;
import com.pivotal.pxf.core.io.Writable;
import com.pivotal.pxf.core.utilities.Utilities;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.util.List;

/*
 * WriteBridge class creates appropriate accessor and resolver.
 * It reads data from inputStream by the resolver,
 * and writes it to the Hadoop storage with the accessor.
 */
public class WriteBridge implements Bridge {
    private static final Log LOG = LogFactory.getLog(WriteBridge.class);
    WriteAccessor fileAccessor = null;
    WriteResolver fieldsResolver = null;
    BridgeInputBuilder inputBuilder;

    /*
     * C'tor - set the implementation of the bridge
     */
    public WriteBridge(InputData input) throws Exception {
        fileAccessor = getFileAccessor(input);
        fieldsResolver = getFieldsResolver(input);
        inputBuilder = new BridgeInputBuilder(input);
    }

    /*
     * Accesses the underlying HDFS file
     */
    public boolean beginIteration() throws Exception {
        return fileAccessor.openForWrite();
    }

    /*
     * Read data from stream, convert it using WriteResolver into OneRow object, and
     * pass to WriteAccessor to write into file.
     */
    @Override
    public boolean setNext(DataInputStream inputStream) throws Exception {

        List<OneField> record = inputBuilder.makeOutput(inputStream);
        if (record == null) {
            close();
            return false;
        }

        OneRow onerow = fieldsResolver.setFields(record);
        if (onerow == null) {
            close();
            return false;
        }
        if (!fileAccessor.writeNextObject(onerow)) {
            close();
            throw new BadRecordException();
        }
        return true;
    }

    private void close() {
        try {
            fileAccessor.closeForWrite();
        } catch (Exception e) {
            LOG.warn("Failed to close bridge resources: " + e.getMessage());
        }
    }

    private static WriteAccessor getFileAccessor(InputData inputData) throws Exception {
        return (WriteAccessor) Utilities.createAnyInstance(InputData.class, inputData.accessor(), inputData);
    }

    private static WriteResolver getFieldsResolver(InputData inputData) throws Exception {
        return (WriteResolver) Utilities.createAnyInstance(InputData.class, inputData.resolver(), inputData);
    }

    @Override
    public Writable getNext() {
        throw new UnsupportedOperationException("getNext is not implemented");
    }

    @Override
    public boolean isThreadSafe() {
        return ((Plugin) fileAccessor).isThreadSafe() && ((Plugin) fieldsResolver).isThreadSafe();
    }
}