package org.apache.hawq.pxf.service;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.hawq.pxf.api.*;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.apache.hawq.pxf.service.io.Writable;
import org.apache.hawq.pxf.service.utilities.ProtocolData;
import org.apache.hawq.pxf.service.utilities.Utilities;
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
    public WriteBridge(ProtocolData protocolData) throws Exception {

        inputBuilder = new BridgeInputBuilder(protocolData);
        /* plugins accept InputData parameters */
        fileAccessor = getFileAccessor(protocolData);
        fieldsResolver = getFieldsResolver(protocolData);

    }

    /*
     * Accesses the underlying HDFS file
     */
    @Override
    public boolean beginIteration() throws Exception {
        return fileAccessor.openForWrite();
    }

    /*
     * Read data from stream, convert it using WriteResolver into OneRow object, and
     * pass to WriteAccessor to write into file.
     */
    @Override
    public boolean setNext(DataInputStream inputStream) throws Exception {

        List<OneField> record = inputBuilder.makeInput(inputStream);
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

    private void close() throws Exception {
        try {
            fileAccessor.closeForWrite();
        } catch (Exception e) {
            LOG.error("Failed to close bridge resources: " + e.getMessage());
            throw e;
        }
    }

    private static WriteAccessor getFileAccessor(InputData inputData) throws Exception {
        return (WriteAccessor) Utilities.createAnyInstance(InputData.class, inputData.getAccessor(), inputData);
    }

    private static WriteResolver getFieldsResolver(InputData inputData) throws Exception {
        return (WriteResolver) Utilities.createAnyInstance(InputData.class, inputData.getResolver(), inputData);
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
