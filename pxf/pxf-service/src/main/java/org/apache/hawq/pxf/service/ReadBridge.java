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

import org.apache.hawq.pxf.api.BadRecordException;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.ReadResolver;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.apache.hawq.pxf.api.utilities.Utilities;
import org.apache.hawq.pxf.service.io.Writable;
import org.apache.hawq.pxf.service.utilities.ProtocolData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.nio.charset.CharacterCodingException;
import java.util.LinkedList;
import java.util.zip.ZipException;

/**
 * ReadBridge class creates appropriate accessor and resolver. It will then
 * create the correct output conversion class (e.g. Text or GPDBWritable) and
 * get records from accessor, let resolver deserialize them and reserialize them
 * using the output conversion class. <br>
 * The class handles BadRecordException and other exception type and marks the
 * record as invalid for HAWQ.
 */
public class ReadBridge implements Bridge {
    ReadAccessor fileAccessor = null;
    ReadResolver fieldsResolver = null;
    BridgeOutputBuilder outputBuilder = null;
    LinkedList<Writable> outputQueue = null;

    private static final Log LOG = LogFactory.getLog(ReadBridge.class);

    /**
     * C'tor - set the implementation of the bridge.
     *
     * @param protData input containing accessor and resolver names
     * @throws Exception if accessor or resolver can't be instantiated
     */
    public ReadBridge(ProtocolData protData) throws Exception {
        outputBuilder = new BridgeOutputBuilder(protData);
        outputQueue = new LinkedList<Writable>();
        fileAccessor = getFileAccessor(protData);
        fieldsResolver = getFieldsResolver(protData);
    }

    /**
     * Accesses the underlying HDFS file.
     */
    @Override
    public boolean beginIteration() throws Exception {
        return fileAccessor.openForRead();
    }

    /**
     * Fetches next object from file and turn it into a record that the HAWQ
     * backend can process.
     */
    @Override
    public Writable getNext() throws Exception {
        Writable output = null;
        OneRow onerow = null;

        if (!outputQueue.isEmpty()) {
            return outputQueue.pop();
        }

        try {
            while (outputQueue.isEmpty()) {
                onerow = fileAccessor.readNextObject();
                if (onerow == null) {
                    output = outputBuilder.getPartialLine();
                    if (output != null) {
                        LOG.warn("A partial record in the end of the fragment");
                    }
                    // if there is a partial line, return it now, otherwise it
                    // will return null
                    return output;
                }

                // we checked before that outputQueue is empty, so we can
                // override it.
                outputQueue = outputBuilder.makeOutput(fieldsResolver.getFields(onerow));
                if (!outputQueue.isEmpty()) {
                    output = outputQueue.pop();
                    break;
                }
            }
        } catch (IOException ex) {
            if (!isDataException(ex)) {
                throw ex;
            }
            output = outputBuilder.getErrorOutput(ex);
        } catch (BadRecordException ex) {
            String row_info = "null";
            if (onerow != null) {
                row_info = onerow.toString();
            }
            if (ex.getCause() != null) {
                LOG.debug("BadRecordException " + ex.getCause().toString()
                        + ": " + row_info);
            } else {
                LOG.debug(ex.toString() + ": " + row_info);
            }
            output = outputBuilder.getErrorOutput(ex);
        } catch (Exception ex) {
            throw ex;
        }

        return output;
    }

    /**
     * Close the underlying resource
     */
    public void endIteration() throws Exception {
        try {
            fileAccessor.closeForRead();
        } catch (Exception e) {
            LOG.error("Failed to close bridge resources: " + e.getMessage());
            throw e;
        }
    }

    public static ReadAccessor getFileAccessor(InputData inputData)
            throws Exception {
        return (ReadAccessor) Utilities.createAnyInstance(InputData.class,
                inputData.getAccessor(), inputData);
    }

    public static ReadResolver getFieldsResolver(InputData inputData)
            throws Exception {
        return (ReadResolver) Utilities.createAnyInstance(InputData.class,
                inputData.getResolver(), inputData);
    }

    /*
     * There are many exceptions that inherit IOException. Some of them like
     * EOFException are generated due to a data problem, and not because of an
     * IO/connection problem as the father IOException might lead us to believe.
     * For example, an EOFException will be thrown while fetching a record from
     * a sequence file, if there is a formatting problem in the record. Fetching
     * record from the sequence-file is the responsibility of the accessor so
     * the exception will be thrown from the accessor. We identify this cases by
     * analyzing the exception type, and when we discover that the actual
     * problem was a data problem, we return the errorOutput GPDBWritable.
     */
    protected boolean isDataException(IOException ex) {
        return (ex instanceof EOFException
                || ex instanceof CharacterCodingException
                || ex instanceof CharConversionException
                || ex instanceof UTFDataFormatException || ex instanceof ZipException);
    }

    @Override
    public boolean setNext(DataInputStream inputStream) {
        throw new UnsupportedOperationException("setNext is not implemented");
    }

    @Override
    public boolean isThreadSafe() {
        boolean result = ((Plugin) fileAccessor).isThreadSafe()
                && ((Plugin) fieldsResolver).isThreadSafe();
        LOG.debug("Bridge is " + (result ? "" : "not ") + "thread safe");
        return result;
    }
}
