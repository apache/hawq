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

import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.pxf.api.BadRecordException;
import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadVectorizedResolver;
import org.apache.hawq.pxf.service.io.Writable;
import org.apache.hawq.pxf.service.utilities.ProtocolData;


public class ReadVectorizedBridge extends ReadBridge {

    private static final Log LOG = LogFactory.getLog(ReadVectorizedBridge.class);

    public ReadVectorizedBridge(ProtocolData protData) throws Exception {
        super(protData);
    }

    @Override
    public Writable getNext() throws Exception {
        Writable output = null;
        OneRow batch = null;

        if (!outputQueue.isEmpty()) {
            return outputQueue.pop();
        }

        try {
            while (outputQueue.isEmpty()) {
                batch = fileAccessor.readNextObject();
                if (batch == null) {
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
                List<List<OneField>> resolvedBatch = ((ReadVectorizedResolver) fieldsResolver).getFieldsForBatch(batch);
                outputQueue = outputBuilder.makeVectorizedOutput(resolvedBatch);
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
            if (batch != null) {
                row_info = batch.toString();
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

    @Override
    public void endIteration() throws Exception {
        fileAccessor.closeForRead();
    }

}
