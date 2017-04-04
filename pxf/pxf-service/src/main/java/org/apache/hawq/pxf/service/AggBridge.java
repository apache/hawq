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

package org.apache.hawq.pxf.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hawq.pxf.api.BadRecordException;
import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.StatsAccessor;
import org.apache.hawq.pxf.service.io.Writable;
import org.apache.hawq.pxf.service.utilities.ProtocolData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.collections.map.LRUMap;

/**
 * Bridge class optimized for aggregate queries.
 *
 */
public class AggBridge extends ReadBridge implements Bridge {
    private static final Log LOG = LogFactory.getLog(AggBridge.class);
    /* Avoid resolving rows with the same key twice */
    private LRUMap outputCache;

    public AggBridge(ProtocolData protData) throws Exception {
        super(protData);
    }

    @Override
    public boolean beginIteration() throws Exception {
        /* Initialize LRU cache with 100 items*/
        outputCache = new LRUMap();
        boolean openForReadStatus = super.fileAccessor.openForRead();
        ((StatsAccessor) fileAccessor).retrieveStats();
        return openForReadStatus;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Writable getNext() throws Exception {
        Writable output = null;
        LinkedList<Writable> cachedOutput = null;
        OneRow onerow = null;

        if (!outputQueue.isEmpty()) {
            return outputQueue.pop();
        }

        try {
            while (outputQueue.isEmpty()) {
                onerow = ((StatsAccessor) fileAccessor).emitAggObject();
                if (onerow == null) {
                    break;
                }
                cachedOutput = (LinkedList<Writable>) outputCache.get(onerow.getKey());
                if (cachedOutput == null) {
                    cachedOutput = outputBuilder.makeOutput(fieldsResolver.getFields(onerow));
                    outputCache.put(onerow.getKey(), cachedOutput);
                }
                outputQueue.addAll(cachedOutput);
                if (!outputQueue.isEmpty()) {
                    output = outputQueue.pop();
                    break;
                }
            }
        } catch (Exception ex) {
            LOG.error("Error occurred when reading next object from aggregate bridge:" + ex.getMessage());
            throw ex;
        }

        return output;
    }

}
