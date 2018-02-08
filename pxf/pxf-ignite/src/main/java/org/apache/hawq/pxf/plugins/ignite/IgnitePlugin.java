package org.apache.hawq.pxf.plugins.ignite;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.pxf.api.UserDataException;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;

/**
 * This class resolves the jdbc connection parameter and manages the opening and closing of the jdbc connection.
 * Implemented subclasses: {@link IgniteReadAccessor}.
 */
public class IgnitePlugin extends Plugin {
    private static final Log LOG = LogFactory.getLog(IgnitePlugin.class);

    // Ignite cache
    protected static final String igniteHostDefault = "127.0.0.1:8080";
    protected String igniteHost = null;
    // PXF buffer for Ignite data. '0' is allowed for INSERT queries
    protected static final int bufferSizeDefault = 128;
    protected int bufferSize = bufferSizeDefault;
    // Ignite cache name
    protected String cacheName = null;

    /**
     * Parse and check the InputData
     * @param inputData
     * @throws UserDataException if the request parameter is malformed
     */
    public IgnitePlugin(InputData inputData) throws UserDataException {
        super(inputData);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Constructor started");
        }
        
        igniteHost = inputData.getUserProperty("IGNITE_HOST");
        if (igniteHost == null) {
            igniteHost = igniteHostDefault;
        }

        cacheName = inputData.getUserProperty("IGNITE_CACHE");
        if (cacheName == null) {
            // pass; Ignite will use the default cache
        }

        String bufferSize_str = inputData.getUserProperty("BUFFER_SIZE");
        if (bufferSize_str != null) {
            try {
                bufferSize = Integer.parseInt(bufferSize_str);
                // Zero value is allowed for INSERT queries
                if (bufferSize < 0) {
                    bufferSize = bufferSizeDefault;
                    LOG.warn("Buffer size is incorrect; set to the default value (" + bufferSizeDefault + ")");
                }
            }
            catch (NumberFormatException e) {
                bufferSize = bufferSizeDefault;
                LOG.warn("Buffer size is incorrect; set to the default value (" + bufferSizeDefault + ")");
            }
        }
        // else: bufferSize is already set to bufferSizeDefault

        if (LOG.isDebugEnabled()) {
            LOG.debug("Constructor successful");
        }
    }

    public boolean isThreadSafe() {
        return true;
    }
}
