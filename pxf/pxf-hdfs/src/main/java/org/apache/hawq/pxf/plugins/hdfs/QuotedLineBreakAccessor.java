package org.apache.hawq.pxf.plugins.hdfs;

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


import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.utilities.InputData;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * A (atomic) PXF Accessor for reading \n delimited files with quoted
 * field delimiter, line delimiter, and quotes. This accessor supports
 * multi-line records, that are read from a single source (non-parallel).
 */
public class QuotedLineBreakAccessor extends HdfsAtomicDataAccessor {
    private BufferedReader reader;

    /**
     * Constructs a QuotedLineBreakAccessor.
     *
     * @param input all input parameters coming from the client request
     */
    public QuotedLineBreakAccessor(InputData input) {
        super(input);
    }

    @Override
    public boolean openForRead() throws Exception {
        if (!super.openForRead()) {
            return false;
        }
        reader = new BufferedReader(new InputStreamReader(inp));
        return true;
    }

    /**
     * Fetches one record (maybe partial) from the  file. The record is returned as a Java object.
     */
    @Override
    public OneRow readNextObject() throws IOException {
        if (super.readNextObject() == null) /* check if working segment */ {
            return null;
        }

        String next_line = reader.readLine();
        if (next_line == null) /* EOF */ {
            return null;
        }

        return new OneRow(null, next_line);
    }
}
