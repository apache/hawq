package org.apache.hawq.pxf.api.examples;

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
import org.apache.hawq.pxf.api.WriteAccessor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * PXF Accessor for writing text data rows as lines into a local file.
 *
 * Demo implementation.
 */

public class DemoFileWritableAccessor extends Plugin implements WriteAccessor {

    private OutputStream out;
    /**
     * Constructs a DemoFileWritableAccessor.
     *
     * @param input all input parameters coming from the client request
     */
    public DemoFileWritableAccessor(InputData input) {
        super(input);
    }

    @Override
    public boolean openForWrite() throws Exception {
        String fileName = inputData.getDataSource();

        Path file = FileSystems.getDefault().getPath(fileName);
        if (Files.exists(file)) {
            throw new IOException("File " + file.toString() + " already exists.");
        }

        Path parent = file.getParent();
        if (Files.notExists(parent)) {
            Files.createDirectories(parent);
        }

        writer = new BufferedOutputStream(Files.newOutputStream(file));
        return true;
    }

    @Override
    public boolean writeNextObject(OneRow onerow) throws Exception {
        writer.dos.write((byte[]) onerow.getData());
        return true;
    }

    @Override
    public void closeForWrite() throws Exception {

    }
}
