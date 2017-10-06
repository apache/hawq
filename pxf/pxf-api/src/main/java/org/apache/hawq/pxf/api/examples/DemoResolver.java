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

import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadResolver;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import java.util.LinkedList;
import java.util.List;
import static org.apache.hawq.pxf.api.io.DataType.INTEGER;
import static org.apache.hawq.pxf.api.io.DataType.VARCHAR;

/**
 * Class that defines the deserializtion of one record brought from the external input data.
 *
 * Demo implementation that returns record custom format
 */
public class DemoResolver extends Plugin implements ReadResolver {
    /**
     * Constructs the DemoResolver
     *
     * @param metaData the InputData
     */
    public DemoResolver(InputData metaData) {
        super(metaData);
    }

    /**
     * Read the next record
     * The record contains as many fields as defined by the DDL schema.
     *
     * @param row one record
     * @return list of fields or columns
     */
    @Override
    public List<OneField> getFields(OneRow row) throws Exception {
        List<OneField> output = new LinkedList<OneField>();
        Object data = row.getData();

        /* break up the row into fields */
        String[] fields = ((String) data).split(",");
        for(int colIndex=0; colIndex<fields.length; colIndex++) {
            output.add(new OneField(VARCHAR.getOID(), fields[colIndex]));
        }

        return output;
    }
}
