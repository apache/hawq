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

import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.ReadResolver;
import org.apache.hawq.pxf.api.WriteResolver;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

import com.google.gson.JsonArray;



/**
 * PXF-Ignite resolver class
 */
public class IgniteResolver extends IgnitePlugin implements ReadResolver, WriteResolver {
    private static final Log LOG = LogFactory.getLog(IgniteResolver.class);
    
    // HAWQ column descriptors
    private ArrayList<ColumnDescriptor> columns = null;

    public IgniteResolver(InputData input) throws Exception {
        super(input);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Constructor started");
        }

        columns = input.getTupleDescription();
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Constructor successful");
        }
    }

    /**
     * Transform a JsonArray object stored in {@link OneRow} into a List {@link OneField}
     */
    @Override
    public List<OneField> getFields(OneRow row) throws Exception {
        JsonArray result = (JsonArray)row.getData();
        LinkedList<OneField> fields = new LinkedList<OneField>();

        for (int i = 0; i < columns.size(); i++) {
            Object value = null;
            OneField oneField = new OneField(columns.get(i).columnTypeCode(), null);

            // Handle null values
            if (result.get(i).isJsonNull()) {
                oneField.val = null;
                fields.add(oneField);
                continue;
            }
            switch (DataType.get(oneField.type)) {
                case INTEGER:
                    value = result.get(i).getAsInt();
                    break;
                case FLOAT8:
                    value = result.get(i).getAsDouble();
                    break;
                case REAL:
                    value = result.get(i).getAsFloat();
                    break;
                case BIGINT:
                    value = result.get(i).getAsLong();
                    break;
                case SMALLINT:
                    value = result.get(i).getAsShort();
                    break;
                case BOOLEAN:
                    value = result.get(i).getAsBoolean();
                    break;
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                case NUMERIC:
                    value = result.get(i).getAsString();
                    break;
                case BYTEA:
                    value = Base64.decodeBase64(result.get(i).getAsString());
                    break;
                case TIMESTAMP:
                    value = new SimpleDateFormat("MMM d, yyyy hh:mm:ss a").parse(result.get(i).getAsString());
                    break;
                case DATE:
                    value = new SimpleDateFormat("yyyy-MM-dd").parse(result.get(i).getAsString());
                    break;
                default:
                    throw new UnsupportedOperationException("Field type not supported: " + DataType.get(oneField.type).toString()
                            + ", Column : " + columns.get(i).columnName());
            }

            oneField.val = value;
            fields.add(oneField);
        }

        return fields;
    }

    /**
     * Create a query string from a List<OneField> for Ignite database REST API containing INSERT query
     */
    @Override
    public OneRow setFields(List<OneField> record) throws Exception {
        StringBuilder sb = new StringBuilder();
        String fieldDivisor = "";
        
        sb.append("(");
        for (OneField oneField : record) {
            sb.append(fieldDivisor);
            fieldDivisor = ", ";
            switch (DataType.get(oneField.type)) {
                case INTEGER:
                case FLOAT8:
                case REAL:
                case BIGINT:
                case SMALLINT:
                case BOOLEAN:
                    sb.append(String.valueOf(oneField.val));
                    break;
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                case NUMERIC:
                    sb.append("'" + String.valueOf(oneField.val) + "'");
                    break;
                case BYTEA:
                    // TODO: Check this code actually works
                    sb.append("'" + Hex.encodeHexString((byte[])(oneField.val)) + "'");
                    break;
                case TIMESTAMP:
                    sb.append(new SimpleDateFormat("'yyyy-MM-dd hh:mm:ss.SSS'").format(oneField.val));
                    break;
                case DATE:
                    sb.append(new SimpleDateFormat("'yyyy-MM-dd'").format(oneField.val));
                    break;
                default:
                    throw new UnsupportedOperationException("Field type not supported: " + DataType.get(oneField.type).toString());
            }
        }
        sb.append(")");
        return new OneRow(sb.toString());
    }
}
