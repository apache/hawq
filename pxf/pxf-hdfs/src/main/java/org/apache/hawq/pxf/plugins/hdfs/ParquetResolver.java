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

import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadResolver;
import org.apache.hawq.pxf.api.UnsupportedTypeException;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;

import org.apache.hawq.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

public class ParquetResolver extends Plugin implements ReadResolver {

    public static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    public static final long MILLIS_IN_DAY = 24 * 3600 * 1000;

    /**
     * Constructs the ParquetResolver
     *
     * @param metaData the InputData
     */
    public ParquetResolver(InputData metaData) {

        super(metaData);
    }

    /**
     * {@inheritDoc}
     * @param schema the MessageType instance, which is obtained from the Parquet file footer
     */
    public List<OneField> getFields(OneRow row, MessageType schema) throws Exception
    {
      ParquetUserData parquetUserData = new ParquetUserData(schema);
      Group g = (Group) row.getData();
      List<OneField> output = resolveRecord(parquetUserData, g);
      return output;
    }

    @Override
    public List<OneField> getFields(OneRow row) throws Exception {
        Object data = row.getData();
        ParquetUserData parquetUserData = HdfsUtilities.parseParquetUserData(inputData);
        Group g = (Group) data;
        List<OneField> output = resolveRecord(parquetUserData, g);

        return output;
    }

    private List<OneField> resolveRecord(ParquetUserData userData, Group g) {
        List<OneField> output = new LinkedList<OneField>();

        for (int i = 0; i < userData.getSchema().getFieldCount(); i++) {
            if (userData.getSchema().getType(i).isPrimitive()) {
                output.add(resolvePrimitive(i, g, userData.getSchema().getType(i)));
            } else {
                throw new UnsupportedTypeException("Only primitive types are supported.");
            }
        }
        return output;
    }

    private OneField resolvePrimitive(Integer columnIndex, Group g, Type type) {
        OneField field = new OneField();
        OriginalType originalType = type.getOriginalType();
        PrimitiveType primitiveType = type.asPrimitiveType();
        switch (primitiveType.getPrimitiveTypeName()) {
            case BINARY: {
                if (originalType == null) {
                    field.type = DataType.BYTEA.getOID();
                    field.val = g.getFieldRepetitionCount(columnIndex) == 0 ?
                            null : g.getBinary(columnIndex, 0).getBytes();
                } else if (originalType == OriginalType.DATE) { // DATE type
                    field.type = DataType.DATE.getOID();
                    field.val = g.getFieldRepetitionCount(columnIndex) == 0 ? null : g.getString(columnIndex, 0);
                } else if (originalType == OriginalType.TIMESTAMP_MILLIS) { // TIMESTAMP type
                    field.type = DataType.TIMESTAMP.getOID();
                    field.val = g.getFieldRepetitionCount(columnIndex) == 0 ? null : g.getString(columnIndex, 0);
                } else {
                    field.type = DataType.TEXT.getOID();
                    field.val = g.getFieldRepetitionCount(columnIndex) == 0 ?
                            null : g.getString(columnIndex, 0);
                }
                break;
            }
            case INT32: {
                if (originalType == OriginalType.INT_8 || originalType == OriginalType.INT_16) {
                    field.type = DataType.SMALLINT.getOID();
                    field.val = g.getFieldRepetitionCount(columnIndex) == 0 ?
                            null : (short) g.getInteger(columnIndex, 0);
                } else {
                    field.type = DataType.INTEGER.getOID();
                    field.val = g.getFieldRepetitionCount(columnIndex) == 0 ?
                            null : g.getInteger(columnIndex, 0);
                }
                break;
            }
            case INT64: {
                field.type = DataType.BIGINT.getOID();
                field.val = g.getFieldRepetitionCount(columnIndex) == 0 ?
                        null : g.getLong(columnIndex, 0);
                break;
            }
            case DOUBLE: {
                field.type = DataType.FLOAT8.getOID();
                field.val = g.getFieldRepetitionCount(columnIndex) == 0 ?
                        null : g.getDouble(columnIndex, 0);
                break;
            }
            case INT96: {
                field.type = DataType.TIMESTAMP.getOID();
                Timestamp ts = g.getFieldRepetitionCount(columnIndex) == 0 ?
                        null : bytesToTimestamp(g.getInt96(columnIndex, 0).getBytes());
                field.val = ts;
                break;
            }
            case FLOAT: {
                field.type = DataType.REAL.getOID();
                field.val = g.getFieldRepetitionCount(columnIndex) == 0 ?
                        null : g.getFloat(columnIndex, 0);
                break;
            }
            case FIXED_LEN_BYTE_ARRAY: {
                field.type = DataType.NUMERIC.getOID();
                if (g.getFieldRepetitionCount(columnIndex) > 0) {
                    int scale = type.asPrimitiveType().getDecimalMetadata().getScale();
                    BigDecimal bd = new BigDecimal(new BigInteger(g.getBinary(columnIndex, 0).getBytes()), scale);
                    field.val = bd;
                }
                break;
            }
            case BOOLEAN: {
                field.type = DataType.BOOLEAN.getOID();
                field.val = g.getFieldRepetitionCount(columnIndex) == 0 ?
                        null : g.getBoolean(columnIndex, 0);
                break;
            }
            default: {
                throw new UnsupportedTypeException("Type " + primitiveType.getPrimitiveTypeName()
                        + "is not supported");
            }
        }
        return field;
    }

    private Timestamp bytesToTimestamp(byte[] bytes) {

        long numberOfDays = ByteBuffer.wrap(new byte[]{
                bytes[7],
                bytes[6],
                bytes[5],
                bytes[4],
                bytes[3],
                bytes[2],
                bytes[1],
                bytes[0]
        }).getLong();

        int julianDays = (ByteBuffer.wrap(new byte[]{bytes[11],
                bytes[10],
                bytes[9],
                bytes[8]
        })).getInt();
        long unixTimeMs = (julianDays - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY + numberOfDays / 1000000;
        Timestamp ts = new Timestamp(unixTimeMs);
        return ts;

    }
}
