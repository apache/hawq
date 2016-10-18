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

package org.apache.hawq.pxf.api.utilities;

import java.io.IOException;

import org.apache.hawq.pxf.api.io.DataType;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.JsonProcessingException;

class EnumHawqTypeSerializer extends JsonSerializer<EnumHawqType> {

    @Override
    public void serialize(EnumHawqType value, JsonGenerator generator,
              SerializerProvider provider) throws IOException,
              JsonProcessingException {
      generator.writeString(value.getTypeName());
    }
  }

/**
 * 
 * HAWQ types which could be used in plugins.
 *
 */
@JsonSerialize(using = EnumHawqTypeSerializer.class)
public enum EnumHawqType {
    Int2Type("int2", DataType.SMALLINT),
    Int4Type("int4", DataType.INTEGER),
    Int8Type("int8", DataType.BIGINT),
    Float4Type("float4", DataType.REAL),
    Float8Type("float8", DataType.FLOAT8),
    TextType("text", DataType.TEXT),
    VarcharType("varchar", DataType.VARCHAR, (byte) 1),
    ByteaType("bytea", DataType.BYTEA),
    DateType("date", DataType.DATE),
    TimestampType("timestamp", DataType.TIMESTAMP),
    BoolType("bool", DataType.BOOLEAN),
    NumericType("numeric", DataType.NUMERIC, (byte) 2),
    BpcharType("bpchar", DataType.BPCHAR, (byte) 1);

    private DataType dataType;
    private String typeName;
    private byte modifiersNum;

    EnumHawqType(String typeName, DataType dataType) {
        this.typeName = typeName;
        this.dataType = dataType;
    }

    EnumHawqType(String typeName, DataType dataType, byte modifiersNum) {
        this(typeName, dataType);
        this.modifiersNum = modifiersNum;
    }

    /**
     * 
     * @return name of type
     */
    public String getTypeName() {
        return this.typeName;
    }

    /**
     * 
     * @return number of modifiers for type
     */
    public byte getModifiersNum() {
        return this.modifiersNum;
    }

    /**
     * 
     * @return data type
     * @see DataType
     */
    public DataType getDataType() {
        return this.dataType;
    }

}
