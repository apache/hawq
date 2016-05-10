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
    Int2Type("int2"),
    Int4Type("int4"),
    Int8Type("int8"),
    Float4Type("float4"),
    Float8Type("float8"),
    TextType("text"),
    VarcharType("varchar", (byte) 1, true),
    ByteaType("bytea"),
    DateType("date"),
    TimestampType("timestamp"),
    BoolType("bool"),
    NumericType("numeric", (byte) 2, true),
    BpcharType("bpchar", (byte) 1, true);

    private String typeName;
    private byte modifiersNum;
    private boolean validateIntegerModifiers;

    EnumHawqType(String typeName) {
        this.typeName = typeName;
    }

    EnumHawqType(String typeName, byte modifiersNum) {
        this(typeName);
        this.modifiersNum = modifiersNum;
    }

    EnumHawqType(String typeName, byte modifiersNum, boolean validateIntegerModifiers) {
        this(typeName);
        this.modifiersNum = modifiersNum;
        this.validateIntegerModifiers = validateIntegerModifiers;
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
     * @return whether modifiers should be integers
     */
    public boolean getValidateIntegerModifiers() {
        return this.validateIntegerModifiers;
    }
}



