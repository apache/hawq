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

package org.apache.hawq.pxf.plugins.hive.utilities;

import java.util.Arrays;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.EnumHawqType;
import org.apache.hawq.pxf.api.UnsupportedTypeException;

/**
 * 
 * Hive types, which are supported by plugin, mapped to HAWQ's types
 * @see EnumHawqType
 */
public enum EnumHiveToHawqType {

    TinyintType("tinyint", EnumHawqType.Int2Type, (byte) 1),
    SmallintType("smallint", EnumHawqType.Int2Type, (byte) 2),
    IntType("int", EnumHawqType.Int4Type),
    BigintType("bigint", EnumHawqType.Int8Type),
    BooleanType("boolean", EnumHawqType.BoolType),
    FloatType("float", EnumHawqType.Float4Type),
    DoubleType("double", EnumHawqType.Float8Type),
    StringType("string", EnumHawqType.TextType),
    BinaryType("binary", EnumHawqType.ByteaType),
    TimestampType("timestamp", EnumHawqType.TimestampType),
    DateType("date", EnumHawqType.DateType),
    DecimalType("decimal", EnumHawqType.NumericType, "[(,)]"),
    VarcharType("varchar", EnumHawqType.VarcharType, "[(,)]"),
    CharType("char", EnumHawqType.BpcharType, "[(,)]"),
    ArrayType("array", EnumHawqType.TextType, "[<,>]"),
    MapType("map", EnumHawqType.TextType, "[<,>]"),
    StructType("struct", EnumHawqType.TextType, "[<,>]"),
    UnionType("uniontype", EnumHawqType.TextType, "[<,>]");

    private String typeName;
    private EnumHawqType hawqType;
    private String splitExpression;
    private byte size;

    EnumHiveToHawqType(String typeName, EnumHawqType hawqType) {
        this.typeName = typeName;
        this.hawqType = hawqType;
    }
    
    EnumHiveToHawqType(String typeName, EnumHawqType hawqType, byte size) {
        this(typeName, hawqType);
        this.setSize(size);
    }

    EnumHiveToHawqType(String typeName, EnumHawqType hawqType, String splitExpression) {
        this(typeName, hawqType);
        this.splitExpression = splitExpression;
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
     * @return corresponding HAWQ type
     */
    public EnumHawqType getHawqType() {
        return this.hawqType;
    }

    /**
     * 
     * @return split by expression
     */
    public String getSplitExpression() {
        return this.splitExpression;
    }

    /**
     * Returns Hive to HAWQ type mapping entry for given Hive type 
     * 
     * @param hiveType full Hive type with modifiers, for example - decimal(10, 0), char(5), binary, array<string>, map<string,float> etc
     * @return corresponding Hive to HAWQ type mapping entry
     * @throws UnsupportedTypeException if there is no corresponding HAWQ type
     */
    public static EnumHiveToHawqType getHiveToHawqType(String hiveType) {
        for (EnumHiveToHawqType t : values()) {
            String hiveTypeName = hiveType;
            String splitExpression = t.getSplitExpression();
            if (splitExpression != null) {
                String[] tokens = hiveType.split(splitExpression);
                hiveTypeName = tokens[0];
            }

            if (t.getTypeName().toLowerCase().equals(hiveTypeName.toLowerCase())) {
                return t;
            }
        }
        throw new UnsupportedTypeException("Unable to map Hive's type: "
                + hiveType + " to HAWQ's type");
    }

    public static EnumHiveToHawqType getCompatibleHawqToHiveType(DataType dataType) {

        SortedSet<EnumHiveToHawqType> types = new TreeSet<EnumHiveToHawqType>(new Comparator<EnumHiveToHawqType>() {

            public int compare(EnumHiveToHawqType a, EnumHiveToHawqType b){
                return Byte.compare(a.getSize(), b.getSize());
            }
        });

        for (EnumHiveToHawqType t : values()) {
            if (t.getHawqType().getDataType().equals(dataType)) {
                types.add(t);
            }
        }

        if (types.size() == 0)
            throw new UnsupportedTypeException("Unable to map HAWQ's type: "
                    + dataType + " to Hive's type");

        return types.last();
    }

    public static String[] extractModifiers(String hiveType) {
        String[] result = null;
        for (EnumHiveToHawqType t : values()) {
            String hiveTypeName = hiveType;
            String splitExpression = t.getSplitExpression();
            if (splitExpression != null) {
                String[] tokens = hiveType.split(splitExpression);
                hiveTypeName = tokens[0];
                result = Arrays.copyOfRange(tokens, 1, tokens.length);
            }
            if (t.getTypeName().toLowerCase().equals(hiveTypeName.toLowerCase())) {
                return result;
            }
        }
        throw new UnsupportedTypeException("Unable to map Hive's type: "
                + hiveType + " to HAWQ's type");
    }

    public byte getSize() {
        return size;
    }

    public void setSize(byte size) {
        this.size = size;
    }
}