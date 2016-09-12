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
        this.size = size;
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


    /**
     * 
     * @param dataType Hawq data type
     * @return compatible Hive type to given Hawq type, if there are more than one compatible types, it returns one with bigger size
     * @throws UnsupportedTypeException if there is no corresponding Hive type for given Hawq type
     */
    public static EnumHiveToHawqType getCompatibleHiveToHawqType(DataType dataType) {

        SortedSet<EnumHiveToHawqType> types = new TreeSet<EnumHiveToHawqType>(
                new Comparator<EnumHiveToHawqType>() {
                    public int compare(EnumHiveToHawqType a,
                            EnumHiveToHawqType b) {
                        return Byte.compare(a.getSize(), b.getSize());
                    }
                });

        for (EnumHiveToHawqType t : values()) {
            if (t.getHawqType().getDataType().equals(dataType)) {
                types.add(t);
            }
        }

        if (types.size() == 0)
            throw new UnsupportedTypeException("Unable to find compatible Hive type for given HAWQ's type: " + dataType);

        return types.last();
    }

    /**
     *
     * @param hiveToHawqType EnumHiveToHawqType enum
     * @param modifiers Array of Modifiers
     * @return full Hive type name including modifiers. eg: varchar(3)
     * This function is used for datatypes with modifier information
     * such as varchar, char, decimal, etc.
     */
    public static String getFullHiveTypeName(EnumHiveToHawqType hiveToHawqType, Integer[] modifiers) {
        hiveToHawqType.getTypeName();
        if(modifiers != null && modifiers.length > 0) {
            String modExpression = hiveToHawqType.getSplitExpression();
            StringBuilder fullType = new StringBuilder(hiveToHawqType.typeName);
            Character start = modExpression.charAt(1);
            Character separator = modExpression.charAt(2);
            Character end = modExpression.charAt(modExpression.length()-2);
            fullType.append(start);
            int index = 0;
            for (Integer modifier : modifiers) {
                if(index++ > 0) {
                    fullType.append(separator);
                }
                fullType.append(modifier);
            }
            fullType.append(end);
            return fullType.toString();
        } else {
            return hiveToHawqType.getTypeName();
        }
    }

    /**
     * 
     * @param hiveType full Hive data type, i.e. varchar(10) etc
     * @return array of type modifiers
     * @throws UnsupportedTypeException if there is no such Hive type supported
     */
    public static Integer[] extractModifiers(String hiveType) {
        Integer[] result = null;
        for (EnumHiveToHawqType t : values()) {
            String hiveTypeName = hiveType;
            String splitExpression = t.getSplitExpression();
            if (splitExpression != null) {
                String[] tokens = hiveType.split(splitExpression);
                hiveTypeName = tokens[0];
                result = new Integer[tokens.length - 1];
                for (int i = 0; i < tokens.length - 1; i++)
                    result[i] = Integer.parseInt(tokens[i+1]);
            }
            if (t.getTypeName().toLowerCase()
                    .equals(hiveTypeName.toLowerCase())) {
                return result;
            }
        }
        throw new UnsupportedTypeException("Unable to map Hive's type: "
                + hiveType + " to HAWQ's type");
    }

    /**
     * This field is needed to find compatible Hive type when more than one Hive type mapped to HAWQ type
     * @return size of this type in bytes or 0
     */
    public byte getSize() {
        return size;
    }

}