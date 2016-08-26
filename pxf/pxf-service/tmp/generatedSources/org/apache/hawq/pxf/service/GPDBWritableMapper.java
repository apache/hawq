package org.apache.hawq.pxf.service;

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


import org.apache.hawq.pxf.api.UnsupportedTypeException;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.service.io.GPDBWritable;
import org.apache.hawq.pxf.service.io.GPDBWritable.TypeMismatchException;

/*
 * Class for mapping GPDBWritable get functions to java types.
 */
public class GPDBWritableMapper {

    private GPDBWritable gpdbWritable;
    private int type;
    private DataGetter getter = null;

    public GPDBWritableMapper(GPDBWritable gpdbWritable) {
        this.gpdbWritable = gpdbWritable;
    }

    public void setDataType(int type) throws UnsupportedTypeException {
        this.type = type;

        switch (DataType.get(type)) {
            case BOOLEAN:
                getter = new BooleanDataGetter();
                break;
            case BYTEA:
                getter = new BytesDataGetter();
                break;
            case BIGINT:
                getter = new LongDataGetter();
                break;
            case SMALLINT:
                getter = new ShortDataGetter();
                break;
            case INTEGER:
                getter = new IntDataGetter();
                break;
            case TEXT:
                getter = new StringDataGetter();
                break;
            case REAL:
                getter = new FloatDataGetter();
                break;
            case FLOAT8:
                getter = new DoubleDataGetter();
                break;
            default:
                throw new UnsupportedTypeException(
                        "Type " + GPDBWritable.getTypeName(type) +
                                " is not supported by GPDBWritable");
        }
    }

    public Object getData(int colIdx) throws TypeMismatchException {
        return getter.getData(colIdx);
    }

    private interface DataGetter {
        abstract Object getData(int colIdx) throws TypeMismatchException;
    }

    private class BooleanDataGetter implements DataGetter {
        public Object getData(int colIdx) throws TypeMismatchException {
            return gpdbWritable.getBoolean(colIdx);
        }
    }

    private class BytesDataGetter implements DataGetter {
        public Object getData(int colIdx) throws TypeMismatchException {
            return gpdbWritable.getBytes(colIdx);
        }
    }

    private class DoubleDataGetter implements DataGetter {
        public Object getData(int colIdx) throws TypeMismatchException {
            return gpdbWritable.getDouble(colIdx);
        }
    }

    private class FloatDataGetter implements DataGetter {
        public Object getData(int colIdx) throws TypeMismatchException {
            return gpdbWritable.getFloat(colIdx);
        }
    }

    private class IntDataGetter implements DataGetter {
        public Object getData(int colIdx) throws TypeMismatchException {
            return gpdbWritable.getInt(colIdx);
        }
    }

    private class LongDataGetter implements DataGetter {
        public Object getData(int colIdx) throws TypeMismatchException {
            return gpdbWritable.getLong(colIdx);
        }
    }

    private class ShortDataGetter implements DataGetter {
        public Object getData(int colIdx) throws TypeMismatchException {
            return gpdbWritable.getShort(colIdx);
        }
    }

    private class StringDataGetter implements DataGetter {
        public Object getData(int colIdx) throws TypeMismatchException {
            return gpdbWritable.getString(colIdx);
        }
    }

    public String toString() {
        return "getter type = " + GPDBWritable.getTypeName(type);
    }
}
