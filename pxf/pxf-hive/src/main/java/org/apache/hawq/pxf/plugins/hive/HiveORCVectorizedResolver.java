package org.apache.hawq.pxf.plugins.hive;

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

import static org.apache.hawq.pxf.api.io.DataType.BIGINT;
import static org.apache.hawq.pxf.api.io.DataType.BOOLEAN;
import static org.apache.hawq.pxf.api.io.DataType.BPCHAR;
import static org.apache.hawq.pxf.api.io.DataType.BYTEA;
import static org.apache.hawq.pxf.api.io.DataType.DATE;
import static org.apache.hawq.pxf.api.io.DataType.FLOAT8;
import static org.apache.hawq.pxf.api.io.DataType.INTEGER;
import static org.apache.hawq.pxf.api.io.DataType.NUMERIC;
import static org.apache.hawq.pxf.api.io.DataType.REAL;
import static org.apache.hawq.pxf.api.io.DataType.SMALLINT;
import static org.apache.hawq.pxf.api.io.DataType.TEXT;
import static org.apache.hawq.pxf.api.io.DataType.TIMESTAMP;
import static org.apache.hawq.pxf.api.io.DataType.VARCHAR;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.sql.Timestamp;
import java.sql.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadVectorizedResolver;
import org.apache.hawq.pxf.api.UnsupportedTypeException;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.apache.hawq.pxf.plugins.hive.utilities.HiveUtilities;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.ql.exec.vector.*;

/**
 * Class which implements resolving a batch of records at once
 */
public class HiveORCVectorizedResolver extends HiveResolver implements ReadVectorizedResolver {

    private static final Log LOG = LogFactory.getLog(HiveORCVectorizedResolver.class);

    private List<List<OneField>> resolvedBatch;
    private StructObjectInspector soi;

    public HiveORCVectorizedResolver(InputData input) throws Exception {
        super(input);
        try {
            soi = (StructObjectInspector) HiveUtilities.getOrcReader(input).getObjectInspector();
        } catch (Exception e) {
            LOG.error("Unable to create an object inspector.");
            throw e;
        }
    }

    @Override
    public List<List<OneField>> getFieldsForBatch(OneRow batch) {

        Writable writableObject = null;
        Object fieldValue = null;
        VectorizedRowBatch vectorizedBatch = (VectorizedRowBatch) batch.getData();

        /* Allocate empty result set */
        int columnsNumber = inputData.getColumns();
        resolvedBatch = new ArrayList<List<OneField>>(vectorizedBatch.size);

        /* Create empty template row */
        ArrayList<OneField> templateRow = new ArrayList<OneField>(columnsNumber);
        ArrayList<OneField> currentRow = null;
        for (int j = 0; j < inputData.getColumns(); j++) {
            templateRow.add(null);
        }
        /* Replicate template row*/
        for (int i = 0; i < vectorizedBatch.size; i++) {
            currentRow = new ArrayList<OneField>(templateRow);
            resolvedBatch.add(currentRow);
        }

        /* process all columns*/
        List<? extends StructField> allStructFieldRefs = soi.getAllStructFieldRefs();
        for (int columnIndex = 0; columnIndex < vectorizedBatch.numCols; columnIndex++) {
            ObjectInspector oi = allStructFieldRefs.get(columnIndex).getFieldObjectInspector();
            if (oi.getCategory() == Category.PRIMITIVE) {
                PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
                resolvePrimitiveColumn(columnIndex, oi, vectorizedBatch);
            } else {
                throw new UnsupportedTypeException("Unable to resolve column index:" + columnIndex
                        + ". Only primitive types are supported.");
            }
        }

        return resolvedBatch;
    }

    /**
     * Resolves a column of a primitive type out of given batch
     *
     * @param columnIndex     index of the column
     * @param oi              object inspector
     * @param vectorizedBatch input batch or records
     */
    private void resolvePrimitiveColumn(int columnIndex, ObjectInspector oi, VectorizedRowBatch vectorizedBatch) {

        OneField field = null;
        Writable writableObject = null;
        PrimitiveCategory poc = ((PrimitiveObjectInspector) oi).getPrimitiveCategory();
        populatePrimitiveColumn(poc, oi, vectorizedBatch, columnIndex);
    }

    private void addValueToColumn(int columnIndex, int rowIndex, OneField field) {
        List<OneField> row = this.resolvedBatch.get(rowIndex);
        row.set(columnIndex, field);
    }

    private void populatePrimitiveColumn(PrimitiveCategory primitiveCategory, ObjectInspector oi, VectorizedRowBatch vectorizedBatch, int columnIndex) {
        ColumnVector columnVector = vectorizedBatch.cols[columnIndex];
        Object fieldValue = null;
        DataType fieldType = null;

        switch (primitiveCategory) {
            case BOOLEAN: {
                fieldType = BOOLEAN;
                LongColumnVector lcv = (LongColumnVector) columnVector;
                for (int rowIndex = 0; rowIndex < vectorizedBatch.size; rowIndex++) {
                    fieldValue = null;
                    if (lcv != null) {
                        int rowId = lcv.isRepeating ? 0 : rowIndex;
                        if (!lcv.isNull[rowId]) {
                            fieldValue = lcv.vector[rowId] == 1;
                        }
                    }
                    addValueToColumn(columnIndex, rowIndex, new OneField(fieldType.getOID(), fieldValue));
                }
                break;
            }
            case SHORT: {
                fieldType = SMALLINT;
                LongColumnVector lcv = (LongColumnVector) columnVector;
                for (int rowIndex = 0; rowIndex < vectorizedBatch.size; rowIndex++) {
                    fieldValue = null;
                    if (lcv != null) {
                        int rowId = lcv.isRepeating ? 0 : rowIndex;
                        if (!lcv.isNull[rowId]) {
                            fieldValue = (short) lcv.vector[rowId];
                        }
                    }
                    addValueToColumn(columnIndex, rowIndex, new OneField(fieldType.getOID(), fieldValue));
                }
                break;
            }
            case INT: {
                fieldType = INTEGER;
                LongColumnVector lcv = (LongColumnVector) columnVector;
                for (int rowIndex = 0; rowIndex < vectorizedBatch.size; rowIndex++) {
                    fieldValue = null;
                    if (lcv != null) {
                        int rowId = lcv.isRepeating ? 0 : rowIndex;
                        if (!lcv.isNull[rowId]) {
                            fieldValue = (int) lcv.vector[rowId];
                        }
                    }
                    addValueToColumn(columnIndex, rowIndex, new OneField(fieldType.getOID(), fieldValue));
                }
                break;
            }
            case LONG: {
                fieldType = BIGINT;
                LongColumnVector lcv = (LongColumnVector) columnVector;
                for (int rowIndex = 0; rowIndex < vectorizedBatch.size; rowIndex++) {
                    fieldValue = null;
                    if (lcv != null) {
                        int rowId = lcv.isRepeating ? 0 : rowIndex;
                        if (!lcv.isNull[rowId]) {
                            fieldValue = lcv.vector[rowId];
                        }
                    }
                    addValueToColumn(columnIndex, rowIndex, new OneField(fieldType.getOID(), fieldValue));
                }
                break;
            }
            case FLOAT: {
                fieldType = REAL;
                DoubleColumnVector dcv = (DoubleColumnVector) columnVector;
                for (int rowIndex = 0; rowIndex < vectorizedBatch.size; rowIndex++) {
                    fieldValue = null;
                    if (dcv != null) {
                        int rowId = dcv.isRepeating ? 0 : rowIndex;
                        if (!dcv.isNull[rowId]) {
                            fieldValue = (float) dcv.vector[rowId];
                        }
                    }
                    addValueToColumn(columnIndex, rowIndex, new OneField(fieldType.getOID(), fieldValue));
                }
                break;
            }
            case DOUBLE: {
                fieldType = FLOAT8;
                DoubleColumnVector dcv = (DoubleColumnVector) columnVector;
                for (int rowIndex = 0; rowIndex < vectorizedBatch.size; rowIndex++) {
                    fieldValue = null;
                    if (dcv != null) {
                        int rowId = dcv.isRepeating ? 0 : rowIndex;
                        if (!dcv.isNull[rowId]) {
                            fieldValue = dcv.vector[rowId];
                        }
                    }
                    addValueToColumn(columnIndex, rowIndex, new OneField(fieldType.getOID(), fieldValue));
                }
                break;
            }
            case DECIMAL: {
                fieldType = NUMERIC;
                DecimalColumnVector dcv = (DecimalColumnVector) columnVector;
                for (int rowIndex = 0; rowIndex < vectorizedBatch.size; rowIndex++) {
                    fieldValue = null;
                    if (dcv != null) {
                        int rowId = dcv.isRepeating ? 0 : rowIndex;
                        if (!dcv.isNull[rowId]) {
                            fieldValue = dcv.vector[rowId];
                        }
                    }
                    addValueToColumn(columnIndex, rowIndex, new OneField(fieldType.getOID(), fieldValue));
                }
                break;
            }
            case VARCHAR: {
                fieldType = VARCHAR;
                BytesColumnVector bcv = (BytesColumnVector) columnVector;
                for (int rowIndex = 0; rowIndex < vectorizedBatch.size; rowIndex++) {
                    fieldValue = null;
                    if (columnVector != null) {
                        int rowId = bcv.isRepeating ? 0 : rowIndex;
                        if (!bcv.isNull[rowId]) {
                            Text textValue = new Text();
                            textValue.set(bcv.vector[rowIndex], bcv.start[rowIndex], bcv.length[rowIndex]);
                            fieldValue = textValue;
                        }
                    }
                    addValueToColumn(columnIndex, rowIndex, new OneField(fieldType.getOID(), fieldValue));
                }
                break;
            }
            case CHAR: {
                fieldType = BPCHAR;
                BytesColumnVector bcv = (BytesColumnVector) columnVector;
                for (int rowIndex = 0; rowIndex < vectorizedBatch.size; rowIndex++) {
                    fieldValue = null;
                    if (columnVector != null) {
                        int rowId = bcv.isRepeating ? 0 : rowIndex;
                        if (!bcv.isNull[rowId]) {
                            Text textValue = new Text();
                            textValue.set(bcv.vector[rowIndex], bcv.start[rowIndex], bcv.length[rowIndex]);
                            fieldValue = textValue;
                        }
                    }
                    addValueToColumn(columnIndex, rowIndex, new OneField(fieldType.getOID(), fieldValue));
                }
                break;
            }
            case STRING: {
                fieldType = TEXT;
                BytesColumnVector bcv = (BytesColumnVector) columnVector;
                for (int rowIndex = 0; rowIndex < vectorizedBatch.size; rowIndex++) {
                    fieldValue = null;
                    if (columnVector != null) {
                        int rowId = bcv.isRepeating ? 0 : rowIndex;
                        if (!bcv.isNull[rowId]) {
                            Text textValue = new Text();
                            textValue.set(bcv.vector[rowIndex], bcv.start[rowIndex], bcv.length[rowIndex]);
                            fieldValue = textValue;
                        }
                    }
                    addValueToColumn(columnIndex, rowIndex, new OneField(fieldType.getOID(), fieldValue));
                }
                break;
            }
            case BINARY: {
                fieldType = BYTEA;
                BytesColumnVector bcv = (BytesColumnVector) columnVector;
                for (int rowIndex = 0; rowIndex < vectorizedBatch.size; rowIndex++) {
                    fieldValue = null;
                    if (columnVector != null) {
                        int rowId = bcv.isRepeating ? 0 : rowIndex;
                        if (!bcv.isNull[rowId]) {
                            fieldValue = new byte[bcv.length[rowId]];
                            System.arraycopy(bcv.vector[rowId], bcv.start[rowId], fieldValue, 0, bcv.length[rowId]);
                        }
                    }
                    addValueToColumn(columnIndex, rowIndex, new OneField(fieldType.getOID(), fieldValue));
                }
                break;
            }
            case DATE: {
                fieldType = DATE;
                LongColumnVector lcv = (LongColumnVector) columnVector;
                for (int rowIndex = 0; rowIndex < vectorizedBatch.size; rowIndex++) {
                    fieldValue = null;
                    if (lcv != null) {
                        int rowId = lcv.isRepeating ? 0 : rowIndex;
                        if (!lcv.isNull[rowId]) {
                            fieldValue = new Date(DateWritable.daysToMillis((int) lcv.vector[rowIndex]));
                        }
                    }
                    addValueToColumn(columnIndex, rowIndex, new OneField(fieldType.getOID(), fieldValue));
                }
                break;
            }
            case BYTE: {
                fieldType = SMALLINT;
                LongColumnVector lcv = (LongColumnVector) columnVector;
                for (int rowIndex = 0; rowIndex < vectorizedBatch.size; rowIndex++) {
                    fieldValue = null;
                    if (lcv != null) {
                        int rowId = lcv.isRepeating ? 0 : rowIndex;
                        if (!lcv.isNull[rowId]) {
                            fieldValue = (short) lcv.vector[rowIndex];
                        }
                    }
                    addValueToColumn(columnIndex, rowIndex, new OneField(fieldType.getOID(), fieldValue));
                }
                break;
            }
            default: {
                throw new UnsupportedTypeException(oi.getTypeName()
                        + " conversion is not supported by "
                        + getClass().getSimpleName());
            }
        }
    }
}
