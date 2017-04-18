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
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.ql.exec.vector.*;

@SuppressWarnings("deprecation")
public class HiveORCBatchResolver extends Plugin implements ReadVectorizedResolver {

    private static final Log LOG = LogFactory.getLog(HiveORCBatchResolver.class);

    private List<List<OneField>> resolvedBatch;
    private StructObjectInspector soi;

    public HiveORCBatchResolver(InputData input) throws Exception {
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

        // Allocate empty result set
        resolvedBatch = new ArrayList<List<OneField>>(vectorizedBatch.size);
        for (int i = 0; i < vectorizedBatch.size; i++) {
            ArrayList<OneField> row = new ArrayList<OneField>(inputData.getColumns());
            resolvedBatch.add(row);
            for (int j = 0; j < inputData.getColumns(); j++) {
                row.add(null);
            }
        }

        /* process all columns*/
        for (int columnIndex = 0; columnIndex < vectorizedBatch.numCols; columnIndex++) {
            ObjectInspector oi = soi.getAllStructFieldRefs().get(columnIndex).getFieldObjectInspector();
            if (oi.getCategory() == Category.PRIMITIVE) {
                PrimitiveObjectInspector poi = (PrimitiveObjectInspector ) oi;
                resolvePrimitiveColumn(columnIndex, oi, vectorizedBatch);
            } else {
                throw new UnsupportedTypeException("Unable to resolve column index:" +  columnIndex + ". Only primitive types are supported.");
            }
        }

        return resolvedBatch;
    }

    private void resolvePrimitiveColumn(int columnIndex, ObjectInspector oi, VectorizedRowBatch vectorizedBatch) {

        OneField field = null;
        Writable writableObject = null;
        /* process all rows from current batch for given column */
        for (int rowIndex = 0; rowIndex < vectorizedBatch.size; rowIndex++) {
            if (vectorizedBatch.cols[columnIndex] != null && !vectorizedBatch.cols[columnIndex].isNull[rowIndex]) {
                writableObject = vectorizedBatch.cols[columnIndex].getWritableObject(rowIndex);
                field = getOneField(((PrimitiveObjectInspector) oi).getPrimitiveCategory(), oi, writableObject);
            } else {
                field = getOneField( ((PrimitiveObjectInspector) oi).getPrimitiveCategory(), oi, null);
            }
            addValueToColumn(columnIndex, rowIndex, field);
        }
    }

    private void addValueToColumn(int columnIndex, int rowIndex, OneField field) {
        List<OneField> row = this.resolvedBatch.get(rowIndex);
        row.set(columnIndex, field);
    }

    private OneField getOneField(PrimitiveCategory primitiveCategory, ObjectInspector oi, Writable w) {
        Object fieldValue = null;
        DataType fieldType = null;
        OneField oneField = null;
        switch (primitiveCategory) {
        case BOOLEAN: {
            if (w != null) {
                fieldValue = ((LongWritable) w).get() == 1;
            }
            fieldType = BOOLEAN;
            break;
        }
        case SHORT: {
            if (w != null) {
                fieldValue = new Short((short) ((LongWritable) w).get());
            }
            fieldType = SMALLINT;
            break;
        }
        case INT: {
            if (w != null) {
                fieldValue = (int) ((LongWritable) w).get();
            }
            fieldType = INTEGER;
            break;
        }
        case LONG: {
            if (w != null) {
                fieldValue = ((LongWritable) w).get();
            }
            fieldType = BIGINT;
            break;
        }
        case FLOAT: {
            if (w != null) {
                fieldValue = (float) ((DoubleWritable) w).get();
            }
            fieldType = REAL;
            break;
        }
        case DOUBLE: {
            if (w != null) {
                fieldValue = ((DoubleWritable) w).get();
            }
            fieldType = FLOAT8;
            break;
        }
        case DECIMAL: {
            if (w != null) {
            HiveDecimal hd = ((HiveDecimalObjectInspector) oi)
                    .getPrimitiveJavaObject(w);
            BigDecimal bd = hd.bigDecimalValue();
            fieldValue = bd.toString();
            }
            fieldType = NUMERIC;
            break;
        }
        case VARCHAR: {
            if (w != null) {
            fieldValue = ((HiveVarcharObjectInspector) oi)
                    .getPrimitiveJavaObject(w);
            }
            fieldType = VARCHAR;
            break;
        }
        case CHAR: {
            if (w != null) {
            fieldValue = ((HiveCharObjectInspector) oi)
                    .getPrimitiveJavaObject(w);
            }
            fieldType = BPCHAR;
            break;
        }
        case STRING: {
            if (w != null) {
                fieldValue = ((StringObjectInspector) oi).getPrimitiveJavaObject(w);
            }
            fieldType = TEXT;
            break;
        }
        case TIMESTAMP: {
            if (w != null) {
            fieldValue = new Timestamp(((LongWritable) w).get() / 1000000);
            }
            fieldType = TIMESTAMP;
            break;
        }
        case DATE: {
            if (w != null) {
                Calendar c = Calendar.getInstance();
                c.setTimeInMillis(0);
                c.add(Calendar.DATE, (int) ((LongWritable) w).get());
                fieldValue = c.getTime();
            }
            fieldType = DATE;
            break;
        }
        case BINARY: {
            if (w != null) {
                fieldValue = ((Text) w).getBytes();
            }
            fieldType = BYTEA;
            break;
        }
        case BYTE: {
            if (w != null) {
                fieldValue = (short) ((LongWritable) w).get();
            }
            fieldType = SMALLINT;
            break;
        }
        default: {
            throw new UnsupportedTypeException(oi.getTypeName()
                    + " conversion is not supported by "
                    + getClass().getSimpleName());
        }
        }
        return new OneField(fieldType.getOID(), fieldValue);
    }
}
