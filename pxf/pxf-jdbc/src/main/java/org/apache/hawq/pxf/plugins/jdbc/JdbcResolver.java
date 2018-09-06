package org.apache.hawq.pxf.plugins.jdbc;

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

import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadResolver;
import org.apache.hawq.pxf.api.UserDataException;
import org.apache.hawq.pxf.api.WriteResolver;

import java.util.List;
import java.util.LinkedList;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * JDBC tables resolver
 */
public class JdbcResolver extends JdbcPlugin implements ReadResolver, WriteResolver {
    /**
     * Class constructor
     */
    public JdbcResolver(InputData input) throws UserDataException {
        super(input);
    }

    /**
     * getFields() implementation
     *
     * @throws SQLException if the provided {@link OneRow} object is invalid
     */
    @Override
    public List<OneField> getFields(OneRow row) throws SQLException {
        ResultSet result = (ResultSet) row.getData();
        LinkedList<OneField> fields = new LinkedList<>();

        for (ColumnDescriptor column : columns) {
            String colName = column.columnName();
            Object value = null;

            OneField oneField = new OneField();
            oneField.type = column.columnTypeCode();

            switch (DataType.get(oneField.type)) {
                case INTEGER:
                    value = result.getInt(colName);
                    break;
                case FLOAT8:
                    value = result.getDouble(colName);
                    break;
                case REAL:
                    value = result.getFloat(colName);
                    break;
                case BIGINT:
                    value = result.getLong(colName);
                    break;
                case SMALLINT:
                    value = result.getShort(colName);
                    break;
                case BOOLEAN:
                    value = result.getBoolean(colName);
                    break;
                case BYTEA:
                    value = result.getBytes(colName);
                    break;
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                case NUMERIC:
                    value = result.getString(colName);
                    break;
                case DATE:
                    value = result.getDate(colName);
                    break;
                case TIMESTAMP:
                    value = result.getTimestamp(colName);
                    break;
                default:
                    throw new UnsupportedOperationException("Field type '" + DataType.get(oneField.type).toString() + "' (column '" + column.toString() + "') is not supported");
            }

            oneField.val = value;
            fields.add(oneField);
        }
        return fields;
    }

    /**
     * setFields() implementation
     *
     * @return OneRow with the data field containing a List<OneField>
     * OneFields are not reordered before being passed to Accessor; at the
     * moment, there is no way to correct the order of the fields if it is not.
     * In practice, the 'record' provided is always ordered the right way.
     *
     * @throws UnsupportedOperationException if field of some type is not supported
     */
    @Override
    public OneRow setFields(List<OneField> record) throws UnsupportedOperationException, ParseException {
        int column_index = 0;
        for (OneField oneField : record) {
            ColumnDescriptor column = columns.get(column_index);
            if (
                LOG.isDebugEnabled() &&
                DataType.get(column.columnTypeCode()) != DataType.get(oneField.type)
            ) {
                LOG.warn("The provided tuple of data may be disordered. Datatype of column with descriptor '" + column.toString() + "' must be '" + DataType.get(column.columnTypeCode()).toString() + "', but actual is '" + DataType.get(oneField.type).toString() + "'");
            }

            // Check that data type is supported
            switch (DataType.get(oneField.type)) {
                case BOOLEAN:
                case INTEGER:
                case FLOAT8:
                case REAL:
                case BIGINT:
                case SMALLINT:
                case NUMERIC:
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                case BYTEA:
                case TIMESTAMP:
                case DATE:
                    break;
                default:
                    throw new UnsupportedOperationException("Field type '" + DataType.get(oneField.type).toString() + "' (column '" + column.toString() + "') is not supported");
            }

            if (
                LOG.isDebugEnabled() &&
                DataType.get(oneField.type) == DataType.BYTEA
            ) {
                LOG.debug("OneField content (conversion from BYTEA): '" + new String((byte[])oneField.val) + "'");
            }

            // Convert TEXT columns into native data types
            if ((DataType.get(oneField.type) == DataType.TEXT) && (DataType.get(column.columnTypeCode()) != DataType.TEXT)) {
                oneField.type = column.columnTypeCode();
                if (oneField.val != null) {
                    String rawVal = (String)oneField.val;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("OneField content (conversion from TEXT): '" + rawVal + "'");
                    }
                    switch (DataType.get(column.columnTypeCode())) {
                        case VARCHAR:
                        case BPCHAR:
                        case TEXT:
                        case BYTEA:
                            break;
                        case BOOLEAN:
                            oneField.val = (Object)Boolean.parseBoolean(rawVal);
                            break;
                        case INTEGER:
                            oneField.val = (Object)Integer.parseInt(rawVal);
                            break;
                        case FLOAT8:
                            oneField.val = (Object)Double.parseDouble(rawVal);
                            break;
                        case REAL:
                            oneField.val = (Object)Float.parseFloat(rawVal);
                            break;
                        case BIGINT:
                            oneField.val = (Object)Long.parseLong(rawVal);
                            break;
                        case SMALLINT:
                            oneField.val = (Object)Short.parseShort(rawVal);
                            break;
                        case NUMERIC:
                            oneField.val = (Object)new BigDecimal(rawVal);
                            break;
                        case TIMESTAMP:
                            boolean isConversionSuccessful = false;
                            for (SimpleDateFormat sdf : timestampSDFs.get()) {
                                try {
                                    java.util.Date parsedTimestamp = sdf.parse(rawVal);
                                    oneField.val = (Object)new Timestamp(parsedTimestamp.getTime());
                                    isConversionSuccessful = true;
                                    break;
                                }
                                catch (ParseException e) {
                                    // pass
                                }
                            }
                            if (!isConversionSuccessful) {
                                throw new ParseException(rawVal, 0);
                            }
                            break;
                        case DATE:
                            oneField.val = (Object)new Date(dateSDF.get().parse(rawVal).getTime());
                            break;
                        default:
                            throw new UnsupportedOperationException("Field type '" + DataType.get(oneField.type).toString() + "' (column '" + column.toString() + "') is not supported");
                    }
                }
            }

            column_index += 1;
        }
        return new OneRow(new LinkedList<OneField>(record));
    }

    /**
     * Decode OneRow object and pass all its contents to a PreparedStatement
     *
     * @throws IOException if data in a OneRow is corrupted
     * @throws SQLException if the given statement is broken
     */
    @SuppressWarnings("unchecked")
    public static void decodeOneRowToPreparedStatement(OneRow row, PreparedStatement statement) throws IOException, SQLException {
        // This is safe: OneRow comes from JdbcResolver
        List<OneField> tuple = (List<OneField>)row.getData();
        for (int i = 1; i <= tuple.size(); i++) {
            OneField field = tuple.get(i - 1);
            switch (DataType.get(field.type)) {
                case INTEGER:
                    if (field.val == null) {
                        statement.setNull(i, Types.INTEGER);
                    }
                    else {
                        statement.setInt(i, (int)field.val);
                    }
                    break;
                case BIGINT:
                    if (field.val == null) {
                        statement.setNull(i, Types.INTEGER);
                    }
                    else {
                        statement.setLong(i, (long)field.val);
                    }
                    break;
                case SMALLINT:
                    if (field.val == null) {
                        statement.setNull(i, Types.INTEGER);
                    }
                    else {
                        statement.setShort(i, (short)field.val);
                    }
                    break;
                case REAL:
                    if (field.val == null) {
                        statement.setNull(i, Types.FLOAT);
                    }
                    else {
                        statement.setFloat(i, (float)field.val);
                    }
                    break;
                case FLOAT8:
                    if (field.val == null) {
                        statement.setNull(i, Types.DOUBLE);
                    }
                    else {
                        statement.setDouble(i, (double)field.val);
                    }
                    break;
                case BOOLEAN:
                    if (field.val == null) {
                        statement.setNull(i, Types.BOOLEAN);
                    }
                    else {
                        statement.setBoolean(i, (boolean)field.val);
                    }
                    break;
                case NUMERIC:
                    if (field.val == null) {
                        statement.setNull(i, Types.NUMERIC);
                    }
                    else {
                        statement.setBigDecimal(i, (BigDecimal)field.val);
                    }
                    break;
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                    if (field.val == null) {
                        statement.setNull(i, Types.VARCHAR);
                    }
                    else {
                        statement.setString(i, (String)field.val);
                    }
                    break;
                case BYTEA:
                    if (field.val == null) {
                        statement.setNull(i, Types.BINARY);
                    }
                    else {
                        statement.setBytes(i, (byte[])field.val);
                    }
                    break;
                case TIMESTAMP:
                    if (field.val == null) {
                        statement.setNull(i, Types.TIMESTAMP);
                    }
                    else {
                        statement.setTimestamp(i, (Timestamp)field.val);
                    }
                    break;
                case DATE:
                    if (field.val == null) {
                        statement.setNull(i, Types.DATE);
                    }
                    else {
                        statement.setDate(i, (Date)field.val);
                    }
                    break;
                default:
                    throw new IOException("The data tuple from JdbcResolver is corrupted");
            }
        }
    }

    private static final Log LOG = LogFactory.getLog(JdbcResolver.class);

    // SimpleDateFormat to parse TEXT into DATE
    private static ThreadLocal<SimpleDateFormat> dateSDF = new ThreadLocal<SimpleDateFormat>() {
        @Override protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };
    // SimpleDateFormat to parse TEXT into TIMESTAMP (with microseconds)
    private static ThreadLocal<SimpleDateFormat[]> timestampSDFs = new ThreadLocal<SimpleDateFormat[]>() {
        @Override protected SimpleDateFormat[] initialValue() {
            SimpleDateFormat[] retRes = {
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS"),
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSS"),
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS"),
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS"),
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S"),
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
                new SimpleDateFormat("yyyy-MM-dd")
            };
            return retRes;
        }
    };
}
