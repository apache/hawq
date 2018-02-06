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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.apache.hawq.pxf.plugins.hdfs.utilities.HdfsUtilities;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.Iterator;

/**
 * Parquet file accessor.
 * Unit of operation is record.
 */
public class ParquetFileAccessor extends Plugin implements ReadAccessor {
    private ParquetFileReader reader;
    private MessageColumnIO columnIO;
    private RecordIterator recordIterator;
    private MessageType schema;


    private class RecordIterator implements Iterator<OneRow> {

        private final ParquetFileReader reader;
        private PageReadStore currentRowGroup;
        private RecordReader<Group> recordReader;
        private long rowsRemainedInRowGroup;

        public RecordIterator(ParquetFileReader reader) {
            this.reader = reader;
            readNextRowGroup();
        }

        @Override
        public boolean hasNext() {
            return rowsRemainedInRowGroup > 0;
        }

        @Override
        public OneRow next() {
            return new OneRow(null, readNextGroup());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void readNextRowGroup() {
            try {
                currentRowGroup = reader.readNextRowGroup();
            } catch (IOException e) {
                throw new RuntimeException("Error occurred during reading new row group", e);
            }
            if (currentRowGroup == null)
                return;
            rowsRemainedInRowGroup = currentRowGroup.getRowCount();
            recordReader = columnIO.getRecordReader(currentRowGroup, new GroupRecordConverter(schema));
        }

        private Group readNextGroup() {
            Group g = null;
            if (rowsRemainedInRowGroup == 0) {
                readNextRowGroup();
                if (currentRowGroup != null) {
                    g = recordReader.read();
                }
            } else {
                g = recordReader.read();
                if (g == null) {
                    readNextRowGroup();
                    if (currentRowGroup == null) {
                        g = null;
                    } else {
                        rowsRemainedInRowGroup--;
                        g = recordReader.read();
                    }
                } else {
                    rowsRemainedInRowGroup--;
                }
            }

            // If current row group is exhausted
            // try to read next row group so next()
            // will have updated rowsRemainedInRowGroup value
            if (rowsRemainedInRowGroup == 0) {
                readNextRowGroup();
            }
            return g;
        }
    }

    /**
     * Constructs a plugin.
     *
     * @param input the input data
     */
    public ParquetFileAccessor(InputData input) {
        super(input);
        ParquetUserData parquetUserData = HdfsUtilities.parseParquetUserData(input);
        schema = parquetUserData.getSchema();
    }

    @Override
    public boolean openForRead() throws Exception {
        Configuration conf = new Configuration();
        Path file = new Path(inputData.getDataSource());
        FileSplit fileSplit = HdfsUtilities.parseFileSplit(inputData);
        // Create reader for a given split, read a range in file
        reader = new ParquetFileReader(conf, file, ParquetMetadataConverter.range(
                fileSplit.getStart(), fileSplit.getStart() + fileSplit.getLength()));
        ParquetUserData parquetUserData = HdfsUtilities.parseParquetUserData(inputData);
        columnIO = new ColumnIOFactory().getColumnIO(schema);
        recordIterator = new RecordIterator(reader);
        return recordIterator.hasNext();
    }

    /**
     * @return one record or null when split is already exhausted
     */
    @Override
    public OneRow readNextObject() {
        if (recordIterator.hasNext()) {
            return recordIterator.next();
        } else {
            return null;
        }
    }

    @Override
    public void closeForRead() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }
}
