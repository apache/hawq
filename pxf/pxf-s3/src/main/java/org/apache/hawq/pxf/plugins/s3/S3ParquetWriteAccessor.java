package org.apache.hawq.pxf.plugins.s3;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

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

import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.WriteAccessor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

// See https://blog.cloudera.com/blog/2014/05/how-to-convert-existing-data-into-parquet/
public class S3ParquetWriteAccessor extends Plugin implements WriteAccessor {

    private static final Log LOG = LogFactory.getLog(S3ParquetWriteAccessor.class);
    ParquetWriter<GenericRecord> writer = null;

    public S3ParquetWriteAccessor(InputData input) {
        super(input);
        LOG.info("Constructor");
    }

    @Override
    public boolean openForWrite() throws Exception {
        String fileName = inputData.getDataSource();
        Schema avroSchema = AvroUtil.schemaFromInputData(this.inputData);
        LOG.info("openForWrite(): fileName = " + fileName); // /pxf-s3-devel/test-write/1524148597-0000000430_1
        PxfS3 pxfS3 = PxfS3.fromInputData(inputData);
        // pxfS3.setObjectName(new String(inputData.getFragmentMetadata())); // This yields a NPE
        String s3aURI = pxfS3.getS3aURI(this.inputData);
        LOG.info("s3aURI: " + s3aURI);
        Path file = new Path(s3aURI);
        writer = AvroParquetWriter
                .<GenericRecord>builder(file)
                .withSchema(avroSchema)
                // TODO: Expose the compression codec via a user-supplied parameter on the table definition URI
                //.withCompressionCodec(CompressionCodecName.SNAPPY) // 49 MB (was 95 MB w/o compression)
                .withCompressionCodec(CompressionCodecName.GZIP) // 33 MB
                .withDictionaryEncoding(true)
                //.withDictionaryPageSize(3145728) // No effect
                //.withRowGroupSize(16777216) // No effect
                .build();
        return true;
    }

    @Override
    public boolean writeNextObject(OneRow oneRow) throws Exception {
    	writer.write((GenericRecord) oneRow.getData());
        return true;
    }

    @Override
    public void closeForWrite() throws Exception {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }
}
