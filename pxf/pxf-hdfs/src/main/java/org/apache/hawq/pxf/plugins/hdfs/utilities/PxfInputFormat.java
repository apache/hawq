package org.apache.hawq.pxf.plugins.hdfs.utilities;

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


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * PxfInputFormat is not intended to read a specific format, hence it implements
 * a dummy getRecordReader Instead, its purpose is to apply
 * FileInputFormat.getSplits from one point in PXF and get the splits which are
 * valid for the actual InputFormats, since all of them we use inherit
 * FileInputFormat but do not override getSplits.
 */
public class PxfInputFormat extends FileInputFormat {

    @Override
    public RecordReader getRecordReader(InputSplit split,
                                        JobConf conf,
                                        Reporter reporter) throws IOException {
        throw new UnsupportedOperationException("PxfInputFormat should not be used for reading data, but only for obtaining the splits of a file");
    }

    /*
     * Return true if this file can be split.
     */
    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return HdfsUtilities.isSplittableCodec(filename);
    }

}
