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


import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.hive.utilities.HiveUtilities;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileRecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

import static org.apache.hawq.pxf.plugins.hive.utilities.HiveUtilities.PXF_HIVE_SERDES;

/**
 * Specialization of HiveAccessor for a Hive table that stores only RC files.
 * This class replaces the generic HiveAccessor for a case where a table is stored entirely as RC files.
 * Use together with {@link HiveInputFormatFragmenter}/{@link HiveColumnarSerdeResolver}
 */
public class HiveRCFileAccessor extends HiveAccessor {

    /**
     * Constructs a HiveRCFileAccessor.
     *
     * @param input input containing user data
     * @throws Exception if user data was wrong
     */
    public HiveRCFileAccessor(InputData input) throws Exception {
        super(input, new RCFileInputFormat());
        HiveUserData hiveUserData = HiveUtilities.parseHiveUserData(input, PXF_HIVE_SERDES.COLUMNAR_SERDE, PXF_HIVE_SERDES.LAZY_BINARY_COLUMNAR_SERDE);
        initPartitionFields(hiveUserData.getPartitionKeys());
        filterInFragmenter = hiveUserData.isFilterInFragmenter();
    }

    @Override
    protected Object getReader(JobConf jobConf, InputSplit split) throws IOException {
        return new RCFileRecordReader(jobConf, (FileSplit) split);
    }
}
