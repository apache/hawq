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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Fragmenter for Parquet on HDFS.
 * Returns list of splits for a given HDFS path.
 */
public class ParquetDataFragmenter extends Fragmenter {
    private Job job;

    public ParquetDataFragmenter(InputData md) {
        super(md);
        JobConf jobConf = new JobConf(new Configuration(), ParquetDataFragmenter.class);
        try {
            job = Job.getInstance(jobConf);
        } catch (IOException e) {
            throw new RuntimeException("Unable to instantiate a job for reading fragments", e);
        }
    }


    @Override
    public List<Fragment> getFragments() throws Exception {
        String absoluteDataPath = HdfsUtilities.absoluteDataPath(inputData.getDataSource());
        List<InputSplit> splits = getSplits(new Path(absoluteDataPath));

        for (InputSplit split : splits) {
            FileSplit fsp = (FileSplit) split;

            String filepath = fsp.getPath().toUri().getPath();
            String[] hosts = fsp.getLocations();

            Path file = new Path(filepath);

            ParquetMetadata metadata = ParquetFileReader.readFooter(
                    job.getConfiguration(), file, ParquetMetadataConverter.NO_FILTER);
            MessageType schema = metadata.getFileMetaData().getSchema();

            byte[] fragmentMetadata = HdfsUtilities.prepareFragmentMetadata(fsp.getStart(), fsp.getLength(), fsp.getLocations());
            Fragment fragment = new Fragment(filepath, hosts, fragmentMetadata, HdfsUtilities.makeParquetUserData(schema));
            fragments.add(fragment);
        }

        return fragments;
    }

        private List<InputSplit> getSplits (Path path) throws IOException {
            ParquetInputFormat<Group> parquetInputFormat = new ParquetInputFormat<Group>();
            ParquetInputFormat.setInputPaths(job, path);
            List<InputSplit> splits = parquetInputFormat.getSplits(job);
            ArrayList<InputSplit> result = new ArrayList<InputSplit>();

            if (splits != null) {
                for (InputSplit split : splits) {
                    try {
                        if (split.getLength() > 0) {
                            result.add(split);
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Unable to read split's length", e);
                    }
                }
            }

            return result;
        }
    }
