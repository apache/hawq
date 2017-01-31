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


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hawq.pxf.api.Metadata;
import org.apache.hawq.pxf.api.MetadataFetcher;
import org.apache.hawq.pxf.api.OutputFormat;
import org.apache.hawq.pxf.api.UnsupportedTypeException;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.ProfilesConf;
import org.apache.hawq.pxf.plugins.hive.utilities.HiveUtilities;
import org.apache.hawq.pxf.plugins.hive.utilities.ProfileFactory;

/**
 * Class for connecting to Hive's MetaStore and getting schema of Hive tables.
 */
public class HiveMetadataFetcher extends MetadataFetcher {

    private static final String DELIM_FIELD = InputData.DELIMITER_KEY;

    private static final Log LOG = LogFactory.getLog(HiveMetadataFetcher.class);
    private HiveMetaStoreClient client;
    private JobConf jobConf;

    public HiveMetadataFetcher(InputData md) {
        super(md);

        // init hive metastore client connection.
        client = HiveUtilities.initHiveClient();
        jobConf = new JobConf(new Configuration());
    }

    /**
     * Fetches metadata of hive tables corresponding to the given pattern
     * For patterns matching more than one table, the unsupported tables are skipped.
     * If the pattern correspond to exactly one table, throws an exception if
     * the table type is not supported or contains unsupported field types.
     * Supported HCatalog types: TINYINT,
     * SMALLINT, INT, BIGINT, BOOLEAN, FLOAT, DOUBLE, STRING, BINARY, TIMESTAMP,
     * DATE, DECIMAL, VARCHAR, CHAR.
     *
     * @param pattern pattern table/file name or pattern in the given source
     */
    @Override
    public List<Metadata> getMetadata(String pattern) throws Exception {

        boolean ignoreErrors = false;
        List<Metadata.Item> tblsDesc = HiveUtilities.extractTablesFromPattern(client, pattern);

        if(tblsDesc == null || tblsDesc.isEmpty()) {
            LOG.warn("No tables found for the given pattern: " + pattern);
            return null;
        }

        List<Metadata> metadataList = new ArrayList<Metadata>();

        if(tblsDesc.size() > 1) {
            ignoreErrors = true;
        }

        for(Metadata.Item tblDesc: tblsDesc) {
            try {
                Metadata metadata = new Metadata(tblDesc);
                Table tbl = HiveUtilities.getHiveTable(client, tblDesc);
                HiveUtilities.getSchema(tbl, metadata);
                boolean hasComplexTypes = HiveUtilities.hasComplexTypes(metadata);
                metadataList.add(metadata);
                List<Partition> tablePartitions = client.listPartitionsByFilter(tblDesc.getPath(), tblDesc.getName(), "", (short) -1);
                Set<OutputFormat> formats = new HashSet<OutputFormat>();
                //If table has partitions - find out all formats
                for (Partition tablePartition : tablePartitions) {
                    String inputFormat = tablePartition.getSd().getInputFormat();
                    OutputFormat outputFormat = getOutputFormat(inputFormat, hasComplexTypes);
                    formats.add(outputFormat);
                }
                //If table has no partitions - get single format of table
                if (tablePartitions.size() == 0 ) {
                    String inputFormat = tbl.getSd().getInputFormat();
                    OutputFormat outputFormat = getOutputFormat(inputFormat, hasComplexTypes);
                    formats.add(outputFormat);
                }
                metadata.setOutputFormats(formats);
                Map<String, String> outputParameters = new HashMap<String, String>();
                Integer delimiterCode = HiveUtilities.getDelimiterCode(tbl.getSd());
                outputParameters.put(DELIM_FIELD, delimiterCode.toString());
                metadata.setOutputParameters(outputParameters);
            } catch (UnsupportedTypeException | UnsupportedOperationException e) {
                if(ignoreErrors) {
                    LOG.warn("Metadata fetch for " + tblDesc.toString() + " failed. " + e.getMessage());
                    continue;
                } else {
                    throw e;
                }
            }
        }

        return metadataList;
    }

    private OutputFormat getOutputFormat(String inputFormat, boolean hasComplexTypes) throws Exception {
        OutputFormat outputFormat = null;
        InputFormat<?, ?> fformat = HiveDataFragmenter.makeInputFormat(inputFormat, jobConf);
        String profile = ProfileFactory.get(fformat, hasComplexTypes);
        String outputFormatClassName = ProfilesConf.getProfilePluginsMap(profile).get("X-GP-OUTPUTFORMAT");
        outputFormat = OutputFormat.getOutputFormat(outputFormatClassName);
        return outputFormat;
    }

}
