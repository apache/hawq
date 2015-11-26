#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
## ======================================================================

##-----------------------------------------------------------------------
##                        NOTICE!!
## Before using this script to run feature tests, you should
## 0. source greenplum_path.sh
## 1. start HDFS and HAWQ, and create a "gptest" database
## 2. if you HAWQ are not running at localhost:5432, specify PG_BASE_ADDRESS and PG_BASE_PORT
## 3. make sure HADOOP_HOME points to right place and hadoop commands have been added to PATH
## 4. mvn clean package to generate jar files needed
##
## This script must be run in hawq-mapreduce-tool folder!
##-----------------------------------------------------------------------

##
## List of default test cases to run.  This can be overwritten with
## the FEATURE_TEST_LIST environment variable.
##

FEATURE_TEST_LIST=${FEATURE_TEST_LIST:=com.pivotal.hawq.mapreduce.ft.HAWQInputFormatFeatureTest_AO_Compression \
                                       com.pivotal.hawq.mapreduce.ft.HAWQInputFormatFeatureTest_AO_Misc \
                                       com.pivotal.hawq.mapreduce.ft.HAWQInputFormatFeatureTest_AO_Options \
                                       com.pivotal.hawq.mapreduce.ft.HAWQInputFormatFeatureTest_AO_Types \
                                       com.pivotal.hawq.mapreduce.ft.HAWQInputFormatFeatureTest_Parquet_Compression \
                                       com.pivotal.hawq.mapreduce.ft.HAWQInputFormatFeatureTest_Parquet_Misc \
                                       com.pivotal.hawq.mapreduce.ft.HAWQInputFormatFeatureTest_Parquet_Options \
                                       com.pivotal.hawq.mapreduce.ft.HAWQInputFormatFeatureTest_Parquet_Types \
                                       com.pivotal.hawq.mapreduce.ft.HAWQInputFormatFeatureTest_TPCH }

##
## Default location for test working tmp directory.
##

HAWQ_MAPREDUCE_TOOL_TMPDIR=${HAWQ_MAPREDUCE_TOOL_TMPDIR:=/tmp/hawq-mapreduce-tool}

##
## Bring in hadoop config environment.
##

export HADOOP_HOME=${HADOOP_HOME:=/usr/lib/gphd/hadoop/}
echo "HADOOP_HOME is set to ${HADOOP_HOME}"

source /usr/lib/gphd/hadoop/libexec/hadoop-config.sh

##
## Cleanup previous tmp working directory
##

rm -rf ${HAWQ_MAPREDUCE_TOOL_TMPDIR}
mkdir -p ${HAWQ_MAPREDUCE_TOOL_TMPDIR}

##
## Populate tmp working directory
##

cp ../../demo_mapreduce/* ${HAWQ_MAPREDUCE_TOOL_TMPDIR}

##
## Clean up any previous tmp working directory and copy contents to
## remote systems.
##

for host in smdw sdw1 sdw2; do
    ssh ${host} rm -rf ${HAWQ_MAPREDUCE_TOOL_TMPDIR}
    scp -r ${HAWQ_MAPREDUCE_TOOL_TMPDIR} ${host}:${HAWQ_MAPREDUCE_TOOL_TMPDIR}
done

##
## Setup CLASSPATH
##

#
# After source hadoop-config.sh, our CLASSPATH contains an old version of snappy-java
# which doesn't work on centos 5. To handle this, HAWQ_MAPREDUCE_TOOL_TMPDIR includes
# an updated version (snappy-java-1.1.0.jar). But we should put in front of the CLASSPATH
# to make it work.
# 
CLASSPATH=${HAWQ_MAPREDUCE_TOOL_TMPDIR}/*:/usr/lib/gphd/hadoop-yarn/*:$CLASSPATH

rm inputformat.logs
for test in ${FEATURE_TEST_LIST}; do
    command="java -cp $CLASSPATH org.junit.runner.JUnitCore ${test}"

    echo ""
    echo "======================================================================"
    echo "Timestamp ..... : $(date)"
    echo "Running test .. : ${test}"
    echo "command ....... : ${command}"
    echo "----------------------------------------------------------------------"

    ${command} | tee -a inputformat.logs 2>&1

    echo "======================================================================"
    echo ""
done

##
## Generate reports parsed by pulse.
##
python generate_mr_report.py 
