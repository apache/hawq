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
## Before using this script to run performance tests, you should
## 0. source greenplum_path.sh
## 1. start HDFS and HAWQ, and create a "gptest" database
## 2. if you HAWQ are not running at localhost:5432, specify PG_BASE_ADDRESS and PG_BASE_PORT
## 3. make sure HADOOP_HOME points to right place and hadoop commands have been added to PATH
## 4. mvn clean package to generate jar files needed
##
## This script must be run in hawq-mapreduce-tool folder!
##-----------------------------------------------------------------------

##-----------------------------------------------------------------------
## Usage: 
## 1. load and query
## ./run-performance-tests-locally ao|parquet scale is_partition tableName [columns]
##
## 2. query-only
## ./run-performance-tests-locally --query-only tableName [columns]
##
## Example:
## ./run-performance-tests-locally ao 0.1 false lineitem_ao_row l_orderkey,l_partkey,l_comment
## ./run-performance-tests-locally --query-only lineitem_ao_row
##-----------------------------------------------------------------------

##
## Bring in hadoop config environment.
##

source $HADOOP_HOME/libexec/hadoop-config.sh


##
## Clean up any previous tmp working directory and copy contents to
## remote systems.
##

LIBS=(`find .. -name "*.jar" | grep -v javadoc.jar`)
CLASSPATH=$(IFS=:; echo "${LIBS[*]}"):$CLASSPATH

COMMAND="java -cp $CLASSPATH com.pivotal.hawq.mapreduce.pt.HAWQInputFormatPerformanceTest_TPCH"

echo "command: $COMMAND $@"
echo "--------------------"
$COMMAND $@
