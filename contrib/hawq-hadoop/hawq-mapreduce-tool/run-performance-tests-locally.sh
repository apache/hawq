#!/bin/sh
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
