#! /bin/bash

METADATA_FILE=$1
OUTPUT_PATH=$2

LIBS=(`find . -name "*.jar" | grep -v tests.jar | grep -v javadoc.jar`)

export HADOOP_CLASSPATH=hawq-mapreduce-tool/target/test-classes/:$(IFS=:; echo "${LIBS[*]}")
echo $HADOOP_CLASSPATH

hadoop fs -rm -r $OUTPUT_PATH
hadoop com.pivotal.hawq.mapreduce.demo.HAWQInputFormatDemo2 $METADATA_FILE $OUTPUT_PATH
