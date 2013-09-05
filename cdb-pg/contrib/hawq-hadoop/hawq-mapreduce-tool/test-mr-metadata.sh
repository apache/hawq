#!/bin/sh
# A script to run the tests
# test-mr-metadata.sh

if [ $# -lt 2 ]
then
  echo "#USAGE: ./test-mr-metadata.sh <metadata_path> <whetherToLog> [<logfile>]"
  exit
fi

SRC_JAR_NAME=hawq-mapreduce-tool-1.0.0
COMMON_JAR_NAME=../hawq-mapreduce-common/target/hawq-mapreduce-common-1.0.0
AO_JAR_NAME=../hawq-mapreduce-ao/target/hawq-mapreduce-ao-1.0.0
PARQUET_JAR_NAME=../hawq-mapreduce-parquet/target/hawq-mapreduce-parquet-1.0.0

export HADOOP_CLASSPATH=lib/postgresql-9.2-1003-jdbc4.jar:lib/snakeyaml-1.12.jar:target/${SRC_JAR_NAME}.jar:target/${SRC_JAR_NAME}-tests.jar:${COMMON_JAR_NAME}.jar:${AO_JAR_NAME}.jar:${PARQUET_JAR_NAME}.jar:${HADOOP_CLASSPATH}

METADATA_PATH=$1
OUTPUT_PATH=/mapreduce/hawqintest

hadoop fs -rm -r -f ${OUTPUT_PATH}

time hadoop jar target/${SRC_JAR_NAME}-tests.jar com.pivotal.hawq.mapreduce.HAWQInputFormatMetadataDriver -libjars target/${SRC_JAR_NAME}.jar,${COMMON_JAR_NAME}.jar,${AO_JAR_NAME}.jar,${PARQUET_JAR_NAME}.jar ${METADATA_PATH} ${OUTPUT_PATH}

if [ $2 = "Y" ]
then
  if [ $# -eq 3 ]
  then
    hadoop fs -cat ${OUTPUT_PATH}/* > $3
  else
    hadoop fs -cat ${OUTPUT_PATH}/*
  fi
fi
