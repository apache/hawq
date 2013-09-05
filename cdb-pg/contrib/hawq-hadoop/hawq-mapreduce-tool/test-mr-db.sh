#!/bin/sh
# A script to run the tests
# test-startup.sh

if [ $# -lt 3 ]
then
  echo "#USAGE: ./test-startup.sh <database> <tablename> <whetherToLog> [<logfile>]"
  exit
fi

SRC_JAR_NAME=hawq-mapreduce-tool-1.0.0
COMMON_JAR_NAME=../hawq-mapreduce-common/target/hawq-mapreduce-common-1.0.0
AO_JAR_NAME=../hawq-mapreduce-ao/target/hawq-mapreduce-ao-1.0.0
PARQUET_JAR_NAME=../hawq-mapreduce-parquet/target/hawq-mapreduce-parquet-1.0.0

export HADOOP_CLASSPATH=lib/postgresql-9.2-1003-jdbc4.jar:target/${SRC_JAR_NAME}.jar:target/${SRC_JAR_NAME}-tests.jar:${COMMON_JAR_NAME}.jar:${AO_JAR_NAME}.jar:${PARQUET_JAR_NAME}.jar:${HADOOP_CLASSPATH}

DB_NAME=$1
DB_URL=localhost:5432/${DB_NAME}
TABLE_NAME=$2
OUTPUT_PATH=/mapreduce/hawqintest

hadoop fs -rm -r -f ${OUTPUT_PATH}

time hadoop jar target/${SRC_JAR_NAME}-tests.jar com.pivotal.hawq.mapreduce.HAWQInputFormatDriver -libjars target/${SRC_JAR_NAME}.jar,${COMMON_JAR_NAME}.jar,${AO_JAR_NAME}.jar,${PARQUET_JAR_NAME}.jar ${DB_URL} ${TABLE_NAME} ${OUTPUT_PATH}

if [ $3 = "Y" ]
then
  if [ $# -eq 4 ]
  then
    hadoop fs -cat ${OUTPUT_PATH}/* > $4
  else
    hadoop fs -cat ${OUTPUT_PATH}/*
  fi
fi
