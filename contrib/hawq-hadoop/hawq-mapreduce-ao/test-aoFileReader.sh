#!/bin/sh
# A script to run the tests
# test-startup.sh

if [ $# -ne 3 ]
then
  echo "#USAGE: ./test-aoFileReader.sh <database> <tablename> <whetherToLog>"
  exit
fi

SRC_JAR_NAME=hawq-mapreduce-ao-1.0.0
COMMON_JAR_NAME=../hawq-mapreduce-common/target/hawq-mapreduce-common-1.0.0

export HADOOP_CLASSPATH=lib/postgresql-9.2-1003-jdbc4.jar:target/${SRC_JAR_NAME}.jar:target/${SRC_JAR_NAME}-tests.jar:${COMMON_JAR_NAME}.jar:${HADOOP_CLASSPATH}

DB_NAME=$1
DB_URL=localhost:5432/${DB_NAME}
TABLE_NAME=$2

time hadoop jar target/${SRC_JAR_NAME}-tests.jar com.pivotal.hawq.mapreduce.ao.io.HAWQAOFileReaderTest ${DB_URL} ${TABLE_NAME} $3
