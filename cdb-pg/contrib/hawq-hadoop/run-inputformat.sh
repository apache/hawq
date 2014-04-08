#! /bin/bash

DBNAME=$1
TABLE_NAME=$2
OUTPUT_PATH=$3
DBURL=localhost:5432/$DBNAME

LIBS=(`find . -name "*.jar" | grep -v tests.jar | grep -v javadoc.jar`)

export HADOOP_CLASSPATH=hawq-mapreduce-tool/target/test-classes/:$(IFS=:; echo "${LIBS[*]}")
echo $HADOOP_CLASSPATH

hadoop fs -rm -r $OUTPUT_PATH

#export HADOOP_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5002"
hadoop com.pivotal.hawq.mapreduce.demo.HAWQInputFormatDemo $DBURL $TABLE_NAME $OUTPUT_PATH
