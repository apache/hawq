#!/bin/bash

hadoop fs -rm -r $2

export HADOOP_CLASSPATH=target/test-classes:target/hawq-mapreduce-parquet-1.0.0.jar:../hawq-mapreduce-common/target/hawq-mapreduce-common-1.0.0.jar:lib/parquet-column-1.3.2.jar:lib/parquet-common-1.3.2.jar:lib/parquet-encoding-1.3.2.jar:lib/parquet-hadoop-1.3.2.jar:lib/parquet-format-1.0.0.jar

# enable debug
# export HADOOP_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5002"
hadoop com.pivotal.hawq.mapreduce.parquet.HAWQParquetInputDriver -conf conf/hadoop-localjob.xml $1 $2
