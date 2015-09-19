#!/bin/bash

hadoop fs -rm -r $2

# TODO HAWQ only support 1.0.0 format, can we use higher version of parquet library to write file of specific version?

export HADOOP_CLASSPATH=target/test-classes:target/hawq-mapreduce-parquet-1.0.0.jar:../hawq-mapreduce-common/target/hawq-mapreduce-common-1.0.0.jar:lib/parquet-column-1.0.0.jar:lib/parquet-common-1.0.0.jar:lib/parquet-encoding-1.0.0.jar:lib/parquet-hadoop-1.0.0.jar:lib/parquet-format-1.0.0.jar

hadoop com.pivotal.hawq.mapreduce.parquet.HAWQParquetOutputDriver -conf conf/hadoop-localjob.xml $1 $2
