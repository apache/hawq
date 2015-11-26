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
