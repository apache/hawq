#! /bin/bash
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

METADATA_FILE=$1
OUTPUT_PATH=$2

LIBS=(`find . -name "*.jar" | grep -v tests.jar | grep -v javadoc.jar`)

export HADOOP_CLASSPATH=hawq-mapreduce-tool/target/test-classes/:$(IFS=:; echo "${LIBS[*]}")
echo $HADOOP_CLASSPATH

hadoop fs -rm -r $OUTPUT_PATH
hadoop com.pivotal.hawq.mapreduce.demo.HAWQInputFormatDemo2 $METADATA_FILE $OUTPUT_PATH
