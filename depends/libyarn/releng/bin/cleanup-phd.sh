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

if [ "x${DATA_PATH}" != "x" ]; then
	if [ -e ${DATA_PATH} ]; then
		echo "dump namenode log"
		cat ${DATA_PATH}/hadoop*/logs/*namenode*log*
		echo "dump datanode log"
		cat ${DATA_PATH}/hadoop*/logs/*datanode*log*
	fi 

	if [ "x${KEEP_LOG}" != "xtrue" ]; then
		rm -rf ${DATA_PATH}/*
	fi

	rm -rf ${DATA_PATH}/fs
	rm -rf ${DATA_PATH}/data
fi

NAMENODE_PIDS=`ps aux | grep java | grep NameNode | awk '{print $2}'`
if [ "x${NAMENODE_PIDS}" != "x" ]; then
	kill -9 ${NAMENODE_PIDS}
fi

DATANODE_PIDS=`ps aux | grep java | grep DataNode | awk '{print $2}'`
if [ "x${DATANODE_PIDS}" != "x" ]; then
	kill -9 ${DATANODE_PIDS}
fi
