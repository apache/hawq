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

if [ -z ${DATA_PATH} ]; then
	echo "DATA_PATH not set"
	exit 1 
fi

if [ -d ${DATA_PATH} ]; then
	cd ${DATA_PATH}
else
	echo "DATA_PATH not exist"
        exit 1
fi

HADOOP_TARBALL=`curl --silent --no-buffer "http://hdp4-mdw1.wbe.dh.greenplum.com/dist/PHD/latest/?C=M;O=D" | grep -o 'PHD-2.[0-9].[0-9].[0-9]-bin-[0-9]*.tar.gz' | head -n 1`
echo "HADOOP tarball: " ${HADOOP_TARBALL}

HADOOP_URL="http://hdp4-mdw1.wbe.dh.greenplum.com/dist/PHD/latest/"${HADOOP_TARBALL}
echo "Download HADOOP from " ${HADOOP_URL}

curl --silent -o ${DATA_PATH}/${HADOOP_TARBALL} -L ${HADOOP_URL}

tar -xzf ${DATA_PATH}/${HADOOP_TARBALL}

HADOOP_PACKAGE=`echo ${HADOOP_TARBALL} | grep -o 'PHD-[0-9].[0-9].[0-9].[0-9]-bin-[0-9]*'`
HADOOP_VERSION=`ls ${DATA_PATH}/${HADOOP_PACKAGE}/hadoop/tar/*.tar.gz | grep -o 'hadoop-[0-9].[0-9].[0-9]-[A-Za-z0-9\-]*-[0-9].[0-9].[0-9].[0-9]'`
echo "HADOOP version: " ${HADOOP_VERSION}

if [ -z ${HADOOP_VERSION} ]; then
	echo "cannot get HADOOP version"
	exit 1
fi

tar -xzf ${HADOOP_PACKAGE}/hadoop/tar/${HADOOP_VERSION}.tar.gz

if [ -z ${HDFS_CONFIG_PATH} ]; then
        echo "HDFS_CONFIG_PATH not set"
        exit 1
fi

if [ -f ${HDFS_CONFIG_PATH} ]; then
	cp -f ${HDFS_CONFIG_PATH} ${DATA_PATH}/${HADOOP_VERSION}/etc/hadoop/
else
	echo "HDFS_CONFIG_PATH not a file"
	exit 1
fi

HADOOP_BIN=${DATA_PATH}/${HADOOP_VERSION}/bin
HADOOP_SBIN=${DATA_PATH}/${HADOOP_VERSION}/sbin

${HADOOP_BIN}/hdfs namenode -format
${HADOOP_SBIN}/hadoop-daemon.sh start namenode
${HADOOP_SBIN}/hadoop-daemon.sh start datanode
${HADOOP_BIN}/hdfs dfs -mkdir hdfs://localhost:9000/user
${HADOOP_BIN}/hdfs dfs -chmod 777 hdfs://localhost:9000/user
