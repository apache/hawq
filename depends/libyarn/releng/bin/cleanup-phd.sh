#!/bin/sh

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
