#!/usr/bin/env bash

# Set GPHD_ROOT
# This assumes gphd-env.sh is always being sourced
export GPHD_ROOT=`cd \`dirname $BASH_SOURCE\`/..;pwd`
export GPHD_CONF=$GPHD_ROOT/conf

# Load settings file
settings_file=$GPHD_CONF/gphd-conf.sh
if [ ! -f $settings_file ]; then
	echo cannot find settings file at $settings_file
fi
. $settings_file

if [ $SLAVES -lt 1 -o $SLAVES -gt 10 ]; then
	echo SLAVES valid range 1-10 \($SLAVES\)
	exit 1
fi

if [ ! -x $JAVA_HOME/bin/java ]; then
	echo cannot find java at $JAVA_HOME
	echo check your conf/gphd-conf.sh file
	exit 1
fi

# Some basic definitions
export HADOOP_ROOT=$GPHD_ROOT/hadoop
export HBASE_ROOT=$GPHD_ROOT/hbase
export ZOOKEEPER_ROOT=$GPHD_ROOT/zookeeper
export PXF_ROOT=$GPHD_ROOT/pxf
export HIVE_ROOT=$GPHD_ROOT/hive
export TCSERVER_ROOT=$GPHD_ROOT/tcserver

export LOGS_ROOT=$STORAGE_ROOT/logs
export PIDS_ROOT=$STORAGE_ROOT/pids

export HADOOP_BIN=$HADOOP_ROOT/bin
export HADOOP_SBIN=$HADOOP_ROOT/sbin
export ZOOKEEPER_BIN=$ZOOKEEPER_ROOT/bin
export HBASE_BIN=$HBASE_ROOT/bin
export HIVE_BIN=$HIVE_ROOT/bin

export HADOOP_CONF=$HADOOP_ROOT/etc/hadoop
export ZOOKEEPER_CONF=$ZOOKEEPER_ROOT/conf
export HBASE_CONF=$HBASE_ROOT/conf
export HIVE_CONF=$HIVE_ROOT/conf

function cluster_initialized()
{
	if [ -d $HADOOP_STORAGE_ROOT/dfs/name ]; then
		return 0
	else
		return 1
	fi
}

function hdfs_running()
{
	`$bin/hdfs dfsadmin -Dipc.client.connect.max.retries.on.timeouts=0 -safemode get 2>&1 | grep -q "Safe mode is OFF"`
	return $?
}

function zookeeper_running()
{
	local retval=1
	for i in {1..10}; do
		sleep 5s
		$ZOOKEEPER_BIN/zkServer.sh status > /dev/null 2>&1
		retval=$?
		if [ $retval -eq 0 ]; then
			return 0
		fi
	done
	return $retval
}
