#!/usr/bin/env bash
############################
# dov.dorin at gopivotal.com
############################

# Load settings
root=`cd \`dirname $0\`/..;pwd`
bin=$root/bin
. $bin/gphd-env.sh

# Check to see HDFS is up
hdfs_running
if [ $? != 0 ]; then
	echo HDFS is not running, YARN cant start
	echo Please see HDFS is up and out of safemode
	exit 1
fi

# Hadoop
# Start ResourceManager
echo Starting YARN
$HADOOP_SBIN/yarn-daemon.sh --config $HADOOP_CONF start resourcemanager | sed "s/^/master: /"

# Start NodeManager(s)
for (( i=0; i < $SLAVES; i++ ))
do
	$bin/yarn-nodemanager.sh start $i | sed "s/^/node $i: /"
done

# HistoryServer
if [ "$START_YARN_HISTORY_SERVER" != "true" ]; then
	echo Mapreduce History Server wont be started
	exit 0
fi

$HADOOP_SBIN/mr-jobhistory-daemon.sh --config $HADOOP_CONF start historyserver | sed "s/^/master: /"
