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
	echo HDFS is not ready, HBase cannot start
	echo Please see HDFS is up and out of safemode
	exit 1
fi

# Check to see Zookeeper is up
zookeeper_running
if [ $? != 0 ]; then
	echo Zookeeper is not running, HBase cannot start
	echo Have you start-zookeeper.sh ?
	exit 1
fi

echo Starting HBase...
# Start master
$HBASE_BIN/hbase-daemon.sh --config $HBASE_CONF start master

# Start regions
for (( i=0; i < $SLAVES; i++ )); do
	$bin/hbase-regionserver.sh start $i | sed "s/^/node $i: /"
done

# Start Stargate
if [ "$START_STARGATE" == "true" ]; then
	echo Starting Stargate...
	$HBASE_BIN/hbase-daemon.sh --config $HBASE_CONF start rest -p $STARGATE_PORT
fi
