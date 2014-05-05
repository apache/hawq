#!/usr/bin/env bash
############################
# dov.dorin at gopivotal.com
############################

# Load settings
root=`cd \`dirname $0\`/..;pwd`
bin=$root/bin
. $bin/gphd-env.sh

if [ "$START_HBASE" == "true" ]; then
	echo Stopping HBase...
	$bin/stop-hbase.sh

	echo Stopping Zookeeper...
	$bin/stop-zookeeper.sh
fi

if [ "$START_PXF" == "true" ]; then
	echo Stopping PXF...
	$bin/stop-pxf.sh
fi

if [ "$START_YARN" == "true" ]; then
	echo Stopping YARN...
	$bin/stop-yarn.sh
fi

if [ "$START_HIVEMETASTORE" == "true" ]; then
	$bin/stop-hive.sh
fi

echo Stopping HDFS...
$bin/stop-hdfs.sh
