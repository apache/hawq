#!/usr/bin/env bash
############################
# dov.dorin at gopivotal.com
############################

# Load settings
root=`cd \`dirname $0\`/..;pwd`
bin=$root/bin
. $bin/gphd-env.sh

$bin/start-hdfs.sh || exit 1

$HADOOP_BIN/hdfs dfsadmin -safemode wait

if [ "$START_YARN" == "true" ]; then
	$bin/start-yarn.sh || exit 1
else
	echo Yarn/Mapreduce wont be started
fi

if [ "$START_HIVEMETASTORE" == "true" ]; then
	$bin/start-hive.sh || exit 1
fi

if [ "$START_HBASE" != "true" ]; then
	echo HBase wont be started
	exit 0
fi

$bin/start-zookeeper.sh || exit 1
$bin/start-hbase.sh || exit 1
