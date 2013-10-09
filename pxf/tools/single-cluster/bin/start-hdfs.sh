#!/usr/bin/env bash
############################
# dov.dorin at gopivotal.com
############################

# Load settings
root=`cd \`dirname $0\`/..;pwd`
bin=$root/bin
. $bin/gphd-env.sh

cluster_initialized
if [ $? -ne 0 ]; then
	echo cluster not initialized 
	echo please run $bin/init-gphd.sh
	exit 1
fi

# Hadoop
# Start NameNode
echo Starting HDFS...
$HADOOP_SBIN/hadoop-daemon.sh --config $HADOOP_CONF start namenode | sed "s/^/master: /"

# Start DataNodes
for (( i=0; i < $SLAVES; i++ ))
do
	$bin/hadoop-datanode.sh start $i | sed "s/^/node $i: /"
done
