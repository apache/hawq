#!/usr/bin/env bash
############################
# dov.dorin at gopivotal.com
############################

# Load settings
root=`cd \`dirname $0\`/..;pwd`
bin=$root/bin
. $bin/gphd-env.sh

# Stop HistoryServer
if [ "$START_YARN_HISTORY_SERVER" == "true" ]; then
	$HADOOP_SBIN/mr-jobhistory-daemon.sh --config $HADOOP_CONF stop historyserver | sed "s/^/master: /"
fi

# Stop NodeManager(s)
for (( i=0; i < $SLAVES; i++ ))
do
	$bin/yarn-nodemanager.sh stop $i | sed "s/^/node $i: /"
done

# Stop NameNode
$HADOOP_SBIN/yarn-daemon.sh --config $HADOOP_CONF stop resourcemanager | sed "s/^/master: /"
