#!/usr/bin/env bash

usage="Usage: `basename $0` <start|stop> <node_id>"

if [ $# -ne 2 ]; then
    echo $usage
    exit 1
fi

command=$1
nodeid=$2

# Load settings
root=`cd \`dirname $0\`/..;pwd`
bin=$root/bin
. $bin/gphd-env.sh

datanode_root=$HADOOP_STORAGE_ROOT/datanode$nodeid
datanode_conf=$datanode_root/etc/hadoop

# export YARN_NODEMANAGER_OPTS="-Dhadoop.tmp.dir=$datanode_root/data"
export HADOOP_CONF_DIR=$datanode_conf
export YARN_CONF_DIR=$datanode_conf
export YARN_IDENT_STRING=$USER-node$nodeid

# add single-cluster properties
function patch_yarn_site()
{
	cat $HADOOP_CONF/yarn-site.xml | \
	sed "/^<configuration>$/ a\\
	<property>\\
	<name>yarn.nodemanager.localizer.address</name>\\
	<value>0.0.0.0:804$nodeid</value>\\
	</property>\\
	<property>\\
	<name>yarn.nodemanager.webapp.address</name>\\
	<value>0.0.0.0:805$nodeid</value>\\
	</property>\\
	<property>\\
	<name>yarn.nodemanager.local-dirs</name>\\
	<value>$datanode_root/nm-local-dir</value>\\
	</property>\\
	<property>\\
	<name>yarn.nodemanager.log-dirs</name>\\
	<value>$datanode_root/nm-logs</value>\\
	</property>\\
	" > $datanode_conf/yarn-site.xml
}

function patch_mapred_site()
{
	cat $HADOOP_CONF/mapred-site.xml | \
	sed "/^<configuration>$/ a\\
	<property>\\
	<name>mapreduce.shuffle.port</name>\\
	<value>806$nodeid</value>\\
	</property>\\
	" > $datanode_conf/mapred-site.xml
}

function dostart()
{
	patch_yarn_site
	patch_mapred_site
}

case "$command" in
    "start" )
		dostart
        ;;
    "stop" )
        ;;
    * )
        echo unknown command "$command"
        echo $usage
        exit 1
        ;;
esac

$HADOOP_SBIN/yarn-daemon.sh --config $datanode_conf $command nodemanager
