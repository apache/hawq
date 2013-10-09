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

export HADOOP_DATANODE_OPTS="-Dhadoop.tmp.dir=$datanode_root/data"
export HADOOP_CONF_DIR=$datanode_conf
export HADOOP_IDENT_STRING=$USER-node$nodeid

# remove the conf directory
# this allows a refresh
function clear_conf_directory()
{
    if [ -d $datanode_conf ]; then
        rm -rf $datanode_conf
    fi
    mkdir -p $datanode_conf
}

# copy all files from original hadoop conf directory
function copy_conf_files()
{
    for file in $(find $HADOOP_CONF -type f -not -iname "*~"); do
        cp $file $datanode_conf/`basename $file`
    done
}

# add single-cluster properties
function patch_hdfs_site()
{
	cat $HADOOP_CONF/hdfs-site.xml | \
	sed "/^<configuration>$/ a\\
    <property>\\
    <name>dfs.datanode.address</name>\\
    <value>0.0.0.0:5001$nodeid</value>\\
    </property>\\
    <property>\\
    <name>dfs.datanode.http.address</name>\\
    <value>0.0.0.0:5008$nodeid</value>\\
    </property>\\
    <property>\\
    <name>dfs.datanode.ipc.address</name>\\
    <value>0.0.0.0:5002$nodeid</value>\\
    </property>
	" > $datanode_conf/hdfs-site.xml
}

function dostart()
{
	clear_conf_directory
	copy_conf_files
	patch_hdfs_site
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

$HADOOP_SBIN/hadoop-daemon.sh --config $datanode_conf $command datanode
