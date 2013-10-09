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

hbase_root=$HBASE_ROOT
hbase_conf=$HBASE_CONF
hbase_bin=$HBASE_BIN

regionserver_root=$HBASE_STORAGE_ROOT/regionserver$nodeid
regionserver_conf=$regionserver_root/conf

export HBASE_REGIONSERVER_OPTS="-Dhbase.tmp.dir=$regionserver_root/tmp"
export HBASE_CONF_DIR=$regionserver_conf
export HBASE_IDENT_STRING="$USER-node$nodeid"

function clear_conf_directory()
{
    if [ -d $regionserver_conf ]; then
        rm -rf $regionserver_conf
    fi
    mkdir -p $regionserver_conf
}

function setup_hbasesite()
{
	cat $hbase_conf/hbase-site.xml | \
	sed "/^<configuration>$/ a\\
    <property>\\
    <name>hbase.regionserver.port</name>\\
    <value>6002$nodeid</value>\\
    </property>\\
    <property>\\
    <name>hbase.regionserver.info.port</name>\\
    <value>6003$nodeid</value>\\
    </property>\\
	" > $regionserver_conf/hbase-site.xml
}

function copy_conf_files()
{
    for file in $(find $hbase_conf -type f -not -iname "*~"); do
        cp $file $regionserver_conf/`basename $file`
    done
}

function handle_start()
{
    clear_conf_directory
    copy_conf_files
    setup_hbasesite
}

case "$command" in
    "start" )
        handle_start
        ;;
    "stop" )
        ;;
    * )
        echo unknown command "$command"
        echo $usage
        exit 1
        ;;
esac

$hbase_bin/hbase-daemon.sh --config $regionserver_conf $command regionserver
