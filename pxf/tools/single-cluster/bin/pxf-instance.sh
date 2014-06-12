#!/usr/bin/env bash

############################
# dov.dorin at gopivotal.com
############################

usage="Usage: `basename $0` <start|stop|init> <node_id>"

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

# template used for PXF instaces
# TODO consider making this one public
instance_template=bio

instance_root=$PXF_STORAGE_ROOT/pxf$nodeid
instance_name=pxf-service-$nodeid

function doinit()
{
	instance_port=5120${nodeid}
	create_options="\
	--template $instance_template \
	--property ${instance_template}.http.port=$instance_port \
	--instance-directory $instance_root"

	mkdir -p $instance_root
	$TCSERVER_ROOT/tcruntime-instance.sh create $create_options $instance_name
}

function docommand()
{
	command=$1
	$TCSERVER_ROOT/tcruntime-ctl.sh $instance_name $command -n $instance_root -d $TCSERVER_ROOT
}

case "$command" in
	"init" )
		doinit
		;;
	"start" )
		docommand $command
		;;
	"stop" )
		docommand $command
		;;
	* )
		;;
esac
