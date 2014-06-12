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

function createInstance()
{
	instance_port=5120${nodeid}
	create_options="\
	--template $instance_template \
	--property ${instance_template}.http.port=$instance_port \
	--instance-directory $instance_root"

	mkdir -p $instance_root
	$TCSERVER_ROOT/tcruntime-instance.sh create $create_options $instance_name > /dev/null

	if [ $? -gt 0 ]; then
		echo instance creation failed
		return 1
	fi
}

function configureInstance()
{
	serverXml=$instance_root/$instance_name/conf/server.xml
	cat $serverXml | \
	sed "/^[[:blank:]]*maxKeepAliveRequests=.*$/ a\\
	maxHeaderCount=\"10000\"\\
	maxHttpHeaderSize=\"65536\"
	" > ${serverXml}.tmp

	rm $serverXml
	mv ${serverXml}.tmp $serverXml
}

function deployWebapp()
{
    cp $GPHD_ROOT/pxf/pxf.war $instance_root/$instance_name/webapps/
    cp $GPHD_ROOT/pxf/pxf-service-*[0-9].jar $instance_root/$instance_name/lib/

	pushd $instance_root/$instance_name/webapps
	mkdir pxf
	cd pxf
	unzip ../pxf.war
	popd

	context_file=$instance_root/$instance_name/webapps/pxf/META-INF/context.xml
	cat $context_file | \
	sed "s/classpathFiles=\"[a-zA-Z0-9\/\;-]*\"/classpathFiles=\"pxf\/conf\/pxf-classpath\"/" > context.xml.tmp
	mv context.xml.tmp $context_file
}

function verifyRegresionJar()
{
	if [ ! -f $PXF_ROOT/regression-test.jar ]; then
		echo TEMPORARY HACK ALERT
		echo \$GPHD_ROOT/pxf/regression-test.jar does not exist
		echo Please compile it from PXF sourcetree and reinitialize
		echo GPHD_ROOT=$root make regression-resources
		return 1
	fi
}

function doinit()
{
	verifyRegresionJar || return 1
	createInstance || return 1
	configureInstance || return 1
	deployWebapp || return 1
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
	"restart" )
		docommand stop
		docommand start
		;;
	* )
		;;
esac
