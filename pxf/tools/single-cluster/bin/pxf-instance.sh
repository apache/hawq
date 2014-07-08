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

# template used for PXF instances
# TODO consider making this one public
instance_template=bio

instance_root=$PXF_STORAGE_ROOT/pxf$nodeid
instance_name=pxf-service-$nodeid
instance_port=5120$nodeid

function createInstance()
{
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
	maxHeaderCount=\"30000\"\\
	maxHttpHeaderSize=\"1048576\"
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
	sed  -e "s/classpathFiles=\"[a-zA-Z0-9\/\;.-]*\"/classpathFiles=\"pxf\/conf\/pxf-private.classpath\"/" \
    -e "s/secondaryClasspathFiles=\"[a-zA-Z0-9\/\;.-]*\"/secondaryClasspathFiles=\"pxf\/conf\/pxf-public.classpath\"/" > context.xml.tmp
	mv context.xml.tmp $context_file

	web_file=$instance_root/$instance_name/webapps/pxf/WEB-INF/web.xml
	cat $web_file | \
	sed "s/<param-value>.*pxf-log4j.properties<\/param-value>/<param-value>..\/..\/..\/..\/..\/..\/pxf\/conf\/pxf-log4j.properties<\/param-value>/" > web.xml.tmp
	mv web.xml.tmp $web_file

}

function checkWebapp()
{
	curl=`which curl`
	attempts=0
	max_attempts=300 # try to connect for 5 minutes
	sleep_time=1 # sleep 1 second between attempts
	
	# wait until tomcat is up:
	echo Waiting for tcServer to start...
	until [[ "`curl --silent --connect-timeout 1 -I http://localhost:${instance_port} | grep 'Coyote'`" != "" 
	   || "$attempts" -eq "$max_attempts" ]];
	do
        let attempts=attempts+1
        echo "tcServer not responding, re-trying after ${sleep_time} second (attempt number ${attempts})"
        sleep $sleep_time
    done
	if [[ "$attempts" -eq "$max_attempts" ]]; then
		echo ERROR: cannot connect to tcServer after 5 minutes
		return 1
	fi
	
	# check if PXF webapp is up:
	echo Checking if PXF webapp is up and running...
	curlResponse=$($curl --silent "http://localhost:${instance_port}/pxf/v0")
	expectedResponse="Wrong version v0, supported version is v[0-9]+"
	
	if [[ $curlResponse =~ $expectedResponse ]]; then
		echo PXF webapp is up
		return 0
	fi
	
	echo ERROR: PXF webapp is inaccessible, check logs for more information
	return 1
}

function doinit()
{
	createInstance || return 1
	configureInstance || return 1
	deployWebapp || return 1
}

function docommand()
{
	command=$1
	$TCSERVER_ROOT/tcruntime-ctl.sh $instance_name $command -n $instance_root -d $TCSERVER_ROOT
	if [ $? -ne 0 ]; then
		return 1
	fi 
	
	if [ "$command" = "start" ]; then
		checkWebapp || return 1
	fi
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

exit $?
