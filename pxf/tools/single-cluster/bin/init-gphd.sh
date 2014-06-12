#!/usr/bin/env bash

############################
# dov.dorin at gopivotal.com
############################

# Load settings
root=`cd \`dirname $0\`/..;pwd`
bin=$root/bin
. $bin/gphd-env.sh

cluster_initialized

if [ $? -eq 0 ]; then
	echo storage directory exists, would you like to reset?
	echo this will remove ALL data from singlecluster
	echo "press [y|Y] to continue"

	read reply
	case $reply in
		[yY])
			;;
		*)
			echo abort
			exit 1
			;;
	esac

	rm -rf $STORAGE_ROOT
fi

# Initialize HDFS
$bin/hdfs namenode -format

if [ $? -ne 0 ]; then
	echo cluster initialization failed
	echo check error log in console output
	exit 1
fi

# Initialize PXF instances
for (( i=0; i < $SLAVES; i++ ))
do
	echo initializing PXF instance $i
	$bin/pxf-instance.sh init $i
	if [ $? -ne 0 ]; then
		echo
		echo tcServer instance \#${i} initialization failed
		echo check console output
		exit 1
	fi
done

echo
echo
echo Cluster initialized
echo For ease of use, add the following to your environment
echo export GPHD_ROOT=$GPHD_ROOT
echo export HADOOP_ROOT=\$GPHD_ROOT/hadoop
echo export HBASE_ROOT=\$GPHD_ROOT/hbase
echo export HIVE_ROOT=\$GPHD_ROOT/hive
echo export ZOOKEEPER_ROOT=\$GPHD_ROOT/zookeeper
echo export PATH=\$PATH:\$GPHD_ROOT/bin:\$HADOOP_ROOT/bin:\$HBASE_ROOT/bin:\$HIVE_ROOT/bin:\$ZOOKEEPER_ROOT/bin
echo -------------------------------------------------------------
