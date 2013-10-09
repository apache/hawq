#!/usr/bin/env bash

utils="sed mkdir rm hostname ed jps"
for util in $utils; do
	which $util > /dev/null 2>&1
	if [ $? != 0 ] ; then
		echo $util was not found
		exit 1
	fi
done

root=`cd \`dirname $0\`;pwd`

PHD_ROOT=$root/phd
HAWQ_ROOT=$root/hawq

echo initialzing PHD
pushd $PHD_ROOT > /dev/null

# cleanup old instance
rm -rf storage

# init HDFS
bin/init-gphd.sh
if [ $? != 0 ]; then
	echo Failed PHD init
	exit 1
fi

# start hdfs
bin/start-hdfs.sh
if [ $? != 0 ]; then
	echo Failed starting HDFS
	exit 1
fi

popd > /dev/null

echo initialzing HAWQ

sed "s,^GPHOME.*,GPHOME=$HAWQ_ROOT," $HAWQ_ROOT/greenplum_path.sh > $HAWQ_ROOT/greenplum_path.sh.tmp
mv $HAWQ_ROOT/greenplum_path.sh.tmp $HAWQ_ROOT/greenplum_path.sh

source $HAWQ_ROOT/greenplum_path.sh
export GPDATA=$HAWQ_ROOT/storage
export MASTER_DATA_DIRECTORY=$GPDATA/master/gpseg-1
echo $GPDATA

# cleanup old instance
rm -rf $GPDATA

# init the instance
mkdir -p $GPDATA
mkdir $GPDATA/master $GPDATA/p1 $GPDATA/p2
hostname > $GPDATA/hosts
gpinitsystem -a -c $HAWQ_ROOT/conf/hawq_config
if [ $? -gt 1 ]; then
	echo gpinitsystem failed
	exit 1
fi

echo cleanup
gpstop -a
$PHD_ROOT/bin/stop-hdfs.sh

echo -----------------------------------------------------------
echo -----------------------------------------------------------
echo
echo cluster initialized successfully
echo For ease of use, please add the following to your environment
echo ------
echo export GPHOME=$HAWQ_ROOT
echo export GPDATA=$GPDATA
echo export MASTER_DATA_DIRECTORY=\$GPDATA/master/gpseg-1
echo source \$GPHOME/greenplum_path.sh
echo
echo export GPHD_ROOT=$PHD_ROOT
echo export HADOOP_ROOT=\$GPHD_ROOT/hadoop
echo export HBASE_ROOT=\$GPHD_ROOT/hbase
echo export HIVE_ROOT=\$GPHD_ROOT/hive
echo export ZOOKEEPER_ROOT=\$GPHD_ROOT/zookeeper
echo export PATH=\$PATH:\$GPHD_ROOT/bin:\$HADOOP_ROOT/bin:\$HBASE_ROOT/bin:\$HIVE_ROOT/bin:\$ZOOKEEPER_ROOT/bin
echo ------
echo
