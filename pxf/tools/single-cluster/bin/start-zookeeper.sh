#!/usr/bin/env bash
############################
# dov.dorin at gopivotal.com
############################

# Load settings
root=`cd \`dirname $0\`/..;pwd`
bin=$root/bin
. $bin/gphd-env.sh

zookeeper_cfg=$ZOOKEEPER_CONF/zoo.cfg
zookeeper_cfg_tmp=$zookeeper_cfg.preped

sed "s|dataDir.*$|dataDir=$ZOOKEEPER_STORAGE_ROOT|" $zookeeper_cfg > $zookeeper_cfg_tmp
rm -f $zookeeper_cfg
mv $zookeeper_cfg_tmp $zookeeper_cfg

mkdir -p $ZOOKEEPER_STORAGE_ROOT

export ZOO_LOG_DIR=$LOGS_ROOT

echo Starting Zookeeper...
pushd $ZOOKEEPER_ROOT > /dev/null
bin/zkServer.sh start
popd > /dev/null
