#!/usr/bin/env bash
############################
# dov.dorin at gopivotal.com
############################

# Load settings
root=`cd \`dirname $0\`/..;pwd`
bin=$root/bin
. $bin/gphd-env.sh

pushd $ZOOKEEPER_ROOT > /dev/null
bin/zkServer.sh stop
popd > /dev/null
