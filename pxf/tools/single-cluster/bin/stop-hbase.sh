#!/usr/bin/env bash
############################
# dov.dorin at gopivotal.com
############################

# Load settings
root=`cd \`dirname $0\`/..;pwd`
bin=$root/bin
. $bin/gphd-env.sh

# TODO cleanup after hbase?

if [ "$START_STARGATE" == "true" ]; then
	echo Stopping Stargate...
	$HBASE_BIN/hbase-daemon.sh --config $HBASE_CONF stop rest
fi

pushd $HBASE_ROOT > /dev/null
bin/stop-hbase.sh
popd > /dev/null
