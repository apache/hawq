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

# stop region servers
for (( i=0; i < $SLAVES; i++ )); do
	$bin/hbase-regionserver.sh stop $i | sed "s/^/node $i: /"
done

# stop master
pid=`echo $PIDS_ROOT/hbase-*-master.pid`
hbase_pid=`cat $pid`
command=master
hbase_stop_timeout=9

$bin/hbase $command stop > /dev/null 2>&1 &

if kill -0 $hbase_pid > /dev/null 2>&1; then
	echo stopping $command
	kill $hbase_pid
	sleep $hbase_stop_timeout
	if kill -0 $hbase_pid > /dev/null 2>&1; then
		echo "$command did not stop gracefully after $hbase_stop_timeout seconds: killing with kill -9"
		kill -9 $hbase_pid
	fi
else
	echo no $command to stop
fi
