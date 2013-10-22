#!/usr/bin/env bash
############################
# dov.dorin at gopivotal.com
############################

# Load settings
root=`cd \`dirname $0\`/..;pwd`
bin=$root/bin
. $bin/gphd-env.sh

# Start Hive Metastore service

HIVE_PID_DIR=$PIDS_ROOT
HIVE_LOG_DIR=$LOGS_ROOT
HIVE_IDENT_STRING=`whoami`
HIVE_NICENESS=0
command=metastore
log=$HIVE_LOG_DIR/hive-$HIVE_IDENT_STRING-$command-`hostname`.out
pid=$HIVE_PID_DIR/hive-$HIVE_IDENT_STRING-$command.pid

. $HIVE_ROOT/conf/hive-env.sh

# Keep last 5 logs
hive_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

[ -w "$HIVE_PID_DIR" ] ||  mkdir -p "$HIVE_PID_DIR"

if [ -f $pid ]; then
	if kill -0 `cat $pid` > /dev/null 2>&1; then
		echo $command running as process `cat $pid`.  Stop it first.
		exit 1
	fi
fi

hive_rotate_log $log
echo starting $command, logging to $log
nohup nice -n $HIVE_NICENESS $HIVE_BIN/hive --config $HIVE_ROOT/conf --service $command $HIVE_OPTS > $log 2>&1 < /dev/null &
echo $! > $pid
