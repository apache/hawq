#!/usr/bin/env bash
############################
# dov.dorin at gopivotal.com
############################

# Load settings
root=`cd \`dirname $0\`/..;pwd`
bin=$root/bin
. $bin/gphd-env.sh

if [ "x$1" == "x" -o "x$2" == "x" ]; then
	echo "usage: $0 <hiveservice> <stop|start>"
	echo "services: hiveserver, hiveserver2, metastore"
	exit 1
fi

service=$1
command=$2

case "$service" in 
	"hiveserver" )
		;;
	"hiveserver2" )
		;;
	"metastore" )
		;;
	* )
		echo "unknown service $service"
		echo "services: hiveserver, hiveserver2, metastore"
		exit 1
		;;
esac

# Start a Hive service

HIVE_PID_DIR=$PIDS_ROOT
HIVE_LOG_DIR=$LOGS_ROOT
HIVE_IDENT_STRING=`whoami`
HIVE_NICENESS=0
HIVE_STOP_TIMEOUT=3
log=$HIVE_LOG_DIR/hive-$HIVE_IDENT_STRING-$service-`hostname`.out
pid=$HIVE_PID_DIR/hive-$HIVE_IDENT_STRING-$service.pid

. $HIVE_ROOT/conf/hive-env.sh

# Keep last 5 logs
hive_rotate_log ()
{
	if [ ! -d $HIVE_LOG_DIR ]; then
		mkdir $HIVE_LOG_DIR
	fi

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

start()
{
	[ -w "$HIVE_PID_DIR" ] ||  mkdir -p "$HIVE_PID_DIR"

	if [ -f $pid ]; then
		if kill -0 `cat $pid` > /dev/null 2>&1; then
			echo $service running as process `cat $pid`.  Stop it first.
			exit 1
		fi
	fi

	hive_rotate_log $log
	echo starting $service, logging to $log
	nohup nice -n $HIVE_NICENESS $HIVE_BIN/hive --config $HIVE_ROOT/conf --service $service $HIVE_OPTS > $log 2>&1 < /dev/null &
	echo $! > $pid
}

stop()
{
	if [ -f $pid ]; then
		TARGET_PID=`cat $pid`
		if ! kill -0 $TARGET_PID > /dev/null 2>&1; then
			TARGET_PID_REAL=`ps -e -o pid= -o ppid= | grep "$TARGET_PID\$" | awk '{print $1}'`
			TARGET_PID=$TARGET_PID_REAL
		fi
		if kill -0 $TARGET_PID > /dev/null 2>&1; then
			echo stopping $command
			kill $TARGET_PID
			sleep $HIVE_STOP_TIMEOUT
			if kill -0 $TARGET_PID > /dev/null 2>&1; then
				echo "$command did not stop gracefully after $HIVE_STOP_TIMEOUT seconds: killing with kill -9"
				kill -9 $TARGET_PID
			fi
		else
			echo no $command to stop
		fi
	else
		echo no $command to stop
	fi
}

case "$command" in
	"start" )
		start
		;;
	"stop" )
		stop
		;;
	* )
		echo unknown command $command
		;;
esac

exit $?
