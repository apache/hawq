#!/usr/bin/env bash
############################
# dov.dorin at gopivotal.com
############################

# Load settings
root=`cd \`dirname $0\`/..;pwd`
bin=$root/bin
. $bin/gphd-env.sh

# Stop Hive MetaStore

HIVE_PID_DIR=$PIDS_ROOT
HIVE_IDENT_STRING=`whoami`
HIVE_NICENESS=0
HIVE_STOP_TIMEOUT=3
command=metastore
pid=$HIVE_PID_DIR/hive-$HIVE_IDENT_STRING-$command.pid

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
