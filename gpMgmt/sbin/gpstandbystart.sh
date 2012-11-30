#!/bin/bash
DATADIR=$1
PORT=$2
DBID=$3
CONTENTID=-1 # always to the case for a master
CLUSTERCONTENTS=$4

#
# save prior startup.log
#
if [ -e $DATADIR/pg_log/startup.log ]; then
    mv -f $DATADIR/pg_log/startup.log $DATADIR/pg_log/`date +startup.log-saved-%Y-%m-%d_%H%M%S`
    touch $DATADIR/pg_log/startup.log
fi

# MPP-15405
GPKILL=NEVER
export GPKILL

#
# start syncmaster
#
$GPHOME/bin/gpsyncmaster -D $DATADIR \
	-p $PORT \
	-b $DBID \
	-C $CONTENTID \
	-z $CLUSTERCONTENTS \
	-i >> $DATADIR/pg_log/startup.log 2>&1 &

#
# watch syncmaster output for a moment
#
exec $GPHOME/sbin/gpstandbywatch.py $DATADIR
