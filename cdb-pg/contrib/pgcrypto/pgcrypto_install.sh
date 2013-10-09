#!/bin/sh
#
# Copyright (c) Greenplum Inc 2013. All Rights Reserved.
#
USAGE="$0 -f <hosts file>"

if [ ! -d $GPHOME ]; then
    echo "GPHOME is either not set or is not a directory."
    exit 1
fi

which gpssh > /dev/null
if [ 0 -ne $? ]; then
    echo "gpssh not found in PATH."
    exit 1
fi

which gpscp > /dev/null
if [ 0 -ne $? ]; then
    echo "gpscp not found in PATH."
    exit 1
fi

hosts=""
while getopts f: opt; do
  case $opt in
  f)
      hosts=$OPTARG
      ;;
  esac
done

if [ "$hosts" = "" ]; then
    echo "<hosts file> not specified."
    echo $USAGE
    exit 1
fi

# Validate GPHOME exists on all segments.
cmd="gpssh -f $hosts test -d $GPHOME"
output=$($cmd)
if [ 0 -ne $? ]; then
    echo "Directory $GPHOME not found on one or more segments."
    exit 1
fi
if [[ $output == *ERROR* ]]; then
    echo "Error running gpssh."
    echo "Command: $cmd"
    exit 1
fi

echo "Copying artifacts to master and segments."
# Copy pgcrypto.so to master and segments.
cp lib/postgresql/pgcrypto.so $GPHOME/lib/postgresql
if [ 0 -ne $? ]; then
    echo "Failed to copy artifacts on master."
    exit 1
fi
# SQL scripts are needed only on the master.
cp share/postgresql/contrib/pgcrypto.sql \
    share/postgresql/contrib/uninstall_pgcrypto.sql \
    $GPHOME/share/postgresql/contrib
if [ 0 -ne $? ]; then
    echo "Failed to copy artifacts on master."
    exit 1
fi

cmd="gpscp -f $hosts lib/postgresql/pgcrypto.so =:$GPHOME/lib/postgresql"
output=$($cmd)
if [ 0 -ne $? ]; then
    echo "Failed to copy artifacts to one or more segments."
    # Clean up.
    output=$(gpssh -f $hosts rm -f $GPHOME/lib/postgresql/pgcrypto.so)
    exit 1
fi
if [[ $output == *ERROR* ]]; then
    echo "Error running gpscp."
    echo "Command: $cmd"
    exit 1
fi

echo "Creating pgcrypto functions."
psql -d template1 -f share/postgresql/contrib/pgcrypto.sql
if [ 0 -ne $? ]; then
    echo "Failed to create pgcrypto functions."
    exit 1
fi
echo "pgcrypto functions are now ready to use."