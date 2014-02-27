#!/bin/sh
#
# Copyright (c) Greenplum Inc 2013. All Rights Reserved.
#
USAGE="$0 -f <hosts file> [-x]"

function install_master() {
  echo "Copying artifacts to master"
  # Copy pgcrypto.so to master. (Assuming this script is run from the master)
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
}

function install_segments() {
  echo "Copying artifacts to segment hosts"
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
}

function create_functions() {
  echo "Creating pgcrypto functions."
  psql -d template1 -f share/postgresql/contrib/pgcrypto.sql
  if [ 0 -ne $? ]; then
      echo "Failed to create pgcrypto functions."
      exit 1
  fi
  echo "pgcrypto functions are now ready to use."
}


### Validation 
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

expand=false
hosts=""
while getopts f:x opt; do
  case $opt in
  f)
      hosts=$OPTARG
      ;;
  x)
      echo "Running in expand mode"
      expand=true
      ;;   
  esac
done

if [ "$hosts" = "" ]; then
    echo "<hosts file> not specified."
    echo $USAGE
    exit 1
fi
if [ ! -f $hosts ]; then
    echo "Cannot read file: $hosts."
    exit 1
fi

# Validate GPHOME exists on all hosts.
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


###Install
if $expand; then 
  #Only install on segments when in expand mode
  install_segments
else
  #Install on master and segments when in non-expand mode
  install_segments
  install_master
  create_functions
fi
