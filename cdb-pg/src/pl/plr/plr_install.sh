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
while getopts f:d: opt; do
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
    echo "Output: $output"
    exit 1
fi

if [ "$BLD_ARCH" == "" ]; then
    if [[ $(uname) == "Darwin*" ]]; then
	BLD_ARCH=osx106_x86
    elif [[ $(uname) == "Linux*" ]]; then
	BLD_ARCH=rhel5_x86_64
    else
	echo "Platform not supported."
	exit 1
    fi
fi

# Copy R binaries on master and segments in $GPHOME/ext.
echo "Installing R binaries on master"
# Wipe out existing R binaries if any.
cmd="rm -rf $GPHOME/ext/R-2.13.0-1"
$($cmd)
output=$(gpssh -f $hosts $cmd)
cp -R R-2.13.0-1 $GPHOME/ext
if [ 0 -ne $? ]; then
    echo "Failed to install R binaries on master."
    exit 1
fi

# Handle single node cluster case.  The following grep will find
# R-2.13.0-1 directory already because it was copied by cp above.  In
# which case, we should skip gpscp steps.
single_node=0
output=$(gpssh -f $hosts "ls -ld $GPHOME/ext/*/" | grep "R-2.13.0-1")
[ 0 -eq $? ] && single_node=1

if [ $single_node -eq 0 ]; then
    echo "Installing R binaries on segments"
    cmd="gpscp -f $hosts -r $GPHOME/ext/R-2.13.0-1 =:$GPHOME/ext"
    output=$($cmd)
    if [ 0 -ne $? ]; then
	echo "Failed to copy R binaries to one or more segments."
	echo "Command: $cmd"
	echo "Output: $output"
    # Clean up.
	output=$(gpssh -f $hosts rm -rf $GPHOME/ext/R-2.13.0-1)
	exit 1
    fi
    if [[ $output == *ERROR* ]]; then
	echo "Error running gpscp."
	echo "Command: $cmd"
	exit 1
    fi
fi

# Copy plr.so to master and segments.
echo "Installing plr extenstion on master"
cp lib/postgresql/plr.so $GPHOME/lib/postgresql
if [ 0 -ne $? ]; then
    echo "Failed to copy plr artifacts on master."
    exit 1
fi

if [ $single_node -eq 0 ]; then
    echo "Installing plr extension on segments"
    cmd="gpscp -f $hosts lib/postgresql/plr.so =:$GPHOME/lib/postgresql"
    output=$($cmd)
    if [ 0 -ne $? ]; then
	echo "Failed to copy artifacts to one or more segments."
	# Clean up.
	output=$(gpssh -f $hosts rm -f $GPHOME/lib/postgresql/plr.so)
	exit 1
    fi
    if [[ $output == *ERROR* ]]; then
	echo "Error running gpscp."
	echo "Command: $cmd"
	exit 1
    fi
fi

# SQL file is needed only on the master.
cp share/postgresql/contrib/plr.sql \
    $GPHOME/share/postgresql/contrib
if [ 0 -ne $? ]; then
    echo "Failed to copy artifacts on master."
    exit 1
fi

# Patch greenplum_path.sh to include R library in LD_LIBRARY_PATH.
# Remove existing R_HOME related lines, if any.
grep -v R_HOME $GPHOME/greenplum_path.sh > ./greenplum_path.sh.tmp
if [ ! -f ./greenplum_path.sh.tmp ]; then
    echo "Failed to patch $GPHOME/greenplum_path.sh."
    echo "Command: $cmd"
    echo "Output: $output"
    exit 1
fi
echo 'export R_HOME=$GPHOME/ext/R-2.13.0-1' >> ./greenplum_path.sh.tmp
if [ $BLD_ARCH == "rhel5_x86_64" ]; then
    echo 'export LD_LIBRARY_PATH=$R_HOME/lib:$LD_LIBRARY_PATH' >> ./greenplum_path.sh.tmp
else
    echo 'export DYLD_LIBRARY_PATH=$R_HOME/lib:$DYLD_LIBRARY_PATH' >> ./greenplum_path.sh.tmp
fi
cmd="mv ./greenplum_path.sh.tmp $GPHOME/greenplum_path.sh"
output=$($cmd)
if [ 0 -ne $? ]; then
    echo "Failed to patch $GPHOME/greenplum_path.sh."
    echo "Command: $cmd"
    echo "Output: $output"
    exit 1
fi

# Verify if we patched greenplum_path.sh correctly.
output=$(grep LD_LIBRARY_PATH.*R_HOME $GPHOME/greenplum_path.sh)
if [ 0 -ne $? ]; then
    echo "Failed to patch $GPHOME/greenplum_path.sh"
    exit 1
fi

if [ $single_node -eq 0 ]; then
    # Copy the modified greenplum_path.sh to segments.
    cmd="gpscp -f $hosts $GPHOME/greenplum_path.sh =:$GPHOME"
    output=$($cmd)
    if [ 0 -ne $? ]; then
	echo "Failed to copy greenplum_path.sh to one or more segments."
	exit 1
    fi
    if [[ $output == *ERROR* ]]; then
	echo "Error running gpscp."
	echo "Command: $cmd"
	exit 1
    fi
fi

echo "plr installation was successful."
echo "The following steps are needed before plr language may be used:"
echo "(1) source greenplum_path.sh."
echo "(2) Restart HAWQ using 'gpstop -ar'."
echo "(3) Log into desired database using psql and execute 'CREATE LANGUAGE plr'."
echo "(4) Create library of plr functions by executing $GPHOME/share/postgres/contrib/plr.sql file."