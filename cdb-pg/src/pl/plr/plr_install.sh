#!/bin/sh
#
# Copyright (c) Greenplum Inc 2013. All Rights Reserved.
#
# Disclaimer: There is a lot of common code among pgcrypto_install.sh,
# plr_install.sh and pljava_install.sh.  These scripts must be
# refactored at an opportune time.  In fact, HAWQ segments should be
# intelligent enough so as to pick up a list of packages from master
# and install them.

USAGE="$0 -f <hosts file> [-x]"

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
expand_mode=0
while getopts f:x opt; do
  case $opt in
  f)
      hosts=$OPTARG
      ;;
  x)
      # If we are running post gpexpand.
      expand_mode=1
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
    if [[ $(uname) =~ Darwin* ]]; then
	BLD_ARCH=osx106_x86
    elif [[ $(uname) =~ Linux* ]]; then
	BLD_ARCH=rhel5_x86_64
    else
	echo "Platform not supported."
	exit 1
    fi
fi

function install_R_segments() {
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
}

# Copy plr.so and modified greenplum_path.sh on segments.
function install_plr_segments() {
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
}

if [ $expand_mode -eq 1 ]; then
    # Install PL/R artifacts only on the newly added segments, found
    # in $hosts file.
    install_R_segments
    install_plr_segments
    echo "plr installation on segments succeeded."
    echo "Restart HAWQ using 'gpstop -ar' for the changes to take effect."
    exit 0
fi

# Copy R binaries on master in $GPHOME/ext.
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

# Copy plr.so on master.
echo "Installing plr extension on master"
cp lib/postgresql/plr.so $GPHOME/lib/postgresql
if [ 0 -ne $? ]; then
    echo "Failed to copy plr artifacts on master."
    exit 1
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


# Handle single node cluster case.  The following grep will find
# R-2.13.0-1 directory already because it was copied by cp above.  In
# which case, we should skip gpscp steps.
single_node=0
output=$(gpssh -f $hosts "ls -ld $GPHOME/ext/*/" | grep "R-2.13.0-1")
[ 0 -eq $? ] && single_node=1

if [ $single_node -eq 0 ]; then
    install_R_segments
    install_plr_segments
fi

echo "plr installation was successful."
echo "The following steps are needed before plr language may be used:"
echo "(1) source greenplum_path.sh."
echo "(2) Restart HAWQ using 'gpstop -ar'."
echo "(3) Log into desired database using psql and execute 'CREATE LANGUAGE plr'."
echo "(4) Create library of plr functions by executing $GPHOME/share/postgres/contrib/plr.sql file."
