#!/bin/sh

# Post installation script

env_script=/etc/pxf/conf/pxf-env.sh
user=pxf
group=pxf

# load pxf-env.sh script
if [ ! -f $env_script ]; then
  echo WARNING: failed to find $env_script
else
  source $env_script
fi

# create log directory with necessary permission
mkdir -p $PXF_LOGDIR
chown $user:$group -R $PXF_LOGDIR
chmod 755 $PXF_LOGDIR

# create run directory with necessary permission
mkdir -p $PXF_RUNDIR
chown $user:$group -R $PXF_RUNDIR
chmod 755 $PXF_RUNDIR