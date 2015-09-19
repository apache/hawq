#!/bin/sh

# Path to HDFS native libraries
export LD_LIBRARY_PATH=/usr/lib/hadoop/lib/native:${LD_LIBRARY_PATH}

# Path to JAVA
export JAVA_HOME=${JAVA_HOME:=/usr/java/default}

# Path to Log directory
export PXF_LOGDIR=/var/log/pxf
export CATALINA_OUT=${PXF_LOGDIR}/catalina.out

# Path to Run directory
export PXF_RUNDIR=/var/run/pxf