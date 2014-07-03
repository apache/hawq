#!/bin/sh

# Path to HDFS native libraries
export LD_LIBRARY_PATH=/usr/lib/gphd/hadoop/lib/native:${LD_LIBRARY_PATH}

# Path to JAVA
export JAVA_HOME=${JAVA_HOME:=/usr/java/default}

