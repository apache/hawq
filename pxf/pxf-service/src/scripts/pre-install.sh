#!/bin/sh

instance_root=/var/gphd/pxf

if [ "x$JAVA_HOME" == "x" ]; then
	echo JAVA_HOME undefined, exiting...
	exit 1
fi

if [ -d $instance_root ]; then
	echo PXF instance already exists in $instance_root, exiting...
	exit 2
fi
