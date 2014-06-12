#!/usr/bin/env bash

############################
# dov.dorin at gopivotal.com
############################

# Load settings
root=`cd \`dirname $0\`/..;pwd`
bin=$root/bin
. $bin/gphd-env.sh

cluster_initialized
if [ $? -ne 0 ]; then
	echo cluster not initialized 
	echo please run $bin/init-gphd.sh
	exit 1
fi

# Start PXF
for (( i=0; i < $SLAVES; i++ ))
do
	$bin/pxf-instance.sh start $i | sed "s/^/node $i: /"
done
