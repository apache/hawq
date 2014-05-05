#!/usr/bin/env bash

############################
# dov.dorin at gopivotal.com
############################

# Load settings
root=`cd \`dirname $0\`/..;pwd`
bin=$root/bin
. $bin/gphd-env.sh

for (( i=0; i < $SLAVES; i++ ))
do
	$bin/pxf-instance.sh stop $i | sed "s/^/node $i: /"
done
