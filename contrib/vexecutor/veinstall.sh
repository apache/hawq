#!/bin/bash
if [ -z "$1" ]; then
	echo "master directory required"
	exit 1
fi
if [ -z "$2" ]; then
	echo "segment directory required"
	exit 1
fi
if [ -z "$3" ]; then
	echo "dbname required"
	exit 1
fi

MASTER_DIRECTORY=$1 
SEGMENT_DIRECTORY=$2
DBNAME=$3

echo "shared_preload_libraries = 'vexecutor' "  >> $MASTER_DIRECTORY/postgresql.conf
echo "shared_preload_libraries = 'vexecutor' "  >> $SEGMENT_DIRECTORY/postgresql.conf

hawq restart cluster -a

psql -d $DBNAME -f ./create_type.sql

exit 0
