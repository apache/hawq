#!/bin/bash

if [ $# -ne 1 ] ; then
  echo "usage: sh create_table_file.sh <file path>"
  exit 1
fi

echo "creating file $1"
file=$1
tmp_file=${1}_tmp

for i in {1..1000}; do echo "t$i,$i"; done > $file
for i in {1..15}; do cat $file $file > $tmp_file && mv $tmp_file $file; done
