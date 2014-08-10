#!/usr/bin/env bash

# fetches latest file matching <pattern> from the <url>

if [ "x$1" == "x" -o "x$2" == "x" ]; then
	echo "usage: `basename $0` <url> <pattern>"
	echo "       will fetch latest artifact matching <pattern> from <url>"
	exit 1
fi

server=$1
pattern=$2

log_file=fetch.log
tmpfile=/tmp/curldata.${RANDOM}.tmp
sort_order=?O=A
page_url=$server/$sort_order

echo Access page: $page_url | tee -a $log_file
curl -s $page_url > $tmpfile
if [ $? -ne 0 ]; then
	echo page download failed | tee -a $log_file
	exit 1
fi

echo ----- page start ----- >> $log_file
cat $tmpfile >> $log_file
echo ----- page end ----- >> $log_file

last_build_file=`cat $tmpfile | grep -o "href=\"${pattern}\.tar\.gz\"" | grep -o "${pattern}.tar.gz" | tail -n1`

if [ "x$last_build_file" == "x" ]; then
	echo could not find a download link | tee -a $log_file
	exit 1
fi

find . -regex "\.\/${pattern}.*.tar.gz" | xargs rm 

echo Latest artifact: $last_build_file | tee -a $log_file
echo Downloading: $server/$last_build_file | tee -a $log_file
echo use tail -f `pwd`/$log_file to track download

wget -a $log_file $server/$last_build_file
if [ $? -ne 0 ]; then
	echo download failed
	exit 1
fi

rm $tmpfile
