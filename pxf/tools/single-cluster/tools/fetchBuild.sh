#!/usr/bin/env bash

# fetches latest build from a dist server

if [ "x$1" == "x" ]; then
	echo "usage: $0 <job path> <artifact pattern>"
	exit 1
fi

if [ "x$2" == "x" ]; then
	echo "usage: $0 <job path> <artifact pattern>"
	exit 1
fi

jobpath=$1
name=$2

version_file=stackversion.txt
log_file=fetch.log
dist_server=http://hdsh129.lss.emc.com/dist/PHD
tmpfile=/tmp/curldata.${RANDOM}.tmp
sort_order=?O=A
url=$dist_server/$jobpath

# reset log file
echo -n > $log_file

echo Parsing Dist Server page
echo Parsing Dist Server page $url/$sort_order >> $log_file
curl -s $url/$sort_order > $tmpfile
last_build_path=`cat $tmpfile | grep -o "href=\"$name\.tar\.gz\"" | grep -o "$name.tar.gz" | grep -v "\-src" | grep -v "SNAPSHOT" | tail -n1`

echo ----- page start ----- >> $log_file
cat $tmpfile >> $log_file
echo ----- page end ----- >> $log_file

# reserved for SNAPSHOT
#if [ "x$last_build_path" == "x" ]; then
#	last_build_path=`cat $tmpfile | grep -o "href=\"lastSuccessfulBuild[-_\/\.a-zA-Z0-9]*$name\.tar\.gz\"" | grep -o "lastSuccessful.*\.gz" | grep -v "\-src" | head -n1`
#fi

if [ "x$last_build_path" == "x" ]; then
	echo could not find a download link
	exit 1
fi

last_build_number=`cat $tmpfile | grep -o "Last successful build (#[0-9]\+)" | grep -o "[0-9]\+"`
view=`cat $tmpfile | grep -o "<title>[-_0-9a-zA-Z]*" | cut -f 2 -d '>'`

echo Job name: $view
echo Job name: $view >> $log_file

echo Last build: $last_build_number
echo Last build: $last_build_number >> $log_file


find . -regex "\.\/${name}.*.tar.gz" | xargs rm 

echo Downloading $url/$last_build_path
echo use tail -f $log_file to track download
echo Downloading $url/$last_build_path >> $log_file
wget -a $log_file $url/$last_build_path || exit 1

echo creating version file
echo job: $view > $version_file
echo build: $last_build_number >> $version_file
echo file: `basename $last_build_path` >> $version_file

echo creating version file >> $log_file
cat $version_file >> $log_file

rm $tmpfile
