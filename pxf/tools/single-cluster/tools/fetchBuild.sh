#!/usr/bin/env bash

# fetches latest build from a Jenkins job

if [ "x$1" == "x" ]; then
	echo "usage: $0 <job path> <name>"
	exit 1
fi

if [ "x$2" == "x" ]; then
	echo "usage: $0 <job path> <name>"
	exit 1
fi

jobpath=$1
name=$2

jenkins_server=http://hdsh132.lss.emc.com:8080
tmpfile=/tmp/curldata.${RANDOM}.tmp
url=$jenkins_server/$jobpath

echo Getting Jenkins page
curl $url/ > $tmpfile
last_build_path=`cat $tmpfile | grep -o "href=\"lastSuccessfulBuild[-_\/\.a-zA-Z0-9]*\.tar\.gz\"" | grep -o "lastSuccessful.*\.gz" | grep -v "\-src" | grep -v "SNAPSHOT" | head -n1`
if [ "x$last_build_path" == "x" ]; then
	last_build_path=`cat $tmpfile | grep -o "href=\"lastSuccessfulBuild[-_\/\.a-zA-Z0-9]*\.tar\.gz\"" | grep -o "lastSuccessful.*\.gz" | grep -v "\-src" | head -n1`
fi

last_build_number=`cat $tmpfile | grep -o "Last successful build (#[0-9]\+)" | grep -o "[0-9]\+"`
view=`cat $tmpfile | grep -o "<title>[-_0-9a-zA-Z]*" | cut -f 2 -d '>'`

echo Job name: $view
echo Last build: $last_build_number

rm $name*.tar.gz

echo Downloading $url/$last_build_path
wget $url/$last_build_path || exit 1

echo renaming `basename $last_build_path` to `basename $last_build_path .tar.gz`__$view-$last_build_number.tar.gz
mv `basename $last_build_path` `basename $last_build_path .tar.gz`__$view-$last_build_number.tar.gz || exit 1

rm $tmpfile
