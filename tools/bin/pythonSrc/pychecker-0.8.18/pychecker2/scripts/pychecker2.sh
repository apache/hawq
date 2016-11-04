#! /bin/sh
NAME=$0				# this script
NAME=`dirname $NAME`		# trim scripts 
NAME=$NAME/../..
export PYTHONPATH=$PYTHONPATH:$NAME
exec python2.2 $NAME/pychecker2/main.py $@
