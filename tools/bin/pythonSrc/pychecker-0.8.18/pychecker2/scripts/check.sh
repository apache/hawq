#! /bin/sh
PYTHONPATH=$PYTHONPATH:.. pychecker --stdlib *.py
echo ==============================================================
scripts/pychecker2.sh $* *.py
