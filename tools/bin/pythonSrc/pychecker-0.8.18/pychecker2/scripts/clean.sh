#! /bin/sh
for i in `find . -type d -print`
do
    /bin/rm -f *.pyc *.pyo *~ 
done
