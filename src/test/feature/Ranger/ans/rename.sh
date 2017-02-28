#!/bin/bash

for  i in `ls|grep out`
do
    newName=`echo $i | sed 's/.\out/\.ans/g'`
    mv $i $newName
done
