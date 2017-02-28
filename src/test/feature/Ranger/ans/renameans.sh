#!/bin/bash

for  i in `ls|grep ans|grep admin`
do
    newName=`echo $i | sed 's/admin/adminfirst/g'`
    cp $i $newName
done
