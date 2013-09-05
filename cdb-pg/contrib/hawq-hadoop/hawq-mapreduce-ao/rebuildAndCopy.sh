#!/bin/sh

mvn clean package
rm -rf lineitem_test
rm -rf lineitem_db
rm -rf lineitem_copy
rm -rf lineitem_input
rm -rf alltype_test
cd ../..
tar -cf hawq-hadoop.tar hawq-hadoop
scp -r hawq-hadoop.tar ioformat@sdw0:~
ssh ioformat@sdw0 "tar -xf hawq-hadoop.tar"
ssh ioformat@sdw0 "rm -rf /data2/ioformat/hawq-hadoop"
ssh ioformat@sdw0 "mv hawq-hadoop /data2/ioformat"
ssh ioformat@sdw0 "rm -rf hawq-hadoop.tar"
rm -rf hawq-hadoop.tar
cd hawq-hadoop
