#!/bin/bash

nn1_status=`/usr/bin/hdfs haadmin -getServiceState nn1`
echo nn1 status is: $nn1_status
nn2_status=`/usr/bin/hdfs haadmin -getServiceState nn2`
echo nn2 status is: $nn2_status
if [ "${nn1_status}" = "active" ]; then
    echo "hdfs haadmin -failover nn1 nn2"
    hdfs haadmin -failover nn1 nn2
elif [ "${nn1_status}" = "standby" ]; then
    echo "hdfs haadmin -failover nn2 nn1"
    hdfs haadmin -failover nn2 nn1
else
    echo "Can't get valid status of nn1, exit."
    exit 1
fi
nn1_status=`/usr/bin/hdfs haadmin -getServiceState nn1`
echo nn1 status now is: $nn1_status
nn2_status=`/usr/bin/hdfs haadmin -getServiceState nn2`
echo nn2 status now is: $nn2_status
