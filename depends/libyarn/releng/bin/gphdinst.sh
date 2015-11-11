#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
## ======================================================================
## TODO
## * make script work for single and muli-node installs
## * should we be able to identify the usage of the hdfs instance
## * need to determine how to setup/start httpd
## * how to pass in server name values
## ======================================================================

# Do not run on gpdb9.rel.dh.greenplum.com as this script will delete its shared HDFS cluster
if [[ $(hostname) =~ "gpdb9.rel" ]]; then
    echo "ABORT! Do not run this script on gpdb9.rel.dh.greenplum.com as this script will delete its shared cluster!"
    exit 1
fi

# Make sure only RHEL6.2 machines can run our script
cat /etc/redhat-release | grep "6." > /dev/null
if [ $? -ne 0 ]; then
    echo "This script must be run on a RHEL6.2 cluster"
    exit 1
fi

# Make sure only root can run our script
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root" 1>&2
   exit 1
fi

# Make sure if using HDFS HA that there are at least 3 datanodes not including a namenode
if [ "${ENABLE_QUORUM}" = "true" ] && [ ! $( echo ${DATANODES} | wc -w ) -ge 3 ]; then
    echo "You must have at least three datanodes to have as the zkservers"
    exit 1
fi

##
## Courtesy - http://stevemorin.blogspot.com/2007/10/bash-get-self-directory-trick.html
## Updated to run on rhel3 (kite12)
##

script_path="$(cd $(dirname $0); pwd -P)/$(basename $0)"

[[ ! -f "$script_path" ]] && script_path="$(cd $(/usr/bin/dirname "$0"); pwd -P)/$(basename "$0")"
[[ ! -f "$script_path" ]] && script_path="" && echo 'No full path to running script found!' && exit 1

script_dir="${script_path%/*}"

##
## Global variables
##

DCA_SETUP=${DCA_SETUP:=false}
DOWNLOAD_DIR=${DOWNLOAD_DIR:=~/downloads}
ARCHIVE_URL=${ARCHIVE_URL:=http://hdsh132.lss.emc.com:8080/view/Ongoing-sprint-release-build/job/HudsonHD2_0_1_0ReleaseBuild_ongoing_sprint_release/lastSuccessfulBuild/artifact/*zip*/archive.zip}
ARCHIVE=$( basename ${ARCHIVE_URL} )
HADOOP_CONF_DIR=/etc/gphd/hadoop/conf
ZK_CONF_DIR=/etc/gphd/zookeeper/conf
NAMENODE=${NAMENODE:=smdw}
SECONDARY_NAMENODE=${SECONDARY_NAMENODE:=mdw}
DATANODES=${DATANODES:=sdw1 sdw2}
RESOURCEMANAGER=${RESOURCEMANAGER:=smdw}
NODEMANAGERS=${DATANODES:=sdw1 sdw2}
HDFS_DATA_DIRECTORY_ROOT=${HDFS_DATA_DIRECTORY_ROOT:=/data}
JOURNAL_DATA_ROOT=${JOURNAL_DATA_ROOT:=/data/journal}
export HADOOP_HOME=${HADOOP_HOME:=/usr/lib/gphd/hadoop}
export HADOOP_YARN_HOME=${HADOOP_YARN_HOME:=/usr/lib/gphd/hadoop-yarn}
export ZK_HOME=${ZK_HOME:=/usr/lib/gphd/zookeeper}
export ZOO_LOG_DIR=${ZOO_LOG_DIR:=/tmp/zookeeper/logs}
export JAVA_HOME=${JAVA_HOME:=/opt/jdk1.7.0_15}

## ======================================================================
## Functions
## ======================================================================

## ----------------------------------------------------------------------
## gphd_stop_remove()
##   o Stop any existing namenode and datanode processes
##   o Display running java processes with "jps" command
##   o Remove hadoop*, bigtop* and zookeeper rpms
##   o Remove /usr/lib/gphd
## ----------------------------------------------------------------------

display_jps(){
    echo "======================================================================"
    for node in ${NAMENODE} ${SECONDARY_NAMENODE} ${DATANODES/%${SECONDARY_NAMENODE}/}; do
        echo $node
        ssh $node "${JAVA_HOME}/bin/jps"
        echo
    done
    echo "======================================================================"
}

gphd_stop_remove(){

    ## Sanity Stop request for NN, 2NN, and DN only (Actual stop is when run finishes in sys_mgmt_utils.sh)
    ssh ${NAMENODE}           "[ -f ${HADOOP_HOME}/sbin/hadoop-daemon.sh ] && sudo -u hdfs ${HADOOP_HOME}/sbin/hadoop-daemon.sh stop namenode"
    ssh ${SECONDARY_NAMENODE} "[ -f ${HADOOP_HOME}/sbin/hadoop-daemon.sh ] && sudo -u hdfs ${HADOOP_HOME}/sbin/hadoop-daemon.sh stop secondarynamenode"

    for node in ${DATANODES}; do
	ssh $node "[ -f ${HADOOP_HOME}/sbin/hadoop-daemon.sh ] && sudo -u hdfs ${HADOOP_HOME}/sbin/hadoop-daemon.sh stop datanode"
    done

    display_jps

    for node in ${NAMENODE} ${SECONDARY_NAMENODE} ${DATANODES/%${SECONDARY_NAMENODE}/}; do
	    ssh $node "rm -rf /data*/hdfs /tmp/hadoop-*.pid /tmp/hsperfdata_*;"
    done

    # Remove hadoop, bigtop, and zookeeper rpms in each node
    for node in ${NAMENODE} ${SECONDARY_NAMENODE} ${DATANODES/%${SECONDARY_NAMENODE}/}; do
        ssh $node '\
        hostname; \
        for rpm in hadoop bigtop zookeeper; do \
            echo "----------------------------------------------------------------------"; \
            echo "Processing $rpm"; \
            rpm -qa | grep $rpm; \
            if [ $? = 0 ]; then \
                yum remove -y $( rpm -qa | grep $rpm ); \
                rpm -qa | grep $rpm; \
            fi; \
            rm -rf /etc/gphd/hadoop* /etc/gphd/zookeeper* /usr/lib/gphd/zookeeper* /var/lib/gphd/hadoop* /var/log/gphd/hadoop* /var/lib/gphd/zookeeper*; \
            echo "----------------------------------------------------------------------"; \
        done'
    done
}

retrieve_hdfs_archive(){
    echo "======================================================================"
    
    echo "Retrieving ${ARCHIVE_URL}"
    
    if [ ! -d "${DOWNLOAD_DIR}" ]; then
        mkdir ${DOWNLOAD_DIR}
    fi
    
    rm -rf ${DOWNLOAD_DIR}/*
    
    wget --no-verbose ${ARCHIVE_URL} -O ${DOWNLOAD_DIR}/${ARCHIVE}
    if [ $? != 0 ]; then
        echo "FATAL: retrieval failed (${ARCHIVE_URL})."
        exit 2
    fi
    
    echo "======================================================================"
}

## ######################################################################
## MAIN Script
## ######################################################################

##
## If necessary, stop hadoop services and remove previously installed
## rpms (and files).
##

gphd_stop_remove

##
## Retrieve latest hdfs archive from jenkins server
##

retrieve_hdfs_archive

##
## Process retrieved archive
##

cd ${DOWNLOAD_DIR}

if [ ! -f "${ARCHIVE}" ]; then
    echo "FATAL: archive does note exist (${ARCHIVE})"
    exit 2
fi

ls | grep archive.zip >> /dev/null
if [ $? == 0 ]; then
    unzip ${ARCHIVE}
    if [ $? != 0 ]; then
	echo "FATAL: unzip of archive failed (${ARCHIVE})."
	exit 2
    fi

    cd archive/dist
fi

GPHD_TARBALL=$( ls )
if [ ! -f "${GPHD_TARBALL}" ]; then
    echo "FATAL: gphd tarball does not exist."
    exit 2
fi
    
GPHD_DIR=$( basename * .tar.gz )

tar xf ${GPHD_TARBALL}
if [ $? != 0 ]; then
    echo "FATAL: tar expansion failed (${GPHD_TARBALL})."
    exit 2
fi

##
## Prepare file based yum repository
##

echo "======================================================================"

cd ${GPHD_DIR}
REPO_DIR=$( pwd )
createrepo ${REPO_DIR}

echo "======================================================================"

rm -f /etc/yum.repos.d/gphd.repo

cat > /etc/yum.repos.d/gphd.repo <<-EOF
	[gphd]
	name=Greenplum HD repository
	baseurl=http://${SECONDARY_NAMENODE}/yum/base
	enabled=1
	gpgcheck=0
EOF

if [ ! -d /var/www/html/yum ] ; then
    mkdir -p /var/www/html/yum
    chmod 777 /var/www/html/yum
fi
rm -rf /var/www/html/yum/base

rsync -auv ${REPO_DIR}/* /var/www/html/yum/base
chmod -R 777 /var/www/html/yum/base
cd ${DOWNLOAD_DIR}
rm -rf ${DOWNLOAD_DIR}/*

service httpd start

##
## Configure hdfs installation
##

if [ "${ENABLE_QUORUM}" = "true" ] && [ -n "${KDC}" ] ; then
    CORE_SITE=core-site-secure-ha.xml
    HADOOP_ENV=hadoop-env-secure.sh
    HDFS_SITE=hdfs-site-secure-ha.xml
elif [ "${ENABLE_QUORUM}" = "true" ] ; then
    CORE_SITE=core-site-ha.xml
    HADOOP_ENV=hadoop-env-ha.sh
    HDFS_SITE=hdfs-site-ha.xml
elif [ -n "${KDC}" ] ; then
    CORE_SITE=core-site-secure.xml
    HADOOP_ENV=hadoop-env-secure.sh
    HDFS_SITE=hdfs-site-secure.xml
else
    CORE_SITE=core-site.xml
    HADOOP_ENV=hadoop-env.sh
    HDFS_SITE=hdfs-site.xml
fi


if [ "${ENABLE_QUORUM}" = "true" ] ; then
    ZOO_LOG=log4j-ha.properties
    ZOO_CFG=zoo.cfg

    # Deploy zookeeper and jounalnode depend on datanodes number.
    datanode_num=0

    for node in ${DATANODES/%${SECONDARY_NAMENODE}/}; do 
       datanode_num=`expr $datanode_num + 1` 
    done

    for num in {1..3}; do
        declare ZKSERVER_$num=$( echo $DATANODES | cut -d" " -f ${num} )

        if [ $datanode_num -ge 6 ]; then
            num_jn=`expr $num + 3`
        elif [ $datanode_num -gt 3 ]; then
            num_jn=`expr $datanode_num - 3 + $num` 
        else
            num_jn=$num
        fi 

        declare JOURNALNODE_$num=$( echo $DATANODES | cut -d " " -f ${num_jn} )
    done
fi

sed -e "s|%HDFS_HOST%|${NAMENODE}:9000|" --in-place=.orig ${script_dir}/templates/${CORE_SITE}

if [ "${DCA_SETUP}" = true ]; then
    sed -e "s|%JAVA_HOME%|${JAVA_HOME}|" \
        -e "s|%NAMENODE_MEMORY%|-Xmx10240m|" \
        -e "s|%DATANODE_MEMORY%|-Xmx8192m|" \
        -e "s|%SECONDARYNAMENODE_MEMORY%|-Xmx10240m|" \
        -e "s|%HADOOP_HOME%|${HADOOP_HOME}|" \
        --in-place=.orig ${script_dir}/templates/${HADOOP_ENV}
elif [ "${DCA_SETUP}" = "half" ]; then
    sed -e "s|%JAVA_HOME%|${JAVA_HOME}|" \
        -e "s|%NAMENODE_MEMORY%|-Xmx10240m|" \
        -e "s|%DATANODE_MEMORY%|-Xmx8192m|" \
        -e "s|%SECONDARYNAMENODE_MEMORY%|-Xmx10240m|" \
        -e "s|%HADOOP_HOME%|${HADOOP_HOME}|" \
        --in-place=.orig ${script_dir}/templates/${HADOOP_ENV}
else
    sed -e "s|%JAVA_HOME%|${JAVA_HOME}|" \
        -e "s|%NAMENODE_MEMORY%|-Xmx4096m|" \
        -e "s|%DATANODE_MEMORY%|-Xmx2048m|" \
        -e "s|%SECONDARYNAMENODE_MEMORY%|-Xmx2048m|" \
        -e "s|%HADOOP_HOME%|${HADOOP_HOME}|" \
        --in-place=.orig ${script_dir}/templates/${HADOOP_ENV}
fi

if [ "${TWO_NAMENODE_DIR}" = true ]; then
    sed -e "s|%DATA_DIR_ROOT%/hdfs/name<|${HDFS_DATA_DIRECTORY_ROOT}/hdfs/name,file:${HDFS_DATA_DIRECTORY_ROOT}/hdfs/name2<|" --in-place=.orig ${script_dir}/templates/${HDFS_SITE}
fi

sed -e "s|%DATA_DIR_ROOT%|${HDFS_DATA_DIRECTORY_ROOT}|" --in-place=.orig ${script_dir}/templates/${HDFS_SITE}

DATA_FILESYSTEMS=$( ssh $( echo ${DATANODES} | cut -d" " -f 1 ) "df -kh 2> /dev/null | grep data | wc -l" )

if [ $DATA_FILESYSTEMS -ge 2 ] ; then
    echo "/data1 and /data2 found... using both!"
    sed -e "s|%HDFS_DATANODE_DIR%|file:${HDFS_DATA_DIRECTORY_ROOT}1/hdfs/data,file:${HDFS_DATA_DIRECTORY_ROOT}2/hdfs/data|" --in-place=.orig ${script_dir}/templates/${HDFS_SITE}
else
    sed -e "s|%HDFS_DATANODE_DIR%|${HDFS_DATA_DIRECTORY_ROOT}/hdfs/data|" --in-place=.orig ${script_dir}/templates/${HDFS_SITE}
fi

if [ "${ENABLE_QUORUM}" = "true" ] ; then
    sed -e "s|%ZKSERVER1%|${ZKSERVER_1}|" -e "s|%ZKSERVER2%|${ZKSERVER_2}|" -e "s|%ZKSERVER3%|${ZKSERVER_3}|" --in-place=.orig ${script_dir}/templates/${ZOO_CFG}
    sed -e "s|%ZKSERVER1%|${ZKSERVER_1}|" -e "s|%ZKSERVER2%|${ZKSERVER_2}|" -e "s|%ZKSERVER3%|${ZKSERVER_3}|" --in-place=.orig ${script_dir}/templates/${CORE_SITE}

    sed -e "s|%NAMENODE%|${NAMENODE}|" -e "s|%SECONDARY_NAMENODE%|${SECONDARY_NAMENODE}|" -e "s|%JOURNALNODE1%|${JOURNALNODE_1}|" -e "s|%JOURNALNODE2%|${JOURNALNODE_2}|" \
        -e "s|%JOURNALNODE3%|${JOURNALNODE_3}|" -e "s|%JOURNAL_DATA_ROOT%|${JOURNAL_DATA_ROOT}|" --in-place=.orig ${script_dir}/templates/${HDFS_SITE}

    sed -e "s|%ZOO_LOG_DIR%|${ZOO_LOG_DIR}|" --in-place=.orig ${script_dir}/templates/${ZOO_LOG}
fi

if [ "${YARN}" = "true" ] ; then
    if [ "${DCA_SETUP}" = true ]; then
       RESOURCE_MEM="8192" 
    elif [ "${DCA_SETUP}" = "half" ]; then
       RESOURCE_MEM="8192"
    else
       RESOURCE_MEM="2048"
    fi
    sed -e "s|%RESOURCE_MEM%|${RESOURCE_MEM}|" --in-place=.orig ${script_dir}/templates/yarn-site.xml
    sed -e "s|%JAVA_HOME%|${JAVA_HOME}|" --in-place=.orig ${script_dir}/templates/yarn-env.sh
fi

rm ${script_dir}/templates/slaves
for datanode in ${DATANODES}; do
    echo "${datanode}" >> ${script_dir}/templates/slaves
done

rm ${script_dir}/templates/masters
echo "${NAMENODE}" >> ${script_dir}/templates/masters
echo "${SECONDARY_NAMENODE}" >> ${script_dir}/templates/masters

##
## Install hdfs rpms
##

for node in ${NAMENODE} ${SECONDARY_NAMENODE} ${DATANODES/%${SECONDARY_NAMENODE}/}; do
    echo ""
    echo "Remove old gphd directories in $node"
    ssh $node "rm -rf /var/log/gphd/* /usr/lib/gphd/*"
done

for node in ${NAMENODE} ${SECONDARY_NAMENODE} ${DATANODES/%${SECONDARY_NAMENODE}/}; do
    echo ""
    echo "Passing YUM repo file to $node"
    scp /etc/yum.repos.d/gphd.repo $node:/etc/yum.repos.d/gphd.repo

    echo "Installing hdfs in $node"
    ssh $node 'yum --disablerepo "*" --enablerepo "gphd" list available; \
               yum install -y hadoop-conf-pseudo; \
               yum install -y zookeeper-server;'

    ##
    ## Post rpm installation processing
    ##

    echo "cp libexec files in $node"
    ssh $node "cp ${HADOOP_HOME}/libexec/* ${HADOOP_HOME}-hdfs/libexec"
    ssh $node "cp /usr/lib/gphd/hadoop/libexec/* /usr/lib/gphd/hadoop-yarn/libexec/"

    echo "scp slaves, masters, and core-site.xml file to $node"
    scp ${script_dir}/templates/slaves $node:${HADOOP_CONF_DIR}/slaves
    scp ${script_dir}/templates/masters $node:${HADOOP_CONF_DIR}/masters
    scp ${script_dir}/templates/${CORE_SITE} $node:${HADOOP_CONF_DIR}/core-site.xml
    scp ${script_dir}/templates/${HADOOP_ENV} $node:${HADOOP_CONF_DIR}/hadoop-env.sh

    if [ "${YARN}" = "true" ] ; then
        scp ${script_dir}/templates/yarn-env.sh $node:${HADOOP_CONF_DIR}/yarn-env.sh
        scp ${script_dir}/templates/yarn-site.xml $node:${HADOOP_CONF_DIR}/yarn-site.xml

        ssh $node mkdir ${HADOOP_YARN_HOME}/logs
        ssh $node chmod 777 ${HADOOP_YARN_HOME}/logs
        ssh $node chmod 777 /var/log/gphd/hadoop-yarn
    fi

    if [ "${ENABLE_QUORUM}" = "true" ] ; then
	ssh $node "mkdir /usr/lib/gphd/zookeeper/etc; \
                   ln -s /etc/gphd/zookeeper/conf /usr/lib/gphd/zookeeper/etc/zookeeper; \
                   echo ZOO_LOG_DIR=/tmp/zookeeper/logs/ >> ${ZK_HOME}/bin/zkEnv.sh"
	scp ${script_dir}/templates/${ZOO_CFG} $node:${ZK_CONF_DIR}/zoo.cfg
	scp ${script_dir}/templates/${ZOO_LOG} $node:${ZK_CONF_DIR}/log4j.properties

	if [ $node == $ZKSERVER_1 ] ; then
	    ZK_NUM=1
	elif [ $node == $ZKSERVER_2 ] ; then
	    ZK_NUM=2
	elif [ $node == $ZKSERVER_3 ] ; then
	    ZK_NUM=3
	fi

	if [ $node == $ZKSERVER_1 ] || [ $node == $ZKSERVER_2 ] || [ $node == $ZKSERVER_3 ] ; then
	    ssh $node "mkdir -p /tmp/zookeeper/logs && chown -R zookeeper.hadoop /tmp/zookeeper/ && echo ${ZK_NUM} > /tmp/zookeeper/myid; \
                       grep ZK_HOME ~/.bash_profile >> /dev/null; \
                       if [ $? != 0 ] ; then \
                           echo 'export ZK_HOME=/usr/lib/gphd/zookeeper' >> ~/.bash_profile; \
                           echo 'export ZOO_LOG_DIR=/tmp/zookeeper/logs' >> ~/.bash_profile; \
                       fi"
	fi
    fi

    scp ${script_dir}/templates/${HDFS_SITE} $node:${HADOOP_CONF_DIR}/hdfs-site.xml

    ssh $node mkdir ${HADOOP_HOME}/logs
    ssh $node chmod 777 ${HADOOP_HOME}/logs
    echo ""
done

if [ "${YARN}" = "true" ] ; then
    for node in ${RESOURCEMANAGER} ${NODEMANAGERS}; do
        ssh ${node} cp /etc/gphd/hadoop/conf.empty/capacity-scheduler.xml /usr/lib/gphd/hadoop/etc/hadoop/
    done
fi

if [ "${ENABLE_QUORUM}" = "true" ] ; then
    if [ -n "${KDC}" ] ; then
        HDFS_DN_SSH_COMMAND=""
    else
        HDFS_DN_SSH_COMMAND="sudo -u hdfs"
    fi

    for zkserver in ${ZKSERVER_1} ${ZKSERVER_2} ${ZKSERVER_3}; do
	ssh $zkserver "sudo JAVA_HOME=${JAVA_HOME} /etc/init.d/zookeeper-server start"
    done

    for journalnode in ${JOURNALNODE_1} ${JOURNALNODE_2} ${JOURNALNODE_3}; do
	ssh $journalnode "rm -rf ${JOURNAL_DATA_ROOT}; mkdir ${JOURNAL_DATA_ROOT}; chown hdfs.hdfs ${JOURNAL_DATA_ROOT};\
                          JAVA_HOME=${JAVA_HOME} ${HDFS_DN_SSH_COMMAND} ${HADOOP_HOME}/sbin/hadoop-daemon.sh start journalnode"
    done

    ssh ${NAMENODE} "\
      JAVA_HOME=${JAVA_HOME} sudo -u hdfs ${HADOOP_HOME}-hdfs/bin/hdfs namenode -format -force 2>&1 | tee ${HADOOP_HOME}/logs/hdfs-namenode-format.out; \
      grep 'Exiting with status 0' ${HADOOP_HOME}/logs/hdfs-namenode-format.out; \
      if [ \$? != 0 ]; then \
        echo 'FATAL: format failed.'; \
        exit 1; \
      fi;
      sudo -u hdfs ${HADOOP_HOME}/sbin/hadoop-daemon.sh start namenode"

    ssh ${SECONDARY_NAMENODE} "\
      JAVA_HOME=${JAVA_HOME} sudo -u hdfs ${HADOOP_HOME}-hdfs/bin/hdfs namenode -bootstrapStandby 2>&1 | tee ${HADOOP_HOME}/logs/hdfs-namenode-bootstrapStandby.out; \
      grep 'Exiting with status 0' ${HADOOP_HOME}/logs/hdfs-namenode-bootstrapStandby.out; \
      if [ \$? != 0 ]; then \
        echo 'FATAL: bootstrapStandby failed.'; \
        exit 1; \
      fi;
      sudo -u hdfs ${HADOOP_HOME}/sbin/hadoop-daemon.sh start namenode"

    ssh ${NAMENODE} "JAVA_HOME=${JAVA_HOME} sudo -u hdfs ${HADOOP_HOME}-hdfs/bin/hdfs zkfc -formatZK -force"
    for namenode in ${NAMENODE} ${SECONDARY_NAMENODE}; do
	ssh $namenode "JAVA_HOME=${JAVA_HOME} sudo -u hdfs ${HADOOP_HOME}/sbin/hadoop-daemon.sh start zkfc"
    done

    for datanode in ${DATANODES}; do
	    ssh $datanode ${HDFS_DN_SSH_COMMAND} /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh start datanode
    done
else
    ##
    ## Format hdfs file system
    ##

    ssh ${NAMENODE} "\
      sudo -u hdfs ${HADOOP_HOME}-hdfs/bin/hdfs namenode -format -force 2>&1 | tee ${HADOOP_HOME}/logs/hdfs-namenode-format.out; \
      grep 'Exiting with status 0' ${HADOOP_HOME}/logs/hdfs-namenode-format.out; \
      if [ \$? != 0 ]; then \
        echo 'FATAL: format failed.'; \
        exit 1; \
      fi"

    ##
    ## Startup hadoop namenode and datanode services
    ##

    ssh ${NAMENODE}           sudo -u hdfs /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh start namenode

    ssh ${SECONDARY_NAMENODE} sudo -u hdfs /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh start secondarynamenode

    for datanode in ${DATANODES}; do
	if [ -n "${KDC}" ] ; then
	    ssh $datanode     /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh start datanode
	else
	    ssh $datanode     sudo -u hdfs /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh start datanode
	fi
    done

    if [ "${YARN}" = "true" ] ; then
        ssh ${PHD_USER}@${RESOURCEMANAGER} JAVA_HOME=${JAVA_HOME} /usr/lib/gphd/hadoop-yarn/sbin/yarn-daemon.sh start resourcemanager 
        for node in ${NODEMANAGERS}; do
            ssh ${PHD_USER}@${node} JAVA_HOME=${JAVA_HOME} /usr/lib/gphd/hadoop-yarn/sbin/yarn-daemon.sh start nodemanager 
        done
    fi
fi

##
## Before exiting, display running java processes with "jps" command
##

display_jps

exit 0
