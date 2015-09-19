#!/bin/bash
#
# Usage: ./setup_gphdinst.sh setup / start HK / stop HK 
# HK means this is for HA/Kerberos
#

JAVA_HOME=${JAVA_HOME:="/opt/jdk1.7.0_15"}
HADOOP_HOME="/usr/lib/gphd/hadoop"
LOCAL_HDFS_RPMINSTALL="true"
DCA_SETUP=${DCA_SETUP:=false}
TWO_NAMENODE_DIR=${TWO_NAMENODE_DIR:=false}
SEGMENT_SYSTEM_LIST=${SEGMENT_SYSTEM_LIST:="sdw1 sdw2 sdw3"}
MASTERHOST=${MASTERHOST:=mdw}
STANDBYHOST=${STANDBYHOST:=smdw}
DATANODES_LIST=${SEGMENT_SYSTEM_LIST}
USER="gpadmin"
STEP="$1"
ENABLE_QUORUM=${ENABLE_QUORUM:=true}

if [ "${ENABLE_QUORUM}" = "true" ]; then 
    ZKSERVER_LIST=$( echo ${SEGMENT_SYSTEM_LIST} | cut -d' ' -f1-3 )

    # Deploy zookeeper and jounalnode depend on datanodes number.
    datanode_num=0

    for node in ${SEGMENT_SYSTEM_LIST}; do   
       datanode_num=`expr $datanode_num + 1` 
    done 

    if [ $datanode_num -ge 6 ]; then 
        JOURNALNODE_LIST=$( echo ${SEGMENT_SYSTEM_LIST} | cut -d' ' -f4-6 )
    elif [ $datanode_num -eq 5 ]; then 
        JOURNALNODE_LIST=$( echo ${SEGMENT_SYSTEM_LIST} | cut -d' ' -f3-5 )
    elif [ $datanode_num -eq 4 ]; then 
        JOURNALNODE_LIST=$( echo ${SEGMENT_SYSTEM_LIST} | cut -d' ' -f2-4 )
    else 
        JOURNALNODE_LIST=$( echo ${SEGMENT_SYSTEM_LIST} | cut -d' ' -f1-3 )
    fi
    echo "ZKSERVER_LIST is ${ZKSERVER_LIST}"
    echo "JOURNALNODE_LIST is ${JOURNALNODE_LIST}"
fi

if [ "${STEP}" = "setup" ]; then
    PHD_PACKAGE=$( curl --silent --no-buffer "http://hdp4-mdw1.wbe.dh.greenplum.com/dist/PHD/testing/?C=N;O=D" | grep -o 'PHD-[0-9].[0-9].[0-9].[0-9]-[0-9]*.tar.gz' | head -n 1 )
    HDFS_DOWNLOAD_URL="http://hdp4-mdw1.wbe.dh.greenplum.com/dist/PHD/testing/${PHD_PACKAGE}"

    echo "Hadoop Package Download URL: ${HDFS_DOWNLOAD_URL}"
    echo "Setting up latest HDFS..."

    sudo DATANODES="${DATANODES_LIST}" \
         NAMENODE=${STANDBYHOST} \
         SECONDARY_NAMENODE=${MASTERHOST} \
         RESOURCEMANAGER=${STANDBYHOST} \
         NODEMANAGERS="${DATANODES_LIST}" \
         ARCHIVE_URL=${HDFS_DOWNLOAD_URL} \
         JAVA_HOME=${JAVA_HOME} \
         DCA_SETUP=${DCA_SETUP} \
         TWO_NAMENODE_DIR=${TWO_NAMENODE_DIR} \
         KDC=${KDC} \
         ENABLE_QUORUM=${ENABLE_QUORUM} \
         YARN=${YARN} \
         PHD_USER=${USER} \
         bash gphdinst.sh
fi

if [ -n "${KDC}" ] ; then
    HDFS_DN_SSH_COMMAND="sudo"
elif [ "$2" = "HK" ]; then
    HDFS_DN_SSH_COMMAND="sudo"
else
    HDFS_DN_SSH_COMMAND="sudo -u hdfs"
fi

if [ "${STEP}" = "stop" ]; then
    if [ -f "${HADOOP_HOME}/bin/stop-all.sh" ]; then 
        ${HADOOP_HOME}/bin/stop-all.sh
    fi   

    if [ "${LOCAL_HDFS_RPMINSTALL}" = "true" ]; then
        if [ "${USE_MASTER_AS_DATANODE}" = "true" ]; then
            DATANODES_LIST="${SEGMENT_SYSTEM_LIST} ${MASTERHOST}"
        else
            DATANODES_LIST=${SEGMENT_SYSTEM_LIST}
        fi

        if [ "${ENABLE_QUORUM}" = "true" ]; then
            echo "Stopping local HDFS HA cluster..."
            for datanode in ${DATANODES_LIST}; do
                ssh $datanode ${HDFS_DN_SSH_COMMAND} /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh stop datanode
            done

            for node in ${JOURNALNODE_LIST}; do
                ssh $node ${HDFS_DN_SSH_COMMAND} /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh stop journalnode
            done

            for node in ${ZKSERVER_LIST}; do
                ssh $node sudo JAVA_HOME=${JAVA_HOME} /etc/init.d/zookeeper-server stop
            done

            sudo -u hdfs /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh stop zkfc
            ssh ${STANDBYHOST} sudo -u hdfs /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh stop zkfc
            sudo -u hdfs /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh stop namenode
            ssh ${STANDBYHOST} sudo -u hdfs /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh stop namenode
        else
            echo "Stopping local HDFS cluster..."

            sudo -u hdfs /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh stop secondarynamenode

            for datanode in ${DATANODES_LIST}; do
                ssh $datanode ${HDFS_DN_SSH_COMMAND} /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh stop datanode
                if [ "${YARN}" = "true" ] ; then
                    ssh $datanode "/usr/lib/gphd/hadoop-yarn/sbin/yarn-daemon.sh stop nodemanager"
                fi
            done

            ssh ${STANDBYHOST} sudo -u hdfs /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh stop namenode
            if [ "${YARN}" = "true" ] ; then
                ssh ${STANDBYHOST} /usr/lib/gphd/hadoop-yarn/sbin/yarn-daemon.sh stop resourcemanager
            fi
         fi
    fi
fi

if [ "${STEP}" = "start" ]; then

                                if [ "${ENABLE_QUORUM}" = "true" ]; then
                                    echo "Starting up local HDFS HA"

                                    for node in ${JOURNALNODE_LIST}; do
                                        ssh $node ${HDFS_DN_SSH_COMMAND} /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh start journalnode
                                    done

                                    for node in ${ZKSERVER_LIST}; do
                                        ssh $node sudo JAVA_HOME=${JAVA_HOME} /etc/init.d/zookeeper-server start
                                    done
                    
                                    ssh ${STANDBYHOST} sudo -u hdfs /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh start zkfc
                                    sudo -u hdfs /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh start zkfc
                                    ssh ${STANDBYHOST} sudo -u hdfs /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh start namenode
                                    sudo -u hdfs /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh start namenode

                                    for datanode in ${DATANODES_LIST}; do
                                        ssh $datanode ${HDFS_DN_SSH_COMMAND} /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh start datanode
                                    done
                                else
                                    echo "Starting up local HDFS"
                                    ssh ${STANDBYHOST} sudo -u hdfs /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh start namenode
                                    if [ "${YARN}" = "true" ]; then
                                        ssh ${STANDBYHOST} /usr/lib/gphd/hadoop-yarn/sbin/yarn-daemon.sh start resourcemanager
                                    fi
                                    for datanode in ${DATANODES_LIST}; do
                                        ssh $datanode ${HDFS_DN_SSH_COMMAND} /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh start datanode
                                        if [ "${YARN}" = "true" ]; then
                                            ssh $datanode /usr/lib/gphd/hadoop-yarn/sbin/yarn-daemon.sh start nodemanager
                                        fi
                                    done
                                    sudo -u hdfs /usr/lib/gphd/hadoop/sbin/hadoop-daemon.sh start secondarynamenode
                                fi
fi
