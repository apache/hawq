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

build() {
    source ${HAWQ_HOME}/greenplum_path.sh
    export PXF_HOME=${GPHOME}/pxf

    sudo chown -R gpadmin:gpadmin ${PXF_HOME}/conf/
    sudo echo "source ${GPHOME}/greenplum_path.sh" >> /home/gpadmin/.bashrc

    cd /data/hawq/pxf
    # make -j16
    make
    make install

    sudo sed 's|-pxf|-gpadmin|g' -i ${PXF_HOME}/conf/pxf-env.sh

    rm -rf ${PXF_HOME}/conf/pxf-private.classpath
    rm -rf ${PXF_HOME}/conf/pxf-log4j.properties

    sudo cp /tmp/pxf-private.classpath ${PXF_HOME}/conf/pxf-private.classpath
    sudo cp /tmp/pxf-log4j.properties ${PXF_HOME}/conf/pxf-log4j.properties
    sudo sed 's|pxf\.log\.dir|PXF_HOME|g' -i ${PXF_HOME}/conf/pxf-log4j.properties

    echo "Make PXF Done!"
}

init() {
    source ${HAWQ_HOME}/greenplum_path.sh
    export PXF_HOME=${GPHOME}/pxf

    export BASEDIR=/data
    export HAWQSITE_CONF=${GPHOME}/etc/hawq-site.xml
    export HOME=/home/gpadmin
    export JAVA_HOME=/etc/alternatives/java_sdk
    export LD_LIBRARY_PATH=${GPHOME}/lib:/${GPHOME}/lib:
    export LIBHDFS3_CONF=${GPHOME}/etc/hdfs-client.xml
    export LIBYARN_CONF=${GPHOME}/etc/yarn-client.xml
    export NAMENODE=${NAMENODE}
    export OPENSSL_CONF=${GPHOME}/etc/openssl.cnf
    export PATH=/${GPHOME}/bin:/${GPHOME}/bin:/usr/lib64/qt-3.3/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
    export PWD=/data
    export PYTHONPATH=/${GPHOME}/lib/python:/${GPHOME}/lib/python:
    export USER=gpadmin

    sudo passwd -d gpadmin
    sudo mkdir -p /var/run/pxf
    sudo mkdir -p /var/log/pxf
    sudo touch /var/log/pxf/catalina.out
    sudo chmod -R 777 /var/run/pxf
    sudo chmod -R 777 /var/log/pxf

    echo "Initializing PXF Service"
    ${PXF_HOME}/bin/pxf init
    echo "Initializing PXF Service Done!"
}

start() {
    source ${HAWQ_HOME}/greenplum_path.sh
    export PXF_HOME=${GPHOME}/pxf

    export JAVA_HOME=/etc/alternatives/java_sdk

    echo "Starting PXF Service"
    ${PXF_HOME}/bin/pxf start
    echo "Starting PXF Service Done!"
}

stop() {
    source ${HAWQ_HOME}/greenplum_path.sh
    export PXF_HOME=${GPHOME}/pxf

    export JAVA_HOME=/etc/alternatives/java_sdk

    echo "Stopping PXF Service"
    ${PXF_HOME}/bin/pxf stop
    echo "Stopping PXF Service Done!"
}

status() {
    curl "localhost:51200/pxf/ProtocolVersion"; echo
}

case "$1" in
    '--build')
        build
        ;;
    '--init')
        init
        ;;
    '--start')
        start
        ;;
    '--stop')
        stop
        ;;
    '--status')
        status
        ;;
    *)
        echo "Usage: $0 {--build|--init|--start|--stop|--status}"
esac

exit 0
