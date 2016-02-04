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

source ${GPHOME}/bin/lib/hawq_bash_functions.sh
#SOURCE_PATH="source ${GPHOME}/greenplum_path.sh"
#${SOURCE_PATH}
hostname="$1"
host_parameter_name="$2"

if [ -z "$hostname" ]; then
    echo "Please input a hostname" 
    exit
fi

lowercase(){
    echo "$1" | sed "y/ABCDEFGHIJKLMNOPQRSTUVWXYZ/abcdefghijklmnopqrstuvwxyz/"
}

OS=`lowercase \`uname\``

if [ "${OS}" = "darwin" ]; then
    OS=mac
    distro_based_on='Mac'
    distro_name=`sw_vers -productName`
    distro_version=`sw_vers -productVersion`
    distro_major_version=`echo $distro_version |awk -F '.' '{print $1}'`
else
    if [ "${OS}" = "linux" ] ; then
        if [ -f /etc/redhat-release ] ; then
            distro_based_on='RedHat'
            distro_name=`cat /etc/redhat-release |sed s/\ release.*//`
            psuedo_name=`cat /etc/redhat-release | sed s/.*\(// | sed s/\)//`
            distro_version=`cat /etc/redhat-release | sed s/.*release\ // | sed s/\ .*//`
            distro_major_version=`echo $distro_version | awk -F '.' '{print $1}'`
        elif [ -f /etc/SuSE-release ] ; then
            distro_based_on='SuSe'
            distro_name=`cat /etc/SuSE-release |sed s/\ release.*//`
            psuedo_name=`cat /etc/SuSE-release | tr "\n" ' '| sed s/VERSION.*//`
            distro_version=`cat /etc/SuSE-release | tr "\n" ' ' | sed s/.*=\ //`
            distro_major_version=`echo $distro_version |awk -F '.' '{print $1}'`
        elif [ -f /etc/debian_version ] ; then
            distro_based_on='Debian'
            distro_name=`cat /etc/lsb-release | grep '^DISTRIB_ID' | awk -F=  '{ print $2 }'`
            psuedo_name=`cat /etc/lsb-release | grep '^DISTRIB_CODENAME' | awk -F=  '{ print $2 }'`
            distro_version=`cat /etc/lsb-release | grep '^DISTRIB_RELEASE' | awk -F=  '{ print $2 }'`
            distro_major_version=`echo $distro_version |awk -F '.' '{print $1}'`
        fi
    fi

fi

get_all_ip_address() {
    if [ "${distro_based_on}" = "RedHat" ] && [ "${distro_major_version}" -ge 7 ]; then
        cmd_str="${IFCONFIG} |${GREP} -v '127.0.0' | ${GREP} 'inet '|${AWK} '{print \$2}'"
    elif [ "${distro_based_on}" = "Mac" ]; then
        cmd_str="${IFCONFIG} |${GREP} -v '127.0.0' | ${GREP} 'inet '|${AWK} '{print \$2}'"
    else
        cmd_str="${IFCONFIG} |${GREP} -v '127.0.0' |${AWK} '/inet addr/{print substr(\$2,6)}'"
    fi

    ip_address_all=`${SSH} -o 'StrictHostKeyChecking no' ${hostname} "${cmd_str}"`
}

get_all_ip_address
all_ip_inline=""
for host_ip in ${ip_address_all}; do 
    all_ip_inline="${all_ip_inline} ${host_ip}"
done

echo "${host_parameter_name}=\"${all_ip_inline}\"" >> ${GPHOME}/etc/_mgmt_config
