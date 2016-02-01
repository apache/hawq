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

object_type=$1
GPHOME=$2
VERBOSE=0
if [ "$3" == "1" ]; then
    VERBOSE=1
fi
source ${GPHOME}/bin/lib/hawq_bash_functions.sh
SOURCE_PATH="source ${GPHOME}/greenplum_path.sh"
${SOURCE_PATH}

host_name=`${HOSTNAME}`

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

mgmt_config_file=${GPHOME}/etc/_mgmt_config
if [ -f ${mgmt_config_file} ]; then
    source ${mgmt_config_file} > /dev/null 2>&1
else
    ${ECHO} "${mgmt_config_file} is not exist, exit"
    exit 1
fi

if [ ${object_type} = "master" ] || [ ${object_type} = "standby" ]; then
   hawq_data_directory=${master_data_directory}
   hawq_port=${master_port}
   tmp_dir_list=${hawq_master_temp_directory//,/ }
elif [ ${object_type} = "segment" ]; then
   hawq_data_directory=${segment_data_directory}
   hawq_port=${segment_port}
   tmp_dir_list=${hawq_segment_temp_directory//,/ }
else
   ${ECHO} "hawq init object should be one of master/standby/segment"
   exit 1
fi

master_max_connections=${max_connections}
segment_max_connections=${max_connections}
master_ip_address_all=""
standby_ip_address_all=""
standby_host_lowercase=`lowercase "${standby_host_name}"`

get_all_ip_address() {
    if [ "${distro_based_on}" = "RedHat" ] && [ "${distro_major_version}" -ge 7 ]; then
        cmd_str="${IFCONFIG} |${GREP} -v '127.0.0' | ${GREP} 'inet '|${AWK} '{print \$2}'"
    elif [ "${distro_based_on}" = "Mac" ]; then
        cmd_str="${IFCONFIG} |${GREP} -v '127.0.0' | ${GREP} 'inet '|${AWK} '{print \$2}'"
    else
        cmd_str="${IFCONFIG} |${GREP} -v '127.0.0' |${AWK} '/inet addr/{print substr(\$2,6)}'"
    fi

    master_ip_address_all=`${SSH} ${master_host_name} "${cmd_str}"`
    if [ -z "${master_ip_address_all}" ];then
        ${ECHO} "Failed to get master ip addresses"
        exit 1
    fi

    if [ "${standby_host_lowercase}" != "none" ] && [ -n "${standby_host_lowercase}" ];then
        standby_ip_address_all=`${SSH} ${standby_host_name} "${cmd_str}"`
        if [ -z "${standby_ip_address_all}" ];then
            ${ECHO} "Failed to get standby ip addresses"
            exit 1
        fi
    fi

    segment_ip_address_all=`${SSH} localhost "${cmd_str}"`

    if [ -z "${segment_ip_address_all}" ];then
        ${ECHO} "Failed to get segment ip addresses"
        exit 1
    fi
}

get_all_ip_address

PG_HBA=pg_hba.conf
TMP_PG_HBA=/tmp/pg_hba_conf_master.$$

MASTER_LOG_FILE=${log_filename}
STANDBY_LOG_FILE=${log_filename}
SEGMENT_LOG_FILE=${log_filename}
LOG_FILE=${log_filename}

PSQL=${GPHOME}/bin/psql
PG_CTL=${GPHOME}/bin/pg_ctl
if [ "${log_dir}" = "None" ]; then
    log_dir=${HOME}/hawqAdminLogs
fi
if [ ! -d ${log_dir} ]; then
    ${MKDIR} -p ${log_dir}
fi

if [ ! -f ${log_filename} ]; then
    touch ${log_filename}
fi

GET_CIDRADDR () {
    if [ `${ECHO} $1 | ${GREP} -c :` -gt 0 ]; then
        ${ECHO} $1/128
    else
        ${ECHO} $1/32
    fi
}

LOAD_GP_TOOLKIT () {
    CUR_DATE=`${DATE} +%Y%m%d`
    FILE_TIME=`${DATE} +%H%M%S`
    TOOLKIT_FILE=/tmp/_gp_toolkit_tmp_${CUR_DATE}_$FILE_TIME
    LOG_MSG "[INFO]:-Loading hawq_toolkit..." verbose
    ROLNAME=`$PSQL -q -t -A -p ${hawq_port} -c "select rolname from pg_authid where oid=10" template1`
    if [ x"$ROLNAME" == x"" ];then
        LOG_MSG "[FATAL]:-Failed to retrieve rolname." verbose
        exit 1
    fi

    if [ -f ${TOOLKIT_FILE} ]; then
        ${RM} -f ${TOOLKIT_FILE}
    fi

    # We need SET SESSION AUTH here to load the toolkit
    ${ECHO} "SET SESSION AUTHORIZATION $ROLNAME;"  >> ${TOOLKIT_FILE} 2>&1
    RETVAL=$?
    if [ $RETVAL -ne 0 ];then
        ${ECHO} "[FATAL]:-Failed to create the hawq_toolkit sql file." | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    ${CAT} $GPHOME/share/postgresql/gp_toolkit.sql >> ${TOOLKIT_FILE} 2>&1
    RETVAL=$?
    if [ $RETVAL -ne 0 ];then
        ${ECHO} "[FATAL]:-Failed to create the hawq_toolkit sql file." | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    $PSQL -q -p ${hawq_port} -f ${TOOLKIT_FILE} template1
    RETVAL=$?
    if [ $RETVAL -ne 0 ];then
        ${ECHO} "[FATAL]:-Failed to create the hawq_toolkit schema." | tee -a ${MASTER_LOG_FILE} 
        exit 1
    fi

    $PSQL -q -p ${hawq_port} -f ${TOOLKIT_FILE} postgres
    RETVAL=$?
    if [ $RETVAL -ne 0 ];then
        ${ECHO} "[FATAL]:-Failed to create the hawq_toolkit schema." | tee -a ${MASTER_LOG_FILE} 
        exit 1
    fi

    ${RM} -f ${TOOLKIT_FILE}

    return $RETVAL
}

get_master_ipv6_addresses() {
    if [ "${distro_based_on}" = "Mac" ] && [ "${distro_version:0:5}" = "10.11" ]; then
        MASTER_IPV6_LOCAL_ADDRESS_ALL=(`${IFCONFIG} | ${GREP} inet6 | ${AWK} '{print $2}' | cut -d'%' -f1`)
    else
        MASTER_IPV6_LOCAL_ADDRESS_ALL=(`ip -6 address show |${GREP} inet6|${AWK} '{print $2}' |cut -d'/' -f1`)
    fi
}

update_master_pg_hba(){
    # Updatepg_hba.conf for master.
    ${CAT} ${hawq_data_directory}/${PG_HBA} |${GREP} '^#' > ${TMP_PG_HBA}
    mv ${TMP_PG_HBA} ${hawq_data_directory}/${PG_HBA}
    # Setting local access"
    ${ECHO} "local    all         $USER         ident" >> ${hawq_data_directory}/${PG_HBA}
    # ${ECHO} "[INFO]:-Setting local host access"
    ${ECHO} "host     all         $USER         127.0.0.1/28    trust" >> ${hawq_data_directory}/${PG_HBA}
    get_master_ipv6_addresses
    MASTER_HBA_IP_ADDRESS=(`${ECHO} ${master_ip_address_all[@]} ${MASTER_IPV6_LOCAL_ADDRESS_ALL[@]} ${standby_ip_address_all[@]}|tr ' ' '\n'|sort -u|tr '\n' ' '`)
    for ip_address in ${MASTER_HBA_IP_ADDRESS[@]}; do
        CIDR_MASTER_IP=$(GET_CIDRADDR ${ip_address})
        CHK_COUNT=`${GREP} -c ${CIDR_MASTER_IP} ${hawq_data_directory}/${PG_HBA}`
        if [ "$CHK_COUNT" -eq "0" ];then
            ${ECHO} "host  all     ${USER}    ${CIDR_MASTER_IP}       trust" >> ${hawq_data_directory}/${PG_HBA}
        else
            ${ECHO} "${CIDR_MASTER_IP} already exist in ${hawq_data_directory}/${PG_HBA}"
        fi
    done
}

update_standby_pg_hba(){
    # Updatepg_hba.conf for standby master.
    ${ECHO} "host  all     all    0.0.0.0/0       trust" >> ${hawq_data_directory}/${PG_HBA}
}

update_segment_pg_hba(){
    # Updatepg_hba.conf for segment.
    # Setting local access"
    MASTERS_HBA_IP_ADDRESSES=(`${ECHO} ${master_ip_address_all[@]} ${standby_ip_address_all[@]}|tr ' ' '\n'|sort -u|tr '\n' ' '`)
    for ip_address in ${MASTERS_HBA_IP_ADDRESSES[@]}; do
        CIDR_MASTER_IP=$(GET_CIDRADDR ${ip_address})
        CHK_COUNT=`${GREP} -c ${CIDR_MASTER_IP} ${hawq_data_directory}/${PG_HBA}`
        if [ "$CHK_COUNT" -eq "0" ];then
            ${ECHO} "host  all     all    ${CIDR_MASTER_IP}       trust" >> ${hawq_data_directory}/${PG_HBA}
        fi
    done
    for ip_address in ${segment_ip_address_all[@]}; do
        CIDR_MASTER_IP=$(GET_CIDRADDR ${ip_address})
        CHK_COUNT=`${GREP} -c ${CIDR_MASTER_IP} ${hawq_data_directory}/${PG_HBA}`
        if [ "$CHK_COUNT" -eq "0" ];then
            ${ECHO} "host  all     ${USER} ${CIDR_MASTER_IP}       trust" >> ${hawq_data_directory}/${PG_HBA}
        fi
    done
}

master_init() {
    ${GPHOME}/bin/initdb -E UNICODE -D ${hawq_data_directory} --locale=${locale} --lc-collate=${hawq_lc_collate} \
        --lc-ctype=${hawq_lc_ctype} --lc-messages=${hawq_lc_messages} --lc-monetary=${hawq_lc_monetary} \
        --lc-numeric=${hawq_lc_numeric} --lc-time=${hawq_lc_time} --max_connections=${master_max_connections} \
        --shared_buffers=${shared_buffers} --backend_output=${log_dir}/master.initdb 1>>${MASTER_LOG_FILE} 2>&1

    if [ $? -ne 0 ] ; then
        ${ECHO} "Master postgres initdb failed" | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    update_master_pg_hba 1>>${MASTER_LOG_FILE} 2>&1

    ${PG_CTL} -D ${hawq_data_directory} -l ${hawq_data_directory}/pg_log/startup.log -w -t 60 -o " -p ${hawq_port}  --silent-mode=true -M master -i" start >> ${MASTER_LOG_FILE}
    if [ $? -ne 0  ] ; then
        ${ECHO} "Start hawq master failed"  | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    $PSQL -p ${hawq_port} -d template1 -c "create filespace dfs_system on hdfs ('${dfs_url}');" 1>>${MASTER_LOG_FILE} 2>&1
    if [ $? -ne 0  ] ; then
        ${ECHO} "Create filespace failed, please check your hdfs settings"  | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    $PSQL -p ${hawq_port} -d template1 -c "create tablespace dfs_default filespace dfs_system;" 1>>${MASTER_LOG_FILE} 2>&1
    if [ $? -ne 0  ] ; then
        ${ECHO} "Create tablespace failed"  | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    env PGOPTIONS="-c gp_session_role=utility" $PSQL -p ${hawq_port} -d template1 -c \
        "SET allow_system_table_mods='dml';UPDATE pg_database SET dat2tablespace = (SELECT oid FROM pg_tablespace WHERE spcname = 'dfs_default') WHERE datname = 'template1';" 1>>${MASTER_LOG_FILE} 2>&1
    if [ $? -ne 0  ] ; then
        ${ECHO} "Configure database template1 failed" | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    $PSQL -p ${hawq_port} -d template1 -c "create database template0 tablespace dfs_default template template1;" 1>>${MASTER_LOG_FILE} 2>&1
    if [ $? -ne 0  ] ; then
        ${ECHO} "Create database template0 failed" | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    env PGOPTIONS="-c gp_session_role=utility" $PSQL -p ${hawq_port} -d template1 -c "SET allow_system_table_mods='dml';UPDATE pg_database SET datistemplate = 't', datallowconn = false WHERE datname = 'template0';" 1>>${MASTER_LOG_FILE} 2>&1
    if [ $? -ne 0  ] ; then
        ${ECHO} "Configure database template0 failed" | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    $PSQL -p ${hawq_port} -d template1 -c "create database postgres tablespace dfs_default;" 1>>${MASTER_LOG_FILE} 2>&1
    if [ $? -ne 0  ] ; then
        ${ECHO} "Create database postgres failed" | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    env PGOPTIONS="-c gp_session_role=utility" $PSQL -p ${hawq_port} -d template1 -c "SET allow_system_table_mods='dml';UPDATE pg_database SET datistemplate = 't' WHERE datname = 'postgres';" 1>>${MASTER_LOG_FILE} 2>&1
    if [ $? -ne 0  ] ; then
        ${ECHO} "Configure database postgres failed" | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    env PGOPTIONS="-c gp_session_role=utility" $PSQL -p ${hawq_port} -d template1 -c "CHECKPOINT;" 1>>${MASTER_LOG_FILE} 2>&1
    if [ $? -ne 0  ] ; then
        ${ECHO} "CHECKPOINT failed" | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    LOAD_GP_TOOLKIT
    if [ $? -ne 0  ] ; then
        ${ECHO} "Load TOOLKIT failed" | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi
    $PSQL -p ${hawq_port} -d template1 -c "alter user \"${USER}\" password 'gparray';" 1>>${MASTER_LOG_FILE} 2>&1

    if [ $? -ne 0  ] ; then
        ${ECHO} "Alter user failed" 1>> ${MASTER_LOG_FILE} 2>&1
        exit 1
    fi
}

standby_init() {
    # Make sure log file are created.
    if [ ! -f ${STANDBY_LOG_FILE} ]; then
        touch ${STANDBY_LOG_FILE};
    fi

    LOG_MSG ""
    LOG_MSG "[INFO]:-Stopping HAWQ master"
    ${SSH} -o 'StrictHostKeyChecking no' ${hawqUser}@${master_host_name} \
        "${SOURCE_PATH}; hawq stop master -a -M fast;" >> ${STANDBY_LOG_FILE} 2>&1
    if [ $? -ne 0 ] ; then
        LOG_MSG "[ERROR]:-Stop master failed" verbose
        exit 1
    else
        LOG_MSG "[INFO]:-HAWQ master stopped" verbose
    fi

    # Sync data directories to standby master.
    LOG_MSG "[INFO]:-Sync files to standby from master"
    ${SSH} -o 'StrictHostKeyChecking no' ${hawqUser}@${master_host_name} \
        "cd ${master_data_directory}; \
         ${SOURCE_PATH}; ${GPHOME}/bin/lib/pysync.py -x gpperfmon/data -x pg_log -x db_dumps \
         ${master_data_directory} ${standby_host_name}:${master_data_directory};" >> ${STANDBY_LOG_FILE} 2>&1
    if [ $? -ne 0 ] ; then
        LOG_MSG "[FATAL]:-Sync master files to standby failed" verbose
        exit 1
    fi

    ${MKDIR} -p ${master_data_directory}/pg_log | tee -a ${STANDBY_LOG_FILE}

    STANDBY_IP_ADDRESSES=`${SSH} -o 'StrictHostKeyChecking no' ${hawqUser}@${master_host_name} \
        "${PING} -c1 -n ${standby_host_name} | head -n1 | sed 's/.*(\([0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\)).*/\1/g';"`
    if [ -z "${STANDBY_IP_ADDRESSES}" ] ; then
        LOG_MSG "[FATAL]:-Standby ip address is empty" verbose
        exit 1
    else
        LOG_MSG "[INFO]:-Standby ip address is ${STANDBY_IP_ADDRESSES}" verbose
    fi

    
    LOG_MSG "[INFO]:-Start hawq master" verbose
    ${SSH} -o 'StrictHostKeyChecking no' ${hawqUser}@${master_host_name} \
        "${SOURCE_PATH}; hawq start master -a --masteronly >> ${STANDBY_LOG_FILE}"
    if [ $? -ne 0 ] ; then
        LOG_MSG "[ERROR]:-Start HAWQ master failed" verbose
        exit 1
    else
        LOG_MSG "[INFO]:-HAWQ master started" verbose
    fi

    LOG_MSG "[INFO]:-Try to remove existing standby from catalog" verbose
    ${SSH} -o 'StrictHostKeyChecking no' ${hawqUser}@${master_host_name} \
        "${SOURCE_PATH}; env PGOPTIONS=\"-c gp_session_role=utility\" $PSQL -p ${master_port} -d template1 \
         -c\"select gp_remove_master_standby() where (select count(*) from gp_segment_configuration where role='s') = 1;\";" >> ${STANDBY_LOG_FILE} 2>&1

    ${SSH} -o 'StrictHostKeyChecking no' ${hawqUser}@${master_host_name} \
        "${SOURCE_PATH}; env PGOPTIONS=\"-c gp_session_role=utility\" $PSQL -p ${master_port} -d template1 -c \
         \"select gp_add_master_standby('${standby_host_name}','${STANDBY_IP_ADDRESSES}','');\";" >>${STANDBY_LOG_FILE} 2>&1
    if [ $? -ne 0 ] ; then
        LOG_MSG "[FATAL]:-Register standby infomation failed" verbose
        exit 1
    else
        LOG_MSG "[INFO]:-Register standby to master successfully" verbose
    fi

    ${SSH} -o 'StrictHostKeyChecking no' ${hawqUser}@${master_host_name} \
        "${SOURCE_PATH}; hawq stop master -a -M fast;" >> ${STANDBY_LOG_FILE}
    if [ $? -ne 0 ] ; then
        LOG_MSG "[ERROR]:-Stop HAWQ master failed" verbose
        exit 1
    else
        LOG_MSG "[INFO]:-HAWQ master stopped" verbose
    fi
 
    ${SSH} -o 'StrictHostKeyChecking no' ${hawqUser}@${master_host_name} \
        "${SOURCE_PATH}; hawq start standby -a;" >> ${STANDBY_LOG_FILE}
    if [ $? -ne 0 ] ; then
        LOG_MSG "[ERROR]:-Start HAWQ standby failed" verbose
        exit 1
    else
        LOG_MSG "[INFO]:-HAWQ standby started" verbose
    fi

    sleep 5

    ${SSH} -o 'StrictHostKeyChecking no' ${hawqUser}@${master_host_name} \
        "${SOURCE_PATH}; hawq start master -a;" >> ${STANDBY_LOG_FILE}
    if [ $? -ne 0 ] ; then
        LOG_MSG "[ERROR]:-Start HAWQ master failed" verbose
        exit 1
    else
        LOG_MSG "[INFO]:-HAWQ master started" verbose
    fi

    ${SSH} -o 'StrictHostKeyChecking no' ${hawqUser}@${master_host_name} \
        "${SOURCE_PATH}; env PGOPTIONS=\"-c gp_session_role=utility\" $PSQL -p ${master_port} -d template1 \
         -c\"select * from gp_segment_configuration;\";" >>${STANDBY_LOG_FILE} 2>&1

}

segment_init() {
    source ${GPHOME}/greenplum_path.sh
    for tmp_path in `${ECHO} ${hawqSegmentTemp} | sed 's|,| |g'`; do
        if [ ! -d ${tmp_path} ]; then
            ${ECHO} "Temp directory is not exist, please create it" | tee -a ${SEGMENT_LOG_FILE}
            ${ECHO} "Segment init failed on ${host_name}"
            exit 1
        else
           if [ ! -w "${tmp_path}" ]; then 
               ${ECHO} "Do not have write permission to temp directory, please check" | tee -a ${SEGMENT_LOG_FILE}
               ${ECHO} "Segment init failed on ${host_name}"
               exit 1
           fi
        fi
    done
    export LD_LIBRARY_PATH=${GPHOME}/lib:${GPHOME}/ext/python/lib:${LD_LIBRARY_PATH}

    ${GPHOME}/bin/initdb -E UNICODE -D ${hawq_data_directory} --locale=${locale} --lc-collate=${hawq_lc_collate} \
         --lc-ctype=${hawq_lc_ctype} --lc-messages=${hawq_lc_messages} --lc-monetary=${hawq_lc_monetary} \
         --lc-numeric=${hawq_lc_numeric} --lc-time=${hawq_lc_time} --max_connections=${segment_max_connections} \
         --shared_buffers=${shared_buffers} --backend_output=${log_dir}/segment.initdb 1>>${SEGMENT_LOG_FILE} 2>&1

    if [ $? -ne 0 ] ; then
        ${ECHO} "Postgres initdb failed" | tee -a ${SEGMENT_LOG_FILE}
        ${ECHO} "Segment init failed on ${host_name}"
        exit 1
    fi

    update_segment_pg_hba 1>>${SEGMENT_LOG_FILE} 2>&1

    ${PG_CTL} -D ${hawq_data_directory} -l ${hawq_data_directory}/pg_log/startup.log -w -t 60 -o \
         " -p ${hawq_port} --silent-mode=true -M segment -i" start >> ${SEGMENT_LOG_FILE}

    if [ $? -ne 0  ] ; then
        ${ECHO} "Segment init failed on ${host_name}" | tee -a ${SEGMENT_LOG_FILE}
        exit 1
    fi
    }

check_data_directorytory() {
    # If it's default directory, create it if not exist.
    default_mdd=~/hawq-data-directory/masterdd
    default_sdd=~/hawq-data-directory/segmentdd
    if [ "${hawq_data_directory}" = "${default_mdd}" ]; then
        ${MKDIR} -p ${default_mdd}
    elif [ "${hawq_data_directory}" = "${default_sdd}" ]; then
        ${MKDIR} -p ${default_sdd}
    fi
    # Check if data directory already exist and clean.
    if [ -d ${hawq_data_directory} ]; then
        if [ "$(ls -A ${hawq_data_directory})" ] && [ "${hawq_data_directory}" != "" ]; then
             ${ECHO} "Data directory ${hawq_data_directory} is not empty on ${host_name}"
             exit 1
        fi
    else
        ${ECHO} "Data directory ${hawq_data_directory} does not exist, please create it"
        exit 1
    fi
}

check_temp_directory() {
    # Check if temp directory exist.
    for tmp_dir in ${tmp_dir_list}; do
        if [ ! -d ${tmp_dir} ]; then
            ${ECHO} "Temporary directory ${tmp_dir} does not exist, please create it"
            exit 1
        fi
        if [ ! -w ${tmp_dir} ]; then
            ${ECHO} "Temporary directory ${tmp_dir} is not writable, exit." ;
            exit 1
        fi
    done
}


if [ ${object_type} == "master" ]; then
    check_data_directorytory
    check_temp_directory
    master_init
elif [ ${object_type} == "standby" ]; then
    check_data_directorytory
    standby_init
elif [ ${object_type} == "segment" ]; then
    check_data_directorytory
    check_temp_directory
    segment_init
else
    ${ECHO} "Please input correct node object"
    exit 1
fi
exit 0
