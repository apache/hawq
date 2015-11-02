#!/bin/bash

object_type=$1
GPHOME=$2

current_time=`date '+%s'`
mgmt_config_file=${GPHOME}/etc/_mgmt_config

if [ -f /etc/redhat-release ]; then
    os_version=`cat /etc/redhat-release | awk '{print substr($7,0,1)}'`
else
    os_version='other'
fi

source ${GPHOME}/greenplum_path.sh

if [ -f ${mgmt_config_file} ]; then
    source ${mgmt_config_file} > /dev/null 2>&1
else
    echo "${mgmt_config_file} is not exist, exit"
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
   echo "Node object should be master/standby/segment"
   exit 1
fi
master_max_connections=${max_connections}
segment_max_connections=${max_connections}
master_ip_address_all=""
standby_ip_address_all=""
if [ "${os_version}" = "7" ];then
    master_ip_address_all=`ssh ${master_host_name} "/sbin/ifconfig |grep -v '127.0.0' | grep 'inet '|awk '{print \\$2}'"`
    if [ "${standby_host_name}" != "" ] && [ "${standby_host_name}" != "None" ] \
        && [ "${standby_host_name}" != "none" ] && [ "${standby_host_name}" != "NONE" ];then
        standby_ip_address_all=`ssh ${standby_host_name} "/sbin/ifconfig |grep -v '127.0.0' | grep 'inet '|awk '{print \\$2}'"`
    fi
    segment_ip_address_all=`/sbin/ifconfig | grep -v '127.0.0' | awk '/inet addr/{print substr($2,6)}'`
else
    master_ip_address_all=`ssh ${master_host_name} "/sbin/ifconfig |grep -v '127.0.0' |awk '/inet addr/{print substr(\\$2,6)}'"`
    if [ "${standby_host_name}" != "" ] && [ "${standby_host_name}" != "None" ] \
        && [ "${standby_host_name}" != "none" ] && [ "${standby_host_name}" != "NONE" ];then
        standby_ip_address_all=`ssh ${standby_host_name} "/sbin/ifconfig |grep -v '127.0.0' |awk '/inet addr/{print substr(\\$2,6)}'"`
    fi
    segment_ip_address_all=`/sbin/ifconfig | grep -v '127.0.0' | awk '/inet addr/{print substr($2,6)}'`
fi

PG_HBA=pg_hba.conf
TMP_PG_HBA=/tmp/pg_hba_conf_master.$$

MASTER_LOG_FILE=${log_filename}
STANDBY_LOG_FILE=${log_filename}
SEGMENT_LOG_FILE=${log_filename}

PSQL=${GPHOME}/bin/psql
if [ "${log_dir}" = "None" ]; then
log_dir=${HOME}/hawqAdminLogs
fi
if [ ! -d ${log_dir} ]; then
    mkdir -p ${log_dir}
fi

if [ ! -f ${log_filename} ]; then
    touch ${log_filename}
fi

GET_CIDRADDR () {
    if [ `echo $1 | grep -c :` -gt 0 ]; then
        echo $1/128
    else
        echo $1/32
    fi
}

LOAD_GP_TOOLKIT () {
    CUR_DATE=`date +%Y%m%d`
    FILE_TIME=`date +%H%M%S`
    echo "[INFO]:-Loading hawq_toolkit..." >> ${MASTER_LOG_FILE}
    ROLNAME=`$PSQL -q -t -A -p ${hawq_port} -c "select rolname from pg_authid where oid=10" template1`
    if [ x"$ROLNAME" == x"" ];then
        echo "[FATAL]:-Failed to retrieve rolname." | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    if [ -f /tmp/_gp_toolkit_tmp_${CUR_DATE}_$FILE_TIME ]; then
        rm -f /tmp/_gp_toolkit_tmp_${CUR_DATE}_$FILE_TIME
    fi

    # We need SET SESSION AUTH here to load the toolkit
    echo "SET SESSION AUTHORIZATION $ROLNAME;"  >> /tmp/_gp_toolkit_tmp_${CUR_DATE}_$FILE_TIME 2>&1
    RETVAL=$?
    if [ $RETVAL -ne 0 ];then
        echo "[FATAL]:-Failed to create the hawq_toolkit sql file." | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    cat $GPHOME/share/postgresql/gp_toolkit.sql >> /tmp/_gp_toolkit_tmp_${CUR_DATE}_$FILE_TIME 2>&1
    RETVAL=$?
    if [ $RETVAL -ne 0 ];then
        echo "[FATAL]:-Failed to create the hawq_toolkit sql file." | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    $PSQL -q -p ${hawq_port} -f /tmp/_gp_toolkit_tmp_${CUR_DATE}_$FILE_TIME template1
    RETVAL=$?
    if [ $RETVAL -ne 0 ];then
        echo "[FATAL]:-Failed to create the hawq_toolkit schema." | tee -a ${MASTER_LOG_FILE} 
        exit 1
    fi

    $PSQL -q -p ${hawq_port} -f /tmp/_gp_toolkit_tmp_${CUR_DATE}_$FILE_TIME postgres
    RETVAL=$?
    if [ $RETVAL -ne 0 ];then
        echo "[FATAL]:-Failed to create the hawq_toolkit schema." | tee -a ${MASTER_LOG_FILE} 
        exit 1
    fi

    rm -f /tmp/_gp_toolkit_tmp_${CUR_DATE}_$FILE_TIME

    return $RETVAL
}

update_master_pg_hba(){
    # Updatepg_hba.conf for master.
    cat ${hawq_data_directory}/${PG_HBA} |grep '^#' > ${TMP_PG_HBA}
    mv ${TMP_PG_HBA} ${hawq_data_directory}/${PG_HBA}
    # Setting local access"
    echo "local    all         $USER         ident" >> ${hawq_data_directory}/${PG_HBA}
    # echo "[INFO]:-Setting local host access"
    echo "host     all         $USER         127.0.0.1/28    trust" >> ${hawq_data_directory}/${PG_HBA}
    MASTER_IPV6_LOCAL_ADDRESS_ALL=(`/sbin/ip -6 address show |grep inet6|awk '{print $2}' |cut -d'/' -f1`)
    MASTER_HBA_IP_ADDRESS=(`echo ${master_ip_address_all[@]} ${MASTER_IPV6_LOCAL_ADDRESS_ALL[@]} ${standby_ip_address_all[@]}|tr ' ' '\n'|sort -u|tr '\n' ' '`)
    for ip_address in ${MASTER_HBA_IP_ADDRESS[@]}; do
        CIDR_MASTER_IP=$(GET_CIDRADDR ${ip_address})
        CHK_COUNT=`grep -c ${CIDR_MASTER_IP} ${hawq_data_directory}/${PG_HBA}`
        if [ "$CHK_COUNT" -eq "0" ];then
            echo "host  all     ${USER}    ${CIDR_MASTER_IP}       trust" >> ${hawq_data_directory}/${PG_HBA}
        else
            echo "${CIDR_MASTER_IP} already exist in ${hawq_data_directory}/${PG_HBA}"
        fi
    done
}

update_standby_pg_hba(){
    # Updatepg_hba.conf for standby master.
    echo "host  all     all    0.0.0.0/0       trust" >> ${hawq_data_directory}/${PG_HBA}
}

update_segment_pg_hba(){
    # Updatepg_hba.conf for segment.
    # Setting local access"
    MASTERS_HBA_IP_ADDRESSES=(`echo ${master_ip_address_all[@]} ${standby_ip_address_all[@]}|tr ' ' '\n'|sort -u|tr '\n' ' '`)
    for ip_address in ${MASTERS_HBA_IP_ADDRESSES[@]}; do
        CIDR_MASTER_IP=$(GET_CIDRADDR ${ip_address})
        CHK_COUNT=`grep -c ${CIDR_MASTER_IP} ${hawq_data_directory}/${PG_HBA}`
        if [ "$CHK_COUNT" -eq "0" ];then
            echo "host  all     all    ${CIDR_MASTER_IP}       trust" >> ${hawq_data_directory}/${PG_HBA}
        fi
    done
    for ip_address in ${segment_ip_address_all[@]}; do
        CIDR_MASTER_IP=$(GET_CIDRADDR ${ip_address})
        CHK_COUNT=`grep -c ${CIDR_MASTER_IP} ${hawq_data_directory}/${PG_HBA}`
        if [ "$CHK_COUNT" -eq "0" ];then
            echo "host  all     ${USER} ${CIDR_MASTER_IP}       trust" >> ${hawq_data_directory}/${PG_HBA}
        fi
    done
}

master_init() {
    ${GPHOME}/bin/initdb -E UNICODE -D ${hawq_data_directory} --locale=${locale} --lc-collate=${hawq_lc_collate} \
        --lc-ctype=${hawq_lc_ctype} --lc-messages=${hawq_lc_messages} --lc-monetary=${hawq_lc_monetary} \
        --lc-numeric=${hawq_lc_numeric} --lc-time=${hawq_lc_time} --max_connections=${master_max_connections} \
        --shared_buffers=${shared_buffers} --backend_output=${log_dir}/master.initdb 1>>${MASTER_LOG_FILE} 2>&1

    if [ $? -ne 0 ] ; then
        echo "Master postgres initdb failed" | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    update_master_pg_hba 1>>${MASTER_LOG_FILE} 2>&1

    ${GPHOME}/bin/pg_ctl -D ${hawq_data_directory} -l ${hawq_data_directory}/pg_log/startup.log -w -t 60 -o " -p ${hawq_port}  --silent-mode=true -M master -i" start >> ${MASTER_LOG_FILE}
    if [ $? -ne 0  ] ; then
        echo "Start hawq master failed"  | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    ${GPHOME}/bin/psql -p ${hawq_port} -d template1 -c "create filespace dfs_system on hdfs ('${dfs_url}');" 1>>${MASTER_LOG_FILE} 2>&1
    if [ $? -ne 0  ] ; then
        echo "Create filespace failed, please check your hdfs settings"  | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    ${GPHOME}/bin/psql -p ${hawq_port} -d template1 -c "create tablespace dfs_default filespace dfs_system;" 1>>${MASTER_LOG_FILE} 2>&1
    if [ $? -ne 0  ] ; then
        echo "Create tablespace failed"  | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    env PGOPTIONS="-c gp_session_role=utility" ${GPHOME}/bin/psql -p ${hawq_port} -d template1 -c \
        "SET allow_system_table_mods='dml';UPDATE pg_database SET dat2tablespace = (SELECT oid FROM pg_tablespace WHERE spcname = 'dfs_default') WHERE datname = 'template1';" 1>>${MASTER_LOG_FILE} 2>&1
    if [ $? -ne 0  ] ; then
        echo "Configure database template1 failed" | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    ${GPHOME}/bin/psql -p ${hawq_port} -d template1 -c "create database template0 tablespace dfs_default template template1;" 1>>${MASTER_LOG_FILE} 2>&1
    if [ $? -ne 0  ] ; then
        echo "Create database template0 failed" | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    env PGOPTIONS="-c gp_session_role=utility" ${GPHOME}/bin/psql -p ${hawq_port} -d template1 -c "SET allow_system_table_mods='dml';UPDATE pg_database SET datistemplate = 't', datallowconn = false WHERE datname = 'template0';" 1>>${MASTER_LOG_FILE} 2>&1
    if [ $? -ne 0  ] ; then
        echo "Configure database template0 failed" | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    ${GPHOME}/bin/psql -p ${hawq_port} -d template1 -c "create database postgres tablespace dfs_default;" 1>>${MASTER_LOG_FILE} 2>&1
    if [ $? -ne 0  ] ; then
        echo "Create database postgres failed" | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    env PGOPTIONS="-c gp_session_role=utility" ${GPHOME}/bin/psql -p ${hawq_port} -d template1 -c "SET allow_system_table_mods='dml';UPDATE pg_database SET datistemplate = 't' WHERE datname = 'postgres';" 1>>${MASTER_LOG_FILE} 2>&1
    if [ $? -ne 0  ] ; then
        echo "Configure database postgres failed" | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    env PGOPTIONS="-c gp_session_role=utility" ${GPHOME}/bin/psql -p ${hawq_port} -d template1 -c "CHECKPOINT;" 1>>${MASTER_LOG_FILE} 2>&1
    if [ $? -ne 0  ] ; then
        echo "CHECKPOINT failed" | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi

    LOAD_GP_TOOLKIT
    if [ $? -ne 0  ] ; then
        echo "Load TOOLKIT failed" | tee -a ${MASTER_LOG_FILE}
        exit 1
    fi
    ${GPHOME}/bin/psql -p ${hawq_port} -d template1 -c "alter user \"${USER}\" password 'gparray';" 1>>${MASTER_LOG_FILE} 2>&1

    if [ $? -ne 0  ] ; then
        echo "Alter user failed" 1>> ${MASTER_LOG_FILE} 2>&1
        exit 1
    fi
}

standby_init() {
    # Make sure log file are created.
    ssh -o 'StrictHostKeyChecking no' ${hawqUser}@${standby_host_name} \
        "if [ ! -d ${log_dir} ]; then echo \"Try to create log directory for standby master.\"; mkdir -p ${log_dir}; fi" 
    ssh -o 'StrictHostKeyChecking no' ${hawqUser}@${standby_host_name} \
        "if [ ! -f ${STANDBY_LOG_FILE} ]; then touch ${STANDBY_LOG_FILE}; fi" 
    STANDBY_IP_ADDRESSES=`ping -c1 -n ${standby_host_name} | head -n1 | sed 's/.*(\([0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\)).*/\1/g'`
    echo "Try to stop HAWQ cluster" 1>>${STANDBY_LOG_FILE}
    source $GPHOME/greenplum_path.sh
    # Stop hawq cluster before add new standby master.
    hawq stop cluster -a >> ${STANDBY_LOG_FILE}
    # Check if data directory are exist and keep clean.
    ssh -o 'StrictHostKeyChecking no' ${hawqUser}@${standby_host_name} \
        "if [ ! -d ${master_data_directory} ]; then echo \"Data directory ${master_data_directory} is not exist, please create it.\"; exit 1; fi" 
    if [ $? -ne 0  ] ; then
        echo "Standby master data directory check failed" | tee -a ${STANDBY_LOG_FILE}
        exit 1
    fi
    ssh -o 'StrictHostKeyChecking no' ${hawqUser}@${standby_host_name} \
        "if [ \"\$(ls -A ${hawq_data_directory})\" ] && [ \"${hawq_data_directory}\" != \"\" ]; then echo \"Data directory ${hawq_data_directory} is not empty, please clean it.\"; exit 1; fi" 
    if [ $? -ne 0  ] ; then
        echo "Standby master data directory check failed" | tee -a ${STANDBY_LOG_FILE}
        exit 1
    fi
    pushd ${master_data_directory} >> ${STANDBY_LOG_FILE}
    # Sync data directories to standby master.
    echo "Sync master files to standby from master" >> ${STANDBY_LOG_FILE}
    tar cf - * --exclude="pg_log" --exclude="db_dumps" --exclude="gpperfmon/data"  \
        | ssh ${standby_host_name} tar xf - -C ${master_data_directory} 1>>${STANDBY_LOG_FILE}
    if [ $? -ne 0  ] ; then
        echo "Sync master files to standby failed" | tee -a ${STANDBY_LOG_FILE}
        exit 1
    fi
    
    ssh -o 'StrictHostKeyChecking no' ${hawqUser}@${standby_host_name} "mkdir -p ${master_data_directory}/pg_log | tee -a ${STANDBY_LOG_FILE};"
    hawq start standby -a >>  ${STANDBY_LOG_FILE}
    hawq stop standby -a >>  ${STANDBY_LOG_FILE}
    
    hawq start master -a >> ${STANDBY_LOG_FILE}
    env PGOPTIONS="-c gp_session_role=utility" psql -p ${master_port} -d template1 -c"select gp_remove_master_standby() where (select count(*) from gp_segment_configuration where role='s') = 1;" >> ${STANDBY_LOG_FILE} 2>&1
    env PGOPTIONS="-c gp_session_role=utility" psql -p ${master_port} -d template1 -c \
       "select gp_add_master_standby('${standby_host_name}','${STANDBY_IP_ADDRESSES}','');" 1>>${STANDBY_LOG_FILE} 2>&1
    if [ $? -ne 0  ] ; then
        echo "Register standby infomation failed" | tee -a ${STANDBY_LOG_FILE}
        exit 1
    fi
    hawq stop master -a >> ${STANDBY_LOG_FILE}
    
    hawq start cluster -a >> ${STANDBY_LOG_FILE}
    env PGOPTIONS="-c gp_session_role=utility" psql -p ${master_port} -d template1 -c"select * from gp_segment_configuration;" 1>>${STANDBY_LOG_FILE} 2>&1
    popd >> ${STANDBY_LOG_FILE}

}

segment_init() {
    source ${GPHOME}/greenplum_path.sh
    for tmp_path in `echo ${hawqSegmentTemp} | sed 's|,| |g'`; do
        if [ ! -d ${tmp_path} ]; then
            echo "Temp directory is not exist, please create it" | tee -a ${SEGMENT_LOG_FILE}
            echo "Segment init failed on ${HOSTNAME}"
            exit 1
        else
           if [ ! -w "${tmp_path}" ]; then 
               echo "Do not have write permission to temp directory, please check" | tee -a ${SEGMENT_LOG_FILE}
               echo "Segment init failed on ${HOSTNAME}"
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
        echo "Postgres initdb failed" | tee -a ${SEGMENT_LOG_FILE}
        echo "Segment init failed on ${HOSTNAME}"
        exit 1
    fi

    update_segment_pg_hba 1>>${SEGMENT_LOG_FILE} 2>&1

    ${GPHOME}/bin/pg_ctl -D ${hawq_data_directory} -l ${hawq_data_directory}/pg_log/startup.log -w -t 60 -o \
         " -p ${hawq_port} --silent-mode=true -M segment -i" start >> ${SEGMENT_LOG_FILE}

    if [ $? -ne 0  ] ; then
        echo "Segment init failed on ${HOSTNAME}" | tee -a ${SEGMENT_LOG_FILE}
        exit 1
    fi
    }

check_data_directorytory() {
    # If it's default directory, create it if not exist.
    default_mdd=~/hawq-data-directory/masterdd
    default_sdd=~/hawq-data-directory/segmentdd
    if [ "${hawq_data_directory}" = "${default_mdd}" ]; then
        mkdir -p ${default_mdd}
    elif [ "${hawq_data_directory}" = "${default_sdd}" ]; then
        mkdir -p ${default_sdd}
    fi
    # Check if data directory already exist and clean.
    if [ -d ${hawq_data_directory} ]; then
        if [ "$(ls -A ${hawq_data_directory})" ] && [ "${hawq_data_directory}" != "" ]; then
             echo "Data directory ${hawq_data_directory} is not empty on ${HOSTNAME}"
             exit 1
        fi
    else
        echo "Data directory ${hawq_data_directory} does not exist, please create it"
        exit 1
    fi
}

check_temp_directory() {
    # Check if temp directory exist.
    for tmp_dir in ${tmp_dir_list}; do
        if [ ! -d ${tmp_dir} ]; then
            echo "Temporary directory ${tmp_dir} does not exist, please create it"
            exit 1
        fi
        if [ ! -w ${tmp_dir} ]; then
            echo "Temporary directory ${tmp_dir} is not writable, exit." ;
            exit 1
        fi
    done
}


if [ ${object_type} == "master" ]; then
    check_data_directorytory
    check_temp_directory
    master_init
elif [ ${object_type} == "standby" ]; then
    standby_init
elif [ ${object_type} == "segment" ]; then
    check_data_directorytory
    check_temp_directory
    segment_init
else
    echo "Please input correct node type"
    exit 1
fi
exit 0
