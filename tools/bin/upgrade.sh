#!/bin/sh
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
#

# parse parameters
usage() { echo "Usage: $0 [-s <3.3.x.x|3.4.x.x|2.4.0.0|2.4.0.1>] [-h]" ; exit 1; }

delete_hcatalog=true;
while getopts ":s:h" opt; do
    case "${opt}" in
        s)
            source_version=${OPTARG}
            if [[ "$source_version" =~ 3\.[34]\.[0-9]\.[0-9] || "$source_version" == "2.4.0.0" || "$source_version" == "2.4.0.1" ]];then
                echo "Input source version is $source_version."
            else
                usage
            fi
            ;;
        h)
            delete_hcatalog=false
            echo "Don't delete hcatalog."
            ;;
        *)
            usage
            ;;
    esac
done

shift $((OPTIND-1))

if [ -z "${source_version}" ]; then
    usage
fi

if [[ $source_version == "2.4.0.0" || $source_version == "2.4.0.1" ]]; then
    upgrade_total=true
else
    upgrade_total=false
fi

upgrade_magmaview_only=false
if [[ $source_version =~  3\.4\.[0-9]\.[0-9] ]]; then
    upgrade_magmaview_only=true
else
    upgrade_magmaview_only=false
fi

check_error() {
    if [[ $? -ne 0 ]];then
        echo "Failed to $1."
        exit 1
    fi
    if [[ -n $2 && $2 -ne 0 ]];then
        echo "Failed to $1. Error info in output file."
        exit 1
    fi
}

get_guc(){
    local res=`grep -A 2 $1 $GPHOME/etc/hawq-site.xml| grep \<value\> | awk -F '[>]' '{print $2}'|awk -F '[<]' '{print $1}'`
    echo $res
}
# check environment
if [[ -z $GPHOME ]];then
    echo "Environment variable GPHOME is not set."
    exit 1
fi
source $GPHOME/greenplum_path.sh

user=`whoami`
if [[ "$user" != "gpadmin" ]]; then
    echo "Failed! User running the script shoule be gpadmin."
    exit 1
fi

# check master version
version_str=`hawq --version`
check_error "get hawq version on master"
target_version=`echo "$version_str"| awk -F '[ ]' '{print $3}'`
echo "Upgrade begin, you can find logs of each module in folder $HOME/hawqAdminLogs/upgrade"

MASTER_HOST=`get_guc "hawq_master_address_host"`
MASTER_PORT=`get_guc "hawq_master_address_port"`
SEGMENT_PORT=`get_guc "hawq_segment_address_port"`
MASTER_TEMP_DIR=`get_guc "hawq_master_temp_directory"`
SEGMENT_HOSTS=`cat $GPHOME/etc/slaves`
OPTIONS='-c gp_maintenance_conn=true'

# check whether all tmp dir exsits
echo $MASTER_TEMP_DIR | sed "s/,/ /g" | xargs ls
check_error "check master and segment temp dir on master"

# check whether all segments replaced with new binary
result=`gpssh -f $GPHOME/etc/slaves "source $GPHOME/greenplum_path.sh;hawq --version;"`
check_error "check version on all hosts"

# result returned by gpssh have special character ^M
count=`echo $result | sed 's//\\n/g'|grep $target_version | wc -l`
expected_count=`echo $SEGMENT_HOSTS|wc -w`

if [[ $count -ne $expected_count ]] ; then
    echo "Not all segments replaced with new binary. segment num is $expected_count, there are $count segment be replaced."
    exit 1
fi
echo "All segments have new version binary."

if [ ! -d $HOME/hawqAdminLogs/upgrade ]; then
    mkdir $HOME/hawqAdminLogs/upgrade
    check_error "create dir $HOME/hawqAdminLogs/upgrade"
  # Control will enter here if $DIRECTORY doesn't exist.
fi

hawq config -c upgrade_mode -v on --skipvalidation
check_error "set cluster to upgrade mode"

hawq config -c allow_system_table_mods -v all --skipvalidation
check_error "set allow_system_table_mods to all"

hawq start cluster -a
check_error "start cluster in upgrade mode"
echo "Start hawq cluster in upgrade mode successfully."

if $delete_hcatalog ; then
    # 删除master节点hcatalog数据库
    PGOPTIONS="$OPTIONS" psql -t -p $MASTER_PORT -d template1 -c "delete from pg_database where datname='hcatalog';"
    check_error " delete hcatalog database in master"
    echo "Delete hacatalog database in master successfully."
fi

# 删除segment节点hcatalog数据库
gpssh -f $GPHOME/etc/slaves "source $GPHOME/greenplum_path.sh;PGOPTIONS='$OPTIONS' psql -p $SEGMENT_PORT -d template1 -c \"delete from pg_database where datname='hcatalog';\""
check_error "delete hcatalog database in segment"
echo "Delete hacatalog database in segment successfully."

# 获取所有的用户数据库名称
dbnames=`PGOPTIONS="$OPTIONS" psql -t -p $MASTER_PORT -d template1 -c "select datname from pg_database where datname not in ('template0') order by datname;"`
check_error "get database names in upgrade mode"
echo "Get all database name successfully."

install_function_by_database(){
    # master节点函数注册
    result=`PGOPTIONS="$OPTIONS" psql -a -p $MASTER_PORT -d $1 -f $GPHOME/share/postgresql/${2}.sql 2>&1 > $HOME/hawqAdminLogs/upgrade/${1}_${2}_master.out`
    check_error "install $2 in database $1 in master"

    error_count=`grep -E '(ERROR|FATAL|PANIC)' $HOME/hawqAdminLogs/upgrade/${1}_${2}_master.out|wc -l`
    check_error "install $2 in database $1 in master" $error_count
    echo "Install $2 in database $1 in master successfully."

    if [[ $1 == "template1" && $2 != "monitor_install" ]];then
        #segment节点函数注册
        gpssh -f $GPHOME/etc/slaves "source $GPHOME/greenplum_path.sh;PGOPTIONS='$OPTIONS' psql -a -p $SEGMENT_PORT -d $1 -f $GPHOME/share/postgresql/${2}.sql 2>&1" > $HOME/hawqAdminLogs/upgrade/${1}_${2}.out
        check_error "install $2 in database $1 in segment"
        
        error_count=`grep -E '(ERROR|FATAL|PANIC)' $HOME/hawqAdminLogs/upgrade/${1}_${2}.out|wc -l`
        check_error "install $2 in database $1 in segment" $error_count
        echo "Install $2 in database $1 in segment successfully."
    fi
}

upgrade_catalog() {
    if $upgrade_magmaview_only ; then
        PGOPTIONS="$OPTIONS" psql -p $MASTER_PORT -d $1 -c "drop view pg_catalog.hawq_magma_status;
        CREATE VIEW pg_catalog.hawq_magma_status AS
        SELECT * FROM hawq_magma_status() AS s
        (node text,
         compactJobRunning text,
         compactJob text,
         compactActionJobRunning text,
         compactActionJob text,
         dirs text,
         description text);
        "
        check_error "update hawq_magma_status view in database $1 in master"
        echo "update hawq_magma_status view in database $1 in master"
        return
    fi
    # template1库更改元数据
    if $2 ; then
        # 1、增加hive权限认证列
        # master
        PGOPTIONS="$OPTIONS" psql -p $MASTER_PORT -d $1 -c "alter table pg_authid add column rolcreaterexthive bool;alter table pg_authid add column rolcreatewexthive bool;"
        check_error "add column for hive auth in pg_authid in database $1 in master"
        echo "add column for hive auth in pg_authid in database $1 in master successfully."

        # segment
        if [[ $1 == "template1" ]];then
            # segment
            gpssh -f $GPHOME/etc/slaves "source $GPHOME/greenplum_path.sh;PGOPTIONS='$OPTIONS' psql -p $SEGMENT_PORT -d $1 -c \"alter table pg_authid add column rolcreaterexthive bool;alter table pg_authid add column rolcreatewexthive bool;\""
            check_error "add column for hive auth in pg_authid in database $1 in segment"
            echo "Add column for hive auth in pg_authid in database $1 in segment successfully."
        fi

        # 2、hive 安装
        install_function_by_database $1 "hive_install"
    
        # 4、orc安装
        install_function_by_database $1 "orc_install"
    
        # 5、欧式距离安装
        install_function_by_database $1 "array_distance_install"
    fi
    
    # 6、增加magma权限认证
    PGOPTIONS="$OPTIONS" psql -t -p $MASTER_PORT -d $1 -c "alter table pg_authid add column rolcreaterextmagma bool;alter table pg_authid add column rolcreatewextmagma bool;"
    check_error "add magma role column in pg_authid in database $1 in master"
    echo "Add magma role column in pg_authid in database $1 in master successfully."
    
    if [[ $1 == "template1" ]];then
        #segment节点添加magma权限
        gpssh -f $GPHOME/etc/slaves "source $GPHOME/greenplum_path.sh;PGOPTIONS='$OPTIONS' psql -p $SEGMENT_PORT -d $1 -c \"alter table pg_authid add column rolcreaterextmagma bool;alter table pg_authid add column rolcreatewextmagma bool;\""
        check_error "add magma role column in pg_authid in database $1 in segment"
        echo "Add magma role column in pg_authid in database $1 in segment successfully."
    fi
    
    # 7、json函数注册
    install_function_by_database $1 "json_install"

    # 8、magma函数注册
    install_function_by_database $1 "magma_install"

    # 9、监控函数注册
    install_function_by_database $1 "monitor_install"
}

#升级所有的数据库
for dbname in $dbnames
do
    echo "upgrade $dbname"
    upgrade_catalog $dbname $upgrade_total
done

#停止集群
hawq config -c upgrade_mode -v off --skipvalidation
check_error "set cluster to normal mode"

hawq config -c allow_system_table_mods -v none --skipvalidation
check_error "set allow_system_table_mods to none"
echo "Set cluster to normal mode successfully."

hawq stop cluster -a
check_error "stop cluster"
echo "Upgrade to version $target_version sucessfully. cluster can be started now!"
