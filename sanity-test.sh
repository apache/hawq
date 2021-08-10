#!/bin/bash -e
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

source /usr/local/hawq/greenplum_path.sh
TEST_DB_NAME="hawq_feature_test_db"
export PGDATABASE=$TEST_DB_NAME

function check_feature_test_data
{
    if [ ! -f src/test/feature/query/data/test_insert_groupby.data ] || [ ! -d src/test/feature/query/data/joindata ];then
    echo -e '
    \033[01;32m
    ###############################################################################

    Note:

    Please download data from

    https://s3.cn-north-1.amazonaws.com.cn/perfdata/generateddata/datatype.tar.gz

    1. login in to https://console.amazonaws.cn/s3/home?region=cn-north-1# and it can not directly download from link.
    2. Found datatype data from perfdata/generateddata/datatype.tar.gz
       Found join data from perfdata/generateddata/joindata.tar.gz

    And put the "*.data" files into

    ./src/test/feature/query/data/

    using

    "tar -xvf ~/Downloads/datatype.tar.gz -C ./src/test/feature/query/data/"
    "tar -xvf ~/Downloads/joindata.tar.gz     -C ./src/test/feature/query/data/"

    You would not receive this NOTE again once you did what mention above.

    ###############################################################################
    \033[00m'
   exit 1
    fi
}

function initTestDB()
{
   echo "Init Test Database ..."
   psql -d postgres -c "drop database if exists $TEST_DB_NAME;"
   psql -d postgres -c "create database $TEST_DB_NAME;"
   psql -c  "alter database $TEST_DB_NAME set lc_messages to 'C';"
   psql -c "alter database $TEST_DB_NAME set lc_monetary to 'C';"
   psql -c  "alter database $TEST_DB_NAME set lc_numeric to 'C';"
   psql -c  "alter database $TEST_DB_NAME set lc_time to 'C';"
   psql -c "alter database $TEST_DB_NAME set timezone_abbreviations to 'Default';"
   psql -c  "alter database $TEST_DB_NAME set timezone to 'PST8PDT';"
   psql -c  "alter database $TEST_DB_NAME set datestyle to 'postgres,MDY';"
   echo "Done."
}

function printf_usage ()
{
    echo -e "Usage: ./sanity-test.sh [-i] [-c] [-t <orctiny/orcsanity/orcall/magmatiny/magmasanity/magmaall>] [-s <'serial case'>] [-p <'parallel case'>]"
}

function printf_help ()
{
    printf_usage
    echo
    echo -e " -c\t Make feature test clean"
    echo -e " -i\t Init test db, Note: it only drop database and recreate database not reinitialize hawq cluster" 
    echo -e " -t\t orctiny/orcsanity/orcall/magmatiny/magmasanity/magmaall/magmaaptiny/magmaapsanity/magmaapall/user if not specified, magmatiny is default"
    echo -e " -s\t Feature test serial case. It can be used together with -s option"
    echo -e " -p\t Feature test parallel case. It can be used togher with -p option"
    echo -e " -h\t Print help"
}

function compile_featuretest()
{
  echo "Clean feature-test ..."
  if [[ $cleanfeature == "true" ]]; then 
     make feature-test-clean
     echo "Done."
  fi

  echo "Build feature-test ..."
  make -j8 feature-test
  echo "Done."
}

function clean_and_check_test()
{
  rm -rf src/test/feature/testresult
  check_feature_test_data

  echo "Check Hdfs/python/GPHOME command ..."
  command -v hdfs >/dev/null 2>&1 || { echo >&2 "Need to add hdfs command in PATH"; exit 1; }
  command -v python >/dev/null 2>&1 || { echo >&2 "Need to add python command in PATH"; exit 1; }
  : ${GPHOME?"Need to source greenplum_path.sh"}
  echo "Done."

  if [[ $inittestdb == "true" ]]; then
    initTestDB
  fi
}


DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PREFIX="/opt/dependency"
#source $PREFIX/env.sh
export DEPENDENCY_PATH=$PREFIX/package

serial_case1=""
serial_case2=""
parallel_case1="TestExtMagma.TestMagmaAP*:TestSelectQuery.TestMagmaAP*"
parallel_case2=""
inittestdb=false
cleanfeature=false
while getopts s:p:t:cih opt
do
    case "$opt" in
    s)
        serial_case2=$OPTARG
        ;;
    p)
        parallel_case2=$OPTARG
        ;;
    i)
        inittestdb=true 
        ;;
    c)
        cleanfeature=true
        ;;
    h)
        printf_help
        exit 0
        ;;
    t)
        echo "param"
        echo "$OPTARG"
        case $OPTARG in
        "user" )
            serial_case1=""
            parallel_case1=""
            ;;
        "orctiny" ) 
            serial_case1=""
            parallel_case1="TestNewExecutor.*:-*Magma*"
            ;;
        "orcsanity" )
            serial_case1="TestTPCH.TestORC_NewQE_Hash"
            parallel_case1="TestNewExecutor.*:TestExtOrc.*:TestSelectQuery.*ORC:TestOlapQuery.*ORC:TestQueryPrepare.*ORC:TestSlice.*ORC"
            ;;
        "orcall" )
            serial_case1="TestTPCH.TestORC_NewQE_Partition:TestTPCH.TestORC_OldQE"
            parallel_case1="TestNewExecutor.*:TestExtOrc.*:TestSelectQuery.*ORC:TestOlapQuery.*ORC:TestQueryPrepare.*ORC:TestSlice.*ORC"
            ;;
        "magmatiny" )
            serial_case1=""
            parallel_case1="TestExtMagma.TestMagmaTP*:TestSelectQuery.TestMagmaTP*:TestNewExecutor.TestMagmaTP*"
            ;;
        "magmasanity" )
            serial_case1="TestTPCH.TestMagmaTP"
            parallel_case1="TestExternalDataType.TestMagmaTP*"
            ;;
        "magmaall" )
            serial_case1="TestTPCH.TestMagmaTPPartition:TestTPCH.TestMagmaTP_PK:TestTransaction.TestMagmaTP*"
            parallel_case1="*.TestMagmaTP*:-TestTransaction*.*:TestMagmaQuitQuery.*:TestTPCH.*:TestMagmaFault.*"
            ;;   
        "magmaaptiny" )
            serial_case1=""
            parallel_case1="TestExtMagma.TestMagmaAP*:TestSelectQuery.TestMagmaAP*"
            ;;
        "magmaapsanity" )
            serial_case1="TestTPCH.TestMagmaAP"
            parallel_case1="TestExternalDataType.TestMagmaAP*"
            ;;
        "magmaapall" )
            serial_case1="TestTPCH.TestMagmaAP:TestTransaction.TestMagmaAP*"
            parallel_case1="*.TestMagmaAP*:-TestTransaction*.*:TestMagmaQuitQuery.*:TestTPCH.*:TestMagmaFault.*"
            ;; 
        *)
           echo "It should be orctiny/orcsanity/orcall/magmatiny/magmasanity/magmaall" 
           exit 
        esac
        ;;
    *)
        echo "Unknown option: $opt"
        exit 1
        ;;
    esac
done

#compile_featuretest

clean_and_check_test

echo "The first part test is $serial_case1 and $parallel_case1"
if [[ $serial_case1 != "" ]]; then 
  echo "Run sanity test Serial test and filer is $serial_case1"
  src/test/feature/feature-test --gtest_filter=$serial_case1
fi
 
if [[ $parallel_case1 != "" ]];  then
  rm -rf src/test/feature/testresult
  mkdir -p src/test/feature/testresult
  echo "Run sanity test Parallel test and filer is $parallel_case1"
  python src/test/feature/gtest-parallel --workers=4 --output_dir=src/test/feature/testmagmaresult --print_test_times src/test/feature/feature-test --gtest_filter=$parallel_case1
fi

if [[ $serial_case2 != "" ]]; then
   echo "Run sanity test Serial test and filer is $serial_case2"
   src/test/feature/feature-test --gtest_filter=$serial_case2
fi

if [[ $parallel_case2 != "" ]]; then
  rm -rf mkdir -p src/test/feature/testresult2
  mkdir -p src/test/feature/testresult2
  python src/test/feature/gtest-parallel --workers=4 --output_dir=src/test/feature/testresult2 --print_test_times src/test/feature/feature-test --gtest_filter=$parallel_case2
fi

echo "Finish test"


