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
hawqpath=$(cd $(dirname $0) && pwd)
ENABLE_BRANCH="--rc lcov_branch_coverage=1"

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
    "tar -xvf ~/Downloads/join.tar.gz     -C ./src/test/feature/query/data/"

    You would not receive this NOTE again once you did what mention above.

    ###############################################################################
    \033[00m'
    exit 1
    fi
}

function printf_usage ()
{
    echo -e "Usage: ./coverage-report.sh [-i] [-e] [-b <hornet/hawq/all>] [-s <'serial case'>] [-p <'parallel case'>] -o <hornet/hawq/all> <'hornet path'>"
}

function printf_help ()
{
    printf_usage
    echo
    echo -e   " \t Path of your hornet is needed"
    echo -e " -o\t Specify which project of coverage report is needed"
    echo -e " -b\t Build coverage"
    echo -e " -s\t Feature test serial case"
    echo -e " -p\t Feature test parallel case"
    echo -e " -i\t Init coverage env"
    echo -e " -e\t Init feature test database test env"
    echo -e " -h\t Print help"
}

##
hornetpath=""
serial_case=""
parallel_case=""
hornet_coverage_make=""
hawq_coverage_make=""
hornet_coverage_init=""
hawq_report=""
hornet_report=""
reinit_feature_test_env=""
case_empty="empty"
##

while getopts s:p:b:o:eih opt
do
    case "$opt" in
    s)
        serial_case=$OPTARG
        case_empty="notempty" 
        ;;
    p)
        parallel_case=$OPTARG
        case_empty="notempty" 
        ;;
    b)
        if [ $OPTARG = "hornet" ];then
            hornet_coverage_make="make"
        elif [ $OPTARG = "hawq" ];then
            hawq_coverage_make="make"
        elif [ $OPTARG = "all" ];then
            hornet_coverage_make="make"
            hawq_coverage_make="make"
        else
            echo -e "Unknown param: $OPTARG"
            exit 1
        fi
        ;;
    o)
        if [ $OPTARG = "hornet" ];then
            hornet_report="report"
        elif [ $OPTARG = "hawq" ];then
            hawq_report="report"
        elif [ $OPTARG = "all" ];then
            hornet_report="report"
            hawq_report="report"
        else
            echo -e "Unknown param: $OPTARG"
            exit 1
        fi
        ;;
    h)
        printf_help
        exit 0
        ;;
    i)
        hornet_coverage_init="init" 
        ;;
    e)
        reinit_feature_test="reinit"
        ;;
    *)
        echo "Unknown option: $opt"
        exit 1
        ;;
    esac
done

shift $[ $OPTIND -1 ]
hornetpath=$1

#check param and env start
if [[ -z $hornet_report ]] && [[ -z $hawq_report ]];then
    echo -e "coverage-report.sh: Unspecified which project of coverage report is needed\n"
    echo "coverage-report.sh: try './coverage-report.sh -h' for more information"
    exit 1;
fi
if [[ -z $hornetpath ]];then
    echo -e "coverage-report.sh: Unspecified path of your hornet is needed\n"
    echo "coverage-report.sh: try './coverage-report.sh -h' for more information"
    exit 1
elif [[ ! -d $hornetpath ]];then
    echo "$hornetpath is not exists!!!"
    exit 1
fi

if [[ $case_empty != "empty" ]];then
    check_feature_test_data
fi

echo "Check command ..."
command -v hdfs >/dev/null 2>&1 || { echo >&2 "Need to add hdfs command in PATH"; exit 1; }
command -v python >/dev/null 2>&1 || { echo >&2 "Need to add python command in PATH"; exit 1; }
command -v lcov >/dev/null 2>&1 || { echo >&2 "Need to add lcov command in PATH"; exit 1; }
echo "Done."
#check param and env end

par_hornetpath=$(cd $(dirname $hornetpath) && pwd)

if [[ -n $hornet_coverage_init ]];then
    rm -rf $par_hornetpath/coverage >/dev/null 2>&1
fi

#function start
function hornet-coverage-make ()
{
    if [ ! -d $par_hornetpath/coverage/hornet ];then
        mkdir -p $par_hornetpath/coverage/hornet
        cd $par_hornetpath/coverage/hornet
        $par_hornetpath/hornet/bootstrap
        make coverage || true
        hornet_coverage_make=""
        if [[ $case_empty != "empty" ]];then
            hawq restart cluster -a -M immediate --with_magma
        fi
        cd -
    fi
    if [[ $hornet_coverage_make = "make" ]];then
        cd $par_hornetpath/coverage/hornet && make incremental && cd - ;
        if [[ $case_empty != "empty" ]];then
            hawq restart cluster -a -M immediate --with_magma
        fi
    fi
}

function hawq_coverage_make ()
{
    if [[ $hawq_coverage_make = "make" ]];then
        cd $hawqpath
        hawq stop cluster -a -M immediate --with_magma
        make distclean || true
        ./configure --enable-coverage
        export LD_LIBRARY_PATH=/opt/dependency/package/lib:$LD_LIBRARY_PATH
        make -j8 && make install
        if [[ $case_empty != "empty" ]];then 
            hawq start cluster -a --with_magma
        fi
        cd -
    fi
}

function hornet-coverage-report ()
{
    cd $par_hornetpath/coverage/hornet
    lcov --base-directory . --directory . --capture --output-file CodeCoverage.info --ignore-errors graph ${ENABLE_BRANCH}
    lcov --remove CodeCoverage.info '/opt/*' '/usr/*' '/Library/*' '/Applications/*' '*/test/unit/*' '*/testutil/*' '*/protos/*' '*/proto/*' --output-file CodeCoverage.info.cleaned ${ENABLE_BRANCH}
    genhtml CodeCoverage.info.cleaned -o CodeCoverageReport --prefix $par_hornetpath/hornet ${ENABLE_BRANCH}
    open CodeCoverageReport/index.html
    cd -
}

function hawq-coverage-report ()
{
    cd $hawqpath
    make coverage && open CodeCoverageReport/index.html
    cd -
}

function run_feature_test ()
{
    cd $hawqpath
    set +e
    if [[ -n $serial_case ]];then
        src/test/feature/feature-test --gtest_filter="$serial_case"
    fi
    if [[ -n $parallel_case ]];then
        python src/test/feature/gtest-parallel --workers=4 --output_dir=src/test/feature/testmagmaresult --print_test_times src/test/feature/feature-test --gtest_filter="$parallel_case"
    fi
    set -e
    cd -
}

function coverage-report ()
{
    if [[ -n $hornet_report ]];then
        hornet-coverage-report
    fi
    if [[ -n $hawq_report ]];then
        hawq-coverage-report
    fi
    exit 0
}

function feature_test_env
{
    cd $hawqpath
    DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    PREFIX="/opt/dependency"
    source $PREFIX/package/env.sh
    export DEPENDENCY_PATH=$PREFIX/package

    if [[ ! -x src/test/feature/feature-test ]];then
        echo "Clean feature-test ..."
        rm -rf src/test/feature/testresult
        make feature-test-clean
        echo "Done."

        echo "Build feature-test ..."
        make -j8 feature-test
        echo "Done."
    fi
    set +e
    psql -d postgres -c "select version()"
    if [[ $? != 0 ]]; then 
        hawq start cluster --with_magma
    fi
    set -e
    if [[ $reinit_feature_test = "reinit" ]];then
        echo "Prepare test env ..."
        TEST_DB_NAME="hawq_feature_test_db"
        psql -d postgres -c "drop database if exists $TEST_DB_NAME;"
        psql -d postgres -c "create database $TEST_DB_NAME;"
        export PGDATABASE=$TEST_DB_NAME
        psql -c "alter database $TEST_DB_NAME set lc_messages to 'C';"
        psql -c "alter database $TEST_DB_NAME set lc_monetary to 'C';"
        psql -c "alter database $TEST_DB_NAME set lc_numeric to 'C';"
        psql -c "alter database $TEST_DB_NAME set lc_time to 'C';"
        psql -c "alter database $TEST_DB_NAME set timezone_abbreviations to 'Default';"
        psql -c "alter database $TEST_DB_NAME set timezone to 'PST8PDT';"
        psql -c "alter database $TEST_DB_NAME set datestyle to 'postgres,MDY';"
        echo "Done."
    fi
    cd -
}

function main
{
    hornet-coverage-make
    hawq_coverage_make
    if [[ $case_empty = "empty" ]];then 
        coverage-report
    fi
    feature_test_env
    run_feature_test
    hawq stop cluster -a --with_magma
    coverage-report
}
#function end

main
