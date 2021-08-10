#!/usr/bin/env bash
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

function check_os(){
# This function is used to get node OS and core information
    # Read hostfile from input
    while IFS= read -r line
    do
        echo "------ Node name: $line"

        os_version=`ssh $line cat /etc/redhat-release`
        os_core=`ssh $line uname -a`

        echo $os_version
        echo $os_core

        printf "\n"
    done < "$input"
}

function check_postgres_and_magma_proc(){
# This function is used to to check if there is either postgres or magma process
    # Read hostfile from input
    while IFS= read -r line
    do
        echo "------ Node name: $line"

        postgresProcess=`ssh $line  ps -ef | grep postgres | grep -v grep | wc -l`
        magmaProcess=`ssh $line ps -ef | grep magma | grep -v grep | wc -l`

        if [ postgresProcess > 0 ] ; then
            echo "---------- Fail: $line has postgress process!"
        fi
        if [ magmaProcess > 0 ] ; then
            echo "---------- Fail: $line has magma process!"
        fi

        echo "---------- Detail:"
        echo "---------- Postgres process num: " $postgresProcess
        echo "---------- Magma process num: " $magmaProcess

        printf "\n"
    done < "$input"
}

echo "---------- Execute pre-setup check ----------"
printf "\n"
input=$2
echo "---------- Step 1: Check node OS and Core"
check_os
echo "---------- Step 2: Check if there is either postgres or magma process"
check_postgres_and_magma_proc