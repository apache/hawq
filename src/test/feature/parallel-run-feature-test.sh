#!/bin/sh
#
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

if [ x$GPHOME == 'x' ]; then
  echo "Please source greenplum_path.sh before running feature tests."
  exit 0
fi

PSQL=${GPHOME}/bin/psql
HAWQ_DB=${PGDATABASE:-"postgres"}
HAWQ_HOST=${PGHOST:-"localhost"}
HAWQ_PORT=${PGPORT:-"5432"}
HAWQ_USER=${PGUSER:-}
HAWQ_PASSWORD=${PGPASSWORD:-}

run_sql() {
  suffix=""
  if [ x$HAWQ_USER != 'x' ]; then
    suffix=$suffix:" -U $HAWQ_USER"
    if [ x$HAWQ_PASSWORD != 'x' ]; then
      suffix=$suffix:" -W $HAWQ_PASSWORD"
    fi
  fi
  $PSQL -d $HAWQ_DB -h $HAWQ_HOST -p $HAWQ_PORT -c "$1" $suffix > /dev/null 2>&1
  if [ $? -ne 0 ]; then
    echo "$1 failed."
    exit 1
  fi
}

init_hawq_test() {
  TEST_DB_NAME="hawq_feature_test_db"
  
  $PSQL -d $HAWQ_DB -h $HAWQ_HOST -p $HAWQ_PORT \
        -c "create database $TEST_DB_NAME;" > /dev/null 2>&1
  run_sql "alter database $TEST_DB_NAME set lc_messages to 'C';"
  run_sql "alter database $TEST_DB_NAME set lc_monetary to 'C';"
  run_sql "alter database $TEST_DB_NAME set lc_numeric to 'C';"
  run_sql "alter database $TEST_DB_NAME set lc_time to 'C';"
  run_sql "alter database $TEST_DB_NAME set timezone_abbreviations to 'Default';"
  run_sql "alter database $TEST_DB_NAME set timezone to 'PST8PDT';"
  run_sql "alter database $TEST_DB_NAME set datestyle to 'postgres,MDY';"
  export PGDATABASE=$TEST_DB_NAME
}

run_feature_test() {
  if [ $# -lt 2 ]; then
    echo "Usage: parallel-run-feature-test.sh workers [number of workers] binary [binary ...] -- [additional args]" 
    exit 0
  fi
  init_hawq_test
  python $(dirname $0)/gtest-parallel --worker=$1 ${@:2}
}

run_feature_test $1 ${@:2}
