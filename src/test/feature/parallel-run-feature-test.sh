#! /bin/bash

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
