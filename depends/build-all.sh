#!/bin/bash -e
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

DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PREFIX="/opt/dependency/package"
export LD_LIBRARY_PATH=$PREFIX/lib:$LD_LIBRARY_PATH
export DYLD_FALLBACK_LIBRARY_PATH=$PREFIX/lib:$DYLD_FALLBACK_LIBRARY_PATH

if [[ -z $RUN_UNITTEST ]]; then
   RUN_UNITTEST=YES
fi
OPTION=""
case $1 in
    release)
        echo "Build Release Version ..."
        OPTION=""
        ;;
    debug)
        echo "Build Debug Version ..."
        OPTION="--enable-debug"
        ;;
    coverage)
        echo "Build Coverage Version ..."
        OPTION="--enable-coverage"
        ;;
    noavx)
        echo "Build AVX Version ..."
        OPTION="--enable-avx=OFF"
        ;;
    incremental)
        echo "Build incremental ..."
        OPTION="incremental"
        ;;
    *)
        echo "Build Release Version ..."
        OPTION="";
        ;;
esac

function build() {
  echo "Build $1 ..."
  
  MODULE=$1
  MAGMA_OPTION=""
  ###kv_client && kv_server#####
  if [ $1 = "magma_client" ] ;then
    MAGMA_OPTION="--enable-client"
    MODULE=magma
  fi
  if [ $1 = "magma_server" ] ;then
    MAGMA_OPTION="--enable-server"
    MODULE=magma
  fi
   
  if [ "$OPTION" != "incremental" ]; then
    echo "rm $DIR/$MODULE/build"
    rm -rf $DIR/$MODULE/build

    mkdir -p $DIR/$MODULE/build
    cd $DIR/$MODULE/build
    echo "../bootstrap $OPTION $MAGMA_OPTION"
    ../bootstrap $OPTION $MAGMA_OPTION
    CMAKE_OPTION=$OPTION
  else
    if [ ! -d $DIR/$MODULE/build ]; then mkdir -p $DIR/$MODULE/build; fi
    cd $DIR/$MODULE/build
    CMAKE_OPTION=$(grep 'CMAKE_BUILD_TYPE:STRING=' CMakeCache.txt | cut -d'=' -f2)
    case $CMAKE_OPTION in
      Release)
        CMAKE_OPTION=""
        ;;
      Debug)
        CMAKE_OPTION="--enable-debug"
        codecov=$(grep 'ENABLE_COVERAGE:BOOL=' CMakeCache.txt | cut -d'=' -f2)
        if [ $codecov = 'ON' ]; then
          CMAKE_OPTION="--enable-coverage"
        fi
        ;;
      *)
        CMAKE_OPTION="";
        ;;
      esac
    echo "../bootstrap $CMAKE_OPTION $MAGMA_OPTION"
    ../bootstrap $CMAKE_OPTION $MAGMA_OPTION
  fi

  if [[ $OPTION == "--enable-coverage" || $CMAKE_OPTION == "--enable-coverage" ]]
  then
    make coverage
  else
    make -j8 ;
    if [[ $RUN_UNITTEST == "YES" ]]; then
        echo "Run unittest ...."
        make -j8 punittest
    else
        echo "Do not run unittest...."
    fi
  fi
  make -j8 install

  echo "Done."
}
echo "Delete headers in ${PREFIX}/include ..."
rm -rf $PREFIX/include/dbcommon
rm -rf $PREFIX/include/univplan
echo "Done."

echo "Delete libs in ${PREFIX}/lib ..."
rm -rf $PREFIX/lib/libdbcommon*
rm -rf $PREFIX/lib/libunivplan*
echo "Done."

build dbcommon
build univplan
