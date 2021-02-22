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

export HAWQ_TOOLCHAIN_PATH=$(cd "$( dirname "${BASH_SOURCE[0]-$0}" )" && pwd)/download

# GitHub Release for third-party package
# https://github.com/apache/hawq/releases/tag/thirdparty
REPO=https://github.com/apache/hawq/releases/download/thirdparty/



###
### macOS
###
if [[ $(uname -s) == Darwin ]]; then
  if [ ! -d $HAWQ_TOOLCHAIN_PATH/dependency-Darwin ]; then
    (cd $HAWQ_TOOLCHAIN_PATH && curl -OL $REPO/dependency-Darwin.tar.xz)
    (cd $HAWQ_TOOLCHAIN_PATH && tar xJf dependency-Darwin.tar.xz -C $HAWQ_TOOLCHAIN_PATH)
    ln -snf dependency-Darwin $HAWQ_TOOLCHAIN_PATH/dependency

    for file in $(find $HAWQ_TOOLCHAIN_PATH/dependency/package/bin -name '*' -type f) $(find $HAWQ_TOOLCHAIN_PATH/dependency/package/lib -name '*.dylib' -type f); do
      if [[ $(file $file | grep Mach-O) ]]; then
        install_name_tool -add_rpath $HAWQ_TOOLCHAIN_PATH/dependency/package/lib $file || true
      fi
    done
    install_name_tool -add_rpath $HAWQ_TOOLCHAIN_PATH/dependency/package/lib/perl5/5.28.0/darwin-thread-multi-2level/CORE/ $HAWQ_TOOLCHAIN_PATH/dependency/package/bin/perl
  fi

  export MAKEFLAGS=-j$(sysctl -n hw.ncpu)
fi



###
### Linux
###
if [[ $(uname -s) == Linux ]]; then
  if [ ! -d $HAWQ_TOOLCHAIN_PATH/gcc ]; then
    (cd $HAWQ_TOOLCHAIN_PATH && curl -OL $REPO/gcc-7.4.0-x86_64-linux-sles11.4.tar.xz)
    (cd $HAWQ_TOOLCHAIN_PATH && tar xJf gcc-7.4.0-x86_64-linux-sles11.4.tar.xz -C $HAWQ_TOOLCHAIN_PATH)
    ln -snf gcc-7.4.0-x86_64-linux-sles11.4 $HAWQ_TOOLCHAIN_PATH/gcc
  fi
  if [ ! -d $HAWQ_TOOLCHAIN_PATH/cmake ]; then
    (cd $HAWQ_TOOLCHAIN_PATH && curl -OL $REPO/cmake-3.12.4-Linux-x86_64.tar.gz)
    (cd $HAWQ_TOOLCHAIN_PATH && tar xzf cmake-3.12.4-Linux-x86_64.tar.gz -C $HAWQ_TOOLCHAIN_PATH)
    ln -snf cmake-3.12.4-Linux-x86_64 $HAWQ_TOOLCHAIN_PATH/cmake
  fi
  if [ ! -d $HAWQ_TOOLCHAIN_PATH/dependency-gcc-x86_64-Linux/ ]; then
    (cd $HAWQ_TOOLCHAIN_PATH && curl -OL $REPO/dependency-gcc-x86_64-Linux.tar.gz)
    (cd $HAWQ_TOOLCHAIN_PATH && tar xzf dependency-gcc-x86_64-Linux.tar.gz -C $HAWQ_TOOLCHAIN_PATH)
    ln -snf dependency-gcc-x86_64-Linux $HAWQ_TOOLCHAIN_PATH/dependency
  fi

  export PATH=$HAWQ_TOOLCHAIN_PATH/gcc/bin:$HAWQ_TOOLCHAIN_PATH/cmake/bin:$PATH
  export LD_LIBRARY_PATH=$HAWQ_TOOLCHAIN_PATH/gcc/lib64/:$LD_LIBRARY_PATH

  export CPATH=$HAWQ_TOOLCHAIN_PATH/gcc/include/c++/7.4.0/:$HAWQ_TOOLCHAIN_PATH/gcc/include/c++/7.4.0/x86_64-pc-linux-gnu/
  export CPATH=$CPATH:/usr/include/x86_64-linux-gnu/
  export LIBRARY_PATH=$HAWQ_TOOLCHAIN_PATH/gcc/lib64/:/usr/lib/x86_64-linux-gnu/

  unset CPPFLAGS
  export CFLAGS='-std=gnu11 -fno-use-linker-plugin'
  export CXXFLAGS='-fpermissive -fno-use-linker-plugin'
  unset LDFLAGS

  export CC=gcc
  export CXX=g++
  export LD=ld

  export MAKEFLAGS=-j$(nproc)
fi



###
rm -rf $HAWQ_TOOLCHAIN_PATH/dependency/package/include/hdfs
rm -rf $HAWQ_TOOLCHAIN_PATH/dependency/package/lib/libhdfs3*
source $HAWQ_TOOLCHAIN_PATH/dependency/package/env.sh
