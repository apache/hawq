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
#-------------------------------------------------------------------------
#
# Makefile for license information of hawq rpm package
#
#-------------------------------------------------------------------------

subdir = dist/hawq
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

SKIP_INSTALL=.gitignore|.p4ignore|.rcfile|Makefile

all: install

install:
	for files in `find * -type f -maxdepth 0 | grep -v -E "${SKIP_INSTALL}"`; do ${INSTALL_SCRIPT} $${files} ${DESTDIR}/${prefix}; done
	mkdir -p ${DESTDIR}/${prefix}/licenses
	for files in `find ${top_builddir}/licenses/* -type f -maxdepth 0 | grep -v -E "${SKIP_INSTALL}"`; do ${INSTALL_SCRIPT} $${files} ${DESTDIR}/${prefix}/licenses/; done
