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
# Makefile for the managerment utilities
#
#-------------------------------------------------------------------------

subdir = tools/bin/hawqpylib
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SKIP_INSTALL=.gitignore|.p4ignore|.rcfile|Makefile|test/

install:
	${INSTALL_SCRIPT} -d ${DESTDIR}${libdir}/python/hawqpylib
	@for file in `find * -type f | grep -v -E "${SKIP_INSTALL}"`; \
		do \
			echo "install $${file} into ${DESTDIR}${libdir}/python/hawqpylib/$${file}" ; \
			${INSTALL_SCRIPT} $${file} ${DESTDIR}${libdir}/python/hawqpylib/$${file}; \
		done
	@for dirs in `find * -type d | grep -v test` ;\
		do \
			${INSTALL_SCRIPT} -d ${DESTDIR}${libdir}/python/hawqpylib/$${dirs}; \
			for file in `find $${dirs} -type f | grep -v -E "${SKIP_INSTALL}"`; do \
				echo "install $${file} into ${DESTDIR}${libdir}/python/hawqpylib/$${file}" ; \
				${INSTALL_SCRIPT} $${file} ${DESTDIR}${libdir}/python/hawqpylib/$${file}; \
			done \
		done
	
