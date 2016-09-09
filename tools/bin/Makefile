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

# protect the default target for this file from the targets in Makefile.global
# and Makefile.thirdparty
default: all

subdir = tools/bin
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

#
# SOURCE DIRECTORIES
#
SRC=$(CURDIR)
PYLIB_SRC=$(SRC)/pythonSrc


#
# INSTALL DIRECTORY
#
LIB_DIR=$(SRC)/lib
PYLIB_DIR=$(SRC)/ext

all: lockfile pygresql stream pychecker psi unittest2 pexpect

#
# Python Libraries
#


#
#  STREAM
#
STREAM_DIR=$(SRC)/src/stream
stream:
	@echo "--- stream"
	cd $(STREAM_DIR) && NO_M64=TRUE $(CC) $(CFLAGS) stream.c -o stream
	cp $(STREAM_DIR)/stream lib/stream 

#
# PyGreSQL
#
PYGRESQL_VERSION=4.0
PYGRESQL_DIR=PyGreSQL-$(PYGRESQL_VERSION)

pygresql:
	@echo "--- PyGreSQL"
	cd $(PYLIB_SRC)/$(PYGRESQL_DIR) && CC="$(CC)" CFLAGS="${CFLAGS}" LDFLAGS="-L$(top_builddir)/src/interfaces/libpq ${LDFLAGS}" python setup.py build
	mkdir -p $(PYLIB_DIR)/pygresql
	cp -r $(PYLIB_SRC)/$(PYGRESQL_DIR)/build/lib.*/* $(PYLIB_DIR)/pygresql
	touch $(PYLIB_DIR)/__init__.py

#
# LOCKFILE
#
# note the awk commands are used to eliminate references to code in __init__.py
# that we don't use and also to insert a comment to tell pylint not to complain
# about these files since we are not in a position to correct those warnings.
#
LOCKFILE_VERSION=0.9.1
LOCKFILE_DIR=lockfile-$(LOCKFILE_VERSION)
LOCKFILE_SRC=$(PYLIB_SRC)/$(LOCKFILE_DIR)/build/lib/lockfile
LOCKFILE_DST=$(PYLIB_DIR)/lockfile
lockfile:
	@echo "--- lockfile"
	cd $(PYLIB_SRC)/$(LOCKFILE_DIR)/ && python setup.py build
	mkdir -p $(PYLIB_DIR)/lockfile
	awk 'BEGIN{print "# pylint: disable-all"} /^if hasattr/ {exit} { print }' < $(LOCKFILE_SRC)/__init__.py    > $(LOCKFILE_DST)/__init__.py
	awk 'BEGIN{print "# pylint: disable-all"} { print }'                      < $(LOCKFILE_SRC)/pidlockfile.py > $(LOCKFILE_DST)/pidlockfile.py

#
# PYCRYPTO
#
PYCRYPTO_VERSION=2.0.1
PYCRYPTO_DIR=pycrypto-$(PYCRYPTO_VERSION)

pycrypto:
	@echo "--- pycrypto"
	cd $(PYLIB_SRC)/ && $(TAR) xzf $(PYCRYPTO_DIR).tar.gz
	cd $(PYLIB_SRC)/$(PYCRYPTO_DIR)/ && CC="$(CC)" CFLAGS="${CFLAGS}" LDFLAGS="${LDFLAGS}" python setup.py build
	cp -r $(PYLIB_SRC)/$(PYCRYPTO_DIR)/build/lib.*/Crypto $(PYLIB_DIR)

#
# PSI
#
PSI_VERSION=0.3b2_gp
PSI_DIR=PSI-$(PSI_VERSION)

psi:
	@echo "--- psi"
	cd $(PYLIB_SRC)/$(PSI_DIR)/ && CC="$(CC)" CFLAGS="${CFLAGS}" LDFLAGS="${LDFLAGS}" python setup.py build
	cp -r $(PYLIB_SRC)/$(PSI_DIR)/build/lib.*/psi $(PYLIB_DIR)

#
# PEXPECT
#
PEXPECT_VERSION=4.2
PEXPECT_DIR=pexpect-$(PEXPECT_VERSION)

pexpect:
	@echo "--- pexpect"
	cd $(PYLIB_SRC)/$(PEXPECT_DIR)/ && CC="$(CC)" CFLAGS="${CFLAGS}" LDFLAGS="${LDFLAGS}" python setup.py build
	cp -r $(PYLIB_SRC)/$(PEXPECT_DIR)/build/lib/pexpect $(PYLIB_DIR)
	cp -r $(PYLIB_SRC)/ptyprocess-0.5.1/ptyprocess $(PYLIB_DIR)

#
# PYCHECKER
#
PYCHECKER_VERSION=0.8.18
PYCHECKER_DIR=pychecker-$(PYCHECKER_VERSION)

pychecker:
	@echo "--- pychecker"
	cd $(PYLIB_SRC)/$(PYCHECKER_DIR)/ && python setup.py build
	cp -r $(PYLIB_SRC)/$(PYCHECKER_DIR)/build/lib/pychecker  $(PYLIB_DIR)/

UNITTEST2_VERSION=0.5.1
UNITTEST2_DIR=unittest2-${UNITTEST2_VERSION}
unittest2:
	@echo "--- unittest2"
	cd $(PYLIB_SRC)/$(UNITTEST2_DIR)/ && python setup.py build
	cp -r $(PYLIB_SRC)/$(UNITTEST2_DIR)/build/lib/unittest2  $(PYLIB_DIR)/

#
# PARAMIKO
#
PARAMIKO_VERSION=1.7.6-9
PARAMIKO_DIR=paramiko-$(PARAMIKO_VERSION)
paramiko:
	@echo "--- paramiko"
	cd $(PYLIB_SRC)/ && $(TAR) xzf $(PARAMIKO_DIR).tar.gz
	cd $(PYLIB_SRC)/$(PARAMIKO_DIR)/ && python setup.py build
	cp -r $(PYLIB_SRC)/$(PARAMIKO_DIR)/build/lib/paramiko  $(PYLIB_DIR)/


PYTHON_FILES=`grep -l --exclude=Makefile --exclude=gplogfilter --exclude=gpcheckos --exclude=gpgenfsmap.py --exclude=throttlingD.py "/bin/env python" *`\
			 `grep -l "/bin/env python" $(SRC)/../sbin/*`\
			 `find ./gppylib -name "*.py"`\
			 `find ./hawqpylib -name "*.py"`\
			 `find $(SRC)/../sbin -name "*.py"`

checkcode: pylint
	@echo "Running pylint on management scripts..."
	@PYTHONPATH=$(PYTHONPATH):$(PYLINT_PYTHONPATH) $(PYLIB_SRC)/$(PYLINT_DIR)/bin/pylint -i y $(PYTHON_FILES) --rcfile=.rcfile > $(SRC)/../pylint.txt || true
	@echo -n "pylint_score=" > $(SRC)/../pylint_score.properties
	@grep "Your code has been rated at" $(SRC)/../pylint.txt | sed -e "s|Your .* \(.*\)/.*|\1|" >> $(SRC)/../pylint_score.properties

check-regress:
	@echo "Running regression tests..."
	@PYTHONPATH=$(SRC):$(SRC)/ext:$(PYTHONPATH) \
	gppylib/gpunit discover --verbose -s gppylib -p "test_regress*.py" 2> $(SRC)/../gpMgmt_testregress_results.log 1> $(SRC)/../gpMgmt_testregress_output.log

check:
	@echo "Running unit tests..."
	@PYTHONPATH=$(SRC):$(SRC)/ext:$(PYTHONPATH) \
	gppylib/gpunit discover --verbose -s gppylib -p "test_unit*.py" 2> $(SRC)/../gpMgmt_testunit_results.log 1> $(SRC)/../gpMgmt_testunit_output.log

solarisTest:
	@if [ `uname -s` = 'SunOS' ]; then \
		echo "SOLARIS" ; \
	fi

#
#EPYDOC
#

EPYDOC_VERSION=3.0.1
EPYDOC_DIR=epydoc-$(EPYDOC_VERSION)
EPYDOC_PYTHONPATH=$(PYLIB_DIR):$(PYLIB_SRC)/$(EPYDOC_DIR)/build/lib/

epydoc:
	@echo "--- epydoc"
	@cd $(PYLIB_SRC)/ && $(TAR) xzf $(EPYDOC_DIR).tar.gz
	@cd $(PYLIB_SRC)/$(EPYDOC_DIR)/ && python setup.py build 1> /dev/null

docs: epydoc
	@echo "Running epydoc on management scripts..."
	@PYTHONPATH=$(PYTHONPATH):$(EPYDOC_PYTHONPATH) $(PYLIB_SRC)/$(EPYDOC_DIR)/build/scripts-2.6/epydoc --config=.epydoc.config

clean :
	[ "x$(PYGRESQL_DIR)" != "x" ] && rm -rf $(PYLIB_SRC)/$(PYGRESQL_DIR)/build; true
	rm -rf $(STREAM_DIR)/stream lib/stream  
	rm -rf *.pyc lib/*.pyc

distclean: clean

SKIP_INSTALL=.epydoc.config|.gitignore|.p4ignore|.rcfile|Makefile|generate-greenplum-path.sh
install: all
	${INSTALL_SCRIPT} -d ${bindir}
	for files in `find * -maxdepth 0 -type f | grep -x -v -E "${SKIP_INSTALL}"`; do ${INSTALL_SCRIPT} $${files} ${bindir}; done
	${MAKE} -C gppylib $@
	${MAKE} -C hawqpylib $@
	${MAKE} -C ext $@
	${INSTALL_SCRIPT} -d ${bindir}/lib
	for files in `find lib -type f`; do ${INSTALL_SCRIPT} $${files} ${bindir}/lib; done
	unset LIBPATH; ./generate-greenplum-path.sh $(prefix) > ${prefix}/greenplum_path.sh
