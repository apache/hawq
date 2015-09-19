## ======================================================================
## 
## ======================================================================

BLD_TOP:=$(shell sh -c pwd)
SHELL=/bin/bash

##-------------------------------------------------------------------------------------
## dependent modules
##
## NOTE: Dependent project module version is kept in $(BLD_TOP)/make/dependencies/ivy.xml
##-------------------------------------------------------------------------------------

GREP_SED_VAR = $(BLD_TOP)/make/dependencies/ivy.xml | sed -e 's|\(.*\)rev="\(.*\)"[ ]*conf\(.*\)|\2|'
CMAKE_VER  = $(shell grep "\"cmake\"" $(GREP_SED_VAR))

TARGET_VERSION = ${LIBHDFS3_VERSION}

ifeq "$(LIBHDFS3_VERSION)" ""
TARGET_VERSION = `cat ${BLD_TOP}/../build/version`
endif

ifeq "$(LIBHDFS3_NOW)" ""
TARGET_TIME = `date +%Y%m%d%H%M%S`
endif

ifeq "$(LIBHDFS3_TEST_URL)" ""
LIBHDFS3_TEST_URL = localhost:9000
endif

ifneq "$(LIBYARN_PUBLISH_OVERWRITE)" ""
LIBYARN_PUBLISH_OVERWRITE = -overwrite
endif

##-------------------------------------------------------------------------------------

# Determine the default target architecture on this machine, if override not set
ifeq "$(BLD_ARCH)" ""
export BLD_ARCH:=$(shell $(BLD_TOP)/set_bld_arch.sh)
endif

export PATH:=${BLD_TOP}/../ext/${BLD_ARCH}/bin:${BLD_TOP}/../ext/${BLD_ARCH}/cmake-${CMAKE_VER}/bin:${PATH}

# construct test directory prefix on hdfs.
CUR_DATE=$(shell date +%s)
TEST_HDFS_PREFIX="${BLD_ARCH}/${CUR_DATE}/"

# prepare HDFS instance
ifeq "${HDFS_CONFIG}" ""
export HDFS_CONFIG:=local
endif

export HDFS_CONFIG_PATH:=${BLD_TOP}/etc/${HDFS_CONFIG}/hdfs-site.xml

ifeq "${HDFS_USER}" ""
export HDFS_USER:=${USER}
endif

ifeq "${DATA_PATH}" ""
export DATA_PATH:=/tmp/libyarn-pulse
endif

ifneq "${LIBHDFS_HDFS_SUPERUSER}" ""
export HDFS_SUPERUSER_OPTION:=-DHDFS_SUPERUSER=${LIBHDFS_HDFS_SUPERUSER}
endif

# include thirdparty infrastructure which depends on WHERE_THE...
BLD_WHERE_THE_LIBRARY_THINGS_ARE=LD_LIBRARY_PATH
osx106_x86_WHERE_THE_LIBRARY_THINGS_ARE=DYLD_LIBRARY_PATH
ifneq "$($(BLD_ARCH)_WHERE_THE_LIBRARY_THINGS_ARE)" ""
BLD_WHERE_THE_LIBRARY_THINGS_ARE=$($(BLD_ARCH)_WHERE_THE_LIBRARY_THINGS_ARE)
endif

SED_INLINE_EDIT_BAKUP_FILE=

ifeq "$(BLD_ARCH)" "osx106_x86"
	SED_INLINE_EDIT_BAKUP_FILE=''
	export $(BLD_WHERE_THE_LIBRARY_THINGS_ARE):=/opt/gcc-4.4.2-osx106/lib:${BLD_TOP}/../ext/${BLD_ARCH}/lib:$($(BLD_WHERE_THE_LIBRARY_THINGS_ARE))
else
	export $(BLD_WHERE_THE_LIBRARY_THINGS_ARE):=/opt/gcc-4.4.2/lib64:${BLD_TOP}/../ext/${BLD_ARCH}/lib:$($(BLD_WHERE_THE_LIBRARY_THINGS_ARE))
endif

##-------------------
## set compiler
##-------------------

CCOMPILER=${CC}
CXXCOMPILER=${CXX}

ifeq "${CCOMPILER}" ""
CCOMPILER=gcc
endif

ifeq "${CXXCOMPILER}" ""
CXXCOMPILER=g++
endif

## ----------------------------------------------------------------------
## libhdfs targets
## ----------------------------------------------------------------------

all: libhdfs-sync_tools libhdfs-clean libhdfs-configure libhdfs-build libhdfs-unittest

${BLD_TOP}/../build:
	mkdir ${BLD_TOP}/../build

libhdfs-sync_tools:
	make sync_tools

libhdfs-clean:
	@rm -rf ${BLD_TOP}/../dist ${BLD_TOP}/../build

libhdfs-configure: ${BLD_TOP}/../build
	(cd ${BLD_TOP}/../build && cmake ${BLD_TOP}/.. -DTEST_HDFS_PREFIX=${TEST_HDFS_PREFIX} $(LIBHDFS3_CONF_FLAGS) \
													-DCMAKE_C_COMPILER=${CCOMPILER} -DCMAKE_CXX_COMPILER=${CXXCOMPILER} \
	                                                -DCMAKE_INSTALL_PREFIX=${BLD_TOP}/../dist/${BLD_ARCH} \
                                                    -DCMAKE_PREFIX_PATH=${BLD_TOP}/../ext/${BLD_ARCH} \
                                                    ${HDFS_SUPERUSER_OPTION})

libhdfs-build:
	(cd ${BLD_TOP}/../build && make)

libhdfs-install:
	rm -rf ${BLD_TOP}/../dist/${BLD_ARCH}
	(cd ${BLD_TOP}/../build && make install)

libhdfs-test:
	(cd ${BLD_TOP}/../build && make test)
	
libhdfs-showcov:
	(cd ${BLD_TOP}/../build && make ShowCoverage)

libhdfs-functiontest:
	(export GTEST_OUTPUT="xml:functiontest.xml"; cd ${BLD_TOP}/../build && make functiontest)

libhdfs-securetest:
	(export GTEST_OUTPUT="xml:securetest.xml"; cd ${BLD_TOP}/../build && make securetest)

libhdfs-unittest:
	(export GTEST_OUTPUT="xml:unittest.xml"; cd ${BLD_TOP}/../build && make unittest)

libhdfs-update-function-test.xml:
	(cd ${BLD_TOP}/../test/data && chmod a+w function-test.xml && sed -e 's|localhost:9000|${LIBHDFS3_TEST_URL}|' -i ${SED_INLINE_EDIT_BAKUP_FILE} function-test.xml)
	(cd ${BLD_TOP}/../test/data && chmod a+w function-secure.xml && sed -e 's|localhost:9000|${LIBHDFS3_TEST_URL}|' -i ${SED_INLINE_EDIT_BAKUP_FILE} function-secure.xml)

libhdfs-package: libhdfs-install
	(cd ${BLD_TOP}/../dist/${BLD_ARCH} && find . -type f -or -type l | xargs ${MD5} > /tmp/checksums.libyarn && mv /tmp/checksums.libyarn ${BLD_TOP}/../dist/${BLD_ARCH} && \
	 cd ${BLD_TOP}/../dist/ && tar -czvf libyarn-${BLD_ARCH}-${TARGET_VERSION}.targz ${BLD_ARCH} )

libhdfs-update-ivys:
	(cd ${BLD_TOP}/../dist/ && mkdir -p ${TARGET_VERSION}/ivys && cp ${BLD_TOP}/etc/ivy.xml ${TARGET_VERSION}/ivys/ivy-${TARGET_VERSION}.xml \
		&& sed -e 's|<version>|'${TARGET_VERSION}'|' -i ${SED_INLINE_EDIT_BAKUP_FILE} ${TARGET_VERSION}/ivys/ivy-${TARGET_VERSION}.xml \
		&& sed -e 's|<time>|'${TARGET_TIME}'|' -i ${SED_INLINE_EDIT_BAKUP_FILE} ${TARGET_VERSION}/ivys/ivy-${TARGET_VERSION}.xml)

libhdfs-ivy-package: libhdfs-package libhdfs-update-ivys
	(cd ${BLD_TOP}/../dist/ && mkdir -p ${TARGET_VERSION}/targzs && cp libyarn-${BLD_ARCH}-${TARGET_VERSION}.targz ${TARGET_VERSION}/targzs/ && tar -cjvf libyarn-${BLD_ARCH}-${TARGET_VERSION}.tar.bz2 ${TARGET_VERSION})

libhdfs-update-hdfs-conf:
	(cd ${BLD_TOP}/etc/${HDFS_CONFIG} && cp -f hdfs-site.xml.simple hdfs-site.xml && sed -e 's|@user@|${HDFS_USER}|' -i ${SED_INLINE_EDIT_BAKUP_FILE} hdfs-site.xml \
		&& mkdir -p ${DATA_PATH} && sed -e 's|@data_path@|${DATA_PATH}|' -i ${SED_INLINE_EDIT_BAKUP_FILE} hdfs-site.xml)

libhdfs-cleanup-phd:
	(${BLD_TOP}/bin/cleanup-phd.sh)

libhdfs-install-phd: libhdfs-update-hdfs-conf
	(${BLD_TOP}/bin/install-phd.sh)

## ----------------------------------------------------------------------
## Sync/Clean tools
## ----------------------------------------------------------------------
## Populate/clean up dependent releng supported tools.  The projects are
## downloaded and installed into /opt/releng/...
##
## Tool dependencies and platform config mappings are defined in:
##   * Apache Ivy dependency definition file
##       releng/make/dependencies/ivy.xml
## ----------------------------------------------------------------------

opt_write_test:
	@if [ ! -w /opt ]; then \
	    echo ""; \
	    echo "======================================================================"; \
	    echo "ERROR: /opt is not writable."; \
	    echo "----------------------------------------------------------------------"; \
	    echo "  Supporting tools are stored in /opt.  Please ensure you have"; \
	    echo "  write access to /opt"; \
	    echo "======================================================================"; \
	    echo ""; \
	    exit 1; \
	fi

/opt/releng/apache-ant: 
	${MAKE} opt_write_test
	echo "Sync Ivy project dependency management framework ..."
	curl --silent http://build-prod.dh.greenplum.com/tools/apache-ant.1.8.1.tar.gz -o /tmp/apache-ant.1.8.1.tar.gz
	( umask 002; [ ! -d /opt/releng ] && mkdir -p /opt/releng; \
	   cd /opt/releng; \
	   gunzip -qc /tmp/apache-ant.1.8.1.tar.gz | tar xf -; \
	   rm -f /tmp/apache-ant.1.8.1.tar.gz; \
	   chmod -R a+w /opt/releng/apache-ant )

sync_tools: opt_write_test /opt/releng/apache-ant
	@cd make/dependencies; \
	 (umask 002; /opt/releng/apache-ant/bin/ant -DBLD_ARCH=$(BLD_ARCH) resolve); \
	 echo "Resolve finished"

clean_tools: opt_write_test
	@cd releng/make/dependencies; \
	/opt/releng/apache-ant/bin/ant clean; \
	rm -rf /opt/releng/apache-ant; \

# Publish Libhdfs
local_publish: /opt/releng/apache-ant
	@if [ -f publish/ivy.xml ]; then \
		java -Xmx512M -jar /opt/releng/apache-ant/lib/ivy-2.2.0.jar \
			-settings publish/ivysettings.xml \
			-ivy publish/ivy.xml \
			-publish local \
			-publishpattern "../dist/[artifact]-[revision].[ext]" \
			-revision ${TARGET_VERSION} \
			-status release \
			-overwrite; \
			fi; 
public_publish: /opt/releng/apache-ant
	@if [ -f publish/ivy.xml ]; then \
		java -Xmx512M -Djavax.net.ssl.trustStore=$(BLD_TOP)/make/dependencies/cacerts \
			-jar /opt/releng/apache-ant/lib/ivy-2.2.0.jar \
			-settings publish/ivysettings.xml \
			-ivy publish/ivy.xml \
			-publish public \
			-publishpattern "../dist/[artifact]-[revision].[ext]" \
			-revision ${TARGET_VERSION} \
			-status release \
			$(LIBYARN_PUBLISH_OVERWRITE); \
	fi;
