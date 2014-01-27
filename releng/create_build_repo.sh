#!/bin/bash
# ======================================================================
# Pulse clones the source tree to BLDWRAP_TOP, but our system needs it
# as BLDWRAP_TOP/src.  Since the source tree has its own src
# directory, we rename it to .orig and create an empty directory with
# the original name, then rename it as BLDWRAP_TOP/src.
#
# Another task here is to pull releng-build.  Only bin and lib
# directories are interesting to us, but we pull them to
# BLDWRAP_TOP/{bin,lib} to follow the build system rule.
#
# Pulse project should be configured RELENG_BUILD_URL to tell where to
# pull the releng-build.  You could add git options like -c in this
# property.
# ======================================================================

mkdir -p ${BLDWRAP_TOP}/src

mv ${BLDWRAP_TOP} ${BLDWRAP_TOP}.orig

mkdir -p ${BLDWRAP_TOP}

mv ${BLDWRAP_TOP}.orig ${BLDWRAP_TOP}/src

cd ${BLDWRAP_TOP}

##
## Cleanup any previously retrieved release engineering scripts.
##

rm -rf /tmp/releng-build
RETURN=$?
if [ ${RETURN} != 0 ]; then
    echo "FATAL: create_build_repo.sh cleanup or previous releng script retrieval exited with a non-zero status (${RETURN})"
    exit 1
fi

##
## Retrieve release engineering scripts
##

git clone ${RELENG_BUILD_URL} /tmp/releng-build
RETURN=$?
if [ ${RETURN} != 0 ]; then
    echo "FATAL: create_build_repo.sh git exited with a non-zero status (${RETURN})"
    exit 1
fi

##
## Copy release engineering scripts to source area
##

cp -r /tmp/releng-build/* ${BLDWRAP_TOP}
RETURN=$?
if [ ${RETURN} != 0 ]; then
    echo "FATAL: create_build_repo.sh copying releng scripts to source area exited with a non-zero status (${RETURN})"
    exit 1
fi

##
## Cleanup retrieved release engineering scripts
##

rm -rf /tmp/releng-build
