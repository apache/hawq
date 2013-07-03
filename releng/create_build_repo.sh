#!/bin/bash

#
# Pulse clones the source tree to BLDWRAP_TOP, but our system needs it
# as BLDWRAP_TOP/src.  Since the source tree has its own src directory,
# we rename it to .orig and create an empty directory with the original name,
# then rename it as BLDWRAP_TOP/src.
#
# Another task here is to pull releng-build.  Only bin and lib directories
# are interesting to us, but we pull them to BLDWRAP_TOP/{bin,lib} to
# follow the build system rule.
#
# Pulse project should be configured RELENG_BUILD_URL to tell where to
# pull the releng-build.  You could add git options like -c in this property.
#

mkdir -p ${BLDWRAP_TOP}/src

mv ${BLDWRAP_TOP} ${BLDWRAP_TOP}.orig

mkdir -p ${BLDWRAP_TOP}

mv ${BLDWRAP_TOP}.orig ${BLDWRAP_TOP}/src

cd ${BLDWRAP_TOP}

git clone ${RELENG_BUILD_URL} /tmp/releng-build

cp -r /tmp/releng-build/lib /tmp/releng-build/bin ${BLDWRAP_TOP}
