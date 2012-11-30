#!/bin/bash
## ======================================================================
## Post download untar trigger
## ======================================================================

FILE=$1
REVISION=$2

ARTIFACT_DIR=$( dirname ${FILE} )

## echo "    src=${FILE}"
## echo "    dest=${ARTIFACT_DIR}/../${REVISION}"
## echo "======================================================================"
## echo ""

##
## Expand tarball
##

if [ ! -f "${FILE}" ]; then
    echo "WARNING: tarball does not exist (${FILE})"
    exit 2
else
    if [ ! -d ${ARTIFACT_DIR}/../${REVISION} ]; then
        mkdir -p ${ARTIFACT_DIR}/../${REVISION}
        if [ $? != 0 ]; then
            echo "FATAL: Problem creating exapand directory (${ARTIFACT_DIR}/../${REVISION})"
            exit 1
        fi
    fi

    pushd ${ARTIFACT_DIR}/../${REVISION}
    gunzip -qc ${FILE} | tar xf -
    popd

    if [ $? != 0 ]; then
        echo "FATAL: Problem exapanding tarball (${FILE})"
        exit 1
    fi

fi

exit 0
