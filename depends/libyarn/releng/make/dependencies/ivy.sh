#!/bin/bash
## ======================================================================
## Post download untar trigger : For EXT 
## ======================================================================

MODULE=$1
REVISION=$2
ORG=$3
BUILD_STAGE=$4

# ##
# ## Working directory is the source dir from where we do make
# ## This will be used in creating ext dir
# ##

##WORKING_DIRECTORY=$(readlink -m ${PWD}/../../..)

WORKING_DIRECTORY=$(cd ${PWD}/../../..; pwd -P)

EXT_DIRECTORY=${WORKING_DIRECTORY}/ext/${BUILD_STAGE}

if [ ! -d "${EXT_DIRECTORY}" ]; then
    mkdir -p ${EXT_DIRECTORY}
fi

TOOLS_DIRECTORY=/opt/releng/tools/${ORG}/${MODULE}/${REVISION}/${BUILD_STAGE}
##
## Copy all the files from /opt/tools to main/ext
##

if [ -d "${TOOLS_DIRECTORY}" ] && [ ! -f "${EXT_DIRECTORY}/checksums.${MODULE}-${REVISION}" ]; then
    cd $TOOLS_DIRECTORY
    tar cf - * | ( cd ${EXT_DIRECTORY}; tar xfp -)
fi

##
## Softlink all the files for ext.
## 
## This does require the dirs and sub-dirs to 
## to be created, which is done in the 
## first for loop
##

# for dir_entry in `find . -type d`
#  do
#    echo "$dir_entry"
#    mkdir -p ${EXT_DIRECTORY}/${BUILDSTAGE}$dir_entry
# done

# for entry in `find . -type f`
#  do
#    echo "$entry"
#    ln -s ${TOOLS_DIRECTORY}/$entry ${EXT_DIRECTORY}/${BUILDSTAGE}$entry
#  done
