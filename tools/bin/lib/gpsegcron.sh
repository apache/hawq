#!/bin/bash
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

#	Filename:-		gpsegcron.sh
#	Version:-		$Revision$
#	Updated:-		$Date$	
#	Status:-		Released	
#	Author:-		G Coombe	
#	Contact:-		gcoombe@greenplum.com
#	Release date:-		Dec 2006
#	Release stat:-		Released
#******************************************************************************
# Update History
#******************************************************************************
# Ver	Date		Who		Update
#******************************************************************************
# Detailed Description
#******************************************************************************
#******************************************************************************
# Prep Code

WORKDIR=`dirname $0`

# Source required functions file, this required for script to run
# exit if cannot locate this file. Change location of FUNCTIONS variable
# as required.
FUNCTIONS=$WORKDIR/gp_bash_functions.sh
if [ -f $FUNCTIONS ]; then
		. $FUNCTIONS
else
		echo "[FATAL]:-Cannot source $FUNCTIONS file Script Exits!"
		exit 2
fi
#******************************************************************************
# Script Specific Variables
#******************************************************************************
# Log file that will record script actions
CUR_DATE=`$DATE +%Y%m%d`
TIME=`$DATE +%H%M%S`
PROG_NAME=`$BASENAME $0`
# Level of script feedback 0=small 1=verbose
unset VERBOSE
GP_USER=$USER_NAME
EXIT_STATUS=0
TRY_ZIP=0
#******************************************************************************
# Functions
#******************************************************************************
USAGE () {
		$ECHO
		$ECHO "      `basename $0`"
		$ECHO
		$ECHO "      Script called by gpcrondump, this should not be run directly"
		exit $EXIT_STATUS
}

CHK_CALL () {
	FILE_PREFIX=`$ECHO $PARALLEL_STATUS_FILE|$CUT -d"." -f1`
	if [ ! -f ${FILE_PREFIX}.$PARENT_PID ];then
		$ECHO "[FATAL]:-Not called from from correct parent program" 
		exit 2
	fi
}
CALC_COMPRESSION () {
	LOG_MSG "[INFO][$INST_COUNT]:-Start Function $FUNCNAME"
	((REQ_FREE_SPACE_K=$DIR_HOST_USED_K/$COMPRESSION_FACTOR))
	((CALC1=$DIR_HOST_FREE_K*$FREE_SPACE_PERCENT))
	((CALC2=$CALC1/100))
	((CALC_FREE_SPACE_K=$DIR_HOST_FREE_K-$CALC2))
	LOG_MSG "[INFO][$INST_COUNT]:-Revised space less $FREE_SPACE_PERCENT % = $CALC_FREE_SPACE_K K Required $REQ_FREE_SPACE_K K (Estimated size for compressed file)"
	if [ $CALC_FREE_SPACE_K -lt $REQ_FREE_SPACE_K ];then
        	LOG_MSG "[FATAL][$INST_COUNT]:-Insufficient free space $DIR_HOST $DIR required $REQ_FREE_SPACE_K K actual $CALC_FREE_SPACE_K K" 2
		QE_LINE="${QE_LINE}:NO_SPACE"
	else
		LOG_MSG "[INFO][$INST_COUNT]:-Sufficient free space on $DIR_HOST for dump in segment directory $DIR"
		if [ $TRY_ZIP -eq 1 ];then
			QE_LINE="${QE_LINE}:NEED_COMPRESSION"	
		else
			QE_LINE="${QE_LINE}:SPACE_OK"	
		fi
	fi
	LOG_MSG "[INFO][$INST_COUNT]:-End Function $FUNCNAME"
}

CALC_NO_COMPRESSION () {
	LOG_MSG "[INFO][$INST_COUNT]:-Start Function $FUNCNAME"
	REQ_FREE_SPACE_K=$DIR_HOST_USED_K
	((CALC1=$DIR_HOST_FREE_K*$FREE_SPACE_PERCENT))
	((CALC2=$CALC1/100))
	((CALC_FREE_SPACE_K=$DIR_HOST_FREE_K-$CALC2))
	LOG_MSG "[INFO][$INST_COUNT]:-Revised space less $FREE_SPACE_PERCENT % = $CALC_FREE_SPACE_K K Required $REQ_FREE_SPACE_K K "
	if [ $CALC_FREE_SPACE_K -lt $REQ_FREE_SPACE_K ];then
		LOG_MSG "[WARN][$INST_COUNT]:-Insufficient free space $DIR_HOST $DIR required $REQ_FREE_SPACE_K K actual $CALC_FREE_SPACE_K K"
		TRY_ZIP=1
	else
		LOG_MSG "[INFO][$INST_COUNT]:-Sufficient free space on $DIR_HOST for dump in segment directory $DIR"
		QE_LINE="${QE_LINE}:SPACE_OK"
	fi
	LOG_MSG "[INFO][$INST_COUNT]:-End Function $FUNCNAME"
}

CHK_SEG_DUMP_SPACE () {
	LOG_MSG "[INFO][$INST_COUNT]:-Start Function $FUNCNAME"
	DIR_HOST_FREE_K=`$TRUSTED_SHELL $DIR_HOST "$DF -k $DIR|$TAIL -1|$TR -s ' '|$TR ' ' '\n'|$TAIL -3|$HEAD -1"`
	LOG_MSG "[INFO][$INST_COUNT]:-Host $DIR_HOST Directory $DIR has free space $DIR_HOST_FREE_K K"
	SEG=`$BASENAME $DIR`
	((DIR_HOST_USED_K=${DIR_HOST_USED_B}/1024))
	LOG_MSG "[INFO][$INST_COUNT]:-Host $DIR_HOST Directory $DIR Database $DUMP_DATABASE has used $DIR_HOST_USED_K K"
	if [ $ZIP_STATE -eq 1 ];then
		CALC_COMPRESSION
	else
		CALC_NO_COMPRESSION
		if [ $TRY_ZIP -eq 1 ];then
			LOG_MSG "[INFO][$INST_COUNT]:-Host $DIR_HOST Directory $DIR insufficient space"
			LOG_MSG "[INFO][$INST_COUNT]:-Checking for compressed dump space"
			CALC_COMPRESSION
		fi
	fi
	LOG_MSG "[INFO][$INST_COUNT]:-End Function $FUNCNAME"
}

CHK_DUMP_DIR () {
	LOG_MSG "[INFO][$INST_COUNT]:-Start Function $FUNCNAME"
	CHK_DIR ${DIR}/$DUMP_SUB_DIR $DIR_HOST
	if [ $EXISTS -ne 0 ];then
		LOG_MSG "[INFO][$INST_COUNT]:-Directory ${DIR}/$DUMP_SUB_DIR not found on $DIR_HOST, will try to create"
		$TRUSTED_SHELL $DIR_HOST "$MKDIR -p ${DIR}/$DUMP_SUB_DIR"
		RETVAL=$?
		if [ $RETVAL -ne 0 ];then
			LOG_MSG "[FATAL][$INST_COUNT]:-Failed to $MKDIR ${DIR}/$DUMP_SUB_DIR on $DIR_HOST"
			QE_LINE="${QE_LINE}:FAILED_MKDIR"
			return
		else
			LOG_MSG "[INFO][$INST_COUNT]:-Made ${DIR}/$DUMP_SUB_DIR on $DIR_HOST"
		fi
	else
		LOG_MSG "[INFO][$INST_COUNT]:-Directory ${DIR}/$DUMP_SUB_DIR exists on $DIR_HOST"
	fi
	$TRUSTED_SHELL $DIR_HOST "$TOUCH ${DIR}/$DUMP_SUB_DIR/tmp_file_test"
	RETVAL=$?
	if [ $RETVAL -ne 0 ];then
		LOG_MSG "[FATAL][$INST_COUNT]:-Cannot write to ${DIR}/$DUMP_SUB_DIR exists on $DIR_HOST"
		QE_LINE="${QE_LINE}:FAILED_WRITE"
		return
	else
		$TRUSTED_SHELL $DIR_HOST "$RM -f ${DIR}/$DUMP_SUB_DIR/tmp_file_test"
		QE_LINE="${QE_LINE}:PASSED"
	fi
	LOG_MSG "[INFO][$INST_COUNT]:-End Function $FUNCNAME"
}

CHK_POST_DUMP () {
	LOG_MSG "[INFO][$INST_COUNT]:-Start Function $FUNCNAME"
	EXISTS=`$TRUSTED_SHELL $DIR_HOST "if [ -f $DIR/$DUMP_SUB_DIR/$DUMP_DATE/gp_dump_status_?_${DBID}_$TIMESTAMP_KEY  ];then $ECHO 0;else $ECHO 1;fi"`
	if [ $EXISTS -eq 1 ];then
		LOG_MSG "[WARN][$INST_COUNT]:-No Status file in $DIR/$DUMP_SUB_DIR/$DUMP_DATE on $DIR_HOST"
		QE_LINE="${QE_LINE}:NO_STATUS_FILE"
	else
		LOG_MSG "[INFO][$INST_COUNT]:-Located status file in $DIR/$DUMP_SUB_DIR/$DUMP_DATE on $DIR_HOST"
		LOG_MSG "[INFO][$INST_COUNT]:-Checking for errors"
		STATUS=`$TRUSTED_SHELL $DIR_HOST "$GREP -c \"Finished successfully\" $DIR/$DUMP_SUB_DIR/$DUMP_DATE/gp_dump_status_?_${DBID}_$TIMESTAMP_KEY"`
		if [ $STATUS -eq 1 -o $STATUS -eq 2 ];then
			LOG_MSG "[INFO][$INST_COUNT]:-Status report file indicates dump successful"
			QE_LINE="${QE_LINE}:STATUS_FILE_OK"
		else
			LOG_MSG "[WARN][$INST_COUNT]:-Status report file indicates dump failed"
			$TRUSTED_SHELL $DIR_HOST "$CAT $DIR/$DUMP_SUB_DIR/$DUMP_DATE/gp_dump_status_?_${DBID}_$TIMESTAMP_KEY" >> $LOG_FILE
			LOG_MSG "[INFO][$INST_COUNT]:-Status file contents dumped to log file"
			QE_LINE="${QE_LINE}:STATUS_FILE_ERROR"
		fi
	fi
	LOG_MSG "[INFO][$INST_COUNT]:-Checking for dump file in $DIR/$DUMP_SUB_DIR/$DUMP_DATE on $DIR_HOST"
	EXISTS=`$TRUSTED_SHELL $DIR_HOST "if [ -f $DIR/$DUMP_SUB_DIR/$DUMP_DATE/gp_dump_?_${DBID}_$TIMESTAMP_KEY -o -f $DIR/$DUMP_SUB_DIR/$DUMP_DATE/gp_dump_?_${DBID}_${TIMESTAMP_KEY}.gz ];then $ECHO 0;else $ECHO 1;fi"`
	if [ $EXISTS -eq 1 ];then
		LOG_MSG "[WARN][$INST_COUNT]:-Could not locate dump file in $DIR/$DUMP_SUB_DIR/$DUMP_DATE on $DIR_HOST"
		QE_LINE="${QE_LINE}:NO_DUMP_FILE"
	else
		LOG_MSG "[INFO][$INST_COUNT]:-Located dump file in $DIR/$DUMP_SUB_DIR/$DUMP_DATE on $DIR_HOST"
		QE_LINE="${QE_LINE}:DUMP_FILE"
	fi
	LOG_MSG "[INFO][$INST_COUNT]:-End Function $FUNCNAME"
}
#******************************************************************************
# Main Section
#******************************************************************************
trap '$ECHO "KILLED|${QE_LINE}" >> $PARALLEL_STATUS_FILE;ERROR_EXIT "[FATAL]:-[$INST_COUNT]-Recieved INT or TERM signal" 2' INT TERM
while getopts ":v'?'" opt
	do
	case $opt in
		v ) VERSION_INFO ;;
		'?' ) USAGE ;;
		* ) USAGE 
	esac
done
#Now process supplied call parameters
PARENT_PID=$1;shift		#PID of gpstate process calling this script
CHK_CALL
TYPE=$1;shift
case $TYPE in
	1 )
		DIR_HOST=$1;shift
		QE_LINE="${DIR_HOST}"
		DIR=$1;shift
		DIR_HOST_USED_B=$1;shift
		INST_COUNT=$1;shift
		LOG_FILE=$1;shift
		ZIP_STATE=$1;shift
		FREE_SPACE_PERCENT=$1;shift
		COMPRESSION_FACTOR=$1;shift
		CHK_SEG_DUMP_SPACE ;;
	2 )
		DIR_HOST=$1;shift
		QE_LINE="${DIR_HOST}"
		DIR=$1;shift
		DUMP_SUB_DIR=$1;shift
		INST_COUNT=$1;shift
		LOG_FILE=$1;shift
		CHK_DUMP_DIR ;;
	3 )
		DIR_HOST=$1;shift
		QE_LINE="${DIR_HOST}"
		DIR=$1;shift
		DUMP_SUB_DIR=$1;shift
		DUMP_DATE=$1;shift
		DBID=$1;shift
		TIMESTAMP_KEY=$1;shift
		INST_COUNT=$1;shift
		LOG_FILE=$1;shift
		CHK_POST_DUMP ;;
esac
LOG_MSG "[INFO][$INST_COUNT]:-Start Main"
$ECHO "COMPLETED:${QE_LINE}" >> $PARALLEL_STATUS_FILE
LOG_MSG "[INFO][$INST_COUNT]:-End Main"
exit $EXIT_STATUS
