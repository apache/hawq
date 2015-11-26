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
#	Release date:-		Nov 2007
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
TMP_FILE=/tmp/${PROG_NAME}_dump_file_header.$$
PYTHON=${GPHOME}/ext/python/bin/python
#******************************************************************************
# Functions
#******************************************************************************
USAGE () {
		$ECHO
		$ECHO "      `basename $0`"
		$ECHO
		$ECHO "      Script called by gpdbrestore, this should not be run directly"
		exit $EXIT_STATUS
}

CHK_CALL () {
	FILE_PREFIX=`$ECHO $PARALLEL_STATUS_FILE|$CUT -d"." -f1`
	if [ ! -f ${FILE_PREFIX}.$PARENT_PID ];then
		$ECHO "[FATAL]:-Not called from from correct parent program" 
		exit 2
	fi
}

BUILD_HEADER () {
	LOG_MSG "[INFO][$INST_COUNT]:-Start Function $FUNCNAME"
	LOG_MSG "[INFO][$INST_COUNT]:-Building dump file header"
	$TRUSTED_SHELL $QE_NAME "$CAT_CMD ${QE_DIR}/${GP_RESTORE_DIR}/$DUMP_FILE|$HEAD -50 > $TMP_FILE"
	if [ $? -ne 0 ];then
		LOG_MSG "[FATAL][$INST_COUNT]:-Failed to head retore file on $QE_NAME ${QE_DIR}"
		$ECHO "FAILED${QE_LINE}:HEAD_DUMPFILE" >> $PARALLEL_STATUS_FILE
		exit 2
	fi
	#Now strip out the required SET commands
	AWK_LINE="'BEGIN {output=1} {if (\$2==\"search_path\") output=0} {if (output==1) print \$0}'"
	$TRUSTED_SHELL $QE_NAME "$CAT_CMD $TMP_FILE|$AWK $AWK_LINE > ${QE_DIR}/${GP_RESTORE_DIR}/$TAB_FILE"
	if [ $? -ne 0 ];then
		LOG_MSG "[FATAL][$INST_COUNT]:-Failed to strip out SET commands on $QE_NAME ${QE_DIR}"
		$ECHO "FAILED${QE_LINE}:SET_AWK_DUMPFILE"
		exit 2
	fi
	$TRUSTED_SHELL $QE_NAME "$RM -f $TMP_FILE"
	LOG_MSG "[INFO][$INST_COUNT]:-Dump header file build complete"
	LOG_MSG "[INFO][$INST_COUNT]:-End Function $FUNCNAME"
}

BUILD_RESTORE_FILES () {
	LOG_MSG "[INFO][$INST_COUNT]:-Start Function $FUNCNAME"
	OS_TYPE=`uname -s|tr '[A-Z]' '[a-z]'`
	case $OS_TYPE in
		sunos ) AWK=nawk ;;
	esac
	if [ $ZIPPED -eq 0 ];then
		DUMP_FILE="gp_dump_0_${QE_DBID}_${RESTORE_TIMESTAMP}"
		CAT_CMD=$CAT
	else
		DUMP_FILE="gp_dump_0_${QE_DBID}_${RESTORE_TIMESTAMP}.gz"
		CAT_CMD=$ZCAT
	fi
	TAB_FILE="gp_dump_0_${QE_DBID}_${TABLE_TIMESTAMP}"
	CHK_FILE ${QE_DIR}/${GP_RESTORE_DIR}/$DUMP_FILE $QE_NAME
	if [ $EXISTS -ne 0 ];then
		LOG_MSG "[FATAL][$INST_COUNT]:-Unable to locate ${QE_DIR}/${GP_RESTORE_DIR}/$DUMP_FILE on $QE_NAME"	
		$ECHO "FAILED${QE_LINE}:NO_FILE" >> $PARALLEL_STATUS_FILE
		exit 2
	fi	
	#Build the first lines for the dump file
	#BUILD_HEADER
	#Can now start to extact table records
	for TAB_NAME in "${RESTORE_LIST[@]}"
	do
		LOG_MSG "[INFO][$INST_COUNT]:-Processing table recovery $TAB_NAME"
		GET_TABLE_IDENTIFIER "${TAB_NAME}"
		SCHEMA_TXT="${SQL_NAME[0]}"
		QUOTED_SCHEMA_TXT="${SQL_NAME[2]}"
		ESCAPED_QUOTED_SCHEMA_TXT="${QUOTED_SCHEMA_TXT//\"/\\\"}"
		TAB_TXT="${SQL_NAME[1]}"
		QUOTED_TAB_TXT="${SQL_NAME[3]}"
		ESCAPED_QUOTED_TAB_TXT="${QUOTED_TAB_TXT//\"/\\\"}"

		read -d '' PYTHON_CODE <<"END_PYTHON_CODE"
import sys
import re

quoted_schema_text = sys.argv[1] 
quoted_table_text = sys.argv[2]

output = 0
schema = 0
found = 0

search_path_re = re.compile(r"^SET search_path = %s," % quoted_schema_text)
copy_re = re.compile(r"^COPY %s " % quoted_table_text)
copy_end_re = re.compile(r"^\\\\\\.$")

for line in sys.stdin:
    if search_path_re.match(line):
        print line,
        schema=1
    elif schema == 1 and copy_re.match(line):
        found = 1
        output = 1
    elif output == 1 and copy_end_re.match(line):
        print line,
        output = 0
        schema = 0
    if output == 1 and schema == 1:
        print line,

if found == 0:
    print "\\\\echo Selected table not found %s.%s" % (quoted_schema_text, quoted_table_text)
    print "\\\."
    sys.exit(1)
END_PYTHON_CODE

		LOG_MSG "[INFO][$INST_COUNT]:-Commencing data extract for $TAB_NAME on $QE_NAME $QE_DIR"
		$TRUSTED_SHELL $QE_NAME ". $GPHOME/greenplum_path.sh; $CAT_CMD ${QE_DIR}/${GP_RESTORE_DIR}/$DUMP_FILE|$PYTHON -c '$PYTHON_CODE' ${ESCAPED_QUOTED_SCHEMA_TXT} ${ESCAPED_QUOTED_TAB_TXT} >> ${QE_DIR}/${GP_RESTORE_DIR}/$TAB_FILE" >> $LOG_FILE 2>&1
		LOG_MSG "[INFO][$INST_COUNT]:-. $GPHOME/greenplum_path.sh; $CAT_CMD ${QE_DIR}/${GP_RESTORE_DIR}/$DUMP_FILE|$PYTHON -c '$PYTHON_CODE' ${ESCAPED_QUOTED_SCHEMA_TXT} ${ESCAPED_QUOTED_TAB_TXT}" >> $LOG_FILE
		if [ $? -ne 0 ];then
			LOG_MSG "[FATAL][$INST_COUNT]:-Failed to extract data for $TAB_NAME on $QE_NAME $QE_DIR"
			$ECHO "FAILED${QE_LINE}:EXTRACT_DATA_FAIL" >> $PARALLEL_STATUS_FILE
			exit 2
		fi
	done
	LOG_MSG "[INFO][$INST_COUNT]:-End Function $FUNCNAME"
}

CLEAR_FILE () {
	LOG_MSG "[INFO][$INST_COUNT]:-Start Function $FUNCNAME"
	LOG_MSG "[INFO][$INST_COUNT]:-Commencing table dump file deletion"
	TAB_FILE="gp_dump_0_${QE_DBID}_${TABLE_TIMESTAMP}"
	$TRUSTED_SHELL $QE_NAME "$RM -f ${QE_DIR}/${GP_RESTORE_DIR}/$TAB_FILE" >> $LOG_FILE 2>&1
	if [ $? -ne 0 ];then
		LOG_MSG "[FATAL][$INST_COUNT]:-Failed to $RM -f ${QE_DIR}/${GP_RESTORE_DIR}/$TAB_FILE on $QE_NAME"
		$ECHO "FAILED:${QE_LINE}:DELETE_DUMP_FILE" >> $PARALLEL_STATUS_FILE
		exit 2
	fi
		LOG_MSG "[INFO][$INST_COUNT]:-Completed deletion of dump file ${QE_DIR}/${GP_RESTORE_DIR}/$TAB_FILE"
	LOG_MSG "[INFO][$INST_COUNT]:-End Function $FUNCNAME"
}
#******************************************************************************
# Main Section
#******************************************************************************
trap '$ECHO "KILLED:$QE_LINE" >> $PARALLEL_STATUS_FILE;ERROR_EXIT "[FATAL]:-[$INST_COUNT]-Recieved INT or TERM signal" 2' INT TERM
while getopts ":v'?'" opt
	do
	case $opt in
		v ) VERSION_INFO ;;
		'?' ) USAGE ;;
		* ) USAGE 
	esac
done
#Now process supplied call parameters
#Setting APP_PATH
if [ x"" != x"$GPHOME" ];then
        APP_PATH=${GPHOME}/bin
else
        APP_PATH=${BIZHOME}/bin
fi
PARENT_PID=$1;shift		#PID of gpstart process calling this script
CHK_CALL
QE_NAME=$1;shift		#Hostname holding database segment
QE_DIR=$1;shift			#Segment directory
QE_DBID=$1;shift
INST_COUNT=$1;shift		#Unique number for this parallel script, starts at 0
LOG_FILE=$1;shift
RESTORE_TIMESTAMP=$1;shift
TABLE_TIMESTAMP=$1;shift
PARALLEL_STATUS_FILE=$1;shift
GP_RESTORE_DIR=$1;shift
ZIPPED=$1;shift
MODE=$1;shift
oldIFS="${IFS}"
IFS=','
RESTORE_LIST=($1)
IFS="${oldIFS}"
QE_LINE="${QE_NAME}:${QE_DIR}"
LOG_MSG "[INFO][$INST_COUNT]:-Start Main"
LOG_MSG "[INFO][$INST_COUNT]:-Command line options passed to utility = $*"
case $MODE in
	0 ) BUILD_RESTORE_FILES
	    ;;
	1 ) CLEAR_FILE
	    ;;
esac
$ECHO "COMPLETED:$QE_LINE" >> $PARALLEL_STATUS_FILE
LOG_MSG "[INFO][$INST_COUNT]:-End Main"
exit $EXIT_STATUS
