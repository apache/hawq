#!/bin/bash
#	Filename:-		gpcrondump
#	Status:-		Released
#	Author:-		G Coombe
#	Contact:-		gcoombe@greenplum.com
#	Release date:-		March 2006
#	Release stat:-		Greenplum Co internal
#                               Copyright (c) Metapa 2005. All Rights Reserved.
#                               Copyright (c) Greenplum 2005. All Rights Reserved
#	Brief descn:-		Script that can be called from cron or command line to dump a database.
#******************************************************************************
# Update History
#******************************************************************************
# Ver	Date		Who		Update
#******************************************************************************
# Detailed Description
#******************************************************************************
#
#******************************************************************************
# Prep Code
# Source required functions file, this required for script to run
# exit if cannot locate this file. Change location of FUNCTIONS variable
# as required.
WORKDIR=`dirname $0`
FUNCTIONS=$WORKDIR/lib/gp_bash_functions.sh
if [ -f $FUNCTIONS ]; then
		. $FUNCTIONS
else
		echo "[FATAL:]-Cannot source $FUNCTIONS file Script Exits!"
		exit 2
fi
#******************************************************************************
# Location Variables
#******************************************************************************

#******************************************************************************
# Command Variables
#******************************************************************************

#******************************************************************************
# Script Specific Variables
#******************************************************************************
INTERACTIVE=1
VERBOSE=1
TIME=`$DATE +%H":"%M":"%S`
CUR_DATE=`$DATE +%Y%m%d`
DUMP_DATE=`$DATE +%Y%m%d`
PROG_NAME=`$BASENAME $0`
HELP_DOC_NAME=`$ECHO $PROG_NAME|$AWK -F'.' '{print $1}'`_help
EXIT_STATUS=0
DUMP_TYPE=0
CLEAR_DUMPS=0
COMPRESSION=1
FAILED_PRIMARY_COUNT=0
DUMP_SUB_DIR=db_dumps
COMPRESSION_FACTOR=12
FREE_SPACE_PERCENT=10
ROLLBACK=0
RUN_CLEAR_DUMPS=0
CUR_HOST=`$HOSTNAME`
VACUUM_MODE=0 # 1=run before dump starts, 2=run after dump has completed.
TRY_COMPRESSION=0
IS_RUNNING_FLAG=/tmp/${PROG_NAME}_is_running_flag
DUMP_CONFIG=0
DUMP_GLOBAL=0
CATALOG=pg_catalog
RUN_PROG=0
PARALLEL_STATUS_FILE=/tmp/gpcrondump_parallel_status_file.$$
GPSEGCRON=$WORKDIR/lib/gpsegcron.sh
PG_DUMPALL_PREFIX="gp_global_1_1_"
HISTORY_TABLE=public.gpcrondump_history
BACKUPFILEDIR=""
REPORTFILEDIR=""
BACKUPSET=()
DUMP_TABLE_LIST=()
ENCODING=""
OUTPUT_OPTIONS=()
#******************************************************************************
# Functions
#******************************************************************************
#
USAGE () {
	if [ -f ${GPDOCDIR}/$HELP_DOC_NAME ] && [ x"" == x"$SCRIPT_USAGE" ];then
		$LESSCMD ${GPDOCDIR}/$HELP_DOC_NAME
		exit 0
	else
		$ECHO	
		$ECHO "      `basename $0`"
		$ECHO
		$ECHO "      Dumps a Greenplum Database Instance database using gp_dump utility"
		$ECHO
		$ECHO "      Usage:"
		$ECHO "      $0 [OPTIONS]"
		$ECHO
		$ECHO "      General options:"
		$ECHO "      -?, display this help message & exit" 
		$ECHO "      -v, display version information & exit"
		$ECHO
		$ECHO "      Logging options:"
		$ECHO "      -q, quiet mode, do not log progress to screen [default:- verbose output to screen]"
		$ECHO "      -l, <logfile directory> alternative logfile directory [optional]"
		$ECHO "      -a, don't ask to confirm database dump [default:- ask]"
		$ECHO "      -D, set log output to debug level, shows all function calls"
		$ECHO
		$ECHO "      Connection options:"
		$ECHO "      -d, <Master instance directory> name of alternative Master instance to start up [optional]"
		$ECHO
		$ECHO "      Database dump options:"
		if [ x"" == x"$PGDATABASE" ];then
		$ECHO "      -x, database name to dump [mandatory \$PGDATABASE is not set]"
		else
		$ECHO "      -x, database name to dump [default:- $PGDATABASE ]"
		fi
		$ECHO "          For multiple databases, use a comma separated list to -x option"
		$ECHO "          Example -x template1,userdb1,userdb2"
		$ECHO "      -h, Record details of database dump in database table $HISTORY_TABLE in database name"
		$ECHO "          supplied via -x option. Utility will create table if does not exist in database"
		$ECHO "      -c, clear old dump directories [default:- do not clear]"
		$ECHO "          will remove the oldest dump directory except the current dump directory"
		$ECHO "          named $DUMP_DATE if it exists, NOTE:-deletes all dump sets within that directory"
		$ECHO "      -o, clear dump files only, do not run a dump"
		$ECHO "          will remove the oldest dump directory except the current dump directory"
		$ECHO "          named $DUMP_DATE if it exists, NOTE:-deletes all dump sets within that directory"
		$ECHO "      -r, rollback dump files if dump failure detected [default:- no rollback]"
		$ECHO "      -f, % free disk value to reserve when calculating available free space for dump"
		$ECHO "          files [default:- $FREE_SPACE_PERCENT %]"
		$ECHO "      -b, bypass disk space checking [default:- check disk space]"
		$ECHO "      -i, ignore initial parameter check phase [default:- check parameters]"
		$ECHO "      -s, <schema name> dump the schema contained within the database name supplied via -x"
		$ECHO "      -j, run vacuum before dump starts"
		$ECHO "      -k, run vacuum after dump has completed successfully"
		$ECHO "      -g, secure master and segment configuration files postgresql.conf pg_ident.conf and pg_hba.conf"
		$ECHO "          Data will be written in tar format to "
		$ECHO "          ${DUMP_SUB_DIR}/$DUMP_DATE/$CONFIG_BACKUP_FILE directory on this host"  
		$ECHO "      -t, <schema.table1> dump the named table only for the database."
		$ECHO "          Multiple tables can be specified with multiple -t option." 
		$ECHO "      -T, <schema.table1> exclude the named tables from the database dump"
		$ECHO "          Multiple tables can be specified with multiple -T option." 
		$ECHO "      -G, use pg_dumpall to dump global objects, i.e. user accounts"
		$ECHO "          Data will be dumped to ${DUMP_SUB_DIR}/$DUMP_DATE/${PG_DUMPALL_PREFIX}<timestamp>"
		$ECHO "      -C, clean (drop) schema prior to create [default:- do not clean]"
		$ECHO "      -R, <program name> run named program after successful dump completed, include full path name"
		$ECHO "          if program name not in path. Note, program will only be called once if multi database"
		$ECHO "          dump requested"
		$ECHO "      -B, <number> run this batch of state segment processes in parallel [default:- $BATCH_DEFAULT]"
		$ECHO
		$ECHO "      Options controlling the output content:"
		$ECHO "      -E, <encoding> dump the data in encoding"
		$ECHO "      --clean, clean (drop) schema prior to create"
		$ECHO "      --inserts, dump data as INSERT, rather than COPY, commands"
		$ECHO "      --column-inserts, dump data as INSERT, commands with column names"
		$ECHO "      --oids, include OIDS in dump"
		$ECHO "      --no-owner, do not output commands to set object ownership"
		$ECHO "                  in plain text format"
		$ECHO "      --no-privileges, do not dump privileges (grant/revoke)"
		$ECHO "      --use-set-session-authorization"
		$ECHO "                                  use SESSION AUTHORIZATION commands instead of"
		$ECHO "                                  ALTER OWNER commands to set ownsership"
		$ECHO "      Greenplum Database specific options:"
		$ECHO "      -z, do not use compression [default:- use compression]"
		$ECHO "      -u, <BACKUPFILEDIR> directory where backup files are placed [default:- segment data directory]"
		$ECHO "      -y, <REPORTFILEDIR> directory where report file is placed [default:- master segment data directory]"
		$ECHO "      -p, dump all primary segment instances [default]. note: this option is deprecated."
		$ECHO "      -w, <BACKUPSET>, backup set indicator. Individual segdb (must be followed with a list of dbid's"
		$ECHO "          to dump). Master segment is added to the list automatically."
		$ECHO "          For example, script with option -w 2,3,4 will dump segment 1, 2, 3 and 4."
		$ECHO "      --rsyncable, pass the --rsyncable option to gzip, if compression is being used."
		$ECHO
		$ECHO "      Crontab entry (example):"
		$ECHO "      SHELL=/bin/bash"
		$ECHO "      5 0 * * * . \$HOME/.bashrc;\$GPUTIL/gpcrondump -x template1 -aq >> <name of cronout file>" 
		$ECHO "      Set the shell to /bin/bash (default for cron is /bin/sh"
		$ECHO "      Dump the template1 database, start process 5 minutes past midnight on a daily basis"
		$ECHO "      In \$HOME/.bashrc file set GPUTIL=\$GPHOME/bin"
		$ECHO
		$ECHO "      Mail configuration"
		$ECHO "      This utility will send an email to a list of email addresses contained in a file"
		$ECHO "      named mail_contacts. This file can be located in the GPDB super user home directory"
		$ECHO "      or the utility bin directory ${GPHOME}/bin. The format of the file is one email"
		$ECHO "      address per line. If no mail_contacts file is found in either location, a warning message"
		$ECHO "      will be displayed."
		$ECHO
		$ECHO "      Return codes:"
		$ECHO "      0 No problems encountered with requested operation"
		$ECHO "      1 Warning generated, but dump completed"
		$ECHO "      2 Fatal error, dump failed"
		$ECHO
		exit $EXIT_STATUS
	fi
}

UNKNOWN_OPTION () {
		$ECHO
		$ECHO "[ERROR]:-Unknown option $1"
		USAGE
}

CHK_QD_DB_RUNNING () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
		if [  -s $MASTER_DATA_DIRECTORY/$PG_PID ]; then
			LOG_MSG "[INFO]:-$PG_PID file exists on Master" 
		else
			EVENT_MAIL "Report from gpcrondump on host $CUR_HOST [FAILED]" "Failed for database $DUMP_DATABASE database not operational. [Start=$TIME_START End=$TIME_END] Options passed [$OPTIONS_LIST]"
			ERROR_EXIT "[FATAL]:-No Master segment $PG_PID" 2
		fi
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

VACUUM_PROCESS () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	LOG_MSG "[INFO]:-Processing vacuum request for database $DUMP_DATABASE" 1
	$EXPORT_LIB_PATH;$PSQL -p $MASTER_PORT "$DUMP_DATABASE" -c"vacuum;"
	ERROR_CHK $? "complete vacuum on database $DUMP_DATABASE" 1
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

GET_DUMP_LIST () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	case $DUMP_TYPE in
		0) LOG_MSG "[INFO]:-Gathering dbid data for dump type all primary segments" 1
		   DBID_LIST=(`$EXPORT_LIB_PATH;$PSQL -p $MASTER_PORT "$DEFAULTDB" -R" " -A -t -c"select dbid from $GP_PG_VIEW as _gp_pg_view where isprimary='t';"`)  ;;
	    1) LOG_MSG "[INFO]:-Gathering dbid data for dump type selected segments" 1
		   DBID_LIST=()
		   for seg in ${BACKUPSET[@]}
		   do
		       DBID_LIST=(${DBID_LIST[@]} $seg)
	           done ;;
	esac
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

GET_DUMP_DIR () {
        LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	if [ x"" != x"$BACKUPFILEDIR" ]; then
	    #DIR="$BACKUPFILEDIR/`$BASENAME $1`"
	    DIR=$BACKUPFILEDIR
	else
	    DIR=$1
	fi
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

CHK_DATA () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
		CHK_DIR_TARGET=`$EXPORT_LIB_PATH;$PSQL -p $MASTER_PORT "$DEFAULTDB" -R" " -A -t -c"select hostname, datadir, port from $CONFIG_TABLE a where dbid=${DBID};"`
		ERROR_CHK $? "obtained host datadir and port for DBID $DBID" 2
		DIR_HOST=`$ECHO $CHK_DIR_TARGET|$CUT -d"|" -f1`
		DIR=`$ECHO $CHK_DIR_TARGET|$CUT -d"|" -f2`
		DIR_DATA=$DIR
		DIR_PORT=`$ECHO $CHK_DIR_TARGET|$CUT -d"|" -f3`
		GET_DUMP_DIR $DIR
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

CHK_DUMP_DIR () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	LOG_MSG "[INFO]:-Commencing dump directory checks, please wait..." 1
	BATCH_LIMIT=${#DBID_LIST[@]}
	PARALLEL_SETUP  $PARALLEL_STATUS_FILE
	for DBID in ${DBID_LIST[@]}
	do
		CHK_DATA
		if [ $DEBUG_LEVEL -eq 0 ] && [ x"" != x"$VERBOSE" ];then $NOLINE_ECHO ".\c";fi
		$GPSEGCRON $$ 2 $DIR_HOST $DIR $DUMP_SUB_DIR $INST_COUNT $LOG_FILE &
		PARALLEL_COUNT $BATCH_LIMIT $BATCH_DEFAULT
	done	
	FAILED_MKDIR_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "FAILED_MKDIR"`
	FAILED_WRITE_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "FAILED_WRITE"`
	PASSED_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "PASSED"`
	KILLED_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "KILLED"`
	COMPLETED_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "COMPLETED"`
	LOG_MSG "[INFO]:------------------------------------------------" 1
	LOG_MSG "[INFO]:-Parallel dump dir check process exit status" 1
	LOG_MSG "[INFO]:------------------------------------------------" 1
	LOG_MSG "[INFO]:-Total processes marked as completed           = $COMPLETED_COUNT" 1
	LOG_MSG "[INFO]:-Total processes marked as passed              = $PASSED_COUNT" 1
	if [ $KILLED_COUNT -ne 0 ];then
	LOG_MSG "[WARN]:-Total processes marked as killed              = $KILLED_COUNT $WARN_MARK" 1
	else
	LOG_MSG "[INFO]:-Total processes marked as killed              = 0" 1
	fi
	if [ $FAILED_MKDIR_COUNT -ne 0 ];then
	LOG_MSG "[WARN]:-Total processes marked as failed to mkdir     = $FAILED_MKDIR_COUNT $WARN_MARK" 1
	else
	LOG_MSG "[INFO]:-Total processes marked as failed to mkdir     = 0" 1
	fi
	if [ $FAILED_WRITE_COUNT -ne 0 ];then
	LOG_MSG "[WARN]:-Total processes marked as failed dir write    = $FAILED_WRITE_COUNT $WARN_MARK" 1
	else
	LOG_MSG "[INFO]:-Total processes marked as failed dir write    = 0" 1
	fi
	LOG_MSG "[INFO]:------------------------------------------------" 1
	#Now exit if had an error
	if [ $FAILED_MKDIR_COUNT -ne 0 ] || [ $FAILED_WRITE_COUNT -ne 0 ] || [ $KILLED_COUNT -ne 0 ];then
		LOG_MSG "[INFO]:-Dumping status file to log file" 1
		$CAT $PARALLEL_STATUS_FILE >> $LOG_FILE
		$RM -f $PARALLEL_STATUS_FILE
		LOG_MSG "[FATAL]:-Errors during segment dump directory checks, see log file" 1
		ERROR_EXIT "[FATAL]:-Unable to contine" 2
	fi
	$RM -f $PARALLEL_STATUS_FILE
	LOG_MSG "[INFO]:-Completed dump directory checks"
	LOG_MSG "[INFO]:-Checking for $DUMP_SUB_DIR on Master segment"
	GET_DUMP_DIR $MASTER_DATA_DIRECTORY
	CHK_DIR $DIR/$DUMP_SUB_DIR
	if [ $EXISTS -ne 0 ];then
		LOG_MSG "[INFO]:-$DIR/$DUMP_SUB_DIR does not exist, will try to create" 1
		$MKDIR $DIR/$DUMP_SUB_DIR
		ERROR_CHK $? "make dump directory $DIR/$DUMP_SUB_DIR on Master segment" 2
	else
		LOG_MSG "[INFO]:-Directory $DIR/$DUMP_SUB_DIR exists on Master segment"
		$TOUCH $DIR/$DUMP_SUB_DIR/tmp_file_test
		RETVAL=$?
		if [ $RETVAL -ne 0 ];then
			ERROR_EXIT "[FATAL]:-Cannot write to $DIR/$DUMP_SUB_DIR" 2
		else
			$RM -f $DIR/$DUMP_SUB_DIR/tmp_file_test
			LOG_MSG "[INFO]:-Write test passed on $DIR/$DUMP_SUB_DIR"
		fi
	fi
	if [ x"$REPORTFILEDIR" != x"" ] ; then
	    CHK_DIR $REPORTFILEDIR/$DUMP_SUB_DIR
	    if [ $EXISTS -ne 0 ]; then
		LOG_MSG "[INFO]:-$REPORTFILEDIR/$DUMP_SUB_DIR does not exist, will try to create" 1
		$MKDIR $REPORTFILEDIR/$DUMP_SUB_DIR
		ERROR_CHK $? "make dump directory $REPORTFILEDIR/$DUMP_SUB_DIR on Master segment" 2
	    else
		LOG_MSG "[INFO]:-Directory $REPORTFILEDIR/$DUMP_SUB_DIR exists on Master segment"
		$TOUCH $REPORTFILEDIR/$DUMP_SUB_DIR/tmp_file_test
		RETVAL=$?
		if [ $RETVAL -ne 0 ];then
			ERROR_EXIT "[FATAL]:-Cannot write to $REPORTFILEDIR/$DUMP_SUB_DIR" 2
		else
			$RM -f $REPORTFILEDIR/$DUMP_SUB_DIR/tmp_file_test
			LOG_MSG "[INFO]:-Write test passed on $REPORTFILEDIR/$DUMP_SUB_DIR"
		fi
	    fi

	    CHK_DIR $REPORTFILEDIR/$DUMP_SUB_DIR/$DUMP_DATE
	    if [ $EXISTS -ne 0 ]; then
		$MKDIR $REPORTFILEDIR/$DUMP_SUB_DIR/$DUMP_DATE
		ERROR_CHK $? "make dump report directory $REPORTFILEDIR/$DUMP_SUB_DIR on Master segment" 2
	    else
		LOG_MSG "[INFO]:-Report directory $REPORTFILEDIR/$DUMP_SUB_DIR/$DUMP_DATE exists on Master segment"
		$TOUCH $REPORTFILEDIR/$DUMP_SUB_DIR/$DUMP_DATE/tmp_file_test
		RETVAL=$?
		if [ $RETVAL -ne 0 ];then
			ERROR_EXIT "[FATAL]:-Cannot write to report directory $REPORTFILEDIR/$DUMP_SUB_DIR/$DUMP_DATE" 2
		else
			$RM -f $REPORTFILEDIR/$DUMP_SUB_DIR/$DUMP_DATE/tmp_file_test
			LOG_MSG "[INFO]:-Write test passed on $REPORTFILEDIR/$DUMP_SUB_DIR/$DUMP_DATE"
		fi	    
	    fi
	fi
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}
	
CHK_PARAM () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	#Check if have a mirrored system
	CHK_MIRRORS_CONFIGURED
	#Check to see if there are any segment failures
	if [ x"" != x"$DUMP_SCHEMA_NAME" ];then
		LOG_MSG "[INFO]:-Schema option -n $DUMP_SCHEMA_NAME supplied, checking for schema in database $DUMP_DATABASE" 1
		SCHEMA_COUNT=`$EXPORT_LIB_PATH;$PSQL -p $MASTER_PORT "$DUMP_DATABASE" -R" " -A -t -c"select count(*) from pg_namespace where nspname='${DUMP_SCHEMA_NAME}';"`
		if [ $SCHEMA_COUNT -eq 0 ];then
			ERROR_EXIT "[FATAL]:-Schema $DUMP_SCHEMA_NAME does not exist in database $DUMP_DATABASE" 2
		fi
	fi
	FAILED_COUNT=`$EXPORT_LIB_PATH;$PSQL -p $MASTER_PORT "$DEFAULTDB" -R" " -A -t -c"select count(*) from $GP_PG_VIEW as _gp_pg_view where isprimary='t' and valid='f';"`
	if [ $FAILED_COUNT -eq 0 ];then
		LOG_MSG "[INFO]:-No failed primary segment instances detected" 1
	else
		LOG_MSG "[WARN]:-Failed primary segment instances detected, analyzing dump options" 1
		FAILED_PRIMARY_COUNT=`$EXPORT_LIB_PATH;$PSQL -p $MASTER_PORT "$DEFAULTDB" -R" " -A -t -c"select count(*) from $GP_PG_VIEW as _gp_pg_view where content <> -1 and definedprimary='t' and valid='f';"`
		ERROR_CHK $? "obtained failed primary count" 2
		if [ $DUMP_TYPE -eq 1 ]; then
		    for dbid in ${BACKUPSET[@]}
		    do
			VALID=`$EXPORT_LIB_PATH;$PSQL -p $MASTER_PORT "$DEFAULTDB" -R" " -A -t -c"select valid from $GP_PG_VIEW as _gp_pg_view where dbid=$dbid;"`
			ERROR_CHK $? "obtained failed segment valid with dbid=$dbid" 2
			if [ x$VALID == x"f" ] ; then
			    ERROR_EXIT "[FATAL]:-Detected failed segment with dbid=$dbid." 1
			fi
		    done
		fi
	fi
		GET_DUMP_LIST	
		CHK_DUMP_DIR
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

CALC_COMPRESSION () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	((REQ_FREE_SPACE_K=$DIR_HOST_USED_K/$COMPRESSION_FACTOR))
	((CALC1=$DIR_HOST_FREE_K*$FREE_SPACE_PERCENT))
	((CALC2=$CALC1/100))
	((CALC_FREE_SPACE_K=$DIR_HOST_FREE_K-$CALC2))
	LOG_MSG "[INFO]:-Revised space less $FREE_SPACE_PERCENT % = $CALC_FREE_SPACE_K K Required $REQ_FREE_SPACE_K K (Estimated size for compressed file)"
	if [ $CALC_FREE_SPACE_K -lt $REQ_FREE_SPACE_K ];then
		ERROR_EXIT "[FATAL]:-Insufficient free space $DIR_HOST $DIR required $REQ_FREE_SPACE_K K actual $CALC_FREE_SPACE_K K" 2
	else
		LOG_MSG "[INFO]:-Sufficient free space on $DIR_HOST for dump in segment directory $DIR"
	fi
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

CALC_NO_COMPRESSION () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	REQ_FREE_SPACE_K=$DIR_HOST_USED_K
	((CALC1=$DIR_HOST_FREE_K*$FREE_SPACE_PERCENT))
	((CALC2=$CALC1/100))
	((CALC_FREE_SPACE_K=$DIR_HOST_FREE_K-$CALC2))
	LOG_MSG "[INFO]:-Revised space less $FREE_SPACE_PERCENT % = $CALC_FREE_SPACE_K K Required $REQ_FREE_SPACE_K K "
	if [ $CALC_FREE_SPACE_K -lt $REQ_FREE_SPACE_K ];then
		LOG_MSG "[WARN]:-Insufficient free space $DIR_HOST $DIR required $REQ_FREE_SPACE_K K actual $CALC_FREE_SPACE_K K"
		EXIT_STATUS=1
		TRY_COMPRESSION=1
	else
		LOG_MSG "[INFO]:-Sufficient free space on $DIR_HOST for dump in segment directory $DIR"
		TRY_COMPRESSION=0
	fi
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}
CHK_DUMP_SPACE () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	LOG_MSG "[INFO]:-Commencing free disk space checks, please wait..." 1
	#Check to see if we have sufficient space to be able to dump data to disks
	if [ ${#DUMP_TABLE_LIST[@]} -eq 0 ];then
		LOG_MSG "[INFO]:-Obtaining $DUMP_DATABASE disk allocation" 1
		ACTUAL_DB_SIZE=(`$GPSIZECALC -p -x "$DUMP_DATABASE" -fq|$GREP "|"`)
	else
		LOG_MSG "[INFO]:-Obtaining specific table disk allocations, please wait..." 1
		ACTUAL_DB_SIZE=0
		for DUMP_TABLE_NAME in "${DUMP_TABLE_LIST[@]}"
		do
		    SZ=(`$GPSIZECALC -p -a "$DUMP_DATABASE" -t $DUMP_TABLE_NAME -fq|$GREP "|"`)
		    ACTUAL_DB_SIZE=$ACTUAL_DB_SIZE+$SZ
		done
	fi
	LOG_MSG "[INFO]:-Checking free space on segment instances"	
	BATCH_LIMIT=${#DBID_LIST[@]}
	PARALLEL_SETUP  $PARALLEL_STATUS_FILE
	for DBID in ${DBID_LIST[@]}
	do
		if [ $DEBUG_LEVEL -eq 0 ] && [ x"" != x"$VERBOSE" ];then $NOLINE_ECHO ".\c";fi
		if [ $COMPRESSION ];then
			ZIP_STATE=1
		else
			ZIP_STATE=0			
		fi 
		CHK_DATA
		SEG=`$BASENAME $DIR`
		DIR_HOST_USED_B=`$ECHO ${ACTUAL_DB_SIZE[@]}|$TR ' ' '\n'|$TR '|' '~'|$GREP "${DIR_HOST}~${SEG}~${DIR_PORT}~"|$TR '~' '|'|$AWK -F'|' '{print $NF}'`
		$GPSEGCRON $$ 1 $DIR_HOST $DIR $DIR_HOST_USED_B $INST_COUNT $LOG_FILE $ZIP_STATE $FREE_SPACE_PERCENT $COMPRESSION_FACTOR &
		PARALLEL_COUNT $BATCH_LIMIT $BATCH_DEFAULT
	done
	#Now check status file
	NEED_COMPRESSION_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "NEED_COMPRESSION"`
	FAILED_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "FAILED"`
	NO_SPACE_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "NO_SPACE"`
	KILLED_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "KILLED"`
	COMPLETED_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "COMPLETED"`
	SPACE_OK_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "SPACE_OK"`
	LOG_MSG "[INFO]:------------------------------------------------" 1
	LOG_MSG "[INFO]:-Parallel space exit status" 1
	LOG_MSG "[INFO]:------------------------------------------------" 1
	LOG_MSG "[INFO]:-Total processes marked as completed           = $COMPLETED_COUNT" 1
	if [ $KILLED_COUNT -ne 0 ];then
	LOG_MSG "[WARN]:-Total processes marked as killed              = $KILLED_COUNT $WARN_MARK" 1
	else
	LOG_MSG "[INFO]:-Total processes marked as killed              = 0" 1
	fi
	if [ $FAILED_COUNT -ne 0 ];then
	LOG_MSG "[WARN]:-Total processes marked as failed              = $FAILED_COUNT $WARN_MARK" 1
	else
	LOG_MSG "[INFO]:-Total processes marked as failed              = 0" 1
	fi
	if [ $NO_SPACE_COUNT -ne 0 ];then
	LOG_MSG "[WARN]:-Total insufficient dump space reports         = $NO_SPACE_COUNT $WARN_MARK" 1
	else
	LOG_MSG "[INFO]:-Total insufficient dump space reports         = 0" 1
	fi
	if [ $NEED_COMPRESSION_COUNT -ne 0 ];then
	LOG_MSG "[WARN]:-Total requests for dump file compression      = $NEED_COMPRESSION_COUNT $WARN_MARK" 1
	COMPRESSION=1	
	else
	LOG_MSG "[INFO]:-Total requests for dump file compression      = 0" 1
	fi
	LOG_MSG "[INFO]:------------------------------------------------" 1
	#Now see if able to continue
	if [ $FAILED_COUNT -ne 0 ] || [ $NO_SPACE_COUNT -ne 0 ] || [ $KILLED_COUNT -ne 0 ];then
		LOG_MSG "[INFO]:-Dumping status file to log file" 1
		$CAT $PARALLEL_STATUS_FILE >> $LOG_FILE
		$RM -f $PARALLEL_STATUS_FILE
		ERROR_EXIT "[FATAL]:-Unable to continue, see log file" 2
	fi
	$RM -f $PARALLEL_STATUS_FILE
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

SET_DUMP_TYPE_TEXT () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	case $DUMP_TYPE in
		0 ) TYPE="Primary segments" ;;
		1 ) TYPE="Selected segments only" ;;
	esac
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

SET_VAC_TXT () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	case $VACUUM_MODE in
		0) VAC_TXT=Off ;;
		1) VAC_TXT="Before database dump" ;;
		2) VAC_TXT="After database dump" ;;
	esac
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

PROMPT_CONTINUE () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
		LOG_MSG "[INFO]:------------------------------------------------" 1
		LOG_MSG "[INFO]:-Master Greenplum Instance dump parameters:" 1
		LOG_MSG "[INFO]:------------------------------------------------" 1
		if [ $MULTIDB -eq 1 ];then
		LOG_MSG "[INFO]:-Dump type                        = Multiple database" 1
		else
		if [ ${#DUMP_TABLE_LIST[@]} -eq 0 ];then
		LOG_MSG "[INFO]:-Dump type                        = Single database" 1
		else
		LOG_MSG "[INFO]:-Dump type                        = Single database specifc table" 1
		LOG_MSG "[INFO]:------------------------------------------------" 1
		LOG_MSG "[INFO]:-Table to be dumped" 1
		LOG_MSG "[INFO]:------------------------------------------------" 1
		for DUMP_TABLE_NAME in "${DUMP_TABLE_LIST[@]}"
		do
		    LOG_MSG "[INFO]:-Table name                       = $DUMP_TABLE_NAME" 1
		done
		LOG_MSG "[INFO]:------------------------------------------------" 1
		fi
		fi
		if [ ${#EXC_DUMP_TABLE_LIST[@]} -gt 0 ];then
		LOG_MSG "[INFO]:------------------------------------------------" 1
		LOG_MSG "[INFO]:-Table exclusion list" 1
		LOG_MSG "[INFO]:------------------------------------------------" 1
		for EXC_NAME in "${EXC_DUMP_TABLE_LIST[@]}"
		do
		LOG_MSG "[INFO]:-Table name                       = $EXC_NAME" 1
		done
		LOG_MSG "[INFO]:------------------------------------------------" 1
		fi
		LOG_MSG "[INFO]:-Database to be dumped            = $DUMP_DATABASE" 1
		if [ x"" != x"$DUMP_SCHEMA_NAME" ];then
		LOG_MSG "[INFO]:-Schema to dump                   = $DUMP_SCHEMA_NAME" 1
		fi
		if [ x"" != x"$BACKUPFILEDIR" ] ; then
		LOG_MSG "[INFO]:-Dump Directory                   = $BACKUPFILEDIR" 1
		fi
		if [ x"" != x"$REPORTFILEDIR" ] ; then
		LOG_MSG "[INFO]:-Dump Report Directory            = $REPORTFILEDIR" 1
		fi		
		LOG_MSG "[INFO]:-Master Port                      = $MASTER_PORT" 1
		LOG_MSG "[INFO]:-Master directory                 = $MASTER_DATA_DIRECTORY" 1
		SET_DUMP_TYPE_TEXT
		LOG_MSG "[INFO]:-Greenplum Instance dump type     = $TYPE" 1
		if [ $DUMP_TYPE -eq 1 ]; then
		    for seg in ${DBID_LIST[@]}
		    do 
			LOG_MSG "[INFO]:-Selected segment to dump = $seg" 1
		    done
		fi
		if [ $RUN_PROG != "0" ];then
		LOG_MSG "[INFO]:-Run post dump program            = $RUN_PROG" 1
		else
		LOG_MSG "[INFO]:-Run post dump program            = Off" 1
		fi
		if [ $FAILED_PRIMARY_COUNT -ne 0 ];then
			LOG_MSG "[WARN]:-Failed primary count             = $FAILED_PRIMARY_COUNT $WARN_MARK" 1
		else
			LOG_MSG "[INFO]:-Failed primary count             = $FAILED_PRIMARY_COUNT" 1
		fi
		if [ $ROLLBACK -eq 0 ];then
			LOG_MSG "[INFO]:-Rollback dumps                   = Off" 1
		else
			LOG_MSG "[INFO]:-Rollback dumps                   = On" 1
		fi
		if [ $COMPRESSION ];then
			LOG_MSG "[INFO]:-Dump file compression            = On" 1
		else
			LOG_MSG "[INFO]:-Dump file compression            = Off" 1
		fi
		if [ $TRY_COMPRESSION -eq 1 ];then
			LOG_MSG "[INFO]:-Compression override             = On" 1
		else
			LOG_MSG "[INFO]:-Compression override             = Off" 1
		fi
		if [ $CLEAR_DUMPS -eq 1 ];then
			LOG_MSG "[INFO]:-Clear old dump files             = On" 1
		else
			LOG_MSG "[INFO]:-Clear old dump files             = Off" 1
		fi	
		if [ x"" = x"$HISTORY" ];then
			LOG_MSG "[INFO]:-Update history table             = Off" 1
		else
			LOG_MSG "[INFO]:-Update history table             = On" 1
		fi
		if [ $DUMP_CONFIG -eq 1 ];then SWITCH_TXT=On;else SWITCH_TXT=Off;fi
		LOG_MSG "[INFO]:-Secure config files              = $SWITCH_TXT" 1
		if [ $DUMP_GLOBAL -eq 1 ];then SWITCH_TXT=On;else SWITCH_TXT=Off;fi
		LOG_MSG "[INFO]:-Dump global objects              = $SWITCH_TXT" 1
		if [ x"" != x"$ADD_DUMP_OPTIONS" ];then
			LOG_MSG "[INFO]:-Additional options               = $ADD_DUMP_OPTIONS" 1
		fi
		SET_VAC_TXT
		LOG_MSG "[INFO]:-Vacuum mode type                 = $VAC_TXT" 1
		LOG_MSG "[INFO]:-Ensure remaining free disk       > $FREE_SPACE_PERCENT %" 1
		$ECHO
		$ECHO "Continue with Greenplum Database system dump yY|nN >"
		read REPLY
		if [ -z $REPLY ]; then
				LOG_MSG "[WARN]:-User abort requested, Script Exits!" 1
				exit 1
		fi
		if [ $REPLY != Y ] && [ $REPLY != y ]; then
				LOG_MSG "[WARN]:-User abort requested, Script Exits!" 1
				exit 1
		fi
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}	

CHK_DUMP_TABLE_LIST () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	LOG_MSG "[INFO]:-Commencing checks of table"
	for DUMP_TABLE_NAME in "${DUMP_TABLE_LIST[@]}"
	do
	if [ `$ECHO $DUMP_TABLE_NAME|$GREP -c "\."` -ne 1 ];then
		ERROR_EXIT "[FATAL]:-No schema name supplied for table $DUMP_TABLE_NAME" 2
	fi
	CHK_EXISTS_TABLE $DUMP_TABLE_NAME $MASTER_PORT $DUMP_DATABASE
	if [ $EXISTS -eq 0 ];then
		LOG_MSG "[INFO]:-Located table $DUMP_TABLE_NAME in $DUMP_DATABASE database"
	else
		ERROR_EXIT "[FATAL]:-Table $DUMP_TABLE_NAME does not exist in $DUMP_DATABASE database" 2
	fi
	if [ x"" != x"$DUMP_SCHEMA_NAME" ];then
		if [ $DUMP_SCHEMA_NAME != `$ECHO $DUMP_TABLE_NAME|$AWK -F"." '{print $1}'` ];then
			ERROR_EXIT "[FATAL]:-Schema name $DUMP_SCHEMA_NAME not same as schema on $DUMP_TABLE_NAME" 2
		fi
	fi
	done
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}
	
CHK_EXC_DUMP_TABLE_LIST () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	NEW_LIST=()
	for EXC_DUMP_NAME in "${EXC_DUMP_TABLE_LIST[@]}"
	do
		if [ `$ECHO $EXC_DUMP_NAME|$GREP -c "\."` -ne 1 ];then
			ERROR_EXIT "[FATAL]:-No schema name supplied for exclude table $EXC_DUMP_NAME" 2
		fi
		CHK_EXISTS_TABLE $EXC_DUMP_NAME $MASTER_PORT $DUMP_DATABASE
		if [ $EXISTS -eq 0 ];then
			LOG_MSG "[INFO]:-Located table $EXC_DUMP_NAME in $DUMP_DATABASE database " 1
			if [ x"" != x"$DUMP_SCHEMA_NAME" ];then
				#Check to see if schema names the same
				if [ `$ECHO $EXC_DUMP_NAME|$AWK -F"." '{print $1}'` == $DUMP_SCHEMA_NAME ];then
					LOG_MSG "[INFO]:-Adding table $EXC_DUMP_NAME to exclude list" 1
					NEW_LIST=(${NEW_LIST[@]} $EXC_DUMP_NAME)
				else
					LOG_MSG "[WARN]:-Schema dump requested and exclude table $EXC_DUMP_NAME not in that schema, ignoring" 1
				fi
			else
				LOG_MSG "[INFO]:-Adding table $EXC_DUMP_NAME to exclude list" 1
				NEW_LIST=(${NEW_LIST[@]} $EXC_DUMP_NAME)
			fi
		else
			 LOG_MSG "[WARN]:-Exclude table $EXC_DUMP_NAME does not exist in $DUMP_DATABASE database, ignoring" 1
		fi
	done
	if [ ${#NEW_LIST[@]} -eq 0 ];then
		LOG_MSG "[WARN]:-All exclude table names have been removed due to issues, see log file" 1
	else
		EXC_DUMP_TABLE_LIST=(${NEW_LIST[@]})
	fi
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

PROCESS_DUMP_CONFIG () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	LOG_MSG "[INFO]:-Securing master configuration files" 1
	GET_DUMP_DIR $MASTER_DATA_DIRECTORY
	CONFIG_BACKUP_FILE=gp_master_config_files_${TIMESTAMP_KEY}.tar
	$TAR cf ${DIR}/${DUMP_SUB_DIR}/${DUMP_DATE}/${CONFIG_BACKUP_FILE} ${MASTER_DATA_DIRECTORY}/*.conf  >> $LOG_FILE 2>&1
	ERROR_CHK $? "dump config files" 1
	LOG_MSG "[INFO]:-Securing segment configuration files" 1
	for DBID in ${DBID_LIST[@]}
	do
	    CONFIG_BACKUP_FILE=gp_segment_config_files_0_${DBID}_${TIMESTAMP_KEY}.tar
	    CHK_DATA
		COMMAND="$TAR cf ${DIR}/${DUMP_SUB_DIR}/${DUMP_DATE}/${CONFIG_BACKUP_FILE} ${DIR_DATA}/*.conf"
		RUN_COMMAND_REMOTE ${DIR_HOST} "$COMMAND"
		RETVAL=$?
		if [ $RETVAL -ne 0 ]; then
			ERROR_EXIT "[FATAL]:- Command $COMMAND on ${DIR_HOST} failed with error status $RETVAL" 2
		fi
	    
	done
	DUMP_CONFIG=0
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

PROCESS_DUMP_GLOBAL () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	LOG_MSG "[INFO]:-Commencing pg_catalog dump for database $DUMP_DATABASE" 1
	GET_DUMP_DIR $MASTER_DATA_DIRECTORY
	$PG_DUMPALL -g --gp-syntax > ${DIR}/${DUMP_SUB_DIR}/${DUMP_DATE}/${PG_DUMPALL_PREFIX}$TIMESTAMP_KEY
	ERROR_CHK $? "dump global objects" 1
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

PROCESS_DUMP () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	LOG_MSG "[INFO]:-Setting dump running flag" 1
	GET_DUMP_DIR $MASTER_DATA_DIRECTORY
	$TOUCH $IS_RUNNING_FLAG
	LOG_MSG "[INFO]:-Building dump command line" 1
	if [ x"" != x"$BACKUPFILEDIR" ] ; then
	    DUMP_DATA_DIR=${BACKUPFILEDIR}/${DUMP_SUB_DIR}/$DUMP_DATE
	else
	    DUMP_DATA_DIR=${DUMP_SUB_DIR}/$DUMP_DATE
	fi
	if [ x"" != x"$REPORTFILEDIR" ] ; then
	    DUMP_REPORT_DIR=${REPORTFILEDIR}/${DUMP_SUB_DIR}/${DUMP_DATE}
	else
	    DUMP_REPORT_DIR=${DIR}/${DUMP_SUB_DIR}/$DUMP_DATE
        fi
	DUMP_BACKUPSET="i[1,`$ECHO ${DBID_LIST[@]}|$TR ' ' ','`]"
	    
	DUMP_TEXT="$GP_DUMP -p $MASTER_PORT -U $LOGNAME --gp-d=${DUMP_DATA_DIR} --gp-r=${DUMP_REPORT_DIR} --gp-s=${DUMP_BACKUPSET} $ADD_DUMP_OPTIONS"		
	if [ $COMPRESSION ];then
		LOG_MSG "[INFO]:-Adding compression parameter" 1
		DUMP_TEXT=$DUMP_TEXT" --gp-c"
	fi
	if [ x"" != x"$ENCODING" ]; then
	    LOG_MSG "[INFO]:-Adding encoding $ENCODING" 1
	    DUMP_TEXT=$DUMP_TEXT" --encoding=$ENCODING"
	fi
	if [ x"" != x"$DUMP_SCHEMA_NAME" ];then
		LOG_MSG "[INFO]:-Adding schema name $DUMP_SCHEMA_NAME" 1
		DUMP_TEXT=$DUMP_TEXT" -n $DUMP_SCHEMA_NAME"
	fi
	DUMP_TEXT=$DUMP_TEXT" $DUMP_DATABASE"
	for DUMP_TABLE_NAME in "${DUMP_TABLE_LIST[@]}"
	do
		DUMP_TEXT=$DUMP_TEXT" --table=${DUMP_TABLE_NAME}"
	done
	for EXC_DUMP_TABLE_NAME in "${EXC_DUMP_TABLE_LIST[@]}"
	do
		DUMP_TEXT=$DUMP_TEXT" --exclude-table=${EXC_DUMP_TABLE_NAME}"
	done
	for opt in ${OUTPUT_OPTIONS[@]}
	do
	    DUMP_TEXT=$DUMP_TEXT" $opt"
	done
	LOG_MSG "[INFO]:-Dump command line $DUMP_TEXT" 1
	LOG_MSG "[INFO]:-Starting dump process" 1
	TIME_START=`$DATE +%H":"%M":"%S`
	$DUMP_TEXT >> $LOG_FILE 2>&1
	RETVAL=$?
	if [ $RETVAL -ne 0 ];then
		LOG_MSG "[WARN]:-Dump process returned exit code $RETVAL" 1
	else
		LOG_MSG "[INFO]:-Dump process returned exit code $RETVAL" 1
	fi
	DUMP_EXIT_STATUS=$RETVAL
	TIME_END=`$DATE +%H":"%M":"%S`
	LOG_MSG "[INFO]:-Deleting dump running flag" 1
	$RM -f $IS_RUNNING_FLAG
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

CHK_POST_DUMP () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	#Get the latest report file name on Master segment instance
	GET_DUMP_DIR $MASTER_DATA_DIRECTORY
	if [ x"" != x"$REPORTFILEDIR" ]; then
	    MASTER_DUMP_RPT=`$LS -altr ${REPORTFILEDIR}/$DUMP_SUB_DIR/$DUMP_DATE/gp_dump_*.rpt|$TAIL -1 |$AWK '{print \$NF}'`
	else
	    MASTER_DUMP_RPT=`$LS -altr ${DIR}/$DUMP_SUB_DIR/$DUMP_DATE/gp_dump_*.rpt|$TAIL -1 |$AWK '{print \$NF}'`
	fi
	if [ x"" == x"$MASTER_DUMP_RPT" ];then
		LOG_MSG "[WARN]:-Could not locate a report file on Master segment dump directory" 1
		EXIT_STATUS=1
		LOG_MSG "[INFO]:-Checking to see if can locate a status file on dbid ${DBID_LIST[1]}" 1
		DBID=${DBID_LIST[1]}
		CHK_DATA
		EXISTS=`$TRUSTED_SHELL $DIR_HOST "if [ \`$LS -1 $DIR/$DUMP_SUB_DIR/$DUMP_DATE/gp_dump_status_?_${DBID}_$DUMP_DATE* | $WC -l\` -gt 0 ] ;then $ECHO 0;else $ECHO 1;fi"`
		if [ $EXISTS -eq 0 ];then
			LOG_MSG "[INFO]:-Located dump status file on $DIR_HOST" 1
			TIMESTAMP_KEY=`$TRUSTED_SHELL $DIR_HOST "$LS $DIR/$DUMP_SUB_DIR/$DUMP_DATE/gp_dump_status_?_${DBID}_$DUMP_DATE*|$CUT -d'_' -f7"`
			LOG_MSG "[INFO]:-Time stamp key = $TIMESTAMP_KEY" 1
		else
			TIMESTAMP_KEY=0
			LOG_MSG "[WARN]:-Failed to locate a dump status file on $DIR_HOST in directory $DIR" 1
		fi
	else	
		LOG_MSG "[INFO]:-Obtained latest dump report name $MASTER_DUMP_RPT" 1
		TIMESTAMP_KEY=`$GREP "Timestamp Key:" $MASTER_DUMP_RPT|$AWK '{print \$NF}'`
		LOG_MSG "[INFO]:-Time stamp key = $TIMESTAMP_KEY" 1
	fi
	CHK_DB_LIST=(1 ${DBID_LIST[@]})
	BATCH_LIMIT=${#CHK_DB_LIST[@]}
	PARALLEL_SETUP  $PARALLEL_STATUS_FILE
	for DBID in ${CHK_DB_LIST[@]}
	do
		CHK_DATA
		if [ $DEBUG_LEVEL -eq 0 ] && [ x"" != x"$VERBOSE" ];then $NOLINE_ECHO ".\c";fi
		$GPSEGCRON $$ 3 $DIR_HOST $DIR $DUMP_SUB_DIR $DUMP_DATE ${DBID} $TIMESTAMP_KEY $INST_COUNT $LOG_FILE &
		PARALLEL_COUNT $BATCH_LIMIT $BATCH_DEFAULT
	done	
	#Now process status file
	POST_ERROR=0
	KILLED_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "KILLED"`
	COMPLETED_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "COMPLETED"`
	NO_STATUS_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "NO_STATUS_FILE"`
	FAILED_STATUS_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "STATUS_FILE_ERROR"`
	OK_STATUS_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "STATUS_FILE_OK"`
	NO_DUMP_FILE_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "NO_DUMP_FILE"`
	DUMP_FILE_COUNT=`$CAT $PARALLEL_STATUS_FILE|$GREP -c "DUMP_FILE"`
	LOG_MSG "[INFO]:------------------------------------------------" 1
	LOG_MSG "[INFO]:-Parallel post dump checks process exit status" 1
	LOG_MSG "[INFO]:------------------------------------------------" 1
	LOG_MSG "[INFO]:-Total processes marked as completed           = $COMPLETED_COUNT" 1
	if [ $KILLED_COUNT -ne 0 ];then
	LOG_MSG "[WARN]:-Total processes marked as killed              = $KILLED_COUNT $WARN_MARK" 1
	else
	LOG_MSG "[INFO]:-Total processes marked as killed              = 0" 1
	fi
	if [ $NO_STATUS_COUNT -ne 0 ];then
	LOG_MSG "[WARN]:-Total processes returned no status file       = $NO_STATUS_COUNT $WARN_MARK" 1
	POST_ERROR=1
	else
	LOG_MSG "[INFO]:-Total processes returned no status file       = 0" 1
	fi
	if [ $FAILED_STATUS_COUNT -ne 0 ];then
	LOG_MSG "[WARN]:-Total processes returned error in status file = $FAILED_STATUS_COUNT $WARN_MARK" 1
	POST_ERROR=1
	else
	LOG_MSG "[INFO]:-Total processes returned error in status file = 0" 1
	fi
	if [ $NO_DUMP_FILE_COUNT -ne 0 ];then
	LOG_MSG "[WARN]:-Total processes returned no dump file         = $NO_DUMP_FILE_COUNT $WARN_MARK" 1
	POST_ERROR=1
	else
	LOG_MSG "[INFO]:-Total processes returned no dump file         = 0" 1
	LOG_MSG "[INFO]:------------------------------------------------" 1
	fi
	if [ $POST_ERROR -ne 0 ];then	
		ROLLBACK_DUMPS=1
		EXIT_STATUS=1
		LOG_MSG "[INFO]:-Dumping contents of status file to log file"
		$CAT  $PARALLEL_STATUS_FILE >> $LOG_FILE
	fi
	$RM -f $PARALLEL_STATUS_FILE
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

DELETE_CURRENT_DUMP () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	for DBID in ${DBID_LIST[@]}
	do
		CHK_DATA
		LOG_MSG "[INFO]:-Deleting dump files on $DIR_HOST segment dir $DIR" 1
		$TRUSTED_SHELL $DIR_HOST "$RM -f $DIR/$DUMP_SUB_DIR/$DUMP_DATE/gp_dump*$TIMESTAMP_KEY*"
	done
	LOG_MSG "[INFO]:-Deleting Master segment dump files" 1
	GET_DUMP_DIR $MASTER_DATA_DIRECTORY
	$RM -f $DIR/$DUMP_SUB_DIR/$DUMP_DATE/gp_dump_1*$TIMESTAMP_KEY*
	$RM -f $DIR/$DUMP_SUB_DIR/$DUMP_DATE/mpp_c*$TIMESTAMP_KEY
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

DELETE_OLD_DUMPS () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	#Get a list of all dbids
	ALL_PRIMARY_DBID_LIST=(`$EXPORT_LIB_PATH;$PSQL -p $MASTER_PORT "$DEFAULTDB" -R" " -A -t -c"select dbid from $GP_PG_VIEW as _gp_pg_view where content<>-1 and isprimary='t';"`)
	COUNT=0
	for DBID in ${ALL_PRIMARY_DBID_LIST[@]}
	do
		CHK_DATA
		LOG_MSG "[INFO]:-Processing $DIR_HOST segment directory $DIR" 1
		DEL_DIR=`$TRUSTED_SHELL $DIR_HOST "$LS $DIR/$DUMP_SUB_DIR|$TR ' ' '\n'|$GREP -v $DUMP_DATE|$SORT|$HEAD -1"`
		if [ x"" == x"$DEL_DIR" ];then
			LOG_MSG "[INFO]:-No previous old dump directories located $DIR_HOST in segment directory $DIR" 1
		else
			DEL_ARRAY[$COUNT]=$DEL_DIR
		fi
	done
			DEL_SET=`$ECHO ${DEL_ARRAY[@]}|$TR ' ' '\n'|$SORT|$HEAD -1`
	if [ x"" != x"$DEL_SET" ];then
		for DBID in ${ALL_PRIMARY_DBID_LIST[@]}
		do
			CHK_DATA
			LOG_MSG "[INFO]:-Preparing to remove $DEL_SET on $DIR_HOST segment directory $DIR" 1
			$TRUSTED_SHELL $DIR_HOST "$RM -Rf $DIR/$DUMP_SUB_DIR/$DEL_SET"
			ERROR_CHK $? "delete $DEL_SET on $DIR_HOST segment directory $DIR" 1
		done
		LOG_MSG "[INFO]:-Processing Master segment instance" 1
		GET_DUMP_DIR $MASTER_DATA_DIRECTORY
		DEL_DIR=$DIR/$DUMP_SUB_DIR/$DEL_SET
		LOG_MSG "[INFO]:-Preparing to remove $DEL_DIR on Master segment instance"	 1
		$RM -Rf $DEL_DIR
		ERROR_CHK $? "delete $DEL_DIR on Master segment instance" 1
	else
		LOG_MSG "[INFO]:-No old backup sets to remove" 1
		DEL_SET="None Located"
	fi
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

STATUS_REPORT () {
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
	LOG_MSG "[INFO]:-Dump status report" 1
	LOG_MSG "[INFO]:------------------------------------------------" 1
	LOG_MSG "[INFO]:-Target database                    = $DUMP_DATABASE" 1
	LOG_MSG "[INFO]:-Dump sub directory                 = $DUMP_DATE" 1
	if [ $CLEAR_DUMPS -eq 1 ];then
	LOG_MSG "[INFO]:-Clear old dump directories         = On" 1
	LOG_MSG "[INFO]:-Backup set deleted                 = $DEL_SET" 1
	else
	LOG_MSG "[INFO]:-Clear old dump directories         = Off" 1
	fi
	LOG_MSG "[INFO]:-Dump type                          = $TYPE" 1
	LOG_MSG "[INFO]:-Dump start time                    = $TIME_START" 1
	LOG_MSG "[INFO]:-Dump end time                      = $TIME_END" 1
	LOG_MSG "[INFO]:-Number of segments dumped          = ${#DBID_LIST[@]}" 1
	if [ $ROLLBACK_DUMPS ] && [ $ROLLBACK -eq 1 ];then
	LOG_MSG "[WARN]:-Status                             = FAILED, Rollback Called" 1
	LOG_MSG "[INFO]:-See dump log file for errors" 1
	LOG_MSG "[WARN]:-Dump key                           = Not Applicable" 1
	else
		if [ $ROLLBACK_DUMPS ] && [ $ROLLBACK -eq 0 ];then 
		LOG_MSG "[WARN]:-Status                             = FAILED, Rollback Not Called" 1
		LOG_MSG "[INFO]:-See dump log file for errors" 1
		LOG_MSG "[WARN]:-Dump key                           = Not Applicable" 1
		else
		LOG_MSG "[INFO]:-Status                             = COMPLETED" 1
		LOG_MSG "[INFO]:-Dump key                           = $TIMESTAMP_KEY" 1
		fi
	fi
	if [ $COMPRESSION ];then
	LOG_MSG "[INFO]:-Dump file compression              = On" 1
	else
	LOG_MSG "[INFO]:-Dump file compression              = Off" 1
	fi
	SET_VAC_TXT
	LOG_MSG "[INFO]:-Vacuum mode type                   = $VAC_TXT" 1
	if [ $EXIT_STATUS -ne 0 ];then
	LOG_MSG "[WARN]:-Exit code not zero, check log file" 1
	else
	LOG_MSG "[INFO]:-Exit code zero, no warnings generated" 1
	fi
	LOG_MSG "[INFO]:------------------------------------------------" 1
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

UPDATE_HISTORY_TABLE () {
	LOG_MSG "[INFO]:-Start Function $FUNCNAME"
	CREATE_ERROR=0
	CHK_EXISTS_TABLE $HISTORY_TABLE $MASTER_PORT $DUMP_DATABASE
	if [ $EXISTS -eq 1 ];then
		#Need to create the history table
		$EXPORT_LIB_PATH;$PSQL -A -t -q -p  $MASTER_PORT "$DUMP_DATABASE" -c "create table $HISTORY_TABLE \
(rec_date timestamp, start_time char(8), end_time char(8), options text, dump_key varchar(20), dump_exit_status smallint, script_exit_status smallint, exit_text varchar(10)) \
distributed by (rec_date);" >> $LOG_FILE 2>&1
		if [ $? -ne 0 ];then
			LOG_MSG "[WARN]:-Unable to create $HISTORY_TABLE in $DUMP_DATABASE database" 1
			CREATE_ERROR=1
		else
			LOG_MSG "[INFO]:-Created $HISTORY_TABLE in $DUMP_DATABASE database"
		fi
	fi
	if [ $CREATE_ERROR -eq 0 ];then
		#Can update table
		case $EXIT_STATUS in
			0) EXIT_MSG="COMPLETED" ;;
			1) EXIT_MSG="WARNING" ;;
			2) EXIT_MSG="FATAL" ;;
		esac
		$EXPORT_LIB_PATH;$PSQL -A -t -q -p  $MASTER_PORT "$DUMP_DATABASE" -c "insert into $HISTORY_TABLE values \
(now(), '$TIME_START', '$TIME_END', '$OPTIONS_LIST', '$TIMESTAMP_KEY', $DUMP_EXIT_STATUS, $EXIT_STATUS, '$EXIT_MSG');" >> $LOG_FILE 2>&1
		if [ $? -ne 0 ];then
			LOG_MSG "[WARN]:-Failed to insert record into $HISTORY_TABLE in $DUMP_DATABASE database" 1
		else
			LOG_MSG "[INFO]:-Insert dump record into $HISTORY_TABLE in $DUMP_DATABASE database"
		fi
	else
		LOG_MSG "[WARN]:-Unable to insert dump record due to table creation error" 1
	fi
	LOG_MSG "[INFO]:-End Function $FUNCNAME"
}

#******************************************************************************
# Main Section
#******************************************************************************
trap '$RM -f ${IS_RUNNING_FLAG};ERROR_EXIT "[FATAL]:-Recieved INT or TERM signal" 2' INT TERM
if [ $# -ne 0 ]; then
		while getopts ":v'?'ajkbiorqd:x:f:s:pmbczgGC!DR:B:t:T:hl:u:y:w:E:-:" opt;do
			case $opt in
				v ) VERSION_INFO ;;
				'?' ) USAGE ;;
				! ) SCRIPT_USAGE=1
				    USAGE ;;
				d ) MASTER_DATA_DIRECTORY=$OPTARG ;;
				r ) ROLLBACK=1 ;;
				b ) DISK_BYPASS=1 ;;
				i ) CHK_PARAM_BYPASS=1 ;;
				a ) unset INTERACTIVE ;;
				q ) unset VERBOSE ;;
				j ) VACUUM_MODE=1 ;;
				k ) VACUUM_MODE=2 ;;
				p ) DUMP_TYPE=0 ;;
				z ) unset COMPRESSION ;;
				f ) FREE_SPACE_PERCENT=$OPTARG ;;
				c ) CLEAR_DUMPS=1 ;;
				o ) CLEAR_DUMPS_ONLY=1 ;;
				s ) DUMP_SCHEMA_NAME=$OPTARG ;;
				l ) LOG_DIR=$OPTARG ;;
				x ) DUMP_DATABASE=$OPTARG ;;
				g ) DUMP_CONFIG=1 ;;
				G ) DUMP_GLOBAL=1 ;;
				C ) ADD_DUMP_OPTIONS="$ADD_DUMP_OPTIONS -c" ;;
				D ) DEBUG_LEVEL=1 ;;
				R ) RUN_PROG=$OPTARG ;;
				B ) BATCH_DEFAULT=$OPTARG ;;
				t ) DUMP_TABLE_LIST=(${DUMP_TABLE_LIST[@]} $OPTARG) ;;
				T ) EXC_DUMP_TABLE_LIST=(${EXC_DUMP_TABLE_LIST[@]} $OPTARG) ;;
				h ) HISTORY=1 ;;
			    u ) BACKUPFILEDIR=$OPTARG ;;
			    y ) REPORTFILEDIR=$OPTARG ;;
			    w ) BACKUPSET=(`$ECHO $OPTARG|$TR ',' ' '`) 
				    DUMP_TYPE=1 ;;
			    E ) ENCODING=$OPTARG ;;
			    - ) OUTPUT_OPTIONS=(${OUTPUT_OPTIONS[@]} --$OPTARG);;
				* ) USAGE ;;
				esac
		done	
fi

if [ x"$CLEAR_DUMPS_ONLY" == x"1" ]; then
    LOG_MSG "[INFO]:-Start Main"
    LOG_MSG "[INFO]:-Commencing clear old dump files only"
    CHK_QD_DB_RUNNING
    CHK_ON_PASSIVE_STANDBY
    GET_MASTER_PORT $MASTER_DATA_DIRECTORY
    DELETE_OLD_DUMPS
    LOG_MSG "[INFO]:-End Main"
    exit $EXIT_STATUS
fi

for opt in ${OUTPUT_OPTIONS[@]}
do
    case $opt in
#	--data-only ) ;;
	--clean ) ;;
	--inserts ) ;;
	--column-inserts ) ;;
	--oids ) ;;
	--no-owner ) ;;
#	--schema-only ) ;;
	--no-privileges ) ;;
	--use-set-session-authorization ) ;;
	--rsyncable ) ;;
	*) USAGE ;;
    esac
done

#Check to see if we have another dump running
if [ -f $IS_RUNNING_FLAG ];then
	LOG_MSG "[FATAL]:-Located $IS_RUNNING_FLAG file, which indicates another dump in progress" 1
	ERROR_EXIT "[FATAL]:-If dump not running delete $IS_RUNNING_FLAG and restart dump" 2
fi
if [ x"" != x"$LOG_DIR" ];then
	CHK_DIR $LOG_DIR
	if [ $EXISTS -eq 1 ]; then
		LOG_MSG "[WARN]:-Log directory $LOG_DIR does not exist, using default log directory" 1
	else
		LOG_FILE=$LOG_DIR/${PROG_NAME}_${CUR_DATE}.log
	fi
fi
LOG_MSG "[INFO]:-Start Main"
LOG_MSG "[INFO]:-Command line options passed to utility = $*"
if [ x"" != x"$BACKUPFILEDIR" ] && ! [ -d $BACKUPFILEDIR ] ; then
    ERROR_EXIT "[FATAL]:-$BACKUPFILEDIR is not a directory" 2
fi
if [ x"" != x"$REPORTFILEDIR" ] && ! [ -d $REPORTFILEDIR ] ; then
    ERROR_EXIT "[FATAL]:-$REPORTFILEDIR is not a directory" 2
fi
CHK_GPDB_ID
OPTIONS_LIST="$*"
CHK_DB_QD_SET
if [ x"" == x"$DUMP_DATABASE" ] && [ x"" == x"$PGDATABASE" ];then
	ERROR_EXIT "[FATAL]:-Must supply -x <database name to dump>  as \$PGDATABASE is not set" 2
fi
if [ x"" == x"$DUMP_DATABASE" ] && [ x"" != x"$PGDATABASE" ];then
	LOG_MSG "[INFO]:-Setting dump database to value of \$PGDATABASE which is $PGDATABASE" 1
	DUMP_DATABASE=$PGDATABASE
fi
if [ $FREE_SPACE_PERCENT -gt 100 ] || [ $FREE_SPACE_PERCENT -lt 1 ];then
	ERROR_EXIT "[FATAL]:-Invalid value $FREE_SPACE_PERCENT % for  -f option" 2
fi
CHK_QD_DB_RUNNING
CHK_ON_PASSIVE_STANDBY
GET_MASTER_PORT $MASTER_DATA_DIRECTORY
#Check to see if we have multiple databases to dump
if [ `$ECHO $DUMP_DATABASE|$GREP -c ","` -ne 0 ];then
	#Have more than one database to dump
	MULTIDB=1
	if [ x"" != x"$DUMP_SCHEMA_NAME" ];then
		ERROR_EXIT "[FATAL]:-Cannot supply schema name if multiple database dump requested" 2
	fi
	if [ ${#DUMP_TABLE_LIST[@]} -gt 0 ];then
		ERROR_EXIT "[FATAL]:-Cannot supply a table dump list if multiple database dump requested" 2
	fi
	if [ ${#EXC_DUMP_TABLE_LIST[@]} -gt 0 ];then
		ERROR_EXIT "[FATAL]:-Cannot exclude specific tables if multiple database dump requested" 2
	fi
	DUMP_DATABASE_LIST=(`$ECHO $DUMP_DATABASE|$TR ',' ' '`)
	LOG_MSG "[INFO]:-Configuring for multiple database dump" 1
else
	MULTIDB=0
	DUMP_DATABASE_LIST=($DUMP_DATABASE)
	if [ ${#DUMP_TABLE_LIST[@]} -gt 0 ] && [ ${#EXC_DUMP_TABLE_LIST[@]} -gt 0 ];then
		ERROR_EXIT "[FATAL]:-Cannot use -t and -T options at same time" 2
	fi
	if [ ${#DUMP_TABLE_LIST[@]} -gt 0 ];then
		LOG_MSG "[INFO]:-Configuring for single database specific table dump" 1
		CHK_DUMP_TABLE_LIST
	else
		if [ ${#EXC_DUMP_TABLE_LIST[@]} -gt 0 ];then
			LOG_MSG "[INFO]:-Configuring for single database with table exclusion(s) dump" 1
			CHK_EXC_DUMP_TABLE_LIST
		else
			LOG_MSG "[INFO]:-Configuring for single database dump" 1
		fi
	fi
fi
#Check if RUN_PROG has been set
if [ $RUN_PROG != "0" ];then
	#Check to see if the file exists
	which $RUN_PROG >> $LOG_FILE 2>&1
	if [ $? -ne 0 ];then
	#Could not find program in path, see if exists with any path name supplied
		if [ -f $RUN_PROG ];then
			LOG_MSG "[INFO]:-Located $RUN_PROG, will call after dump completed" 1
		else
			LOG_MSG "[WARN]:-Could not locate $RUN_PROG" 1
			RUN_PROG=0
		fi
	else
		LOG_MSG "[INFO]:-Located $RUN_PROG, will call after dump completed" 1
	fi
fi	
#Check that all databases exist
for DUMP_DATABASE in ${DUMP_DATABASE_LIST[@]}
do
	DB_THERE=`$EXPORT_LIB_PATH;$PSQL -p $MASTER_PORT "$DEFAULTDB" -R" " -A -t -c"select oid from pg_database where datname='${DUMP_DATABASE}';"`
	if [ x"" == x"$DB_THERE" ];then
		ERROR_EXIT "[FATAL]:-Database $DUMP_DATABASE does not exist" 2
	fi
done
for DUMP_DATABASE in ${DUMP_DATABASE_LIST[@]}
do
	if [ $EXIT_STATUS -ne 0 ];then
		FINAL_EXIT_STATUS=$EXIT_STATUS
		EXIT_STATUS=0
	else
		FINAL_EXIT_STATUS=0
	fi
	if [ x"" == x"$CHK_PARAM_BYPASS" ];then
		CHK_PARAM
	else
		GET_DUMP_LIST
		LOG_MSG "[WARN]:-Parameter check phase bypassed, -i option supplied" 1
	fi
	if [ x"" == x"$DISK_BYPASS" ];then
		CHK_DUMP_SPACE
	else
		LOG_MSG "[WARN]:-Disk space check bypassed, -b option supplied" 1
		GET_DUMP_LIST
	fi
	if [ $INTERACTIVE ]; then
		PROMPT_CONTINUE
	else
		SET_DUMP_TYPE_TEXT
	fi
	if [ $VACUUM_MODE -eq 1 ];then
		LOG_MSG "[INFO]:-Vacuum before database dump option selected" 1
		VACUUM_PROCESS
	fi
	PROCESS_DUMP
	CHK_POST_DUMP
	if [ $DUMP_GLOBAL -eq 1 ];then
		PROCESS_DUMP_GLOBAL
	fi
	if [ $ROLLBACK_DUMPS ];then
		if [ $ROLLBACK -eq 1 ];then
			LOG_MSG "[WARN]:-Dump request was incomplete, rolling back dump" 1
			DELETE_CURRENT_DUMP
			if [ x"" != x"$HISTORY" ];then UPDATE_HISTORY_TABLE;fi
			EVENT_MAIL "Report from gpcrondump on host $CUR_HOST [FAILED]" "Failed for database $DUMP_DATABASE with return code $EXIT_STATUS dump files rolled back. [Start=$TIME_START End=$TIME_END] Options passed [$OPTIONS_LIST]"
			ERROR_EXIT "[FATAL]:-Dump incomplete, completed rollback" 2
		else
			LOG_MSG "[WARN]:-Dump request was incomplete, not rolling back dump as -r option not supplied" 1
			if [ x"" != x"$HISTORY" ];then UPDATE_HISTORY_TABLE;fi
			EVENT_MAIL "Report from gpcrondump on host $CUR_HOST [FAILED]" "Failed for database $DUMP_DATABASE with return code $EXIT_STATUS dump files not rolled back. [Start=$TIME_START End=$TIME_END] Options passed [$OPTIONS_LIST]"
			ERROR_EXIT "[FATAL]:-Dump incomplete, rollback not processed" 2
		fi
	fi
	if [ $CLEAR_DUMPS -eq 1 ];then
		DELETE_OLD_DUMPS
	fi
	if [ $VACUUM_MODE -eq 2 ];then
		LOG_MSG "[INFO]:-Vacuum after database dump option selected" 1
		VACUUM_PROCESS
	fi
	STATUS_REPORT
	if [ x"" != x"$HISTORY" ];then UPDATE_HISTORY_TABLE;fi
	EVENT_MAIL "Report from gpcrondump on host $CUR_HOST[COMPLETED]" "Completed for database $DUMP_DATABASE with return code $EXIT_STATUS [Start=$TIME_START End=$TIME_END] Options passed [$OPTIONS_LIST]"
done
if [ $DUMP_CONFIG -eq 1 ];then
	PROCESS_DUMP_CONFIG
fi
if [ $RUN_PROG != "0" ];then
	LOG_MSG "[INFO]:-Calling $RUN_PROG" 1
	LOG_MSG "[INFO]:-Program screen output will be directed to $LOG_FILE" 1
	$RUN_PROG >> $LOG_FILE 2>&1
fi
LOG_MSG "[INFO]:-End Main"
exit $FINAL_EXIT_STATUS
