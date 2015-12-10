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

#Check that SHELL is /bin/bash
#if [ $SHELL != /bin/bash ] && [ `ls -al /bin/sh|grep -c bash` -ne 1 ];then
#    echo "[FATAL]:-Scripts must be run by a user account that has SHELL=/bin/bash"
#    if [ -f /bin/bash ];then
#        echo "[INFO]:-/bin/bash exists, please update user account shell"
#    else
#        echo "[WARN]:-/bin/bash does not exist, does bash need to be installed?"
#    fi
#    exit 2
#fi

declare -a CMDPATH
CMDPATH=(/usr/kerberos/bin /usr/sfw/bin /opt/sfw/bin /usr/local/bin /bin /usr/bin /sbin /usr/sbin /usr/ucb /sw/bin)

findCmdInPath() {
        cmdtofind=$1

        if [ $cmdtofind = 'awk' ] && [ `uname` = SunOS ]; then
            if [ -f "/usr/xpg4/bin/awk" ]; then
                CMD=/usr/xpg4/bin/awk
                echo $CMD
                return
            else
                echo $cmdtofind
                return "Problem in hawq_bash_functions, command '/usr/xpg4/bin/awk' not found. \
                        You will need to edit the script named hawq_bash_functions.sh to \
                        properly locate the needed commands for your platform."
            fi
        fi
        for pathel in ${CMDPATH[@]}
                do
                CMD=$pathel/$cmdtofind
                if [ x"$CMD" != x"" ] && [ -f $CMD ]; then
                        echo $CMD
                        return
                fi
        done
        echo $cmdtofind
        return "Problem in hawq_bash_functions, command '$cmdtofind' not found in COMMAND path. \
                You will need to edit the script named hawq_bash_functions.sh to properly locate \
                the needed commands for your platform."
}

AWK=`findCmdInPath awk`
BASENAME=`findCmdInPath basename`
CAT=`findCmdInPath cat`
CLEAR=`findCmdInPath clear`
CKSUM=`findCmdInPath cksum`
CUT=`findCmdInPath cut`
DATE=`findCmdInPath date`
DD=`findCmdInPath dd`
DIRNAME=`findCmdInPath dirname`
DF=`findCmdInPath df`
DU=`findCmdInPath du`
ECHO=`findCmdInPath echo`
EXPR=`findCmdInPath expr`
FIND=`findCmdInPath find`
TABECHO=$ECHO
PROMPT="$ECHO"
GREP=`findCmdInPath grep`
GZIPCMD=`findCmdInPath gzip`
EGREP=`findCmdInPath egrep`
HEAD=`findCmdInPath head`
HOSTNAME=`findCmdInPath hostname`
IPCS=`findCmdInPath ipcs`
IFCONFIG=`findCmdInPath ifconfig`
KILL=`findCmdInPath kill`
LESSCMD=`findCmdInPath less`
LS=`findCmdInPath ls`
LOCALE=`findCmdInPath locale`
MV=`findCmdInPath mv`
MORECMD=`findCmdInPath more`
MKDIR=`findCmdInPath mkdir`
MKFIFO=`findCmdInPath mkfifo`
NETSTAT=`findCmdInPath netstat`
PING=`findCmdInPath ping`
PS=`findCmdInPath ps`
PYTHON=${GPHOME}/ext/python/bin/python
RM=`findCmdInPath rm`
SCP=`findCmdInPath scp`
SED=`findCmdInPath sed`
SLEEP=`findCmdInPath sleep`
SORT=`findCmdInPath sort`
SPLIT=`findCmdInPath split`
SSH=`findCmdInPath ssh`
TAIL=`findCmdInPath tail`
TAR=`findCmdInPath tar`
TEE=`findCmdInPath tee`
TOUCH=`findCmdInPath touch`
TR=`findCmdInPath tr`
WC=`findCmdInPath wc`
WHICH=`findCmdInPath which`
WHOAMI=`findCmdInPath whoami`
ZCAT=`findCmdInPath zcat`

CALL_HOST=`$HOSTNAME|$CUT -d. -f1`
VERBOSE=0
USER_NAME=`id|$AWK '{print $1}'|$CUT -d"(" -f2|$TR -d ')'`
PROG_NAME=`echo $0 | $TR -d '-'`
PROG_NAME=`$BASENAME $PROG_NAME`
PROG_PIDNAME=`echo $$ $PROG_NAME | awk '{printf "%06d %s\n", $1, $2}'`
LOG_FILE=/tmp/mylog
#DEBUG_LEVEL=1

LOG_MSG () {
        EXIT_STATUS=0
        TIME=`$DATE +%H":"%M":"%S`
        CUR_DATE=`$DATE +%Y%m%d`
        DISPLAY_TXT=0
        #Check to see if we need to update value of EXIT_STATUS
        if [ `$ECHO $1|$AWK -F"]" '{print $1}'|$TR -d '\133'|$GREP -c "WARN"` -eq 1 ];then
            EXIT_STATUS=1
        fi
        if [ `$ECHO $1|$AWK -F"]" '{print $1}'|$TR -d '\133'|$GREP -c "FATAL"` -eq 1 ];then
            EXIT_STATUS=2
        fi
        if [ `$ECHO $1|$AWK -F"]" '{print $1}'|$TR -d '\133'|$GREP -c "ERROR"` -eq 1 ];then
            EXIT_STATUS=2
        fi
        if [ "$2" == "verbose" ] || [ "$2" == "VERBOSE" ] || [ "$2" == "v" ] || [ "$2" == "V" ]; then
            VERBOSE=1
        fi

        if [ "$VERBOSE" == "1" ]; then
            $ECHO "${CUR_DATE}:${TIME}:${PROG_PIDNAME}:${CALL_HOST}:${USER_NAME}-$1" | $TEE -a $LOG_FILE
        else
            $ECHO "${CUR_DATE}:${TIME}:${PROG_PIDNAME}:${CALL_HOST}:${USER_NAME}-$1" >> $LOG_FILE
        fi
}

