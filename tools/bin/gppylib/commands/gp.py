#!/usr/bin/env python
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

"""
TODO: docs!
"""
import os, pickle, base64, time

from gppylib.gplog import *
from gppylib.db import dbconn
from gppylib.db import catalog
from gppylib import gparray
from base import *
from unix import *
from pygresql import pg
from gppylib import pgconf
from gppylib.utils import writeLinesToFile, createFromSingleHostFile
from hawqpylib.hawqlib import HawqXMLParser


logger = get_default_logger()

#TODO:  need a better way of managing environment variables.
GPHOME=os.environ.get('GPHOME')

#Default timeout for segment start
SEGMENT_TIMEOUT_DEFAULT=600

def check_passive_standby(name,dir):
    path = os.path.join(dir, 'postmaster.opts')
    if os.path.exists(path):
        cmd=FileContainsTerm(name,'gpsync', path)
        cmd.run(validateAfter=False)
        return cmd.contains_term()
    else:
        return 0

def getSyncmasterPID(hostname, datadir):
    cmdStr="ps -ef | grep gpsyncmaster | grep -v grep | awk '{print $2}' | grep \\`cat %s/postmaster.pid | head -1\\` || echo -1" % (datadir)
    name="get gpsyncmaster"
    cmd=Command(name,cmdStr,ctxt=REMOTE,remoteHost=hostname)
    try:
        cmd.run(validateAfter=True)
        sout=cmd.get_results().stdout.lstrip(' ')
        return int(sout.split()[1])
    except:
        return -1

#-----------------------------------------------
class PySync(Command):
    def __init__(self,name,srcDir,dstHost,dstDir,ctxt=LOCAL,remoteHost=None, options=None):
        psync_executable=GPHOME + "/bin/lib/pysync.py"

        # MPP-13617
        if ':' in dstHost and not ']' in dstHost:
            dstHost = '[' + dstHost + ']'

        self.cmdStr="%s %s %s %s:%s" % (psync_executable,
                                        options if options else "",
                                        srcDir,
                                        dstHost,
                                        dstDir)
        Command.__init__(self,name,self.cmdStr,ctxt,remoteHost)


#-----------------------------------------------

class CmdArgs(list):
    """
    Conceptually this is a list of an executable path and executable options
    built in a structured manner with a canonical string representation suitable
    for execution via a shell.

    Examples
    --------

    >>> str(CmdArgs(['foo']).set_verbose(True))
    'foo -v'
    >>> str(CmdArgs(['foo']).set_verbose(False).set_wait_timeout(True,600))
    'foo -w -t 600'

    """

    def __init__(self, l):
        list.__init__(self, l)

    def __str__(self):
        return " ".join(self)

    def set_verbose(self, verbose):
        """
        @param verbose - true if verbose output desired
        """
        if verbose: self.append("-v")
        return self

    def set_wrapper(self, wrapper, args):
        """
        @param wrapper - wrapper executable ultimately passed to pg_ctl
        @param args - wrapper arguments ultimately passed to pg_ctl
        """
        if wrapper:
            self.append("--wrapper=\"%s\"" % wrapper)
            if args:
                self.append("--wrapper-args=\"%s\"" % args)
        return self

    def set_wait_timeout(self, wait, timeout):
        """
        @param wait: true if should wait until operation completes
        @param timeout: number of seconds to wait before giving up
        """
        if wait:
            self.append("-w")
        if timeout:
            self.append("-t")
            self.append(str(timeout))
        return self

    def set_segments(self, segments):
        """
        The reduces the command line length of the gpsegstart.py and other
        commands. There are shell limitations to the length and if there are a
        large number of segments and filespaces this limit can be exceeded.
        Since filespaces are not used by our callers, we remove all but one of them.

        @param segments - segments (from GpArray.getSegmentsByHostName)
        """
        for seg in segments:
            cfg_array = repr(seg).split('|')[0:-1]
            self.append("-D '%s'" % ('|'.join(cfg_array) + '|'))
        return self



class PgCtlStartArgs(CmdArgs):
    """
    Used by MasterStart, SegmentStart to format the pg_ctl command
    to start a backend postmaster.

    Examples
    --------

    >>> a = PgCtlStartArgs("/data1/master/gpseg-1", str(PgCtlBackendOptions(5432, 1, 2)), 123, None, None, True, 600)
    >>> str(a).split(' ') #doctest: +NORMALIZE_WHITESPACE
    ['env', GPERA=123', '$GPHOME/bin/pg_ctl', '-D', '/data1/master/gpseg-1', '-l',
     '/data1/master/gpseg-1/pg_log/startup.log', '-w', '-t', '600',
     '-o', '"', '-p', '5432', '-b', '1', '-z', '2', '--silent-mode=true', '"', 'start']
    """

    def __init__(self, datadir, backend, era, wrapper, args, wait, timeout=None):
        """
        @param datadir: database data directory
        @param backend: backend options string from PgCtlBackendOptions
        @param era: gpdb master execution era
        @param wrapper: wrapper executable for pg_ctl
        @param args: wrapper arguments for pg_ctl
        @param wait: true if pg_ctl should wait until backend starts completely
        @param timeout: number of seconds to wait before giving up
        """

        CmdArgs.__init__(self, [
            "env",			# variables examined by gpkill/gpdebug/etc
            "GPSESSID=0000000000", 	# <- overwritten with gp_session_id to help identify orphans
            "GPERA=%s" % str(era),	# <- master era used to help identify orphans
            "$GPHOME/bin/pg_ctl",
            "-D", str(datadir),
            "-l", "%s/pg_log/startup.log" % datadir,
        ])
        self.set_wrapper(wrapper, args)
        self.set_wait_timeout(wait, timeout)
        self.extend([
            "-o", "\"", str(backend), "\"",
            "start"
        ])


class PgCtlStopArgs(CmdArgs):
    """
    Used by MasterStop, SegmentStop to format the pg_ctl command
    to stop a backend postmaster

    >>> str(PgCtlStopArgs("/data1/master/gpseg-1", "smart", True, 600))
    '$GPHOME/bin/pg_ctl -D /data1/master/gpseg-1 -m smart -w -t 600 stop'

    """

    def __init__(self, datadir, mode, wait, timeout):
        """
        @param datadir: database data directory
        @param mode: shutdown mode (smart, fast, immediate)
        @param wait: true if pg_ctlshould wait for backend to stop
        @param timeout: number of seconds to wait before giving up
        """
        CmdArgs.__init__(self, [
            "$GPHOME/bin/pg_ctl",
            "-D", str(datadir),
            "-m", str(mode),
        ])
        self.set_wait_timeout(wait, timeout)
        self.append("stop")

#-----------------------------------------------
class MasterStop(Command):
    def __init__(self,name,dataDir,mode='smart',timeout=SEGMENT_TIMEOUT_DEFAULT, ctxt=LOCAL,remoteHost=None):
        self.dataDir = dataDir
        self.cmdStr = str( PgCtlStopArgs(dataDir, mode, True, timeout) )
        Command.__init__(self, name, self.cmdStr, ctxt, remoteHost)

    @staticmethod
    def local(name,dataDir):
        cmd=MasterStop(name,dataDir)
        cmd.run(validateAfter=True)

#-----------------------------------------------
class SendFilerepTransitionMessage(Command):

    # see gpmirrortransition.c and primary_mirror_transition_client.h
    TRANSITION_ERRCODE_SUCCESS                             = 0
    TRANSITION_ERRCODE_ERROR_UNSPECIFIED                   = 1
    TRANSITION_ERRCODE_ERROR_SERVER_DID_NOT_RETURN_DATA    = 10
    TRANSITION_ERRCODE_ERROR_PROTOCOL_VIOLATED             = 11
    TRANSITION_ERRCODE_ERROR_HOST_LOOKUP_FAILED            = 12
    TRANSITION_ERRCODE_ERROR_INVALID_ARGUMENT              = 13
    TRANSITION_ERRCODE_ERROR_READING_INPUT                 = 14
    TRANSITION_ERRCODE_ERROR_SOCKET                        = 15

    #
    # note: this should be cleaned up -- there are two hosts involved,
    #   the host on which to run gp_primarymirror, AND the host to pass to gp_primarymirror -h
    #
    # Right now, it uses the same for both which is pretty wrong for anything but a local context.
    #
    def __init__(self, name, inputFile, port=None,ctxt=LOCAL, remoteHost=None, dataDir=None):
        if not remoteHost:
            remoteHost = "localhost"
        self.cmdStr='$GPHOME/bin/gp_primarymirror -h %s -p %s -i %s' % (remoteHost,port,inputFile)
        self.dataDir = dataDir
        Command.__init__(self,name,self.cmdStr,ctxt,remoteHost)

    @staticmethod
    def local(name,inputFile,port=None,remoteHost=None):
        cmd=SendFilerepTransitionMessage(name, inputFile, port, LOCAL, remoteHost)
        cmd.run(validateAfter=True)
        return cmd

    @staticmethod
    def buildTransitionMessageCommand(transitionData, dir, port):
        dbData = transitionData["dbsByPort"][int(port)]
        targetMode = dbData["targetMode"]

        argsArr = []
        argsArr.append(targetMode)
        if targetMode == 'mirror' or targetMode == 'primary':
            mode = dbData["mode"]
            if mode == 'r' and dbData["fullResyncFlag"]:
                # full resync requested, convert 'r' to 'f'
                argsArr.append( 'f' )
            else:
                # otherwise, pass the mode through
                argsArr.append( dbData["mode"])
            argsArr.append( dbData["hostName"])
            argsArr.append( "%d" % dbData["hostPort"])
            argsArr.append( dbData["peerName"])
            argsArr.append( "%d" % dbData["peerPort"])
            argsArr.append( "%d" % dbData["peerPMPort"])

        #
        # write arguments to input file.  We will leave this file around.  It can be useful for debugging
        #
        inputFile = os.path.join( dir, "gp_pmtransition_args" )
        writeLinesToFile(inputFile, argsArr)

        return SendFilerepTransitionMessage("Changing seg at dir %s" % dir, inputFile, port=port, dataDir=dir)

class SendFilerepTransitionStatusMessage(Command):
    def __init__(self, name, msg, dataDir=None, port=None,ctxt=LOCAL, remoteHost=None):
        if not remoteHost:
            remoteHost = "localhost"
        self.cmdStr='$GPHOME/bin/gp_primarymirror -h %s -p %s' % (remoteHost,port)
        self.dataDir = dataDir

        logger.debug("Sending msg %s and cmdStr %s" % (msg, self.cmdStr))

        Command.__init__(self, name, self.cmdStr, ctxt, remoteHost, stdin=msg)

    def unpackSuccessLine(self):
        """
        After run() has been called on this cmd, call this to find the "Success" data in the output

        That line is returned if successful, otherwise None is returned
        """
        res = self.get_results()
        if res.rc != 0:
            logger.warn("Error getting data stdout:\"%s\"  stderr:\"%s\"" % \
                        (res.stdout.replace("\n", " "), res.stderr.replace("\n", " ")))
            return None
        else:
            logger.info("Result: stdout:\"%s\"  stderr:\"%s\"" % \
                        (res.stdout.replace("\n", " "), res.stderr.replace("\n", " ")))

            line = res.stderr
            if line.startswith("Success:"):
                line = line[len("Success:"):]
            return line

#-----------------------------------------------
class SendFilerepVerifyMessage(Command):

    DEFAULT_IGNORE_FILES = [
        'pg_internal.init', 'pgstat.stat', 'pga_hba.conf',
        'pg_ident.conf', 'pg_fsm.cache', 'gp_pmtransitions_args',
        'gp_dump', 'postgresql.conf', 'postmaster.log', 'postmaster.opts',
        'postmaser.pids', 'postgresql.conf.bak', 'core',  'wet_execute.tbl',
        'recovery.done', 'gp_temporary_files_filespace', 'gp_transaction_files_filespace']

    DEFAULT_IGNORE_DIRS = [
        'pgsql_tmp', 'pg_xlog', 'pg_log', 'pg_stat_tmp', 'pg_changetracking', 'pg_verify', 'db_dumps', 'pg_utilitymodedtmredo', 'gpperfmon'
    ]

    def __init__(self, name, host, port, token, full=None, verify_file=None, verify_dir=None,
                 abort=None, suspend=None, resume=None, ignore_dir=None, ignore_file=None,
                 results=None, results_level=None, ctxt=LOCAL, remoteHost=None):
        """
        Sends gp_verify message to backend to either start or get results of a
        mirror verification.
        """

        self.host = host
        self.port = port

        msg_contents = ['gp_verify']

        ## The ordering of the following appends is critical.  Do not rearrange without
        ## an associated change in gp_primarymirror

        # full
        msg_contents.append('true') if full else msg_contents.append('')
        # verify_file
        msg_contents.append(verify_file) if verify_file else msg_contents.append('')
        # verify_dir
        msg_contents.append(verify_dir) if verify_dir else msg_contents.append('')
        # token
        msg_contents.append(token)
        # abort
        msg_contents.append('true') if abort else msg_contents.append('')
        # suspend
        msg_contents.append('true') if suspend else msg_contents.append('')
        # resume
        msg_contents.append('true') if resume else msg_contents.append('')
        # ignore_directory
        ignore_dir_list = SendFilerepVerifyMessage.DEFAULT_IGNORE_DIRS + (ignore_dir.split(',') if ignore_dir else [])
        msg_contents.append(','.join(ignore_dir_list))
        # ignore_file
        ignore_file_list = SendFilerepVerifyMessage.DEFAULT_IGNORE_FILES + (ignore_file.split(',') if ignore_file else [])
        msg_contents.append(','.join(ignore_file_list))
        # resultslevel
        msg_contents.append(str(results_level)) if results_level else msg_contents.append('')

        logger.debug("gp_verify message sent to %s:%s:\n%s" % (host, port, "\n".join(msg_contents)))

        self.cmdStr='$GPHOME/bin/gp_primarymirror -h %s -p %s' % (host, port)
        Command.__init__(self, name, self.cmdStr, ctxt, remoteHost, stdin="\n".join(msg_contents))


#-----------------------------------------------
class SegmentStop(Command):
    def __init__(self, name, dataDir,mode='smart', nowait=False, ctxt=LOCAL,
                 remoteHost=None, timeout=SEGMENT_TIMEOUT_DEFAULT):

        self.cmdStr = str( PgCtlStopArgs(dataDir, mode, not nowait, timeout) )
        Command.__init__(self,name,self.cmdStr,ctxt,remoteHost)

    @staticmethod
    def local(name, dataDir,mode='smart'):
        cmd=SegmentStop(name, dataDir,mode)
        cmd.run(validateAfter=True)
        return cmd

    @staticmethod
    def remote(name, hostname, dataDir, mode='smart'):
        cmd=SegmentStop(name, dataDir, mode, ctxt=REMOTE, remoteHost=hostname)
        cmd.run(validateAfter=True)
        return cmd

#-----------------------------------------------
class SegmentIsShutDown(Command):
    """
    Get the pg_controldata status, and check that it says 'shut down'
    """
    def __init__(self,name,directory,ctxt=LOCAL,remoteHost=None):
        cmdStr = "$GPHOME/bin/pg_controldata %s" % directory
        Command.__init__(self,name,cmdStr,ctxt,remoteHost)

    def is_shutdown(self):
        for key, value in self.results.split_stdout():
            if key == 'Database cluster state':
                return value.strip() == 'shut down'
        return False

    @staticmethod
    def local(name,directory):
        cmd=SegmentIsShutDown(name,directory)
        cmd.run(validateAfter=True)


#
# list of valid segment statuses that can be requested
#

SEGMENT_STATUS_GET_STATUS = "getStatus"

#
# corresponds to a postmaster string value; result is a string object, or None if version could not be fetched
#
SEGMENT_STATUS__GET_VERSION = "getVersion"

#
# corresponds to a postmaster string value; result is a dictionary object, or None if data could not be fetched
#
# dictionary will contain:
#    mode -> string
#    segmentState -> string
#    dataState -> string
#    resyncNumCompleted -> large integer
#    resyncTotalToComplete -> large integer
#    elapsedTimeSeconds -> large integer
#
SEGMENT_STATUS__GET_MIRROR_STATUS = "getMirrorStatus"

#
# fetch the active PID of this segment; result is a dict with "pid" and "error" values
#
# see comments on getPidStatus in GpSegStatusProgram class
#
SEGMENT_STATUS__GET_PID = "__getPid"

#
# fetch True or False depending on whether the /tmp/.s.PSQL.<port>.lock file is there
#
SEGMENT_STATUS__HAS_LOCKFILE = "__hasLockFile"

#
# fetch True or False depending on whether the postmaster pid file is there
#
SEGMENT_STATUS__HAS_POSTMASTER_PID_FILE = "__hasPostmasterPidFile"

class GpGetStatusUsingTransitionArgs(CmdArgs):
    """
    Examples
    --------

    >>> str(GpGetStatusUsingTransitionArgs([],'request'))
    '$GPHOME/sbin/gpgetstatususingtransition.py -s request'
    """

    def __init__(self, segments, status_request):
        """
        @param status_request
        """
        CmdArgs.__init__(self, [
            "$GPHOME/sbin/gpgetstatususingtransition.py",
            "-s", str(status_request)
        ])
        self.set_segments(segments)


SEGSTART_ERROR_UNKNOWN_ERROR = -1
SEGSTART_SUCCESS = 0
SEGSTART_ERROR_MIRRORING_FAILURE = 1
SEGSTART_ERROR_POSTMASTER_DIED = 2
SEGSTART_ERROR_INVALID_STATE_TRANSITION = 3
SEGSTART_ERROR_SERVER_IS_IN_SHUTDOWN = 4
SEGSTART_ERROR_STOP_RUNNING_SEGMENT_FAILED = 5
SEGSTART_ERROR_DATA_DIRECTORY_DOES_NOT_EXIST = 6
SEGSTART_ERROR_SERVER_DID_NOT_RESPOND = 7
SEGSTART_ERROR_PG_CTL_FAILED = 8
SEGSTART_ERROR_CHECKING_CONNECTION_AND_LOCALE_FAILED = 9
SEGSTART_ERROR_PING_FAILED = 10 # not actually done inside GpSegStartCmd, done instead by caller
SEGSTART_ERROR_OTHER = 1000


class GpSegStartArgs(CmdArgs):
    """
    Examples
    --------

    >>> str(GpSegStartArgs('en_US.utf-8:en_US.utf-8:en_US.utf-8', 'mirrorless', 'gpversion', 1, 123, 600))
    "$GPHOME/sbin/gpsegstart.py -C en_US.utf-8:en_US.utf-8:en_US.utf-8 -M mirrorless -V 'gpversion' -n 1 --era 123 -t 600"
    """

    def __init__(self, localeData, mirrormode, gpversion, num_cids, era, timeout):
        """
        @param localeData - string built from ":".join([lc_collate, lc_monetary, lc_numeric]), e.g. gpEnv.getLocaleData()
        @param mirrormode - mirror start mode (START_AS_PRIMARY_OR_MIRROR or START_AS_MIRRORLESS)
        @param gpversion - version (from postgres --gp-version)
        @param num_cids - number content ids
        @param era - master era
        @param timeout - seconds to wait before giving up
        """
        CmdArgs.__init__(self, [
            "$GPHOME/sbin/gpsegstart.py",
            "-C", str(localeData),
            "-M", str(mirrormode),
            "-V '%s'" % gpversion,
            "-n", str(num_cids),
            "--era", str(era),
            "-t", str(timeout)
        ])

    def set_special(self, special):
        """
        @param special - special mode
        """
        assert(special in [None, 'upgrade', 'maintenance'])
        if special:
            self.append("-U")
            self.append(special)
        return self

    def set_transition(self, data):
        """
        @param data - pickled transition data
        """
        if data is not None:
            self.append("-p")
            self.append(data)
        return self



class GpSegStartCmd(Command):
    def __init__(self, name, gphome, segments, localeData, gpversion,
                 mirrormode, numContentsInCluster, era,
                 timeout=SEGMENT_TIMEOUT_DEFAULT, verbose=False,
                 ctxt=LOCAL, remoteHost=None, pickledTransitionData=None,
                 specialMode=None, wrapper=None, wrapper_args=None):

        # Referenced by calling code (in operations/startSegments.py), create a clone
        self.dblist = [x for x in segments]

        # build gpsegstart command string
        c = GpSegStartArgs(localeData, mirrormode, gpversion, numContentsInCluster, era, timeout)
        c.set_verbose(verbose)
        c.set_special(specialMode)
        c.set_transition(pickledTransitionData)
        c.set_wrapper(wrapper, wrapper_args)
        c.set_segments(segments)

        cmdStr = str(c)
        logger.debug(cmdStr)

        Command.__init__(self,name,cmdStr,ctxt,remoteHost)


class GpSegChangeMirrorModeCmd(Command):
    def __init__(self, name, gphome, localeData, gpversion, dbs, targetMode,
                 pickledParams, verbose=False, ctxt=LOCAL, remoteHost=None):
        self.gphome=gphome
        self.dblist=dbs
        self.dirlist=[]
        for db in dbs:
            datadir = db.getSegmentDataDirectory()
            port = db.getSegmentPort()
            self.dirlist.append(datadir + ':' + str(port))

        dirstr=" -D ".join(self.dirlist)
        if verbose:
            setverbose=" -v "
        else:
            setverbose=""

        cmdStr="$GPHOME/sbin/gpsegtoprimaryormirror.py %s -D %s -C %s -M %s -p %s -V '%s'" % \
                (setverbose,dirstr,localeData,targetMode,pickledParams,gpversion)

        Command.__init__(self,name,cmdStr,ctxt,remoteHost)

#-----------------------------------------------
class GpSegStopCmd(Command):
    def __init__(self, name, gphome, version,mode,dbs,timeout=SEGMENT_TIMEOUT_DEFAULT,
                 verbose=False, ctxt=LOCAL, remoteHost=None):
        self.gphome=gphome
        self.dblist=dbs
        self.dirportlist=[]
        self.mode=mode
        self.version=version
        for db in dbs:
            datadir = db.getSegmentDataDirectory()
            port = db.getSegmentPort()
            self.dirportlist.append(datadir + ':' + str(port))
        self.timeout=timeout
        dirstr=" -D ".join(self.dirportlist)
        if verbose:
            setverbose=" -v "
        else:
            setverbose=""

        self.cmdStr="$GPHOME/sbin/gpsegstop.py %s -D %s -m %s -t %s -V '%s'"  %\
                        (setverbose,dirstr,mode,timeout,version)

        Command.__init__(self,name,self.cmdStr,ctxt,remoteHost)

#-----------------------------------------------
class GpInitSystem(Command):
    def __init__(self,name,configFile,hostsFile, ctxt=LOCAL, remoteHost=None):
        self.configFile=configFile
        self.hostsFile=hostsFile
        self.cmdStr="$GPHOME/bin/gpinitsystem -a -c %s -h %s" % (configFile,hostsFile)
        Command.__init__(self, name, self.cmdStr, ctxt, remoteHost)

#-----------------------------------------------
class GpDeleteSystem(Command):
    def __init__(self,name,datadir, ctxt=LOCAL, remoteHost=None):
        self.datadir=datadir
        self.input="y\ny\n"
        self.cmdStr="$GPHOME/bin/gpdeletesystem -d %s -f " % (datadir)
        Command.__init__(self, name, self.cmdStr, ctxt, remoteHost,stdin=self.input)

#-----------------------------------------------
class GpStart(Command):
    def __init__(self, name, masterOnly=False, restricted=False, verbose=False,ctxt=LOCAL, remoteHost=None, upgrade=False):
        self.cmdStr="$GPHOME/bin/gpstart -a"
        if masterOnly:
            self.cmdStr += " -m"
            self.propagate_env_map['GPSTART_INTERNAL_MASTER_ONLY'] = 1
        if restricted:
            self.cmdStr += " -R"
        if verbose or logging_is_verbose():
            self.cmdStr += " -v"
        if upgrade:
            self.cmdStr += " -Uupgrade"
        Command.__init__(self,name,self.cmdStr,ctxt,remoteHost)

    @staticmethod
    def local(name,masterOnly=False,restricted=False):
        cmd=GpStart(name,masterOnly,restricted)
        cmd.run(validateAfter=True)

#-----------------------------------------------
class NewGpStart(Command):
    def __init__(self, name, masterOnly=False, restricted=False, verbose=False,nostandby=False,ctxt=LOCAL, remoteHost=None, masterDirectory=None):
        self.cmdStr="$GPHOME/bin/gpstart -a"
        if masterOnly:
            self.cmdStr += " -m"
            self.propagate_env_map['GPSTART_INTERNAL_MASTER_ONLY'] = 1
        if restricted:
            self.cmdStr += " -R"
        if verbose or logging_is_verbose():
            self.cmdStr += " -v"
        if nostandby:
            self.cmdStr += " -y"
        if masterDirectory:
            self.cmdStr += " -d " + masterDirectory

        Command.__init__(self,name,self.cmdStr,ctxt,remoteHost)

    @staticmethod
    def local(name,masterOnly=False,restricted=False,verbose=False,nostandby=False,
              masterDirectory=None):
        cmd=NewGpStart(name,masterOnly,restricted,verbose,nostandby,
                       masterDirectory=masterDirectory)
        cmd.run(validateAfter=True)

#-----------------------------------------------
class NewGpStop(Command):
    def __init__(self, name, masterOnly=False, restart=False, fast=False, force=False, verbose=False, ctxt=LOCAL, remoteHost=None):
        self.cmdStr="$GPHOME/bin/gpstop -a"
        if masterOnly:
            self.cmdStr += " -m"
        if verbose or logging_is_verbose():
            self.cmdStr += " -v"
        if fast:
            self.cmdStr += " -f"
        if restart:
            self.cmdStr += " -r"
        if force:
            self.cmdStr += " -M immediate"
        Command.__init__(self,name,self.cmdStr,ctxt,remoteHost)

    @staticmethod
    def local(name,masterOnly=False, restart=False, fast=False, force=False, verbose=False):
        cmd=NewGpStop(name,masterOnly,restart, fast, force, verbose)
        cmd.run(validateAfter=True)

#-----------------------------------------------
class GpStop(Command):
    def __init__(self, name, masterOnly=False, verbose=False, quiet=False, restart=False, fast=False, force=False, datadir=None,ctxt=LOCAL, remoteHost=None):
        self.cmdStr="$GPHOME/bin/gpstop -a"
        if masterOnly:
            self.cmdStr += " -m"
        if restart:
            self.cmdStr += " -r"
        if fast:
            self.cmdStr += " -f"
        if force:
            self.cmdStr += " -M immediate"
        if datadir:
            self.cmdStr += " -d %s" % datadir
        if verbose or logging_is_verbose():
            self.cmdStr += " -v"
        if quiet:
            self.cmdStr += " -q"
        Command.__init__(self,name,self.cmdStr,ctxt,remoteHost)

    @staticmethod
    def local(name,masterOnly=False, verbose=False, quiet=False,restart=False, fast=False, force=False, datadir=None):
        cmd=GpStop(name,masterOnly,verbose,quiet,restart,fast,force,datadir)
        cmd.run(validateAfter=True)
        return cmd

#-----------------------------------------------
class GpRecoverseg(Command):
    def __init__(self, name, ctxt=LOCAL, remoteHost=None):
        self.cmdStr = "$GPHOME/bin/gprecoverseg -a"
        Command.__init__(self, name, self.cmdStr, ctxt, remoteHost)

#-----------------------------------------------
class Psql(Command):
    def __init__(self, name, query=None, filename=None, database='template1', port=None, utilityMode=False, ctxt=LOCAL, remoteHost=None):
        env = ''
        if utilityMode:
            env = 'PGOPTIONS="-c gp_session_role=utility"'
        cmdStr = '%s $GPHOME/bin/psql ' % env
        if port is not None:
            cmdStr += '-p %d ' % port
        if query is not None and filename is not None:
            raise Exception('Psql can accept only a query or a filename, not both.')
        elif query is not None:
            cmdStr += '-c "%s" ' % query
        elif filename is not None:
            cmdStr += '-f %s ' % filename
        else:
            raise Exception('Psql must be passed a query or a filename.')
        cmdStr += '%s ' % database
        # Need to escape " for REMOTE or it'll interfere with ssh
        if ctxt == REMOTE:
             cmdStr = cmdStr.replace('"', '\\"')
        self.cmdStr=cmdStr
        Command.__init__(self, name, self.cmdStr, ctxt, remoteHost)

#-----------------------------------------------
class ModifyPostgresqlConfSetting(Command):
    def __init__(self, name, file, optName, optVal, optType='string', ctxt=LOCAL, remoteHost=None):
        cmdStr = None
        if optType == 'number':
            cmdStr = "perl -p -i.bak -e 's/^%s[ ]*=[ ]*\\d+/%s=%d/' %s" % (optName, optName, optVal, file)
        elif optType == 'string':
            cmdStr = "perl -i -p -e \"s/^%s[ ]*=[ ]*'[^']*'/%s='%s'/\" %s" % (optName, optName, optVal, file)
        else:
            raise Exception, "Invalid optType for ModifyPostgresqlConfSetting"
        self.cmdStr = cmdStr
        Command.__init__(self, name, self.cmdStr, ctxt, remoteHost)

#-----------------------------------------------
class GpCleanSegmentDirectories(Command):
    """
    Clean all of the directories for a set of segments on the host.

    Does NOT delete all files in the data directories -- tries to preserve logs and any non-database
       files the user has placed there
    """
    def __init__(self, name, segmentsToClean, ctxt, remoteHost):
        pickledSegmentsStr = base64.urlsafe_b64encode(pickle.dumps(segmentsToClean))
        cmdStr = "$GPHOME/sbin/gpcleansegmentdir.py -p %s" % pickledSegmentsStr
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

#-----------------------------------------------
class GpDumpDirsExist(Command):
    """
    Checks if gp_dump* directories exist in the given directory
    """
    def __init__(self, name, baseDir, ctxt=LOCAL, remoteHost=None):
        cmdStr = "find %s -name '*dump*' -print" % baseDir
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

    @staticmethod
    def local(name, baseDir):
        cmd = GpDumpDirsExist(name, baseDir)
        cmd.run(validateAfter=True)
        dirCount = len(cmd.get_results().stdout.split('\n'))
        # This is > 1 because the command output will terminate with \n
        return dirCount > 1

#-----------------------------------------------
class GpVersion(Command):
    def __init__(self,name,gphome,ctxt=LOCAL,remoteHost=None):
        # XXX this should make use of the gphome that was passed
        # in, but this causes problems in some environments and
        # requires further investigation.

        self.gphome=gphome
        #self.cmdStr="%s/bin/postgres --gp-version" % gphome
        self.cmdStr="$GPHOME/bin/postgres --gp-version"
        Command.__init__(self,name,self.cmdStr,ctxt,remoteHost)

    def get_version(self):
        return self.results.stdout.strip()

    @staticmethod
    def local(name,gphome):
        cmd=GpVersion(name,gphome)
        cmd.run(validateAfter=True)
        return cmd.get_version()

#-----------------------------------------------
class GpCatVersion(Command):
    """
    Get the catalog version of the binaries in a given GPHOME
    """
    def __init__(self,name,gphome,ctxt=LOCAL,remoteHost=None):
        # XXX this should make use of the gphome that was passed
        # in, but this causes problems in some environments and
        # requires further investigation.
        self.gphome=gphome
        #cmdStr="%s/bin/postgres --catalog-version" % gphome
        cmdStr="$GPHOME/bin/postgres --catalog-version"
        Command.__init__(self,name,cmdStr,ctxt,remoteHost)

    def get_version(self):
        # Version comes out like this:
        #   "Catalog version number:               201002021"
        # We only want the number
        return self.results.stdout.split(':')[1].strip()

    @staticmethod
    def local(name,gphome):
        cmd=GpCatVersion(name,gphome)
        cmd.run(validateAfter=True)
        return cmd.get_version()

#-----------------------------------------------
class GpCatVersionDirectory(Command):
    """
    Get the catalog version of a given database directory
    """
    def __init__(self,name,directory,ctxt=LOCAL,remoteHost=None):
        cmdStr = "$GPHOME/bin/pg_controldata %s" % directory
        Command.__init__(self,name,cmdStr,ctxt,remoteHost)

    def get_version(self):
        "sift through pg_controldata looking for the catalog version number"
        for key, value in self.results.split_stdout():
            if key == 'Catalog version number':
                return value.strip()

    @staticmethod
    def local(name,directory):
        cmd=GpCatVersionDirectory(name,directory)
        cmd.run(validateAfter=True)
        return cmd.get_version()

#-----------------------------------------------
class GpSuspendSegmentsOnHost(Command):
    def __init__(self, name, gpconfigstrings, resume, ctxt=LOCAL, remoteHost=None):
        if resume:
            pauseOrResume = "--resume"
        else:
            pauseOrResume = "--pause"

        cmdStr="echo '%s' | $GPHOME/sbin/gpsuspend.py %s" % (gpconfigstrings, pauseOrResume)
        Command.__init__(self,name,cmdStr,ctxt,remoteHost)

#-----------------------------------------------
class GpAddConfigScript(Command):
    def __init__(self, name, directorystring, entry, value=None, removeonly=False, ctxt=LOCAL, remoteHost=None):
        cmdStr="echo '%s' | $GPHOME/sbin/gpaddconfig.py --entry %s" % (directorystring, entry)
        if value:
            # value will be encoded and unencoded in the script to protect against shell interpretation
            value = base64.urlsafe_b64encode(pickle.dumps(value))
            cmdStr = cmdStr + " --value '" + value + "'"
        if removeonly:
            cmdStr = cmdStr + " --removeonly "

        Command.__init__(self,name,cmdStr,ctxt,remoteHost)

#-----------------------------------------------
class GpAppendGucToFile(Command):

    # guc value will come in pickled and base64 encoded

    def __init__(self,name,file,guc,value,ctxt=LOCAL,remoteHost=None):
        unpickledText = pickle.loads(base64.urlsafe_b64decode(value))
        finalText = unpickledText.replace('"', '\\\"')
        cmdStr = 'echo "%s=%s" >> %s' %  (guc, finalText, file)
        Command.__init__(self,name,cmdStr,ctxt,remoteHost)


#-----------------------------------------------
class GpLogFilter(Command):
    def __init__(self, name, filename, start=None, end=None, duration=None,
                 case=None, count=None, search_string=None,
                 exclude_string=None, search_regex=None, exclude_regex=None,
                 trouble=None, ctxt=LOCAL,remoteHost=None):
        cmdfrags = []
        if start:
            cmdfrags.append('--begin=%s' % start)
        if end:
            cmdfrags.append('--end=%s' % end)
        if duration:
            cmdfrags.append('--duration=%s' % duration)
        if case:
            cmdfrags.append('--case=%s' % case)
        if search_string:
            cmdfrags.append('--find=\'%s\'' % search_string)
        if exclude_string:
            cmdfrags.append('--nofind=\'%s\'' % exclude_string)
        if search_regex:
            cmdfrags.append('--match=\'%s\'' % search_regex)
        if count:
            cmdfrags.append('-n %s' % count)
        if exclude_regex:
            cmdfrags.append('--nomatch=\'%s\'' % exclude_regex)
        if trouble:
            cmdfrags.append('-t')
        cmdfrags.append(filename)

        self.cmdStr = "$GPHOME/bin/gplogfilter %s" % ' '.join(cmdfrags)
        Command.__init__(self, name, self.cmdStr, ctxt,remoteHost)

    @staticmethod
    def local(name, filename, start=None, end=None, duration=None,
               case=None, count=None, search_string=None,
               exclude_string=None, search_regex=None, exclude_regex=None,
               trouble=None):
        cmd = GpLogFilter(name, filename, start, end, duration, case, count, search_string,
                          exclude_string, search_regex, exclude_regex, trouble)
        cmd.run(validateAfter=True)
        return "".join(cmd.get_results().stdout).split("\r\n")

#-----------------------------------------------
def distribute_tarball(queue,list,tarball):
        logger.debug("distributeTarBall start")
        for db in list:
            hostname = db.getSegmentHostName()
            datadir = db.getSegmentDataDirectory()
            (head,tail)=os.path.split(datadir)
            scp_cmd=RemoteCopy("copy master",tarball,hostname,head)
            queue.addCommand(scp_cmd)
        queue.join()
        queue.check_results()
        logger.debug("distributeTarBall finished")




class GpError(Exception): pass

######
def get_user():
    logger.debug("Checking if LOGNAME or USER env variable is set.")
    username = os.environ.get('LOGNAME') or os.environ.get('USER')
    if not username:
        raise GpError('Environment Variable LOGNAME or USER not set')
    return username


def get_gphome():
    logger.debug("Checking if GPHOME env variable is set.")
    gphome=os.getenv('GPHOME',None)
    if not gphome:
        raise GpError('Environment Variable GPHOME not set')
    return gphome


######
def get_masterdatadir():
    logger.debug("Checking if GPHOME env variable is set.")
    HAWQ_CONFIG_DIR = os.environ.get('GPHOME')
    if HAWQ_CONFIG_DIR is None:
        raise GpError("Environment Variable GPHOME not set!")
    hawq_site = HawqXMLParser(HAWQ_CONFIG_DIR)
    master_datadir = hawq_site.get_value_from_name('hawq_master_directory').strip()

    return master_datadir

######
def get_master_port():
    logger.debug("Checking if GPHOME env variable is set.")
    HAWQ_CONFIG_DIR = os.environ.get('GPHOME')
    if HAWQ_CONFIG_DIR is None:
        raise GpError("Environment Variable GPHOME not set!")
    hawq_site = HawqXMLParser(HAWQ_CONFIG_DIR)
    master_port = hawq_site.get_value_from_name('hawq_master_address_port').strip()

    return master_port

######
def get_masterport(datadir):
    return pgconf.readfile(os.path.join(datadir, 'postgresql.conf')).int('port')


######
def check_permissions(username):
    logger.debug("--Checking that current user can use GP binaries")
    chk_gpdb_id(username)


######
def standby_check(master_datadir):
    logger.debug("---Make sure we aren't a passive standby master")
    if (check_passive_standby('check if passive standby',master_datadir)):
        raise GpError('Cannot run this utility on the standby instance')


######
def recovery_startup(datadir):
    """ investigate a db that may still be running """

    pid=read_postmaster_pidfile(datadir)
    if check_pid(pid):
        info_str="found postmaster with pid: %d for datadir: %s still running" % (pid,datadir)
        logger.info(info_str)

        logger.info("attempting to shutdown db with datadir: %s" % datadir )
        cmd=SegmentStop('db shutdown' , datadir,mode='fast')
        cmd.run()

        if check_pid(pid):
            info_str="unable to stop postmaster with pid: %d for datadir: %s still running" % (pid,datadir)
            logger.info(info_str)
            return info_str
        else:
            logger.info("shutdown of db successful with datadir: %s" % datadir)
            return None
    else:
        #if we get this far it means we don't have a pid and need to do some cleanup.
        pgconf_dict = pgconf.readfile(datadir + "/postgresql.conf")
        port = pgconf_dict.int('port')

        lockfile="/tmp/.s.PGSQL.%s" % port
        tmpfile_exists = os.path.exists(lockfile)


        logger.info("No db instance process, entering recovery startup mode")

        if tmpfile_exists:
            logger.info("Clearing db instance lock files")
            os.remove(lockfile)

        postmaster_pid_file = "%s/postmaster.pid" % datadir
        if os.path.exists(postmaster_pid_file):
            logger.info("Clearing db instance pid file")
            os.remove("%s/postmaster.pid" % datadir)

        return None


# these match names from gp_bash_functions.sh
def chk_gpdb_id(username):
    path="%s/bin/initdb" % GPHOME
    if not os.access(path,os.X_OK):
        raise GpError("File permission mismatch.  The current user %s does not have sufficient"
                      " privileges to run the Greenplum binaries and management utilities." % username )
    pass


def chk_local_db_running(datadir, port):
    """Perform a few checks to see if the db is running.  We 1st look at:

       1) /tmp/.s.PGSQL.<PORT> and /tmp/.s.PGSQL.<PORT>.lock
       2) DATADIR/postmaster.pid
       3) netstat

       Returns tuple in format (postmaster_pid_file_exists, tmpfile_exists, lockfile_exists, port_active, postmaster_pid)

       postmaster_pid value is 0 if postmaster_pid_exists is False  Note that this is the PID from the postmaster.pid file

    """

    # determine if postmaster.pid is there, grab pid from it
    postmaster_pid_exists = True
    f = None
    try:
        f = open(datadir + "/postmaster.pid")
    except IOError:
        postmaster_pid_exists = False

    pid_value = 0
    if postmaster_pid_exists:
        try:
            for line in f:
                pid_value = int(line) # grab first line only
                break
        finally:
            f.close()

    cmd=FileDirExists('check for /tmp/.s.PGSQL file file', "/tmp/.s.PGSQL.%d" % port)
    cmd.run(validateAfter=True)
    tmpfile_exists = cmd.filedir_exists()

    cmd=FileDirExists('check for lock file', get_lockfile_name(port))
    cmd.run(validateAfter=True)
    lockfile_exists = cmd.filedir_exists()

    netstat_port_active = PgPortIsActive.local('check netstat for postmaster port',"/tmp/.s.PGSQL.%d" % port, port)

    logger.debug("postmaster_pid_exists: %s tmpfile_exists: %s lockfile_exists: %s netstat port: %s  pid: %s" %\
                (postmaster_pid_exists, tmpfile_exists, lockfile_exists, netstat_port_active, pid_value))

    return (postmaster_pid_exists, tmpfile_exists, lockfile_exists, netstat_port_active, pid_value)

def get_lockfile_name(port):
    return "/tmp/.s.PGSQL.%d.lock" % port


def get_local_db_mode(master_data_dir):
    """ Gets the mode Greenplum is running in.
        Possible return values are:
            'NORMAL'
            'RESTRICTED'
            'UTILITY'
    """
    mode = 'NORMAL'

    if not os.path.exists(master_data_dir + '/postmaster.pid'):
        raise Exception('Greenplum database appears to be stopped')

    try:
        fp = open(master_data_dir + '/postmaster.opts', 'r')
        optline = fp.readline()
        if optline.find('superuser_reserved_connections') > 0:
            mode = 'RESTRICTED'
        elif optline.find('gp_role=utility') > 0:
            mode = 'UTILITY'
    except OSError:
        raise Exception('Failed to open %s.  Is Greenplum Database running?' % master_data_dir + '/postmaster.opts')
    except IOError:
        raise Exception('Failed to read options from %s' % master_data_dir + '/postmaster.opts')
    finally:
        if fp: fp.close()

    return mode

######
def read_postmaster_pidfile(datadir):
    pid=0
    f = None
    try:
        f = open(datadir + '/postmaster.pid')
        pid = int(f.readline().strip())
    except Exception:
        pass
    finally:
        if f: f.close()
    return pid


def pausePg(db):
    """
    This function will pause an instance of postgres which is part of a GPDB

    1) pause the postmaster (this prevents new connections from being made)
    2) get list of processes that are descendent from postmaster process
    3) pause all descendent processes and ignore any failures (a process may have died betwen getting pid list and doing the pauses)
    4) again, get list of processes that are descendent from postmaster process
    5) pause all descendent processes and ignore any failures (do not ignore errors, errors are failures, no pids can die between being paused the first time and getting the pid list)
    """

    datadir = db.getSegmentDataDirectory()
    content = db.getSegmentContentId()
    postmasterPID = read_postmaster_pidfile(datadir)
    if postmasterPID == 0:
        raise Exception, 'print "could not locate postmasterPID during pause'

    Kill.local(name="pausep "+str(content), pid=postmasterPID, signal="STOP")

    decsendentProcessPids = getDescendentProcesses(postmasterPID)

    for killpid in decsendentProcessPids:

        try:
            Kill.local(name="pausep "+str(killpid), pid=killpid, signal="STOP")
        except:
            pass

    decsendentProcessPids = getDescendentProcesses(postmasterPID)

    for killpid in decsendentProcessPids:

        Kill.local(name="pausep "+str(killpid), pid=killpid, signal="STOP")


def resumePg(db):
    """
    1) resume the processes descendent from the postmaster process
    2) resume the postmaster process
    """

    datadir = db.getSegmentDataDirectory()
    content = db.getSegmentContentId()
    postmasterPID = read_postmaster_pidfile(datadir)
    if postmasterPID == 0:
        raise Exception, 'print "could not locate postmasterPID during resume'

    decsendentProcessPids = getDescendentProcesses(postmasterPID)

    for killpid in decsendentProcessPids:

        Kill.local(name="pausep "+str(killpid), pid=killpid, signal="CONT")


    Kill.local(name="pausep "+str(content), pid=postmasterPID, signal="CONT")


def createTempDirectoryName(masterDataDirectory, tempDirPrefix):
    return '%s/%s_%s_%d' % (os.sep.join(os.path.normpath(masterDataDirectory).split(os.sep)[:-1]),
                                tempDirPrefix,
                                datetime.datetime.now().strftime('%m%d%Y'),
                                os.getpid())


#-------------------------------------------------------------------------
class GpRecoverSeg(Command):
   """
   This command will execute the gprecoverseg utility
   """

   def __init__(self, name, options = "", ctxt = LOCAL, remoteHost = None):
       self.name = name
       self.options = options
       self.ctxt = ctxt
       self.remoteHost = remoteHost

       cmdStr = "$GPHOME/bin/gprecoverseg %s" % (options)
       Command.__init__(self,name,cmdStr,ctxt,remoteHost)




if __name__ == '__main__':

    import doctest
    doctest.testmod()
