#!/usr/bin/env python
#
# Copyright (c) Greenplum Inc 2008. All Rights Reserved. 
#
#
# Internal Use Function.
#
#
#
# THIS IMPORT MUST COME FIRST
#
# import mainUtils FIRST to get python version check
from gppylib.mainUtils import *

import os, sys, time, signal

from optparse import Option, OptionGroup, OptionParser, OptionValueError, SUPPRESS_USAGE

from gppylib.gpparseopts import OptParser, OptChecker
from gppylib import gplog
from gppylib.commands import base
from gppylib.commands import unix
from gppylib.commands import gp
from gppylib.commands.gp import SEGMENT_TIMEOUT_DEFAULT
from gppylib.commands import pg
from gppylib.db import catalog
from gppylib.db import dbconn
from gppylib import pgconf
from gppylib.gpcoverage import GpCoverage

description = ("""
This utility is NOT SUPPORTED and is for internal-use only.

stops a set of one or more segment databases.
""")

logger = gplog.get_default_logger()

#-------------------------------------------------------------------------
class SegStopStatus:
    def __init__(self,datadir,stopped,reason):
        self.datadir=datadir
        self.stopped=stopped
        self.reason=reason
    
    def __str__(self):
        return "STATUS--DIR:%s--STOPPED:%s--REASON:%s" % (self.datadir,self.stopped,self.reason)

    
#-------------------------------------------------------------------------    
class GpSegStop:
    ######
    def __init__(self,dblist,mode,gpversion,timeout=SEGMENT_TIMEOUT_DEFAULT):
        self.dblist=dblist
        self.mode=mode
        self.expected_gpversion=gpversion
        self.timeout=timeout
        self.gphome=os.path.abspath(os.pardir)
        self.actual_gpversion=gp.GpVersion.local('local GP software version check',self.gphome)
        if self.actual_gpversion != self.expected_gpversion:
            raise Exception("Local Software Version does not match what is expected.\n"
                            "The local software version is: '%s'\n"
                            "But we were expecting it to be: '%s'\n"
                            "Please review and correct" % (self.actual_gpversion,self.expected_gpversion))                
        self.logger = logger
    
    ######
    def run(self):
        results  = []
        failures = []
        
        self.logger.info("Issuing shutdown commands to local segments...")
        for db in self.dblist:
            datadir, port = db.split(':')[0:2]

            cmd = gp.SegmentStop('segment shutdown', datadir, mode=self.mode, timeout=self.timeout)
            cmd.run()
            res = cmd.get_results()
            if res.rc == 0:

                # MPP-15208
                #
                cmd2 = gp.SegmentIsShutDown('check if shutdown', datadir)
                cmd2.run()
                if cmd2.is_shutdown():
                    status = SegStopStatus(datadir, True, "Shutdown Succeeded")
                    results.append(status)                
                    continue

                # MPP-16171
                # 
                if self.mode == 'immediate':
                    status = SegStopStatus(datadir, True, "Shutdown Immediate")
                    results.append(status)
                    continue

            # read pid and datadir from /tmp/.s.PGSQL.<port>.lock file
            name = "failed segment '%s'" % db
            (succeeded, mypid, file_datadir) = pg.ReadPostmasterTempFile.local(name,port).getResults()
            if succeeded and file_datadir == datadir:

                # now try to terminate the process, first trying with
                # SIGTERM and working our way up to SIGABRT sleeping
                # in between to give the process a moment to exit
                #
                unix.kill_sequence(mypid)

                if not unix.check_pid(mypid):
                    lockfile = "/tmp/.s.PGSQL.%s" % port    
                    if os.path.exists(lockfile):
                        self.logger.info("Clearing segment instance lock files")        
                        os.remove(lockfile)
            
            status = SegStopStatus(datadir,False,"Shutdown failed: rc: %d stdout: %s stderr: %s" % (res.rc,res.stdout,res.stderr))
            failures.append(status)
            results.append(status)
        
        #Log the results!
        status = '\nCOMMAND RESULTS\n'
        for result in results:
            status += str(result) + "\n"
        
        self.logger.info(status)
        return 1 if failures else 0
    
    ######
    def cleanup(self):
        pass
        
    @staticmethod
    def createParser():
        parser = OptParser(option_class=OptChecker,
                    description=' '.join(description.split()),
                    version='%prog version $Revision: #12 $')
        parser.setHelp([])

        addStandardLoggingAndHelpOptions(parser, includeNonInteractiveOption=False)

        parser.add_option("-D","--db",dest="dblist", action="append", type="string")
        parser.add_option("-V", "--gp-version", dest="gpversion",metavar="GP_VERSION",
                          help="expected software version")
        parser.add_option("-m", "--mode", dest="mode",metavar="<MODE>",
                          help="how to shutdown. modes are smart,fast, or immediate")
        parser.add_option("-t", "--timeout", dest="timeout", type="int", default=SEGMENT_TIMEOUT_DEFAULT,
                          help="seconds to wait")
        return parser

    @staticmethod
    def createProgram(options, args):
        return GpSegStop(options.dblist,options.mode,options.gpversion,options.timeout)

#-------------------------------------------------------------------------
if __name__ == '__main__':
    mainOptions = { 'setNonuserOnToolLogger':True}
    simple_main( GpSegStop.createParser, GpSegStop.createProgram, mainOptions)
