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

#
# Used to inject faults into the file replication code
#

#
# THIS IMPORT MUST COME FIRST
#
# import mainUtils FIRST to get python version check
from hawqpylib.mainUtils import *

from optparse import Option, OptionGroup, OptionParser, OptionValueError, SUPPRESS_USAGE

from gppylib.gpparseopts import OptParser, OptChecker
from gppylib.utils import toNonNoneString
from gppylib import gplog
from hawqpylib import hawqarray
from gppylib.commands import base
from gppylib.commands import unix
from gppylib.commands import gp
from gppylib.commands import pg
from gppylib.db import catalog
from gppylib.db import dbconn
from gppylib.system import configurationInterface, fileSystemInterface, osInterface
from gppylib import pgconf
from gppylib.testold.testUtils import testOutput
from gppylib.system.environment import GpMasterEnvironment

logger = gplog.get_default_logger()

#-------------------------------------------------------------------------
class HAWQInjectFaultProgram:
    #
    # Constructor:
    #
    # @param options the options as returned by the options parser
    #
    def __init__(self, options):
        self.options = options

    #
    # Build the fault transition message.  Fault options themselves will NOT be validated by the
    #   client -- the server will do that when we send the fault
    #
    def buildMessage(self) :

        # note that we don't validate these strings -- if they contain newlines
        # (and so mess up the transition protocol) then the server will error
        result = ["faultInject"]
        result.append(toNonNoneString(self.options.faultName))
        result.append(toNonNoneString(self.options.type))
        result.append(toNonNoneString(self.options.ddlStatement))
        result.append(toNonNoneString(self.options.databaseName))
        result.append(toNonNoneString(self.options.tableName))
        result.append(toNonNoneString(self.options.numOccurrences))
        result.append(toNonNoneString(self.options.sleepTimeSeconds))
        return '\n'.join(result)

    #
    # build a message that will get status of the fault
    #
    def buildGetStatusMessage(self) :
        # note that we don't validate this string then the server may error
        result = ["getFaultInjectStatus"]
        result.append(toNonNoneString(self.options.faultName))
        return '\n'.join(result)

    #
    # return True if the segment matches the given role, False otherwise
    #
    def isMatchingRole(self, role, hawqdb):
        hdbRole = hawqdb.getRole()
        if role == "master":
            return hdbRole == 'm'
        elif role == "standby":
            return hdbRole == 's'
        elif role == "primary":
            return hdbRole == 'p'
        else:
            raise ProgramArgumentValidationException("Invalid role specified: %s" % role)

    #
    # load the segments and filter to the ones we should target
    #
    def loadTargetSegments(self) :

        targetHost              = self.options.targetHost
        targetRole              = self.options.targetRole
        targetRegistrationOrder = self.options.targetRegistrationOrder

        if targetHost is None and targetRegistrationOrder is None:
            raise ProgramArgumentValidationException(\
                            "neither --host nor --registration_order is specified.  " \
                            "Exactly one should be specified.")
        if targetHost is not None and targetRegistrationOrder is not None:
            raise ProgramArgumentValidationException(\
                            "both --host and --registration_order are specified.  " \
                            "Exactly one should be specified.")
        if targetHost is not None and targetRole is None:
            raise ProgramArgumentValidationException(\
                            "--role is not specified when --host is specified.  " \
                            "Role is required when targeting a host.")
        if targetRegistrationOrder is not None and targetRole is not None:
            raise ProgramArgumentValidationException(\
                            "--role is specified when --registration_order is specified.  " \
                            "Role should not be specified when targeting a single registration_order.")

        #
        # load from master db
        #
        masterPort = self.options.masterPort
        if masterPort is None:
            gpEnv = GpMasterEnvironment(self.options.masterDataDirectory, False)
            masterPort = gpEnv.getMasterPort()
        conf = configurationInterface.getConfigurationProvider().initializeProvider(masterPort)
        hawqArray = conf.loadSystemConfig(useUtilityMode=True)
        hawqdbs = hawqArray.getDbList()
        
        #
        # prune gpArray according to filter settings
        #
        if targetHost is not None and targetHost != "ALL":
            hawqdbs = [hdb for hdb in hawqdbs if hdb.getHostName() == targetHost]

        if targetRegistrationOrder is not None:
            hawqdbs = gpArray.getDbList()
            regorder = int(targetRegistrationOrder)
            hawqdbs = [hdb for hdb in hawqdbs if hdb.getRegistrationOrder() == regorder]

        if targetRole is not None:
            hawqdbs = [hdb for hdb in hawqdbs if self.isMatchingRole(targetRole, hdb)]

        # only DOWN segments remaining?  Error out
        downhawqdbs = [hdb for hdb in hawqdbs if hdb.getStatus() != 'u']
        if len(downhawqdbs) > 0:
            downhawqdbStr = "\n     Down Segment: "
            raise ExceptionNoStackTraceNeeded(
                "Unable to inject fault.  At least one segment is marked as down in the database.%s%s" % 
                (downhawqdbStr, downhawqdbStr.join([str(downhdb) for downhdb in downhawqdbs])))

        print "### DEBUG: loadTargetSegments"
        print "### DEBUG: HAWQDBS "
        print hawqdbs
        return hawqdbs

    #
    # write string to a temporary file that will be deleted on completion
    #
    def writeToTempFile(self, str):
        inputFile = fileSystemInterface.getFileSystemProvider().createNamedTemporaryFile()
        inputFile.write(str)
        inputFile.flush()
        return inputFile

    def injectFaults(self, segments, messageText):

        inputFile = self.writeToTempFile(messageText)
        logger.info("Injecting fault on %d segment(s)", len(segments))
        testOutput("Injecting fault on %d segment(s)" % len(segments))

        # run the command in serial to each target
        for segment in segments :
            logger.info("Injecting fault on %s", segment)
            print "### DEBUG: injectFaults -> SendFilerepTransitionMessage inputFile.name = %s" % inputFile.name
            print "### DEBUG: injectFaults -> SendFilerepTransitionMessage segment.getPort = %s" % segment.getPort()
            print "### DEBUG: injectFaults -> SendFilerepTransitionMessage base.LOCAL = %s" % base.LOCAL
            print "### DEBUG: injectFaults -> SendFilerepTransitionMessage segment.getHostName = %s" % segment.getHostName()
            # if there is an error then an exception is raised by command execution
            cmd = gp.SendFilerepTransitionMessage("Fault Injector", inputFile.name, \
                                        segment.getPort(), base.LOCAL, segment.getHostName())
            
            print "### DEBUG: injectFaults -> Command = %s ###" % cmd
            print "### DEBUG: injectFaults -> SendFilerepTransitionMessage ###"
            cmd.run(validateAfter=False)

 
            print "### DEBUG: injectFaults -> Run ###"
            # validate ourselves
            if cmd.results.rc != 0:
                print "### DEBUG: injectFaults -> Injection Failed ###"
                raise ExceptionNoStackTraceNeeded("Injection Failed: %s" % cmd.results.stderr)
            elif self.options.type == "status":
                # server side prints nice success messages on status...so print it
                print "### DEBUG: injectFaults -> Report Status ###"
                str = cmd.results.stderr
                print "### DEBUG: injectFaults -> STDERROR = %s ###" % str
                if str.startswith("Success: "):
                    str = str.replace("Success: ", "", 1)
                logger.info("%s", str)
            
            print "### DEBUG: injectFaults -> END ###" 
        inputFile.close()

    def waitForFaults(self, segments, statusQueryText):
        inputFile = self.writeToTempFile(statusQueryText)
        segments = [seg for seg in segments]
        sleepTimeSec = 0.115610199
        sleepTimeMultipler = 1.5  # sleepTimeMultipler * sleepTimeMultipler^11 ~= 10

        logger.info("Awaiting fault on %d segment(s)", len(segments))
        while len(segments) > 0 :
            logger.info("Sleeping %.2f seconds " % sleepTimeSec)
            osInterface.getOsProvider().sleep(sleepTimeSec)

            segmentsForNextPass = []
            for segment in segments:
                logger.info("Checking for fault completion on %s", segment)
                cmd = gp.SendFilerepTransitionMessage.local("Fault Injector Status Check", inputFile.name, \
                                                            segment.getPort(), segment.getHostName())
                resultStr = cmd.results.stderr.strip()
                if resultStr == "Success: waitMore":
                    segmentsForNextPass.append(segment)
                elif resultStr != "Success: done":
                    raise Exception("Unexpected result from server %s" % resultStr)

            segments = segmentsForNextPass
            sleepTimeSec = sleepTimeSec if sleepTimeSec > 7 else sleepTimeSec * sleepTimeMultipler
        inputFile.close()

    def isSyncableFaultType(self):
        type = self.options.type
        return type != "reset" and type != "status"

    ######
    def run(self):

        if self.options.masterPort is not None and self.options.masterDataDirectory is not None:
            raise ProgramArgumentValidationException("both master port and master data directory options are specified;" \
                    " at most one should be specified, or specify none to use MASTER_DATA_DIRECTORY environment variable")

        print "### DEBUG: Build Message ###"
        messageText = self.buildMessage()
        print "### DEBUG: Load Target Segments ###"
        segments = self.loadTargetSegments()

        # inject, maybe wait
        print "### DEBUG: Inject Faults ###"
        self.injectFaults(segments, messageText)
        if self.isSyncableFaultType() :
            statusQueryText = self.buildGetStatusMessage()
            self.waitForFaults(segments, statusQueryText)

        logger.info("DONE")
        return 0  # success -- exit code 0!

    def cleanup(self):
        pass

    #-------------------------------------------------------------------------
    @staticmethod
    def createParser():
        description = ("""
        This utility is NOT SUPPORTED and is for internal-use only.

        Used to inject faults into the file replication code.
        """)

        help = ["""

        Return codes:
          0 - Fault injected
          non-zero: Error or invalid options
        """]

        parser = OptParser(option_class=OptChecker,
                    description='  '.join(description.split()),
                    version='%prog version $Revision$')
        parser.setHelp(help)

        addStandardLoggingAndHelpOptions(parser, False)

        # these options are used to determine the target segments
        addTo = OptionGroup(parser, 'Target Segment Options: ')
        parser.add_option_group(addTo)
        addTo.add_option('-r', '--role', dest="targetRole", type='string', metavar="<role>",
                         help="Role of segments to target: master, standby, primary")
        addTo.add_option("-s", "--registration_order", dest="targetRegistrationOrder", type="string", metavar="<registration_order>",
                         help="The segment registration_order on which fault should be set and triggered.")
        addTo.add_option("-H", "--host", dest="targetHost", type="string", metavar="<host>",
                         help="The hostname on which fault should be set and triggered; pass ALL to target all hosts")

        addTo = OptionGroup(parser, 'Master Connection Options')
        parser.add_option_group(addTo)

        addMasterDirectoryOptionForSingleClusterProgram(addTo)
        addTo.add_option("-p", "--master_port", dest="masterPort", type="int", default=None,
                         metavar="<masterPort>",
                         help="DEPRECATED, use MASTER_DATA_DIRECTORY environment variable or -d option.  " \
                         "The port number of the master database on localhost, " \
                         "used to fetch the segment configuration.")

        addTo = OptionGroup(parser, 'Client Polling Options: ')
        parser.add_option_group(addTo)
        addTo.add_option('-m', '--mode', dest="syncMode", type='string', default="async",
                         metavar="<syncMode>",
                         help="Synchronization mode : sync (client waits for fault to occur)" \
                         " or async (client only sets fault request on server)")

        # these options are used to build the message for the segments
        addTo = OptionGroup(parser, 'Fault Options: ')
        parser.add_option_group(addTo)
        addTo.add_option('-y', '--type', dest="type", type='string', metavar="<type>",
                         help="fault type: sleep (insert sleep), fault (report fault to postmaster and fts prober), " \
                  "fatal (inject FATAL error), panic (inject PANIC error), error (inject ERROR), " \
                  "infinite_loop, data_curruption (corrupt data in memory and persistent media), " \
                  "suspend (suspend execution), resume (resume execution that was suspended), " \
                  "skip (inject skip i.e. skip checkpoint), " \
                  "memory_full (all memory is consumed when injected), " \
                  "reset (remove fault injection), status (report fault injection status), " \
                  "panic_suppress (inject suppressed PANIC in critical section), " \
                  "segv (inject a SEGV), " \
                  "interrupt (inject an Interrupt) ")
        addTo.add_option("-z", "--sleep_time_s", dest="sleepTimeSeconds", type="int", default="10" ,
                            metavar="<sleepTime>",
                            help="For 'sleep' faults, the amount of time for the sleep.  Defaults to %default." \
                 "Min Max Range is [0, 7200 sec] ")
        addTo.add_option('-f', '--fault_name', dest="faultName", type='string', metavar="<name>",
                         help="fault name: " \
                  "postmaster (inject fault when new connection is accepted in postmaster), " \
                  "pg_control (inject fault when global/pg_control file is written), " \
                  "pg_xlog (inject fault when files in pg_xlog directory are written), " \
                  "start_prepare (inject fault during start prepare transaction), " \
                  "filerep_consumer (inject fault before data are processed, i.e. if mirror " \
                  "then before file operation is issued to file system, if primary " \
                  "then before mirror file operation is acknowledged to backend processes), " \
                  "filerep_consumer_verificaton (inject fault before ack verification data are processed on primary), " \
                  "filerep_change_tracking_compacting (inject fault when compacting change tracking log files), " \
                  "filerep_sender (inject fault before data are sent to network), " \
                  "filerep_receiver (inject fault after data are received from network), " \
                  "filerep_flush (inject fault before fsync is issued to file system), " \
                  "filerep_resync (inject fault while in resync when first relation is ready to be resynchronized), " \
                  "filerep_resync_in_progress (inject fault while resync is in progress), " \
                  "filerep_resync_worker (inject fault after write to mirror), " \
                  "filerep_resync_worker_read (inject fault before read required for resync), " \
                  "filerep_transition_to_resync (inject fault during transition to InResync before mirror re-create), " \
                  "filerep_transition_to_resync_mark_recreate (inject fault during transition to InResync before marking re-created), " \
                  "filerep_transition_to_resync_mark_completed (inject fault during transition to InResync before marking completed), " \
                  "filerep_transition_to_sync_begin (inject fault before transition to InSync begin), " \
                  "filerep_transition_to_sync (inject fault during transition to InSync), " \
                  "filerep_transition_to_sync_before_checkpoint (inject fault during transition to InSync before checkpoint is created), " \
                  "filerep_transition_to_sync_mark_completed (inject fault during transition to InSync before marking completed), " \
                  "filerep_transition_to_change_tracking (inject fault during transition to InChangeTracking), " \
                  "checkpoint (inject fault before checkpoint is taken), " \
                  "change_tracking_compacting_report (report if compacting is in progress), " \
                  "change_tracking_disable (inject fault before fsync to Change Tracking log files), " \
                  "transaction_abort_after_distributed_prepared (abort prepared transaction), " \
                  "transaction_commit_pass1_from_create_pending_to_created, " \
                  "transaction_commit_pass1_from_drop_in_memory_to_drop_pending, " \
                  "transaction_commit_pass1_from_aborting_create_needed_to_aborting_create, " \
                  "transaction_abort_pass1_from_create_pending_to_aborting_create, " \
                  "transaction_abort_pass1_from_aborting_create_needed_to_aborting_create, " \
                  "transaction_commit_pass2_from_drop_in_memory_to_drop_pending, " \
                  "transaction_commit_pass2_from_aborting_create_needed_to_aborting_create, " \
                  "transaction_abort_pass2_from_create_pending_to_aborting_create, " \
                  "transaction_abort_pass2_from_aborting_create_needed_to_aborting_create, " \
                  "finish_prepared_transaction_commit_pass1_from_create_pending_to_created, " \
                  "finish_prepared_transaction_commit_pass2_from_create_pending_to_created, " \
                  "finish_prepared_transaction_abort_pass1_from_create_pending_to_aborting_create, " \
                  "finish_prepared_transaction_abort_pass2_from_create_pending_to_aborting_create, " \
                  "finish_prepared_transaction_commit_pass1_from_drop_in_memory_to_drop_pending, " \
                  "finish_prepared_transaction_commit_pass2_from_drop_in_memory_to_drop_pending, " \
                  "finish_prepared_transaction_commit_pass1_aborting_create_needed, " \
                  "finish_prepared_transaction_commit_pass2_aborting_create_needed, " \
                  "finish_prepared_transaction_abort_pass1_aborting_create_needed, " \
                  "finish_prepared_transaction_abort_pass2_aborting_create_needed, " \
                  "twophase_transaction_commit_prepared (inject fault before transaction commit is inserted in xlog), " \
                  "twophase_transaction_abort_prepared (inject fault before transaction abort is inserted in xlog), " \
                  "dtm_broadcast_prepare (inject fault after prepare broadcast), " \
                  "dtm_broadcast_commit_prepared (inject fault after commit broadcast), " \
                  "dtm_broadcast_abort_prepared (inject fault after abort broadcast), " \
                  "dtm_xlog_distributed_commit (inject fault after distributed commit was inserted in xlog), " \
                  "fault_before_pending_delete_relation_entry (inject fault before putting pending delete relation entry, " \
                  "fault_before_pending_delete_database_entry (inject fault before putting pending delete database entry, " \
                  "fault_before_pending_delete_tablespace_entry (inject fault before putting pending delete tablespace entry, " \
                  "fault_before_pending_delete_filespace_entry (inject fault before putting pending delete filespace entry, " \
                  "dtm_init (inject fault before initializing dtm), " \
                  "end_prepare_two_phase_sleep (inject sleep after two phase file creation), " \
                  "segment_transition_request (inject fault after segment receives state transition request), " \
                  "segment_probe_response (inject fault after segment is probed by FTS), " \
                  "sync_persistent_table (inject fault to sync persistent table to disk), " \
                  "xlog_insert (inject fault to skip insert record into xlog), " \
                  "local_tm_record_transaction_commit (inject fault for local transactions after transaction commit is recorded and flushed in xlog ), " \
                  "malloc_failure (inject fault to simulate memory allocation failure), " \
                  "transaction_abort_failure (inject fault to simulate transaction abort failure), " \
                  "update_committed_eof_in_persistent_table (inject fault before committed EOF is updated in gp_persistent_relation_node for Append Only segment files), " \
                  "fault_during_exec_dynamic_table_scan (inject fault during scanning of a partition), " \
                  "internal_flush_error (inject an error during internal_flush), " \
                  "exec_simple_query_end_command (inject fault before EndCommand in exec_simple_query), " \
                  "multi_exec_hash_large_vmem (allocate large vmem using palloc inside MultiExecHash to attempt to exceed vmem limit), " \
                  "execsort_before_sorting (inject fault in nodeSort after receiving all tuples and before sorting), " \
                  "workfile_cleanup_set (inject fault in workfile manager cleanup set)" \
                  "execsort_mksort_mergeruns (inject fault in MKSort during the mergeruns phase), " \
                  "cdb_copy_start_after_dispatch (inject fault in cdbCopyStart after dispatch), " \
                  "fault_in_background_writer_main (inject fault in BackgroundWriterMain), " \
                  "exec_hashjoin_new_batch (inject fault before switching to a new batch in Hash Join), " \
                  "analyze_subxact_error (inject an error during analyze)," \
                  "opt_task_allocate_string_buffer (inject fault while allocating string buffer), " \
                  "runaway_cleanup (inject fault before starting the cleanup for a runaway query)" \
                  "connection_fail_after_gang_creation (inject fault after gang thread creation, set connection null)" \
				  "create_cdb_dispath_result_object (inject fault when create cdb dispatch result object, set out of memory)" \
                  "worker_manager_submit_job (inject fault when worker manager submit job , set error)" \
				  "all (affects all faults injected, used for 'status' and 'reset'), ")
        addTo.add_option("-c", "--ddl_statement", dest="ddlStatement", type="string",
                         metavar="ddlStatement",
                         help="The DDL statement on which fault should be set and triggered " \
                         "(i.e. create_database, drop_database, create_table, drop_table)")
        addTo.add_option("-D", "--database_name", dest="databaseName", type="string",
                         metavar="databaseName",
                         help="The database name on which fault should be set and triggered.")
        addTo.add_option("-t", "--table_name", dest="tableName", type="string",
                         metavar="tableName",
                         help="The table name on which fault should be set and triggered.")
        addTo.add_option("-o", "--occurrence", dest="numOccurrences", type="int", default=1,
                         metavar="numOccurrences",
                         help="The number of occurrence of the DDL statement with the database name " \
                         "and the table name before fault is triggered.  Defaults to %default. Max is 1000. " \
             "Fault is triggered always if set to '0'. ")
        parser.set_defaults()
        return parser

    @staticmethod
    def createProgram(options, args):
        if len(args) > 0 :
            raise ProgramArgumentValidationException(\
                            "too many arguments: only options may be specified")
        return HAWQInjectFaultProgram(options)
