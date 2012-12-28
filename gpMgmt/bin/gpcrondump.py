#!/usr/bin/env python
#
# Copyright (c) Greenplum Inc 2008. All Rights Reserved. 
#
#
# THIS IMPORT MUST COME FIRST
#
# import mainUtils FIRST to get python version check
from gppylib.mainUtils import *

from datetime import datetime
import sys
import socket
from optparse import Option, OptionGroup, OptionParser, OptionValueError
from sets import Set

import re
import stat

try:
    from gppylib import gplog
    from gppylib import pgconf
    from gppylib import userinput
    from gppylib.commands.base import Command
    from gppylib.gpparseopts import OptParser, OptChecker
    from gppylib.operations import Operation
    from gppylib.operations.dump import ValidateDatabaseExists, ValidateSchemaExists, DeleteCurrentDump, DeleteOldestDumps, DumpDatabase, DumpConfig, DumpGlobal, MailDumpEvent, PostDumpDatabase, UpdateHistoryTable, VacuumDatabase
    from gppylib.operations.utils import DEFAULT_NUM_WORKERS
    from gppylib.gparray import GpArray
    from gppylib.db import dbconn
    from getpass import getpass
    
    
except ImportError, e:
    sys.exit('Cannot import modules.  Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))
 
INJECT_ROLLBACK = False

EXECNAME = 'gpcrondump'
GPCRONDUMP_PID_FILE = 'gpcrondump.pid'
FREE_SPACE_PERCENT = 10

logger = gplog.get_default_logger()

class GpCronDump(Operation):
    def __init__(self, options, args):
    	raise ExceptionNoStackTraceNeeded("gpcrondump NOT SUPPORTED YET IN GPSQL")
    
        if args:
            logger.warn("please note that some of the arguments (%s) aren't valid and will be ignored.", args)
        if options.masterDataDirectory is None:
            options.masterDataDirectory = gp.get_masterdatadir()
        self.interactive = options.interactive
        self.master_datadir = options.masterDataDirectory
        self.master_port = self._get_master_port(self.master_datadir)
        self.options_list = " ".join(sys.argv[1:])
        self.cur_host = socket.gethostname()

        self.clear_dumps_only = options.clear_dumps_only
        self.post_script = options.post_script 
        self.dump_config = options.dump_config
        self.history = options.history
        self.pre_vacuum = options.pre_vacuum
        self.post_vacuum = options.post_vacuum
        self.rollback = options.rollback

        self.compress = options.compress
        self.free_space_percent = int(options.free_space_percent)
        self.clear_dumps = options.clear_dumps
        self.dump_schema = options.dump_schema
        self.dump_databases = options.dump_databases
        self.dump_global = options.dump_global
        self.clear_catalog_dumps = options.clear_catalog_dumps
        self.batch_default = options.batch_default
        self.include_dump_tables = options.include_dump_tables
        self.exclude_dump_tables = options.exclude_dump_tables   
        self.include_dump_tables_file = options.include_dump_tables_file
        self.exclude_dump_tables_file = options.exclude_dump_tables_file 
        self.backup_dir = options.backup_dir
        self.report_dir = options.report_dir
        self.encoding = options.encoding
        self.output_options = options.output_options
        self.ddboost_hosts = options.ddboost_hosts
        self.ddboost_user = options.ddboost_user
        self.ddboost_config_remove = options.ddboost_config_remove
        self.ddboost_verify = options.ddboost_verify
        # This variable indicates whether we need to exit after verifying the DDBoost credentials 
        self.ddboost_verify_and_exit = False
    
        # TODO: if action is 'append', wouldn't you expect a lack of input to result in [], as opposed to None?
        if self.include_dump_tables is None: self.include_dump_tables = []
        if self.exclude_dump_tables is None: self.exclude_dump_tables = []  
        if self.output_options is None: self.output_options = []
        if self.ddboost_hosts is None: self.ddboost_hosts = []

        if not (self.clear_dumps_only or bool(self.ddboost_hosts) or bool(self.ddboost_user) or self.ddboost_config_remove):
            if self.dump_databases is not None:
                self.dump_databases = self.dump_databases.split(",")
            elif 'PGDATABASE' in os.environ:
                self.dump_databases = [os.environ['PGDATABASE']]
                logger.info("Setting dump database to value of $PGDATABASE which is %s" % self.dump_databases[0])
            else:
                if self.ddboost_verify is True:
                    # We would expect to verify the credentials here and return some exit code,
                    # but __init__() should return None, not 'int' - hence we are forced to use
                    # some kind of flag  
                    self.ddboost_verify_and_exit = True
                else:
                    raise ExceptionNoStackTraceNeeded("Must supply -x <database name> because $PGDATABASE is not set")

        if options.bypass_disk_check:
            self.free_space_percent = None

        if options.backup_set is not None:
            logger.info("-w is no longer supported, will continue with dump of primary segments.")

        self.ddboost = options.ddboost
        if self.ddboost:
            self.free_space_percent = None
            logger.info('Bypassing disk space checks due to DDBoost parameters')
            if self.backup_dir is not None:
                raise ExceptionNoStackTraceNeeded('-u cannot be used with DDBoost parameters.')
        
    def getGpArray(self):
        return GpArray.initFromCatalog(dbconn.DbURL(port=self.master_port), utility=True)
    
         
    def getHostSet(self, gparray):
        hostlist = gparray.get_hostlist(includeMaster=True)
        hostset = Set(hostlist)
        return hostset


    # check if one of gpcrondump option was specified, except of ddboost options
    def only_ddboost_options(self):
        return ((self.dump_databases is None) and (self.dump_schema is None) and (self.report_dir is None)
                and (self.backup_dir is None)
                and not (self.include_dump_tables or self.output_options or self.clear_dumps_only or self.pre_vacuum
					     or self.clear_dumps_only or self.dump_global or self.rollback or self.exclude_dump_tables
					     or self.dump_config or self.clear_dumps_only or self.clear_dumps or self.post_vacuum or self.history))

    def validateUserName(self, userName):
        if not re.search(r'^[a-zA-Z0-9-_]{1,30}$', userName):
            legal_username_str = """
			The username length must be between 1 to 30 characters.
			
			The following characters are allowed:
                1) Lowercase letters (a-z)
                2) Uppercase letters (A-Z) 
                3) Numbers (0-9)
                4) Special characters (- and _)
				
		    Note: whitespace characters are not allowed.
			"""
            raise ExceptionNoStackTraceNeeded(legal_username_str)


    def validatePassword(self, password):
        if not re.search(r'^[a-zA-Z0-9!@#$%^&+=\*\\/\(\)-_~;:\'\"<>\{\}\[\]|\?\.\,`"]{1,40}$', password):
            legal_password_str = """
            The password length must be between 1 to 40 characters.
			
            The following characters are allowed:
                1) Lowercase letters (a-z) 
                2) Uppercase letters (A-Z) 
                3) Numbers (0-9)
                4) Special characters (! @ # $ % ^ & + = * \ / - _ ~ ; : ' " { [ ( < > ) ] } | ? . and ,). 
            """
            raise ExceptionNoStackTraceNeeded(legal_password_str)


    def writeToFile(self, content, filePathName):
        f = open(filePathName, 'w')
        f.write(content)
        f.close()
        os.chmod(filePathName, stat.S_IREAD | stat.S_IWRITE)

    def sshToHosts(self, gpHostsSet, ddboostHostsSet, user, password):
        logger.debug("DDboost host(s): %s" % ddboostHostsSet)
        numberOfDdboostNics = len(ddboostHostsSet)

		# This for loops over all the segment hosts and executes appropriate 'gpddboost --setCredential' command
        for index, host in enumerate(gpHostsSet):                          
            # If more then one NIC is specified, we configure the GP hosts with these ddboost NICs in a round robin manner
            currentDdboostHost = ddboostHostsSet[index % numberOfDdboostNics]            
            ddcmd = 'source ' + os.environ['GPHOME'] + '/greenplum_path.sh; ' + os.environ['GPHOME'] + \
					'/bin/gpddboost  --setCredential --hostname=' + currentDdboostHost + ' --user=' + re.escape(user)
            logger.debug("running gpddboost on host %s: %s --password=*" % (host, ddcmd))

            # Note: the password can't be printed to log/stdout and therefore was concatenated to ddcmd only after priniting it to log
            ddcmd += " --password=" + re.escape(password)
            cmdline = 'gpssh -h %s \'%s\'' % (host, ddcmd)
            # TODO: capture the output of the 'gpddboost' (e.g. with 'subprocess.Popen()' istead of 'os.system()')
            if not os.system(cmdline):
                logger.info("config delivered successfully to %s " % host)
            else:
                logger.warn("problem delivering the config to %s " % host)

        logger.debug("done with sshToHosts. credentials were set on all servers")


    # Verify the DDBoost credentials using the credentials that stored on the Master
    # TODO: verify also all the hosts in self.ddboost_hosts (ping to the hosts, I think there is some Python function for that)
    def _ddboostVerify(self):
        verifyCmdLine = os.environ['GPHOME'] + '/bin/gpddboost --verify'
        logger.debug("Executing gpddboost to verify DDBoost credentials: %s" % verifyCmdLine)
        if not os.system(verifyCmdLine):
            logger.info("The specified DDBoost credentials are OK")
        else:
            raise ExceptionNoStackTraceNeeded('Failed to connect to DD_host with DD_user and the DD_password')

        
    def execute(self):    
        if bool(self.ddboost_hosts) or bool(self.ddboost_user):
            # check that both options are specified
            if not (bool(self.ddboost_hosts) and bool(self.ddboost_user)):
                raise ExceptionNoStackTraceNeeded('For DDBoost config, both options are required --ddboost-host <ddboost_hostname> --ddboost-user <ddboost_user>.')

            # check that no other gpcrondump option was specified
            if self.only_ddboost_options() and not self.ddboost_config_remove:
                self.validateUserName(userName = self.ddboost_user)
                hostset = self.getHostSet(self.getGpArray())
                p = getpass()
                self.validatePassword(password = p)
                self.sshToHosts(hostset, self.ddboost_hosts, self.ddboost_user, p)
                # Since --ddboost-host and --ddboost-user are standalone - verify and then exit
                if self.ddboost_verify is True:
                    self.ddboost_verify_and_exit = True
                else:
                    # If no verification is needed - just exit
                    return 0
            else:
                raise ExceptionNoStackTraceNeeded('The options --ddboost-host and --ddboost-user are standalone. They are NOT used in conjunction with any other gocrondump options (only with --ddboost-verify).')

        # If --ddboost-verify or --ddboost is specified, we check the credentials that stored on the Master
		# In case of --ddboost we force the verification (MPP-17510)
        if bool(self.ddboost_verify) or bool(self.ddboost):
            self._ddboostVerify()

            # Check if we need to exit now
            if self.ddboost_verify_and_exit is True:
                return 0

        # The ddboost_config_remove actually configures the ddboost with dummy values
        if self.ddboost_config_remove:
            if self.ddboost_config_remove and self.only_ddboost_options():
                hostset = self.getHostSet(self.getGpArray())
                # The username and the password must be at least 2 characters long, so we just use 2 spaces for each
                self.sshToHosts(hostset, [' '],'  ','  ')
                return 0;
            else:
                raise ExceptionNoStackTraceNeeded('The option --ddboost-config-remove is standalone. It is NOT used in conjunction with any other gocrondump options.')
            
        if self.clear_dumps_only:
            DeleteOldestDumps(master_datadir = self.master_datadir,
                              master_port = self.master_port,
                              ddboost = self.ddboost).run()
            return
        
        if self.post_script is not None:
            self._validate_run_program()

        self._validate_dump_target()
        
        # final_exit_status := numeric outcome of this python program
        # current_exit_status := numeric outcome of one particular dump,
        #                        in this loop over the -x arguments
        final_exit_status = current_exit_status = 0
        for dump_database in self.dump_databases:
            if self.interactive:
                self._prompt_continue(dump_database)

            if self.pre_vacuum:
                logger.info('Commencing pre-dump vacuum')
                VacuumDatabase(database = dump_database, 
                               master_port = self.master_port).run()

            dump_outcome = DumpDatabase(dump_database = dump_database,
                                        dump_schema = self.dump_schema,
                                        include_dump_tables = self.include_dump_tables,
                                        exclude_dump_tables = self.exclude_dump_tables,
                                        compress = self.compress,
                                        free_space_percent = self.free_space_percent,  
                                        clear_catalog_dumps = self.clear_catalog_dumps,
                                        backup_dir = self.backup_dir,
                                        include_dump_tables_file = self.include_dump_tables_file,
                                        exclude_dump_tables_file = self.exclude_dump_tables_file,
                                        report_dir = self.report_dir,
                                        encoding = self.encoding,
                                        output_options = self.output_options,
                                        batch_default = self.batch_default,
                                        master_datadir = self.master_datadir,
                                        master_port = self.master_port,
                                        ddboost = self.ddboost).run()

            post_dump_outcome = PostDumpDatabase(timestamp_start = dump_outcome['timestamp_start'],
                                                 compress = self.compress,
                                                 backup_dir = self.backup_dir,
                                                 report_dir = self.report_dir,
                                                 batch_default = self.batch_default,
                                                 master_datadir = self.master_datadir,
                                                 master_port = self.master_port,
                                                 ddboost = self.ddboost).run()
            
            current_exit_status = max(dump_outcome['exit_status'], post_dump_outcome['exit_status'])
            final_exit_status = max(final_exit_status, current_exit_status)

            if self.history:
                # This certainly does not belong under DumpDatabase, due to CLI assumptions. Note the use of options_list.
                UpdateHistoryTable(dump_database = dump_database,
                                   options_list = self.options_list,
                                   time_start = dump_outcome['time_start'],
                                   time_end = dump_outcome['time_end'],
                                   dump_exit_status = dump_outcome['exit_status'],
                                   timestamp = post_dump_outcome['timestamp'],
                                   pseudo_exit_status = current_exit_status,
                                   master_port = self.master_port).run()

            if self.dump_global:
                if current_exit_status == 0:
                    DumpGlobal(timestamp = post_dump_outcome['timestamp'],
                               master_datadir = self.master_datadir,
                               backup_dir = self.backup_dir,
                               ddboost = self.ddboost).run()
                else:
                    logger.info("Skipping global dump due to issues during post-dump checks.")
            
    
            deleted_dump_set = None
            if current_exit_status > 0 or INJECT_ROLLBACK:
                if current_exit_status >= 2 and self.rollback:
                    # A current_exit_status of 2 or higher likely indicates either a serious issue in gp_dump
                    # or that the PostDumpDatabase could not determine a timestamp. Thus, do not attempt rollback.
                    logger.warn("Dump request was incomplete, however cannot rollback since no timestamp was found.")
                    MailDumpEvent("Report from gpcrondump on host %s [FAILED]" % self.cur_host,
                                  "Failed for database %s with return code %d, dump files not rolled back, because no new timestamp was found. [Start=%s End=%s] Options passed [%s]" 
                                    % (dump_database, current_exit_status, dump_outcome['time_start'], dump_outcome['time_end'], self.options_list)).run()
                    raise ExceptionNoStackTraceNeeded("Dump incomplete, rollback not processed")
                elif self.rollback:
                    logger.warn("Dump request was incomplete, rolling back dump")
                    DeleteCurrentDump(timestamp = post_dump_outcome['timestamp'],
                                      master_datadir = self.master_datadir,
                                      master_port = self.master_port,
                                      ddboost = self.ddboost).run()
                    MailDumpEvent("Report from gpcrondump on host %s [FAILED]" % self.cur_host,
                                  "Failed for database %s with return code %d dump files rolled back. [Start=%s End=%s] Options passed [%s]" 
                                    % (dump_database, current_exit_status, dump_outcome['time_start'], dump_outcome['time_end'], self.options_list)).run()
                    raise ExceptionNoStackTraceNeeded("Dump incomplete, completed rollback")
                else:
                    logger.warn("Dump request was incomplete, not rolling back because -r option was not supplied")
                    MailDumpEvent("Report from gpcrondump on host %s [FAILED]" % self.cur_host, 
                                  "Failed for database %s with return code %s dump files not rolled back. [Start=%s End=%s] Options passed [%s]" 
                                    % (dump_database, current_exit_status, dump_outcome['time_start'], dump_outcome['time_end'], self.options_list)).run()
                    raise ExceptionNoStackTraceNeeded("Dump incomplete, rollback not processed")
            else:
                if self.clear_dumps:
                    deleted_dump_set = DeleteOldestDumps(master_datadir = self.master_datadir, 
                                                         master_port = self.master_port,
                                                         ddboost = self.ddboost).run()

            if self.post_vacuum:
                logger.info('Commencing post-dump vacuum...')
                VacuumDatabase(database = dump_database,
                               master_port = self.master_port).run()

            self._status_report(dump_database,
                                post_dump_outcome['timestamp'], 
                                dump_outcome, 
                                current_exit_status, 
                                deleted_dump_set)
            
            MailDumpEvent("Report from gpcrondump on host %s [COMPLETED]" % self.cur_host,
                          "Completed for database %s with return code %d [Start=%s End=%s] Options passed [%s]" 
                          % (dump_database, current_exit_status, dump_outcome['time_start'], dump_outcome['time_end'], self.options_list)).run()

        if self.dump_config:
            DumpConfig(backup_dir = self.backup_dir,
                       master_datadir = self.master_datadir,
                       master_port = self.master_port,
                       ddboost = self.ddboost).run()
        
        if self.post_script is not None:
            self._run_program()

        return final_exit_status

    def _prompt_continue(self, dump_database):
        logger.info("---------------------------------------------------")
        logger.info("Master Greenplum Instance dump parameters")
        logger.info("---------------------------------------------------")
        if len(self.include_dump_tables) > 0 or self.include_dump_tables_file is not None:
            logger.info("Dump type                            = Single database, specific table")
            logger.info("---------------------------------------------------")
            if len(self.include_dump_tables) > 0:
                logger.info("Table inclusion list ")
                logger.info("---------------------------------------------------")
                for table in self.include_dump_tables:
                    logger.info("Table name                             = %s" % table)
                logger.info("---------------------------------------------------")
            if self.include_dump_tables_file is not None:
                logger.info("Table file name                      = %s" % self.include_dump_tables_file)
                logger.info("---------------------------------------------------")
        elif len(self.exclude_dump_tables) > 0 or self.exclude_dump_tables_file is not None:  
            logger.info("Dump type                            = Single database, exclude table")
            logger.info("---------------------------------------------------")
            if len(self.exclude_dump_tables) > 0:
                logger.info("Table exclusion list ")
                logger.info("---------------------------------------------------")
                for table in self.exclude_dump_tables:
                    logger.info("Table name                               = %s" % table)
                logger.info("---------------------------------------------------")
            if self.exclude_dump_tables_file is not None:
                logger.info("Table file name                      = %s" % self.exclude_dump_tables_file)
                logger.info("---------------------------------------------------")
        else:
            logger.info("Dump type                            = Single database")
        logger.info("Database to be dumped                = %s" % dump_database)
        if self.dump_schema is not None:
            logger.info("Schema to be dumped                  = %s" % self.dump_schema) 
        if self.backup_dir is not None:
            logger.info("Dump directory                       = %s" % self.backup_dir)
        if self.report_dir is not None:
            logger.info("Dump report directory                = %s" % self.report_dir)
        logger.info("Master port                          = %s" % self.master_port)
        logger.info("Master data directory                = %s" % self.master_datadir)
        if self.post_script is not None:
            logger.info("Run post dump program                = %s" % self.post_script)
        else:
            logger.info("Run post dump program                = Off")
                    # TODO: failed_primary_count. do we care though? the end user shouldn't be continuing if
                    # we've already detected a primary failure. also, that particular validation is currently
                    # occurring after the _prompt_continue step in GpCronDump, as it should be...
                    #if [ $FAILED_PRIMARY_COUNT -ne 0 ];then
                    #    LOG_MSG "[WARN]:-Failed primary count             = $FAILED_PRIMARY_COUNT $WARN_MARK" 1
                    #else
                    #    LOG_MSG "[INFO]:-Failed primary count             = $FAILED_PRIMARY_COUNT" 1
                    #fi

                    # TODO: TRY_COMPRESSION is a result of insufficient disk space, compelling us to 
                    # attempt compression in order to fit on disk
                    #if [ $TRY_COMPRESSION -eq 1 ];then
                    #    LOG_MSG "[INFO]:-Compression override             = On" 1
                    #else
                    #    LOG_MSG "[INFO]:-Compression override             = Off" 1
                    #fi
        on_or_off = {False: "Off", True: "On"}
        logger.info("Rollback dumps                       = %s" % on_or_off[self.rollback])
        logger.info("Dump file compression                = %s" % on_or_off[self.compress])
        logger.info("Clear old dump files                 = %s" % on_or_off[self.clear_dumps])
        logger.info("Update history table                 = %s" % on_or_off[self.history])
        logger.info("Secure config files                  = %s" % on_or_off[self.dump_config])
        logger.info("Dump global objects                  = %s" % on_or_off[self.dump_global])
        if self.pre_vacuum and self.post_vacuum:
            logger.info("Vacuum mode type                     = pre-dump, post-dump")
        elif self.pre_vacuum:
            logger.info("Vacuum mode type                     = pre-dump")
        elif self.post_vacuum:
            logger.info("Vacuum mode type                     = post-dump")
        else:
            logger.info("Vacuum mode type                     = Off")
        if self.clear_catalog_dumps:
            logger.info("Additional options                   = -c")
        if self.free_space_percent is not None:
            logger.info("Ensuring remaining free disk         > %d" % self.free_space_percent)
    
        if not userinput.ask_yesno(None, "\nContinue with Greenplum dump", 'N'):
            raise UserAbortedException()

    def _status_report(self, dump_database, timestamp, dump_outcome, exit_status, deleted_dump_set):
        logger.info("Dump status report")
        logger.info("---------------------------------------------------")
        logger.info("Target database                          = %s" % dump_database)
        logger.info("Dump subdirectory                        = %s" % timestamp[0:8])
        if self.clear_dumps:
            logger.info("Clear old dump directories               = On")
            logger.info("Backup set deleted                       = %s" % deleted_dump_set)
        else:
            logger.info("Clear old dump directories               = Off")
        logger.info("Dump start time                          = %s" % dump_outcome['time_start'])
        logger.info("Dump end time                            = %s" % dump_outcome['time_end'])
        # TODO: logger.info("Number of segments dumped...
        if exit_status != 0:
            if self.rollback:
                logger.warn("Status                                   = FAILED, Rollback Called")
            else:
                logger.warn("Status                                   = FAILED, Rollback Not Called")
            logger.info("See dump log file for errors")
            logger.warn("Dump key                                 = Not Applicable")
        else:
            logger.info("Status                                   = COMPLETED")
            logger.info("Dump key                                 = %s" % timestamp)
        if self.compress:
            logger.info("Dump file compression                    = On")
        else:
            logger.info("Dump file compression                    = Off")
        if self.pre_vacuum and self.post_vacuum:
            logger.info("Vacuum mode type                         = pre-dump, post-dump")
        elif self.pre_vacuum:
            logger.info("Vacuum mode type                         = pre-dump")
        elif self.post_vacuum:
            logger.info("Vacuum mode type                         = post-dump")
        else:
            logger.info("Vacuum mode type                         = Off")
        if exit_status != 0:
            logger.warn("Exit code not zero, check log file")
        else:
            logger.info("Exit code zero, no warnings generated")
        logger.info("---------------------------------------------------")
            

    def _validate_dump_target(self):
        if len(self.dump_databases) > 1:
            if self.dump_schema is not None:
                raise ExceptionNoStackTraceNeeded("Cannot supply schema name if multiple database dumps are requested")
            if len(self.include_dump_tables) > 0 or self.include_dump_tables_file is not None:
                raise ExceptionNoStackTraceNeeded("Cannot supply a table dump list if multiple database dumps are requested")
            if len(self.exclude_dump_tables) > 0 or self.exclude_dump_tables_file is not None:
                raise ExceptionNoStackTraceNeeded("Cannot exclude specific tables if multiple database dumps are requested")
            logger.info("Configuring for multiple database dump")
        
        for dump_database in self.dump_databases:              
            ValidateDatabaseExists(database = dump_database, master_port = self.master_port).run()
                               
        if self.dump_schema is not None:
            ValidateSchemaExists(database = self.dump_databases[0], schema = self.dump_schema,
                                 master_port = self.master_port).run() 
    
    def _validate_run_program(self):
        #Check to see if the file exists
        cmd = Command('Seeking post script', "which %s" % self.post_script)
        cmd.run()
        if cmd.get_results().rc != 0:
            cmd = Command('Seeking post script file', '[ -f %s ]' % self.post_script)
            cmd.run()
            if cmd.get_results().rc != 0:
                logger.warn("Could not locate %s" % self.post_script)
                self.post_script = None
                return
        logger.info("Located %s, will call after dump completed" % self.post_script)

    def _run_program(self):
        Command('Invoking post script', self.post_script).run()

    def _get_master_port(self, datadir):
        """ TODO: This function will be widely used. Move it elsewhere?
            Its necessity is a direct consequence of allowing the -d <master_data_directory> option. From this,
            we need to deduce the proper port so that the GpArrays can be generated properly. """
        logger.debug("Obtaining master's port from master data directory")
        pgconf_dict = pgconf.readfile(datadir + "/postgresql.conf")
        return pgconf_dict.int('port')

def create_parser():
    parser = OptParser(option_class=OptChecker, 
                       version='%prog version $Revision: #5 $',
                       description='Dumps a Greenplum database')

    addStandardLoggingAndHelpOptions(parser, includeNonInteractiveOption=True)

    addTo = OptionGroup(parser, 'Connection opts')
    parser.add_option_group(addTo)
    addMasterDirectoryOptionForSingleClusterProgram(addTo)

    addTo = OptionGroup(parser, 'Dump options')
    addTo.add_option('-r', action='store_true', dest='rollback', default=False,
                     help="Rollback dump files if dump failure detected [default: no rollback]")
    addTo.add_option('-b', action='store_true', dest='bypass_disk_check', default=False,
                     help="Bypass disk space checking [default: check disk space]")
    addTo.add_option('-j', action='store_true', dest='pre_vacuum', default=False,
                     help="Run vacuum before dump starts.")
    addTo.add_option('-k', action='store_true', dest='post_vacuum', default=False,
                     help="Run vacuum after dump has completed successfully.")
    addTo.add_option('-z', action='store_false', dest='compress', default=True,
                     help="Do not use compression [default: use compression]")
    addTo.add_option('-f', dest='free_space_percent', metavar="<0-99>", default=FREE_SPACE_PERCENT,
                     help="Percentage of disk space to ensure is reserved after dump.")
    addTo.add_option('-c', action='store_true', dest='clear_dumps', default=False,
                     help="Clear old dump directories [default: do not clear]. Will remove the oldest dump directory other than the current dump directory.")
    addTo.add_option('-o', action='store_true', dest='clear_dumps_only', default=False,
                     help="Clear dump files only. Do not run a dump. Like -c, this will clear the oldest dump directory, other than the current dump directory.")
    addTo.add_option('-s', dest='dump_schema', metavar="<schema name>",
                     help="Dump the schema contained within the database name supplied via -x")
    addTo.add_option('-x', dest='dump_databases', metavar="<database name,...>",
                     help="Database name(s) to dump. Multiple database names will preclude the schema and table options.")
    addTo.add_option('-g', action='store_true', dest='dump_config', default=False,
                     help="Dump configuration files: postgresql.conf, pg_ident.conf, and pg_hba.conf.")
    addTo.add_option('-G', action='store_true', dest='dump_global', default=False,
                     help="Dump global objects, i.e. user accounts")
    addTo.add_option('-C', action='store_true', dest='clear_catalog_dumps', default=False,
                     help="Clean (drop) schema prior to dump [default: do not clean]")
    addTo.add_option('-R', dest='post_script', metavar="<program name>",
                     help="Run named program after successful dump. Note: program will only be invoked once, even if multi-database dump requested.")
    addTo.add_option('-B', dest='batch_default', type='int', default=DEFAULT_NUM_WORKERS, metavar="<number>",
                     help="Dispatches work to segment hosts in batches of specified size [default: %s]" % DEFAULT_NUM_WORKERS)
    addTo.add_option('-t', action='append', dest='include_dump_tables', metavar="<schema.tableN>",
                     help="Dump the named table(s) for the specified database. -t can be provided multiple times to include multiple tables.")
    addTo.add_option('-T', action='append', dest='exclude_dump_tables', metavar="<schema.tableN>",
                     help="Exclude the named table(s) from the dump. -T can be provided multiple times to exclude multiple tables.")

    # TODO: HACK to remove existing -h
    help = parser.get_option('-h')
    parser.remove_option('-h')
    help._short_opts.remove('-h')
    parser.add_option(help)
    addTo.add_option('-h', action='store_true', dest='history', default=False,
                     help="Record details of database dump in database table %s in database supplied via -x option. Utility will create table if it does not currently exist." % UpdateHistoryTable.HISTORY_TABLE)

    addTo.add_option('-u', dest='backup_dir', metavar="<BACKUPFILEDIR>",
                     help="Directory where backup files are placed [default: data directories]")
    addTo.add_option('-y', dest='report_dir', metavar="<REPORTFILEDIR>",
                     help="Directory where report file is placed [default: master data directory]")
    addTo.add_option('-E', dest='encoding', metavar="<encoding>", help="Dump the data under the given encoding")

    addTo.add_option('--clean', const='--clean', action='append_const', dest='output_options',
                     help="Clean (drop) schema prior to dump")
    addTo.add_option('--inserts', const='--inserts', action='append_const', dest='output_options',
                     help="Dump data as INSERT, rather than COPY, commands.") 
    addTo.add_option('--column-inserts', const='--column-inserts', action='append_const', dest='output_options',
                     help="Dump data as INSERT commands with colun names.")
    addTo.add_option('--oids', const='--oids', action='append_const', dest='output_options',
                     help="Include OIDs in dump.")
    addTo.add_option('--no-owner', const='--no-owner', action='append_const', dest='output_options',
                     help="Do not output commands to set object ownership.")
    addTo.add_option('--no-privileges', const='--no-privileges', action='append_const', dest='output_options', 
                     help="Do not dump privileges (grant/revoke).")
    addTo.add_option('--use-set-session-authorization', const='--use-set-session-authorization', action='append_const', dest='output_options',
                     help="Use SESSION AUTHORIZATION commands instead of ALTER OWNER commands.")
    addTo.add_option('--rsyncable', const='--rsyncable', action='append_const', dest='output_options',
                     help="Pass the --rsyncable option to gzip, if compression is being used.")
    
    addTo.add_option('--table-file', dest='include_dump_tables_file', metavar="<filename>",
                     help="Dump the tables named in this file for the specified database. Option can be used only once.")
    addTo.add_option('--exclude-table-file', dest='exclude_dump_tables_file', metavar="<filename>",
                     help="Exclude the tables named in this file from the dump. Option can be used only once.")

    # TODO: Dead options. Remove eventually.
    addTo.add_option('-i', action='store_true', dest='bypass_cluster_check', default=False, help="No longer supported.")
    addTo.add_option('-p', action='store_true', dest='dump_primaries', default=True, help="No longer supported.")  
    addTo.add_option('-w', dest='backup_set', help="No longer supported.")
   

    parser.add_option_group(addTo)

    ddOpt = OptionGroup(parser, "DDBoost")
    ddOpt.add_option('--ddboost', dest='ddboost', help="Dump to DDBoost using ~/.ddconfig", action="store_true", default=False)
    parser.add_option_group(ddOpt)
   
    # DDBoostConfig options
    ddConfigOpt = OptionGroup(parser, "DDBoostConfig")
    # ddboost-host may have more then one host
    ddConfigOpt.add_option('--ddboost-host', dest='ddboost_hosts', action='append', default=None,
                           help="Configuration of ddboost hostname.")
    ddConfigOpt.add_option('--ddboost-user', dest='ddboost_user', action='store', default=None, 
                           help="Configuration of ddboost user.")
    ddConfigOpt.add_option('--ddboost-config-remove', dest='ddboost_config_remove', action='store_true', default=False,
                           help="Remove ~/.ddconfig file.")
    ddConfigOpt.add_option('--ddboost-verify', dest='ddboost_verify', action='store_true', default=False, 
                           help="Verify DDBoost credentials on DDBoost host.")

    parser.add_option_group(ddConfigOpt)
    
    
    parser.setHelp([
    """
Crontab entry (example):
SHELL=/bin/bash
5 0 * * * . $HOME/.bashrc; $GPHOME/bin/gpcrondump -x template1 -aq >> <name of cronout file>
Set the shell to /bin/bash (default for cron is /bin/sh
Dump the template1 database, start process 5 minutes past midnight on a daily basis
    """,
    """
Mail configuration
This utility will send an email to a list of email addresses contained in a file
named mail_contacts. This file can be located in the GPDB super user home directory
or the utility bin directory ${GPHOME}/bin. The format of the file is one email
address per line. If no mail_contacts file is found in either location, a warning message
will be displayed.
    """
    ])

    return parser

if __name__ == '__main__':
    sys.argv[0] = EXECNAME                                                              # for cli_help
    simple_main(create_parser, GpCronDump, { "pidfilename" : GPCRONDUMP_PID_FILE,
                                             "programNameOverride" : EXECNAME })        # for logger
