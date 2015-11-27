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
import os
import tempfile
from datetime import datetime

from gppylib import gplog
from gppylib.db import dbconn
from gppylib.db.dbconn import execSQL, execSQLForSingleton, UnexpectedRowsError
from gppylib.commands.base import Command, REMOTE, ExecutionError
from gppylib.commands.gp import Psql
from gppylib.commands.unix import getUserName, findCmdInPath, curr_platform, SUNOS
from gppylib.gparray import GpArray
from gppylib.mainUtils import ExceptionNoStackTraceNeeded
from gppylib.operations import Operation
from gppylib.operations.unix import CheckDir, CheckFile, ListFiles, ListFilesByPattern, MakeDir, RemoveFile, RemoveTree, RemoveRemoteTree
from gppylib.operations.utils import RemoteOperation, ParallelOperation

logger = gplog.get_default_logger()

# MPP-15307
# DUMP_DATE dictates the db_dumps/ subdirectory to which gpcrondump will dump.
# It is computed just once to ensure different pieces of logic herein operate on the same subdirectory.
DUMP_DATE = datetime.now().strftime("%Y%m%d")

DUMP_DIR = 'db_dumps'
GLOBAL_PREFIX = 'gp_global_1_1_'
MASTER_DBDUMP_PREFIX = 'gp_dump_1_1_'
MASTER_STATUS_PREFIX = 'gp_dump_status_1_1_'
SEG_DBDUMP_PREFIX = 'gp_dump_0_'
SEG_STATUS_PREFIX = 'gp_dump_status_0_'
COMPRESSION_FACTOR = 12                 # TODO: Where did 12 come from?

INJECT_GP_DUMP_FAILURE = None

class DumpDatabase(Operation):
    # TODO: very verbose constructor = room for improvement. in the parent constructor, we could use kwargs
    # to automatically take in all arguments and perhaps do some data type validation.
    def __init__(self, dump_database, dump_schema, include_dump_tables, exclude_dump_tables, include_dump_tables_file, exclude_dump_tables_file, backup_dir, report_dir, free_space_percent, compress, clear_catalog_dumps, encoding, output_options, batch_default, master_datadir, master_port):
        self.dump_database = dump_database
        self.dump_schema = dump_schema
        self.include_dump_tables = include_dump_tables
        self.exclude_dump_tables = exclude_dump_tables
        self.include_dump_tables_file = include_dump_tables_file,
        self.exclude_dump_tables_file = exclude_dump_tables_file,
        self.backup_dir = backup_dir
        self.report_dir = report_dir
        self.free_space_percent = free_space_percent
        self.compress = compress
        self.clear_catalog_dumps = clear_catalog_dumps
        self.encoding = encoding
        self.output_options = output_options
        self.batch_default = batch_default
        self.master_datadir = master_datadir
        self.master_port = master_port

    def execute(self):
        self.exclude_dump_tables = ValidateDumpDatabase(dump_database = self.dump_database,
                                                        dump_schema = self.dump_schema,
                                                        include_dump_tables = self.include_dump_tables,
                                                        exclude_dump_tables = self.exclude_dump_tables,
                                                        include_dump_tables_file = self.include_dump_tables_file[0],
                                                        exclude_dump_tables_file = self.exclude_dump_tables_file[0],
                                                        backup_dir = self.backup_dir,
                                                        report_dir = self.report_dir,
                                                        free_space_percent = self.free_space_percent,
                                                        compress = self.compress,
                                                        batch_default = self.batch_default,
                                                        master_datadir = self.master_datadir,
                                                        master_port = self.master_port).run()

        if self.backup_dir is not None:
            dump_path = os.path.join(self.backup_dir, DUMP_DIR, DUMP_DATE) 
        else:
            dump_path = os.path.join(DUMP_DIR, DUMP_DATE)
        if self.report_dir is not None:
            report_path = os.path.join(self.report_dir, DUMP_DIR, DUMP_DATE)
        else:
            report_path = os.path.join(self.master_datadir, DUMP_DIR, DUMP_DATE)
        dump_line = "gp_dump -p %d -U %s --gp-d=%s --gp-r=%s --gp-s=p" % (self.master_port, getUserName(), dump_path, report_path) 
        if self.clear_catalog_dumps:
            dump_line += " -c"
        if self.compress:
            logger.info("Adding compression parameter")
            dump_line += " --gp-c"
        if self.encoding is not None:
            logger.info("Adding encoding %s" % self.encoding)
            dump_line += " --encoding=%s" % self.encoding

        """
        AK: Some ridiculous escaping here. I apologize.
        These options get passed-through gp_dump to gp_dump_agent.
        Commented out lines use escaping that would be reasonable, if gp_dump escaped properly.
        """
        if self.dump_schema is not None:
            logger.info("Adding schema name %s" % self.dump_schema)
            dump_line += " -n \"\\\"%s\\\"\"" % self.dump_schema
            #dump_line += " -n \"%s\"" % self.dump_schema
        dump_line += " %s" % self.dump_database
        for dump_table in self.include_dump_tables:
            schema, table = dump_table.split('.')
            dump_line += " --table=\"\\\"%s\\\"\".\"\\\"%s\\\"\"" % (schema, table)
            #dump_line += " --table=\"%s\".\"%s\"" % (schema, table)
        for dump_table in self.exclude_dump_tables:
            schema, table = dump_table.split('.')
            dump_line += " --exclude-table=\"\\\"%s\\\"\".\"\\\"%s\\\"\"" % (schema, table)
            #dump_line += " --exclude-table=\"%s\".\"%s\"" % (schema, table)
        if self.include_dump_tables_file[0] is not None:
            dump_line += " --table-file=%s" % self.include_dump_tables_file
        if self.exclude_dump_tables_file[0] is not None:
            dump_line += " --exclude-table-file=%s" % self.exclude_dump_tables_file
        for opt in self.output_options:
            dump_line += " %s" % opt
        logger.info("Dump command line %s" % dump_line)
        logger.info("Starting dump process")
        start = datetime.now()
        cmd = Command('Invoking gp_dump', dump_line)
        cmd.run()   
        rc = cmd.get_results().rc
        if INJECT_GP_DUMP_FAILURE is not None:
            rc = INJECT_GP_DUMP_FAILURE
        if rc != 0:
            logger.warn("Dump process returned exit code %d" % rc)
        else:
            logger.info("Dump process returned exit code 0")        
        end = datetime.now()
        return {'timestamp_start': start.strftime("%Y%m%d%H%M%S"),
                'time_start': start.strftime("%H:%M:%S"),
                'time_end': end.strftime("%H:%M:%S"),
                'exit_status': rc}



class PostDumpDatabase(Operation):
    def __init__(self, timestamp_start, compress, backup_dir, report_dir, batch_default, master_datadir, master_port):
        self.timestamp_start = timestamp_start
        self.compress = compress
        self.backup_dir = backup_dir
        self.report_dir = report_dir
        self.batch_default = batch_default
        self.master_datadir = master_datadir
        self.master_port = master_port

    def execute(self):
        # First, get timestamp from .rpt file
        path = self.report_dir if self.report_dir is not None else self.master_datadir
        path = os.path.join(path, DUMP_DIR, DUMP_DATE)
        reports = ListFilesByPattern(path, "gp_dump_*.rpt").run()
        if not reports:
            logger.error("Could not locate a report file on master.")
            return {'exit_status': 2, 'timestamp': 'n/a'}
        reports.sort()
        reports.reverse()
        report = reports[0]
        timestamp = report[-18:-4]    # last 14 digits, just before .rpt
        if int(timestamp) < int(self.timestamp_start):
            logger.error("Could not locate the newly generated report file on master.")
            return {'exit_status': 2, 'timestamp': 'n/a'}
        logger.info("Timestamp key = %s" % timestamp)

        # Check master dumps
        path = self.backup_dir if self.backup_dir is not None else self.master_datadir
        path = os.path.join(path, DUMP_DIR, DUMP_DATE)
        status_file = os.path.join(path, "%s%s" % (MASTER_STATUS_PREFIX, timestamp))
        dump_file = os.path.join(path, "%s%s" % (MASTER_DBDUMP_PREFIX, timestamp))
        if self.compress: dump_file += ".gz"
        try:
            PostDumpSegment(status_file = status_file,
                            dump_file = dump_file).run()
        except NoStatusFile, e:
            logger.warn('Status file %s not found on master' % status_file)
            return {'exit_status': 1, 'timestamp': timestamp}
        except StatusFileError, e:
            logger.warn('Status file %s on master indicates errors' % status_file)
            return {'exit_status': 1, 'timestamp': timestamp}
        except NoDumpFile, e:
            logger.warn('Dump file %s not found on master' % dump_file)
            return {'exit_status': 1, 'timestamp': timestamp}
        else:
            logger.info('Checked master status file and master dump file.')
            
        # Perform similar checks for primary segments
        operations = []
        gparray = GpArray.initFromCatalog(dbconn.DbURL(port = self.master_port), utility=True)
        segs = [seg for seg in gparray.getDbList() if seg.isSegmentPrimary(current_role=True)]
        for seg in segs:
            path = self.backup_dir if self.backup_dir is not None else seg.getSegmentDataDirectory()
            path = os.path.join(path, DUMP_DIR, DUMP_DATE)
            status_file = os.path.join(path, "%s%d_%s" % (SEG_STATUS_PREFIX, seg.getSegmentDbId(), timestamp))
            dump_file = os.path.join(path, "%s%d_%s" % (SEG_DBDUMP_PREFIX, seg.getSegmentDbId(), timestamp))   
            if self.compress: dump_file += ".gz"
            operations.append(RemoteOperation(PostDumpSegment(status_file = status_file,
                                                              dump_file = dump_file),
                                              seg.getSegmentHostName()))

        ParallelOperation(operations, self.batch_default).run()

        success = 0
        for remote in operations:
            host = remote.host
            status_file = remote.operation.status_file
            dump_file = remote.operation.dump_file
            try:
                remote.get_ret()
            except NoStatusFile, e:
                logger.warn('Status file %s not found on %s' % (status_file, host))
            except StatusFileError, e:
                logger.warn('Status file %s on %s indicates errors' % (status_file, host))
            except NoDumpFile, e:
                logger.warn('Dump file %s not found on %s' % (dump_file, host))
            else:
                success += 1

        if success < len(operations):
            logger.warn("Dump was unsuccessful. %d segment(s) failed post-dump checks." % (len(operations) - success))
            return {'exit_status': 1, 'timestamp': timestamp}
        return {'exit_status': 0, 'timestamp': timestamp}



class PostDumpSegment(Operation):
    def __init__(self, status_file, dump_file):
        self.status_file = status_file
        self.dump_file = dump_file
    def execute(self):  
        # Ensure that status file exists
        if not CheckFile(self.status_file).run():
            logger.error('Could not locate status file: %s' % self.status_file)
            raise NoStatusFile()
        # Ensure that status file indicates successful dump
        with open(self.status_file, 'r') as f:
            for line in f:
                if line.find("Finished successfully") != -1:
                    break
            else:
                logger.error("Status report file indicates errors: %s" % self.status_file)
                for line in f:
                    logger.info(line)
                logger.error("Status file contents dumped to log file")
                raise StatusFileError()
        # Ensure that dump file exists
        if not CheckFile(self.dump_file).run():
            logger.error("Could not locate dump file: %s" % self.dump_file)
            raise NoDumpFile()
class NoStatusFile(Exception): pass
class StatusFileError(Exception): pass
class NoDumpFile(Exception): pass



class ValidateDumpDatabase(Operation):
    def __init__(self, dump_database, dump_schema, include_dump_tables, exclude_dump_tables, 
                 include_dump_tables_file, exclude_dump_tables_file, backup_dir, report_dir, 
                 free_space_percent, compress, batch_default, master_datadir, master_port):
        self.dump_database = dump_database
        self.dump_schema = dump_schema
        self.include_dump_tables = include_dump_tables
        self.exclude_dump_tables = exclude_dump_tables
        self.include_dump_tables_file = include_dump_tables_file
        self.exclude_dump_tables_file = exclude_dump_tables_file
        self.backup_dir = backup_dir
        self.report_dir = report_dir
        self.free_space_percent = free_space_percent
        self.compress = compress
        self.batch_default = batch_default
        self.master_datadir = master_datadir
        self.master_port = master_port
    def execute(self):
        ValidateDatabaseExists(database = self.dump_database,
                               master_port = self.master_port).run()

        if self.dump_schema is not None:
            ValidateSchemaExists(database = self.dump_database,
                                 schema = self.dump_schema,
                                 master_port = self.master_port).run()

        ValidateCluster(master_port = self.master_port).run()

        ValidateAllDumpDirs(backup_dir = self.backup_dir,
                            report_dir = self.report_dir,
                            batch_default = self.batch_default, 
                            master_datadir = self.master_datadir,
                            master_port = self.master_port).run()

        self.exclude_dump_tables = ValidateDumpTargets(dump_database = self.dump_database,
                                                       dump_schema = self.dump_schema,
                                                       include_dump_tables = self.include_dump_tables,
                                                       exclude_dump_tables = self.exclude_dump_tables,
                                                       include_dump_tables_file = self.include_dump_tables_file,
                                                       exclude_dump_tables_file = self.exclude_dump_tables_file,
                                                       master_port = self.master_port).run()

        if self.free_space_percent is not None:
            ValidateDiskSpace(free_space_percent = self.free_space_percent,
                              compress = self.compress,
                              dump_database = self.dump_database,
                              include_dump_tables = self.include_dump_tables,
                              batch_default = self.batch_default,
                              master_port = self.master_port).run()

        return self.exclude_dump_tables



class ValidateDiskSpace(Operation):
    # TODO: this doesn't take into account that multiple segments may be dumping to the same logical disk.
    def __init__(self, free_space_percent, compress, dump_database, include_dump_tables, batch_default, master_port):
        self.free_space_percent = free_space_percent
        self.compress = compress
        self.dump_database = dump_database
        self.include_dump_tables = include_dump_tables
        self.batch_default = batch_default
        self.master_port = master_port
    def execute(self):
        ValidateGpToolkit(database = self.dump_database,
                          master_port = self.master_port).run()

        operations = []
        gparray = GpArray.initFromCatalog(dbconn.DbURL(port = self.master_port), utility=True)
        segs = [seg for seg in gparray.getDbList() if seg.isSegmentPrimary(current_role=True)]
        for seg in segs:
            operations.append(RemoteOperation(ValidateSegDiskSpace(free_space_percent = self.free_space_percent,
                                                                   compress = self.compress,
                                                                   dump_database = self.dump_database,
                                                                   include_dump_tables = self.include_dump_tables,
                                                                   datadir = seg.getSegmentDataDirectory(),
                                                                   segport = seg.getSegmentPort()),
                                              seg.getSegmentHostName()))

        ParallelOperation(operations, self.batch_default).run()
    
        success = 0
        for remote in operations:
            host = remote.host
            try:
                remote.get_ret()
            except NotEnoughDiskSpace, e:
                logger.error("%s has insufficient disk space. [Need: %dK, Free %dK]" % (host, e.needed_space, e.free_space))
            else:
                success += 1
        if success < len(operations):
            raise ExceptionNoStackTraceNeeded("Cannot continue. %d segment(s) failed disk space checks" % (len(operations) - success))



class ValidateSegDiskSpace(Operation):
    # TODO: this estimation of needed space needs work. it doesn't include schemas or exclusion tables.
    def __init__(self, free_space_percent, compress, dump_database, include_dump_tables, datadir, segport):
        self.free_space_percent = free_space_percent
        self.compress = compress
        self.dump_database = dump_database
        self.include_dump_tables = include_dump_tables
        self.datadir = datadir
        self.segport = segport
    def execute(self):
        needed_space = 0
        dburl = dbconn.DbURL(dbname=self.dump_database, port=self.segport)
        conn = None
        try:
            conn = dbconn.connect(dburl, utility=True)
            if self.include_dump_tables:
                for dump_table in self.include_dump_tables:
                    needed_space += execSQLForSingleton(conn, "SELECT pg_relation_size('%s')/1024;" % dump_table)
            else:
                needed_space = execSQLForSingleton(conn, "SELECT pg_database_size('%s')/1024;" % self.dump_database)
        finally:
            if conn is not None:
                conn.close()
        if self.compress:
            needed_space = needed_space / COMPRESSION_FACTOR
        
        # get free available space
        stat_res = os.statvfs(self.datadir);
        free_space = (stat_res.f_bavail * stat_res.f_frsize) / 1024

        if free_space == 0 or (free_space - needed_space) / free_space < self.free_space_percent / 100:
            logger.error("Disk space: [Need: %dK, Free %dK]" % (needed_space, free_space))
            raise NotEnoughDiskSpace(free_space, needed_space)
        logger.info("Disk space: [Need: %dK, Free %dK]" % (needed_space, free_space))
class NotEnoughDiskSpace(Exception): 
    def __init__(self, free_space, needed_space):
        self.free_space, self.needed_space = free_space, needed_space
        Exception.__init__(self, free_space, needed_space)



class ValidateGpToolkit(Operation):
    def __init__(self, database, master_port):
        self.database = database
        self.master_port = master_port
    def execute(self):
        dburl = dbconn.DbURL(dbname=self.database, port=self.master_port)
        conn = None
        try:
            conn = dbconn.connect(dburl)
            count = execSQLForSingleton(conn, "select count(*) from pg_class, pg_namespace where pg_namespace.nspname = 'gp_toolkit' and pg_class.relnamespace = pg_namespace.oid")
        finally:
            if conn is not None:
                conn.close()
        if count > 0:
            logger.debug("gp_toolkit exists within database %s." % self.database)
            return
        logger.info("gp_toolkit not found. Installing...")
        Psql('Installing gp_toolkit', 
             filename='$GPHOME/share/postgresql/gp_toolkit.sql',
             database=self.database,
             port=self.master_port).run(validateAfter=True)


        
class ValidateAllDumpDirs(Operation):
    def __init__(self, backup_dir, report_dir, batch_default, master_datadir, master_port):
        self.backup_dir = backup_dir
        self.report_dir = report_dir
        self.batch_default = batch_default
        self.master_datadir = master_datadir
        self.master_port = master_port
    def execute(self):
        if self.backup_dir is not None and self.report_dir is not None:
            master_dirs_to_check = [self.backup_dir, self.report_dir]
        elif self.backup_dir is not None:
            master_dirs_to_check = [self.backup_dir, self.master_datadir]
        elif self.report_dir is not None:
            master_dirs_to_check = [self.report_dir, self.master_datadir]
        else:
            master_dirs_to_check = [self.master_datadir]

        for dir in master_dirs_to_check:
            try:
                ValidateDumpDirs(dir).run()
            except DumpDirCreateFailed, e:
                raise ExceptionNoStackTraceNeeded('Could not create %s on master. Cannot continue.' % dir)
            except DumpDirNotWritable, e:
                raise ExceptionNoStackTraceNeeded('Could not write to %s on master. Cannot continue.' % dir)
            else:
                logger.info('Checked %s on master' % dir)
            
        # Check backup target on segments (either master_datadir or backup_dir, if present)
        operations = []
        gparray = GpArray.initFromCatalog(dbconn.DbURL(port = self.master_port), utility=True)
        segs = [seg for seg in gparray.getDbList() if seg.isSegmentPrimary(current_role=True)]
        for seg in segs:
            dir = self.backup_dir if self.backup_dir is not None else seg.getSegmentDataDirectory()
            operations.append(RemoteOperation(ValidateDumpDirs(dir), seg.getSegmentHostName()))

        ParallelOperation(operations, self.batch_default).run()

        success = 0
        for remote in operations:
            dir = remote.operation.dir
            host = remote.host
            try:
                remote.get_ret()
            except DumpDirCreateFailed, e:
                logger.error("Could not create %s on %s." % (dir, host))
            except DumpDirNotWritable, e:
                logger.error("Could not write to %s on %s." % (dir, host))
            else:
                success += 1

        if success < len(operations):
            raise ExceptionNoStackTraceNeeded("Cannot continue. %d segment(s) failed directory checks" % (len(operations) - success))



class ValidateDumpDirs(Operation):
    def __init__(self, dir):
        self.dir = dir
    def execute(self):
        path = os.path.join(self.dir, DUMP_DIR, DUMP_DATE)
        exists = CheckDir(path).run()
        if exists:
            logger.info("Directory %s exists" % path)
        else:
            logger.info("Directory %s not found, will try to create" % path)
            try:
                MakeDir(path).run()
            except OSError, e:
                logger.exception("Could not create directory %s" % path)
                raise DumpDirCreateFailed()
            else:
                logger.info("Created %s" % path)
        try:
            with tempfile.TemporaryFile(dir=path) as f:
                pass
        except Exception, e:
            logger.exception("Cannot write to %s" % path)
            raise DumpDirNotWritable()
class DumpDirCreateFailed(Exception): pass
class DumpDirNotWritable(Exception): pass



class ValidateDumpTargets(Operation):
    def __init__(self, dump_database, dump_schema, include_dump_tables, exclude_dump_tables, 
                 include_dump_tables_file, exclude_dump_tables_file, master_port):
        self.dump_database = dump_database
        self.dump_schema = dump_schema
        self.include_dump_tables = include_dump_tables
        self.exclude_dump_tables = exclude_dump_tables
        self.include_dump_tables_file = include_dump_tables_file
        self.exclude_dump_tables_file = exclude_dump_tables_file
        self.master_port = master_port
    def execute(self):
        if ((len(self.include_dump_tables) > 0 or (self.include_dump_tables_file is not None)) and 
            (len(self.exclude_dump_tables) > 0 or (self.exclude_dump_tables_file is not None))):
            raise ExceptionNoStackTraceNeeded("Cannot use -t/--table-file and -T/--exclude-table-file options at same time")
        elif len(self.include_dump_tables) > 0 or self.include_dump_tables_file is not None:
            logger.info("Configuring for single-database, include-table dump")
            ValidateIncludeTargets(dump_database = self.dump_database,
                                   dump_schema = self.dump_schema,
                                   include_dump_tables = self.include_dump_tables,
                                   include_dump_tables_file = self.include_dump_tables_file,
                                   master_port = self.master_port).run()
        elif len(self.exclude_dump_tables) > 0 or self.exclude_dump_tables_file is not None:
            logger.info("Configuring for single-database, exclude-table dump")
            self.exclude_dump_tables = ValidateExcludeTargets(dump_database = self.dump_database,
                                                              dump_schema = self.dump_schema,
                                                              exclude_dump_tables = self.exclude_dump_tables,
                                                              exclude_dump_tables_file = self.exclude_dump_tables_file,
                                                              master_port = self.master_port).run()
        else:
            logger.info("Configuring for single database dump")
        return self.exclude_dump_tables



class ValidateIncludeTargets(Operation):
    def __init__(self, dump_database, dump_schema, include_dump_tables, include_dump_tables_file, master_port):
        self.dump_database = dump_database
        self.dump_schema = dump_schema
        self.include_dump_tables = include_dump_tables
        self.include_dump_tables_file = include_dump_tables_file
        self.master_port = master_port
    def execute(self):
        
        dump_tables = []
        for dump_table in self.include_dump_tables:
            dump_tables.append(dump_table)
                
        if self.include_dump_tables_file is not None:
            include_file = open(self.include_dump_tables_file, 'rU')   
            if not include_file:
                raise ExceptionNoStackTraceNeeded("Can't open file %s" % include_dump_tables_file);
            for line in include_file:
                dump_tables.append(line.strip('\n'));
            include_file.close()
     
        for dump_table in dump_tables:
            if '.' not in dump_table:
                raise ExceptionNoStackTraceNeeded("No schema name supplied for table %s" % dump_table)
            schema, table = dump_table.split('.')
            exists = CheckTableExists(schema = schema, 
                                      table = table, 
                                      database = self.dump_database, 
                                      master_port = self.master_port).run()
            if exists:
                logger.info("Located table %s in %s database" % (dump_table, self.dump_database))
            else:
                raise ExceptionNoStackTraceNeeded("Table %s does not exist in %s database" % (dump_table, self.dump_database))
            if self.dump_schema is not None:
                if self.dump_schema != schema:
                    raise ExceptionNoStackTraceNeeded("Schema name %s not same as schema on %s" % (self.dump_schema, dump_table))



class ValidateExcludeTargets(Operation):
    def __init__(self, dump_database, dump_schema, exclude_dump_tables, exclude_dump_tables_file, master_port):
        self.dump_database = dump_database
        self.dump_schema = dump_schema
        self.exclude_dump_tables = exclude_dump_tables
        self.exclude_dump_tables_file = exclude_dump_tables_file
        self.master_port = master_port
    def execute(self):
        rebuild_excludes = []
        
        dump_tables = []
        for dump_table in self.exclude_dump_tables:
            dump_tables.append(dump_table)
                 
        if self.exclude_dump_tables_file is not None:
            exclude_file = open(self.exclude_dump_tables_file, 'rU')   
            if not exclude_file:
                raise ExceptionNoStackTraceNeeded("Can't open file %s" % exclude_dump_tables_file);
            for line in exclude_file:
                dump_tables.append(line.strip('\n'));
            exclude_file.close()
        
        for dump_table in dump_tables:
            if '.' not in dump_table:
                raise ExceptionNoStackTraceNeeded("No schema name supplied for exclude table %s" % dump_table)   
            schema, table = dump_table.split('.')
            exists = CheckTableExists(schema = schema, 
                                      table = table, 
                                      database = self.dump_database, 
                                      master_port = self.master_port).run()
            if exists:
                if self.dump_schema != schema:
                    logger.info("Adding table %s to exclude list" % dump_table)
                    rebuild_excludes.append(dump_table)
                else:
                    logger.warn("Schema dump request and exclude table %s not in that schema, ignoring" % dump_table)
            else:
                logger.warn("Exclude table %s does not exist in %s database, ignoring" % (dump_table, self.dump_database))
        if len(rebuild_excludes) == 0:
            logger.warn("All exclude table names have been removed due to issues, see log file")
        return self.exclude_dump_tables



class ValidateDatabaseExists(Operation):
    """ TODO: move this to gppylib.operations.common? """
    def __init__(self, database, master_port):
        self.master_port = master_port
        self.database = database
    def execute(self):
        conn = None
        try:
            dburl = dbconn.DbURL(port = self.master_port )
            conn = dbconn.connect(dburl)
            count = execSQLForSingleton(conn, "select count(*) from pg_database where datname='%s';" % self.database)
            if count == 0:
                raise ExceptionNoStackTraceNeeded("Database %s does not exist." % self.database)
        finally:
            if conn is not None:
                conn.close()



class ValidateSchemaExists(Operation):
    """ TODO: move this to gppylib.operations.common? """
    def __init__(self, database, schema, master_port):
        self.database = database
        self.schema = schema
        self.master_port = master_port
    def execute(self):
        conn = None
        try:
            dburl = dbconn.DbURL(port = self.master_port, dbname = self.database)
            conn = dbconn.connect(dburl)
            count = execSQLForSingleton(conn, "select count(*) from pg_namespace where nspname='%s';" % self.schema)
            if count == 0:
                raise ExceptionNoStackTraceNeeded("Schema %s does not exist in database %s." % (self.schema, self.database))
        finally:
            if conn is not None:
                conn.close()



class CheckTableExists(Operation):
    """ TODO: move this to gppylib.operations.common? """
    def __init__(self, database, schema, table, master_port):
        self.database = database
        self.schema = schema
        self.table = table
        self.master_port = master_port
    def execute(self):
        try:
            dburl = dbconn.DbURL(port=self.master_port, dbname=self.database)
            conn = dbconn.connect(dburl)
            count = execSQLForSingleton(conn, "select count(*) from pg_class, pg_namespace where pg_class.relname = '%s' and pg_class.relnamespace = pg_namespace.oid and pg_namespace.nspname = '%s'" % (self.table, self.schema))
            return count > 0
        finally:
            if conn is not None:
                conn.close()


class ValidateCluster(Operation):
    def __init__(self, master_port):
        self.master_port = master_port
    def execute(self):
        gparray = GpArray.initFromCatalog(dbconn.DbURL(port = self.master_port), utility=True)
        failed_segs = [seg for seg in gparray.getDbList() if seg.isSegmentPrimary(current_role=True) and seg.isSegmentDown()]
        if len(failed_segs) != 0:
            logger.warn("Failed primary segment instances detected")
            failed_dbids = [seg.getSegmentDbid() for seg in failed_segs]
            raise ExceptionNoStackTraceNeeded("Detected failed segment(s) with dbid=%s" % ",".join(failed_dbids))



class UpdateHistoryTable(Operation):
    HISTORY_TABLE = "public.gpcrondump_history"
    def __init__(self, dump_database, time_start, time_end, options_list, timestamp, dump_exit_status, pseudo_exit_status, master_port):
        self.dump_database = dump_database
        self.time_start = time_start
        self.time_end = time_end
        self.options_list = options_list
        self.timestamp = timestamp
        self.dump_exit_status = dump_exit_status
        self.pseudo_exit_status = pseudo_exit_status
        self.master_port = master_port
    def execute(self):
        schema, table = UpdateHistoryTable.HISTORY_TABLE.split('.')
        exists = CheckTableExists(database = self.dump_database, 
                                  schema = schema,
                                  table = table,
                                  master_port = self.master_port).run()
        if not exists:
            conn = None
            CREATE_HISTORY_TABLE = """ create table %s (rec_date timestamp, start_time char(8), end_time char(8), options text, dump_key varchar(20), dump_exit_status smallint, script_exit_status smallint, exit_text varchar(10)) distributed by (rec_date); """ % UpdateHistoryTable.HISTORY_TABLE
            try:
                dburl = dbconn.DbURL(port=self.master_port, dbname=self.dump_database)
                conn = dbconn.connect(dburl)
                execSQL(conn, CREATE_HISTORY_TABLE)
                conn.commit()
            except Exception, e:
                logger.exception("Unable to create %s in %s database" % (UpdateHistoryTable.HISTORY_TABLE, self.dump_database))
                return
            else:
                logger.info("Created %s in %s database" % (UpdateHistoryTable.HISTORY_TABLE, self.dump_database))
            finally:
                if conn is not None:
                    conn.close()

        translate_rc_to_msg = { 0: "COMPLETED", 1: "WARNING", 2: "FATAL" }
        exit_msg = translate_rc_to_msg[self.pseudo_exit_status]
        APPEND_HISTORY_TABLE = """ insert into %s values (now(), '%s', '%s', '%s', '%s', %d, %d, '%s'); """ % (UpdateHistoryTable.HISTORY_TABLE, self.time_start, self.time_end, self.options_list, self.timestamp, self.dump_exit_status, self.pseudo_exit_status, exit_msg)
        conn = None
        try:
            dburl = dbconn.DbURL(port=self.master_port, dbname=self.dump_database)
            conn = dbconn.connect(dburl)
            execSQL(conn, APPEND_HISTORY_TABLE)
            conn.commit()
        except Exception, e:
            logger.exception("Failed to insert record into %s in %s database" % (UpdateHistoryTable.HISTORY_TABLE, self.dump_database))
        else:
            logger.info("Inserted dump record into %s in %s database" % (UpdateHistoryTable.HISTORY_TABLE, self.dump_database))
        finally:
            if conn is not None:
                conn.close()


class DumpGlobal(Operation):
    def __init__(self, timestamp, master_datadir, backup_dir):
        self.timestamp = timestamp
        self.master_datadir = master_datadir
        self.backup_dir = backup_dir

    def execute(self):
        logger.info("Commencing pg_catalog dump")
        if self.backup_dir is not None:
            global_file = os.path.join(self.backup_dir, DUMP_DIR, DUMP_DATE, "%s%s" % (GLOBAL_PREFIX, self.timestamp))
        else:
            global_file = os.path.join(self.master_datadir, DUMP_DIR, DUMP_DATE, "%s%s" % (GLOBAL_PREFIX, self.timestamp))
        Command('Dump global objects', 
                "pg_dumpall -g --gp-syntax > %s" % global_file).run(validateAfter=True)


class DumpConfig(Operation):
    # TODO: Should we really just give up if one of the tars fails? 
    # TODO: WorkerPool
    def __init__(self, backup_dir, master_datadir, master_port):
        self.backup_dir = backup_dir
        self.master_datadir = master_datadir
        self.master_port = master_port

    def execute(self):
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        config_backup_file = "gp_master_config_files_%s.tar" % timestamp
        if self.backup_dir is not None:
            path = os.path.join(self.backup_dir, DUMP_DIR, DUMP_DATE, config_backup_file)
        else:
            path = os.path.join(self.master_datadir, DUMP_DIR, DUMP_DATE, config_backup_file)
        logger.info("Dumping master config files")
        Command("Dumping master configuration files",
                "tar cf %s %s/*.conf" % (path, self.master_datadir)).run(validateAfter=True)

        logger.info("Dumping segment config files")
        gparray = GpArray.initFromCatalog(dbconn.DbURL(port = self.master_port), utility=True)
        primaries = [seg for seg in gparray.getDbList() if seg.isSegmentPrimary(current_role=True)]
        for seg in primaries:
            config_backup_file = "gp_segment_config_files_0_%d_%s.tar" % (seg.getSegmentDbId(), timestamp)
            if self.backup_dir is not None:
                path = os.path.join(self.backup_dir, DUMP_DIR, DUMP_DATE, config_backup_file)
            else:
                path = os.path.join(seg.getSegmentDataDirectory(), DUMP_DIR, DUMP_DATE, config_backup_file)
            host = seg.getSegmentHostName()
            Command("Dumping segment config files",
                    "tar cf %s %s/*.conf" % (path, seg.getSegmentDataDirectory()),
                    ctxt=REMOTE,
                    remoteHost=host).run(validateAfter=True)


class DeleteCurrentDump(Operation):
    def __init__(self, timestamp, master_datadir, master_port):
        self.timestamp = timestamp
        self.master_datadir = master_datadir
        self.master_port = master_port

    def execute(self):
        try:
            DeleteCurrentSegDump(self.timestamp, self.master_datadir).run()
        except OSError, e:
            logger.warn("Error encountered during deletion of %s on master" % self.timestamp)
        gparray = GpArray.initFromCatalog(dbconn.DbURL(port = self.master_port), utility=True)
        segs = [seg for seg in gparray.getDbList() if seg.isSegmentPrimary(current_role=True)]
        for seg in segs:
            try:
                RemoteOperation(DeleteCurrentSegDump(self.timestamp, seg.getSegmentDataDirectory()), 
                                seg.getSegmentHostName()).run()
            except OSError, e:
                logger.warn("Error encountered during deletion of %s on %s" % (self.timestamp, seg.getSegmentHostName()))


class DeleteCurrentSegDump(Operation):
    """ TODO: Improve with grouping by host. """
    def __init__(self, timestamp, datadir):
        self.timestamp = timestamp
        self.datadir = datadir
    def execute(self):
        path = os.path.join(self.datadir, DUMP_DIR, DUMP_DATE)
        filenames = ListFilesByPattern(path, "*%s*" % self.timestamp).run()
        for filename in filenames:
            RemoveFile(os.path.join(path, filename)).run()



class DeleteOldestDumps(Operation):
    # TODO: This Operation isn't consuming backup_dir. Should it? 
    def __init__(self, master_datadir, master_port):
        self.master_datadir = master_datadir
        self.master_port = master_port

    def execute(self):
        dburl = dbconn.DbURL(port=self.master_port)
        old_dates = ListFiles(os.path.join(self.master_datadir, DUMP_DIR)).run()
        try: 
            old_dates.remove(DUMP_DATE)
        except ValueError, e:            # DUMP_DATE was not found in old_dates
            pass
        if len(old_dates) == 0:
            logger.info("No old backup sets to remove")
            return
        old_dates.sort()
        old_date = old_dates[0]

        # This will avoid the problem where we might accidently end up deleting local backup files
        logger.info("Preparing to remove dump %s from all hosts" % old_date)
        path = os.path.join(self.master_datadir, DUMP_DIR, old_date)

        try:
            RemoveTree(path).run()
        except OSError, e:
            logger.warn("Error encountered during deletion of %s" % path)
        gparray = GpArray.initFromCatalog(dbconn.DbURL(port = self.master_port), utility=True)
        primaries = [seg for seg in gparray.getDbList() if seg.isSegmentPrimary(current_role=True)]
        for seg in primaries:
            path = os.path.join(seg.getSegmentDataDirectory(), DUMP_DIR, old_date)
            try:
                RemoveRemoteTree(path, seg.getSegmentHostName()).run()
            except ExecutionError, e:
                logger.warn("Error encountered during deletion of %s on %s" % (path, seg.getSegmentHostName()))

        return old_date



class VacuumDatabase(Operation):
    # TODO: move this to gppylib.operations.common? 
    def __init__(self, database, master_port):
        self.database = database
        self.master_port = master_port
    def execute(self):
        conn = None
        logger.info('Commencing vacuum of %s database, please wait' % self.database)
        try:
            dburl = dbconn.DbURL(port=self.master_port, dbname=self.database)
            conn = dbconn.connect(dburl)
            cursor = conn.cursor()
            cursor.execute("commit")                          # hack to move drop stmt out of implied transaction
            cursor.execute("vacuum")
            cursor.close()
        except Exception, e:
            logger.exception('Error encountered with vacuum of %s database' % self.database)
        else:
            logger.info('Vacuum of %s completed without error' % self.database)
        finally:
            if conn is not None:
                conn.close()



class MailDumpEvent(Operation):
    def __init__(self, subject, message):
        self.subject = subject
        self.message = message
    def execute(self):
        if "HOME" not in os.environ or "GPHOME" not in os.environ:
            logger.warn("Could not find mail_contacts file. Set $HOME and $GPHOME.")
            return
        mail_file = os.path.join(os.environ["GPHOME"], "bin", "mail_contacts")
        home_mail_file = os.path.join(os.environ["HOME"], "mail_contacts")
        contacts_file = None
        if CheckFile(home_mail_file).run():
            contacts_file = home_mail_file
        elif CheckFile(mail_file).run():
            contacts_file = mail_file
        else:
            logger.warn("Found neither %s nor %s" % (mail_file, home_mail_file))
            logger.warn("Unable to send dump email notification")
            logger.info("To enable email notification, create %s or %s containing required email addresses" % (mail_file, home_mail_file))
            return
        to_addrs = None
        with open(contacts_file, 'r') as f:
            to_addrs = [line.strip() for line in f]
        MailEvent(subject = self.subject,
                  message = self.message,
                  to_addrs = to_addrs).run()



class MailEvent(Operation):
    # TODO: move this to gppylib.operations.common? 
    def __init__(self, subject, message, to_addrs):
        if isinstance(to_addrs, str):
            to_addrs = [to_addrs]
        self.subject = subject
        self.message = message
        self.to_addrs = to_addrs
    def execute(self):
        logger.info("Sending mail to %s" % ",".join(self.to_addrs))
        cmd = "/bin/mailx" if curr_platform == SUNOS else findCmdInPath('mail')
        Command('Sending email', 
                'echo "%s" | %s -s "%s" %s' % (self.message, cmd, self.subject, " ".join(self.to_addrs))).run(validateAfter=True)
