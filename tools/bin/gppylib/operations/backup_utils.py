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
import fnmatch
import tempfile
import glob
#from gppylib.operations.dump import dump_prefix
from gppylib import gplog
from gppylib.gparray import GpArray
from gppylib.commands.base import WorkerPool, Command, REMOTE
from gppylib.commands.unix import Scp
from gppylib.db import dbconn
from gppylib.db.dbconn import execSQL, execSQLForSingleton, UnexpectedRowsError
dump_prefix = ''
DUMP_DIR = 'db_dumps'

logger = gplog.get_default_logger()

def expand_partitions_and_populate_filter_file(dbname, partition_list, file_prefix):
    expanded_partitions = expand_partition_tables(dbname, partition_list)
    dump_partition_list = list(set(expanded_partitions + partition_list))
    return create_temp_file_from_list(dump_partition_list, file_prefix)

def populate_filter_tables(table, rows, non_partition_tables, partition_leaves):
    if not rows:
        non_partition_tables.append(table)
    else:
        for (schema_name, partition_leaf_name) in rows:
            partition_leaf = schema_name.strip() + '.' + partition_leaf_name.strip()
            partition_leaves.append(partition_leaf)
    return (non_partition_tables, partition_leaves)

def get_all_parent_tables(dbname):
    SQL = "SELECT DISTINCT (schemaname || '.' || tablename) FROM pg_partitions"
    data = []
    with dbconn.connect(dbconn.DbURL(dbname=dbname)) as conn:
        curs = dbconn.execSQL(conn, SQL)
        data = curs.fetchall()
    return set([d[0] for d in data])

global_parent_table_set = None
def is_parent_table(dbname, tablename):
    global global_parent_table_set 
    if global_parent_table_set is None:
        global_parent_table_set = get_all_parent_tables(dbname)
    if tablename in global_parent_table_set:
        return True
    else:
        return False

def list_to_quoted_string(filter_tables):
    filter_string = "'" + "', '".join([t.strip() for t in filter_tables]) + "'"
    return filter_string

def convert_parents_to_leafs(dbname, parents):
    partition_leaves_sql = """
                           SELECT x.partitionschemaname || '.' || x.partitiontablename 
                           FROM (
                                SELECT distinct schemaname, tablename, partitionschemaname, partitiontablename, partitionlevel 
                                FROM pg_partitions 
                                WHERE schemaname || '.' || tablename in (%s)
                                ) as X, 
                           (SELECT schemaname, tablename maxtable, max(partitionlevel) maxlevel 
                            FROM pg_partitions 
                            group by (tablename, schemaname)
                           ) as Y
                           WHERE x.schemaname = y.schemaname and x.tablename = Y.maxtable and x.partitionlevel = Y.maxlevel;
"""
    if not parents:
        return []

    conn = dbconn.connect(dbconn.DbURL(dbname=dbname))
    partition_sql = partition_leaves_sql % list_to_quoted_string(parents)
    curs = dbconn.execSQL(conn, partition_sql)
    rows = curs.fetchall()
    return [r[0] for r in rows]


#input: list of tables to be filtered
#output: same list but parent tables converted to leafs
def expand_partition_tables(dbname, filter_tables):

    if filter_tables is None:
        return None
    parent_tables = list()
    non_parent_tables = list()
    expanded_list = list()
    for table in filter_tables:
        if is_parent_table(dbname, table):
            parent_tables.append(table)
        else:
            non_parent_tables.append(table)

    expanded_list += non_parent_tables
  
    local_batch_size = 1000
    for (s, e) in get_batch_from_list(len(parent_tables), local_batch_size):
        parent_table_batch = parent_tables[s:e]
        tmp = convert_parents_to_leafs(dbname, parent_tables)
        expanded_list += tmp

    return expanded_list

def get_batch_from_list(length, batch_size):
    indices = []
    for i in range(0,length, batch_size):
        indices.append((i,i+batch_size))
    return indices
        
def create_temp_file_from_list(entries, prefix):

    if entries is None:
        return None 

    fd = tempfile.NamedTemporaryFile(mode='w', prefix=prefix, delete=False)
    for n in entries:
        fd.write(n.rstrip() + '\n')
    tmp_file_name = fd.name
    fd.close()

    verify_lines_in_file(tmp_file_name, entries)

    return tmp_file_name 

def create_temp_file_with_tables(table_list):
    return create_temp_file_from_list(table_list, 'table_list_')

def validate_timestamp(ts):
    if not ts:
        return False
    if len(ts) != 14:
        return False
    if ts.isdigit():
        return True
    else:
        return False

def check_successful_dump(dbname, report_file_contents):
    for line in report_file_contents:
        if line.strip() == 'gp_dump utility finished successfully.':
            return True
    return False

# raise exception for bad data
def convert_reportfilename_to_cdatabasefilename(report_file):
    (dirname, fname) = os.path.split(report_file)
    ts = fname[-18:-4]
    return "%s/%sgp_cdatabase_1_1_%s" % (dirname, dump_prefix, ts)

def check_cdatabase_exists(dbname, report_file, netbackup_service_host=None, netbackup_block_size=None):
    try:
        filename = convert_reportfilename_to_cdatabasefilename(report_file) 
    except Exception as e:
        return False

    if netbackup_service_host:
        restore_file_with_nbu(netbackup_service_host, netbackup_block_size, filename)
        cdatabase_contents = get_lines_from_file(filename)
    else:
        cdatabase_contents = get_lines_from_file(filename)

    for line in cdatabase_contents:
        if 'CREATE DATABASE' in line:
            parts = line.split()
            if len(parts) < 3:
                continue
            if parts[2] is not None and dbname == parts[2].strip('"'):
                return True
    return False

def get_type_ts_from_report_file(dbname, report_file, backup_type, netbackup_service_host=None, netbackup_block_size=None):
    report_file_contents = get_lines_from_file(report_file)

    if not check_successful_dump(dbname, report_file_contents):
        return None

    if not check_cdatabase_exists(dbname, report_file, netbackup_service_host, netbackup_block_size):
        return None

    if check_backup_type(report_file_contents, backup_type):
        return get_timestamp_val(report_file_contents)

    return None

def get_full_ts_from_report_file(dbname, report_file, netbackup_service_host=None, netbackup_block_size=None):
    return get_type_ts_from_report_file(dbname, report_file, 'Full', netbackup_service_host, netbackup_block_size)

def get_incremental_ts_from_report_file(dbname, report_file, netbackup_service_host=None, netbackup_block_size=None):
    return get_type_ts_from_report_file(dbname, report_file, 'Incremental', netbackup_service_host, netbackup_block_size)

def get_timestamp_val(report_file_contents):
    for line in report_file_contents:
        if line.startswith('Timestamp Key'):
            ts = line.split(':')[-1].strip()
            try:
                int(ts)
                if len(ts) != 14:
                    raise Exception('Invalid timestamp value found in report_file')
            except ValueError as e:
                raise Exception('Invalid timestamp value found in report_file')

            return ts
    return None

def check_backup_type(report_file_contents, backup_type):
    for line in report_file_contents:
        if line.startswith('Backup Type'):
            if line.split(':')[-1].strip() == backup_type:
                return True
    return False

def get_lines_from_file(fname):
    content = []
    with open(fname) as fd:
        for line in fd:
            content.append(line.rstrip())
    return content

def write_lines_to_file(filename, lines):
    with open(filename, 'w') as fp:
        for line in lines:
            fp.write("%s\n" % line.rstrip())

def verify_lines_in_file(fname, expected):
    lines = get_lines_from_file(fname)
    if lines != expected:
        raise Exception("After writing file '%s' contents not as expected, suspected IO error" % fname)

def check_dir_writable(dir):
    fp = None
    try:
        tmp_file = os.path.join(dir, 'tmp_file')
        fp = open(tmp_file, 'w')
    except IOError as e:
        raise Exception('No write access permission on %s' % dir)
    except Exception as e:
        raise Exception(str(e))
    finally:
        if fp is not None:
            fp.close()
        if os.path.isfile(tmp_file):
            os.remove(tmp_file) 

def execute_sql(query, master_port, dbname):
    dburl = dbconn.DbURL(port=master_port, dbname=dbname)
    conn = dbconn.connect(dburl)
    cursor = execSQL(conn, query)
    return cursor.fetchall()

def get_backup_directory(master_data_dir, backup_dir, timestamp):
    if backup_dir:
        use_dir = backup_dir
    elif master_data_dir:
        use_dir = master_data_dir
    else:
        raise Exception("Can not locate backup directory with existing parameters")

    if not timestamp:
        raise Exception("Can not locate backup directory without timestamp")

    if not validate_timestamp(timestamp):
        raise Exception ('Invalid timestamp: "%s"' % timestamp)

    return ("%s/%s/%s" % (use_dir, DUMP_DIR, timestamp[0:8]))

def generate_schema_filename(master_data_dir, backup_dir, timestamp, dump_dir=None):
    use_dir = get_backup_directory(master_data_dir, backup_dir, timestamp)
    return "%s/%sgp_dump_%s_schema" % (use_dir, dump_prefix, timestamp)

def generate_report_filename(master_data_dir, backup_dir, timestamp, dump_dir=None):
    use_dir = get_backup_directory(master_data_dir, backup_dir, timestamp)
    return "%s/%sgp_dump_%s.rpt" % (use_dir, dump_prefix, timestamp)

def generate_increments_filename(master_data_dir, backup_dir, timestamp, dump_dir=None):
    use_dir = get_backup_directory(master_data_dir, backup_dir, timestamp)
    return "%s/%sgp_dump_%s_increments" % (use_dir, dump_prefix, timestamp)

def generate_pgstatlastoperation_filename(master_data_dir, backup_dir, timestamp, dump_dir=None):
    use_dir = get_backup_directory(master_data_dir, backup_dir, timestamp)
    return "%s/%sgp_dump_%s_last_operation" % (use_dir, dump_prefix, timestamp)

def generate_dirtytable_filename(master_data_dir, backup_dir, timestamp, dump_dir=None):
    use_dir = get_backup_directory(master_data_dir, backup_dir, timestamp)
    return "%s/%sgp_dump_%s_dirty_list" % (use_dir, dump_prefix, timestamp)

def generate_plan_filename(master_data_dir, backup_dir, timestamp):
    use_dir = get_backup_directory(master_data_dir, backup_dir, timestamp)
    return "%s/%sgp_restore_%s_plan" % (use_dir, dump_prefix, timestamp)

def generate_metadata_filename(master_data_dir, backup_dir, timestamp):
    use_dir = get_backup_directory(master_data_dir, backup_dir, timestamp)
    return "%s/%sgp_dump_1_1_%s.gz" % (use_dir, dump_prefix, timestamp)

def generate_partition_list_filename(master_data_dir, backup_dir, timestamp):
    use_dir = get_backup_directory(master_data_dir, backup_dir, timestamp)
    return "%s/%sgp_dump_%s_table_list" % (use_dir, dump_prefix, timestamp)

def generate_ao_state_filename(master_data_dir, backup_dir, timestamp, dump_dir=None):
    use_dir = get_backup_directory(master_data_dir, backup_dir, timestamp)
    return "%s/%sgp_dump_%s_ao_state_file" % (use_dir, dump_prefix, timestamp)
 
def generate_co_state_filename(master_data_dir, backup_dir, timestamp, dump_dir=None):
    use_dir = get_backup_directory(master_data_dir, backup_dir, timestamp)
    return "%s/%sgp_dump_%s_co_state_file" % (use_dir, dump_prefix, timestamp)

def generate_files_filename(master_data_dir, backup_dir, timestamp):
    use_dir = get_backup_directory(master_data_dir, backup_dir, timestamp)
    return '%s/%sgp_dump_%s_regular_files' % (use_dir, dump_prefix, timestamp) 

def generate_pipes_filename(master_data_dir, backup_dir, timestamp):
    use_dir = get_backup_directory(master_data_dir, backup_dir, timestamp)
    return '%s/%sgp_dump_%s_pipes' % (use_dir, dump_prefix, timestamp) 

def generate_master_config_filename(timestamp):
    return '%sgp_master_config_files_%s.tar' % (dump_prefix, timestamp)

def generate_segment_config_filename(segId, timestamp):
    return '%sgp_segment_config_files_0_%d_%s.tar' % (dump_prefix, segId, timestamp)

def generate_filter_filename(master_datadir, backup_dir, timestamp):
    use_dir = get_backup_directory(master_datadir, backup_dir, timestamp)
    return '%s/%s%s_filter' % (use_dir, generate_dbdump_prefix(), timestamp)

def generate_global_prefix():
    return '%sgp_global_1_1_' % (dump_prefix)

def generate_master_dbdump_prefix():
    return '%sgp_dump_1_1_' % (dump_prefix)

def generate_master_status_prefix():
    return '%sgp_dump_status_1_1_' % (dump_prefix)

def generate_seg_dbdump_prefix():
    return '%sgp_dump_0_' % (dump_prefix)

def generate_seg_status_prefix():
    return '%sgp_dump_status_0_' % (dump_prefix)

def generate_dbdump_prefix():
    return '%sgp_dump_' % (dump_prefix)

def generate_createdb_prefix():
    return '%sgp_cdatabase_1_1_' % (dump_prefix)

def generate_createdb_filename(master_datadir, backup_dir, timestamp, dump_dir=None):
    use_dir = get_backup_directory(master_datadir, backup_dir, timestamp)
    return '%s/%s%s' % (use_dir, generate_createdb_prefix(), timestamp) 

def get_dump_dirs(dump_dir):
    dump_path = os.path.join(dump_dir, DUMP_DIR)

    if not os.path.isdir(dump_path):
        return []

    initial_list = os.listdir(dump_path)
    initial_list = fnmatch.filter(initial_list, '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]')

    dirnames = []
    for d in initial_list:
        pth = os.path.join(dump_path, d)
        if os.path.isdir(pth):
            dirnames.append(pth)

    if len(dirnames) == 0:
        return []
    dirnames = sorted(dirnames, key=lambda x: int(os.path.basename(x)), reverse=True)
    return dirnames

def get_latest_report_timestamp(backup_dir):
    dump_dirs = get_dump_dirs(backup_dir)

    for d in dump_dirs:
        latest = get_latest_report_in_dir(d)
    
        if latest:
            return latest

    return None

def get_latest_report_in_dir(d):
    files = os.listdir(d)

    if len(files) == 0:
        return None

    dump_report_files = fnmatch.filter(files, '%sgp_dump_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].rpt' % dump_prefix)

    if len(dump_report_files) == 0:
        return None

    dump_report_files = sorted(dump_report_files, key=lambda x: int(x.split('_')[-1].split('.')[0]), reverse=True)
    return dump_report_files[0][-18:-4]

def get_timestamp_from_increments_filename(filename):
    fname = os.path.basename(filename)
    parts = fname.split('_')
    # Check for 4 underscores if there is no prefix, or more than 4 if there is a prefix
    if not ((not dump_prefix and len(parts) == 4) or (dump_prefix and len(parts) > 4)):
        raise Exception("Invalid increments file '%s' passed to get_timestamp_from_increments_filename" % filename)
    return parts[-2].strip()

def get_full_timestamp_for_incremental(dbname, backup_dir, incremental_timestamp, dump_dir=None):
    pattern = '%s/db_dumps/[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]/%sgp_dump_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_increments' % (backup_dir, dump_prefix)

    increments_files = glob.glob(pattern)

    for increments_file in increments_files:
        increment_ts = get_lines_from_file(increments_file)
        if incremental_timestamp in increment_ts:
            full_timestamp = get_timestamp_from_increments_filename(increments_file)
            return full_timestamp

    return None

# backup_dir will be either MDD or some other directory depending on call
def get_latest_full_dump_timestamp(dbname, backup_dir):
    if not backup_dir:
        raise Exception('Invalid None param to get_latest_full_dump_timestamp')

    dump_dirs = get_dump_dirs(backup_dir)

    for dump_dir in dump_dirs:
        files = sorted(os.listdir(dump_dir))

        if len(files) == 0:
            logger.warn('Dump directory %s is empty' % dump_dir)
            continue

        dump_report_files = fnmatch.filter(files, '%sgp_dump_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].rpt' % dump_prefix)

        if len(dump_report_files) == 0:
            logger.warn('No dump report files found in dump directory %s' % dump_dir)
            continue

        dump_report_files = sorted(dump_report_files, key=lambda x: int(x.split('_')[-1].split('.')[0]), reverse=True)
        for dump_report_file in dump_report_files:
            logger.debug('Checking for latest timestamp in report file %s' % os.path.join(dump_dir, dump_report_file))
            ts = get_full_ts_from_report_file(dbname, os.path.join(dump_dir, dump_report_file))
            logger.debug('Timestamp = %s' % ts)
            if ts is not None:
                return ts

    raise Exception('No full backup found for incremental')

def get_all_segment_addresses(master_port):
    gparray = GpArray.initFromCatalog(dbconn.DbURL(port=master_port), utility=True)
    addresses = [seg.getSegmentAddress() for seg in gparray.getDbList() if seg.isSegmentPrimary(current_role=True)]
    return list(set(addresses))

def scp_file_to_hosts(host_list, filename, batch_default):
    pool = WorkerPool(numWorkers = min(len(host_list), batch_default))

    for hname in host_list:
        pool.addCommand(Scp('Copying table_filter_file to %s' % hname,
                        srcFile=filename,
                        dstFile=filename,
                        dstHost=hname))
    pool.join()
    pool.check_results()

def remove_file_from_segments(master_port, filename):
    hostlist = get_all_segment_addresses(master_port)
    for hname in hostlist:
        cmd = Command('Remove tmp files', 'rm -f %s' % filename, ctxt=REMOTE, remoteHost=hname)
        cmd.run(validateAfter=True)

def run_pool_command(host_list, cmd_str, batch_default, check_results=True):
    pool = WorkerPool(numWorkers = min(len(host_list), batch_default))

    for h in host_list:
        cmd = Command(h, cmd_str, ctxt=REMOTE, remoteHost = h)
        pool.addCommand(cmd)

    pool.join()
    if check_results:
        pool.check_results()

def check_funny_chars_in_tablenames(tablenames):
    for tablename in tablenames:
        if '\n' in tablename or ',' in tablename or ':' in tablename:
            raise Exception('Tablename has an invalid character "\\n", ":", "," : "%s"' % tablename) 

#Form and run command line to backup individual file with NBU
def backup_file_with_nbu(netbackup_service_host, netbackup_policy, netbackup_schedule, netbackup_block_size, netbackup_keyword, netbackup_filepath, hostname=None):
    command_string = "cat %s | gp_bsa_dump_agent --netbackup-service-host %s --netbackup-policy %s --netbackup-schedule %s --netbackup-filename %s" % (netbackup_filepath, netbackup_service_host, netbackup_policy, netbackup_schedule, netbackup_filepath)
    if netbackup_block_size is not None:
        command_string += " --netbackup-block-size %s" % netbackup_block_size
    if netbackup_keyword is not None:
        command_string += " --netbackup-keyword %s" % netbackup_keyword
    logger.debug("Command string inside 'backup_file_with_nbu': %s\n", command_string)
    if hostname is None:
        Command("dumping metadata files from master", command_string).run(validateAfter=True)
    else:
        Command("dumping metadata files from segment", command_string, ctxt=REMOTE, remoteHost = hostname).run(validateAfter=True)
    logger.debug("Command ran successfully\n")

#Form and run command line to restore individual file with NBU
def restore_file_with_nbu(netbackup_service_host, netbackup_block_size, netbackup_filepath, hostname=None):
    command_string = "gp_bsa_restore_agent --netbackup-service-host %s  --netbackup-filename %s > %s" % (netbackup_service_host, netbackup_filepath, netbackup_filepath)
    if netbackup_block_size is not None:
        command_string += " --netbackup-block-size %s" % netbackup_block_size
    logger.debug("Command string inside 'restore_file_with_nbu': %s\n", command_string)
    if hostname is None:
        Command("restoring metadata files to master", command_string).run(validateAfter=True)
    else:
        Command("restoring metadata files to segment", command_string, ctxt=REMOTE, remoteHost = hostname).run(validateAfter=True)

def check_file_dumped_with_nbu(netbackup_service_host, netbackup_filepath, hostname=None):
    command_string = "gp_bsa_query_agent --netbackup-service-host %s --netbackup-filename %s" % (netbackup_service_host, netbackup_filepath)
    logger.debug("Command string inside 'check_file_dumped_with_nbu': %s\n", command_string)
    if hostname is None:
        cmd = Command("Querying NetBackup server to check for dumped file", command_string)
    else:
        cmd = Command("Querying NetBackup server to check for dumped file", command_string, ctxt=REMOTE, remoteHost = hostname)

    cmd.run(validateAfter=True)
    if cmd.get_results().stdout.strip() == netbackup_filepath:
        return True
    else:
        return False

def generate_global_filename(master_datadir, backup_dir, dump_dir, dump_date, timestamp):
    if backup_dir is not None:
        global_file = os.path.join(backup_dir, DUMP_DIR, dump_date, "%s%s" % (generate_global_prefix(), timestamp))
    else:
        global_file = os.path.join(master_datadir, dump_dir, dump_date, "%s%s" % (generate_global_prefix(), timestamp))
    return global_file

def generate_cdatabase_filename(master_data_dir, backup_dir, timestamp):
    use_dir = get_backup_directory(master_data_dir, backup_dir, timestamp)
    return "%s/%sgp_cdatabase_1_1_%s" % (use_dir, dump_prefix, timestamp)

def get_full_timestamp_for_incremental_with_nbu(netbackup_service_host, netbackup_block_size, incremental_timestamp):
    if dump_prefix:
        get_inc_files_cmd = "gp_bsa_query_agent --netbackup-service-host=%s --netbackup-list-dumped-objects=%sgp_dump_*_increments" % (netbackup_service_host, dump_prefix)
    else:
        get_inc_files_cmd = "gp_bsa_query_agent --netbackup-service-host=%s --netbackup-list-dumped-objects=gp_dump_*_increments" % netbackup_service_host

    cmd = Command("Query NetBackup server to get the list of increments files backed up", get_inc_files_cmd)
    cmd.run(validateAfter=True)
    files_list = cmd.get_results().stdout.split('\n')

    for line in files_list:
        fname = line.strip()
        restore_file_with_nbu(netbackup_service_host, netbackup_block_size, fname)
        contents = get_lines_from_file(fname)
        if incremental_timestamp in contents:
            full_timestamp = get_timestamp_from_increments_filename(fname)
            return full_timestamp

    return None

def get_latest_full_ts_with_nbu(netbackup_service_host, netbackup_block_size, dbname, backup_dir):
    if dump_prefix:
        get_rpt_files_cmd = "gp_bsa_query_agent --netbackup-service-host=%s --netbackup-list-dumped-objects=%sgp_dump_*.rpt" % (netbackup_service_host, dump_prefix)
    else:
        get_rpt_files_cmd = "gp_bsa_query_agent --netbackup-service-host=%s --netbackup-list-dumped-objects=gp_dump_*.rpt" % netbackup_service_host

    cmd = Command("Query NetBackup server to get the list of report files backed up", get_rpt_files_cmd)
    cmd.run(validateAfter=True)
    files_list = cmd.get_results().stdout.split('\n')

    for line in files_list:
        fname = line.strip()
        if fname == '':
            continue
        if backup_dir not in fname:
            continue
        if ("No object matched the specified predicate" in fname) or ("No objects of the format" in fname):
            return None
        restore_file_with_nbu(netbackup_service_host, netbackup_block_size, fname)
        ts = get_full_ts_from_report_file(dbname, fname, netbackup_service_host=netbackup_service_host, netbackup_block_size=netbackup_block_size)
        logger.debug('Timestamp = %s' % ts)
        if ts is not None:
            return ts

    raise Exception('No full backup found for given incremental on the specified NetBackup server')
