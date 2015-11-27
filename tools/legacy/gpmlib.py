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
from __future__ import with_statement
import os, sys
progname = os.path.split(sys.argv[0])[-1]

if sys.version_info < (2, 5, 0):
    sys.exit(
'''Error: %s is supported on Python versions 2.5.0 or greater
Please upgrade python installed on this machine.''' % progname)

#turning off Deprecation warnings (for now)
import warnings
warnings.simplefilter('ignore', DeprecationWarning)


import platform,  gplib, socket, random, popen2
import threading
from time import localtime, strftime

import pg8000
import pysync

##################
log = {}

log['verbose'] = True
log['module'] = ''
log['host'] = socket.gethostname().split('.')[0]
log['user'] = os.environ.get('USER') or os.environ.get('LOGNAME')
log['file'] = None
##################
def log_set_module(module):
    global log
    log['module'] = module

##################
def log_set_verbose(verbose):
    global log
    log['verbose'] = verbose

##################
def log_set_file(file):
    global log
    log['file'] = file

##################
def log_info(msg, tofile=False):
    global log
    
    logs = '%s:%s:%s:%s-[INFO]:- %s' % (strftime('%Y%m%d:%H:%M:%S', localtime()), log['module'], log['host'], log['user'], msg)
    if log['verbose'] and not tofile:
        print logs
    else:
        if log['file']:
            os.system('%s "%s" >> %s' % (ENV.ECHO, logs, log['file']))

##################
def log_error(msg, tofile=False):
    global log
    logs = '%s:%s:%s:%s-[ERROR]:- %s' % (strftime('%Y%m%d:%H:%M:%S', localtime()), log['module'], log['host'], log['user'], msg)
    if log['verbose'] and not tofile:
        print logs
    else:
        if log['file']:
            os.system('%s "%s" >> %s' % (ENV.ECHO, logs, log['file']))        

##################
def log_warn(msg, tofile=False):
    global log
    logs = '%s:%s:%s:%s-[WARN]:- %s' % (strftime('%Y%m%d:%H:%M:%S', localtime()), log['module'], log['host'], log['user'], msg)
    if log['verbose'] and not tofile:
        print logs
    else:
        if log['file']:
            os.system('%s "%s" >> %s' % (ENV.ECHO, logs, log['file']))        

##################
def log_fatal(msg, tofile=False):
    global log
    logs = '%s:%s:%s:%s-[FATAL]:- %s' % (strftime('%Y%m%d:%H:%M:%S', localtime()), log['module'], log['host'], log['user'], msg)
    if log['verbose'] and not tofile:
        print logs
    else:
        if log['file']:
            os.system('%s "%s" >> %s' % (ENV.ECHO, logs, log['file']))        

##################
def error(msg):
    global log

    logs = '%s:%s:%s:%s-[ERROR]:- %s' % (strftime('%Y%m%d:%H:%M:%S', localtime()), log['module'], log['host'], log['user'], msg)
    if log['file']:
        os.system('%s "%s" >> %s' % (ENV.ECHO, logs, log['file']))    
    print logs
    print '%s:%s:%s:%s-[ERROR]:- Program aborted.' % (strftime('%Y%m%d:%H:%M:%S', localtime()), log['module'], log['host'], log['user'])
    sys.exit(1)

##################
def fatal(msg):
    global log

    logs = '%s:%s:%s:%s-[FATAL]:- %s' % (strftime('%Y%m%d:%H:%M:%S', localtime()), log['module'], log['host'], log['user'], msg)
    if log['file']:
        os.system('%s "%s" >> %s' % (ENV.ECHO, logs, log['file']))    

    print logs
    print '%s:%s:%s:%s-[FATAL]:- Program aborted.' % (strftime('%Y%m%d:%H:%M:%S', localtime()), log['module'], log['host'], log['user'])
    sys.exit(2)

#############
def findCmdInPath_noerror(cmd):
    CMDPATH = ('/usr/kerberos/bin', '/usr/sfw/bin', '/opt/sfw/bin', '/usr/local/bin', '/bin', 
               '/usr/bin', '/sbin', '/usr/sbin', '/usr/ucb', '/sw/bin', '/opt/Navisphere/bin')

    for p in CMDPATH:
        f = os.path.join(p, cmd)
        if os.path.exists(f):
            return f
    return ''

def findCmdInPath(cmd):
    cmd = findCmdInPath_noerror(cmd)
    if cmd == '':
        fatal('Command %s not found' % cmd)

    return cmd

#############
def makeCommand(cmd):
    GPHOME=os.environ.get('GPHOME')
    LIB_PATH=os.environ.get(ENV.LIB_TYPE)
    if not LIB_PATH:
        LIB_PATH='%s/lib:%s/ext/python/lib:.' % (GPHOME, GPHOME)

    PATH=os.environ.get('PATH')
    if not PATH:
        PATH='%s/bin:%s/ext/python/bin:.' % (GPHOME, GPHOME)
        
    PYTHONPATH=os.environ.get('PYTHONPATH')
    if not PYTHONPATH:
        PYTHONPATH="%(gphome)s/lib/python" % {'gphome':GPHOME}

    return ('GPHOME=%s && export GPHOME '
            '&& PATH=%s && export PATH '
            '&& %s=%s && export %s '
            '&& PYTHONPATH=%s && export PYTHONPATH '
            '&& %s'
            % (GPHOME, 
               PATH,
               ENV.LIB_TYPE,
               LIB_PATH,
               ENV.LIB_TYPE,
               PYTHONPATH,
               cmd))


#############
def run2(cmd, on_error_warn=False, setpid_callback=None):
    p = None
    ok = False
    out = []
    try:
        p = popen2.Popen3(cmd, capturestderr=True)

        if setpid_callback:
            setpid_callback(p.pid)

        e = p.wait()
        for line in p.fromchild:
            out.append(line)
        ok = not e
        if not ok and on_error_warn:
            log_warn('-----------------------------------------------------')
            log_warn('Command Failed: %s' % cmd)
            log_warn('Exit status: %d' % os.WEXITSTATUS(e))
            if len(out) > 0:
                log_warn('Standard output:')
                for l in out:
                    log_warn('\t %s' % l.strip())
            else:
                log_warn('Standard output:  None')

            err = []
            for line in p.childerr:
                err.append(line)
            if len(err) > 0:
                log_warn('Standard error:')
                for l in err:
                    log_warn('\t %s' % l.strip())
            else:
                log_warn('Standard error:   None')
            log_warn('-----------------------------------------------------')
    finally:
        if p:
            if p.fromchild:  
                p.fromchild.close()
            if p.childerr:   
                p.childerr.close()

    return (ok, out)


def run(cmd):
    return run2(cmd, False)

def run_warn(cmd, setpid_callback=None):
    return run2(cmd, on_error_warn=True,setpid_callback=setpid_callback)

#############
def file_exists(file, host=None):
    if not host or host == 'localhost':
        return os.path.isfile(file)
    else:
        (ok, out) = run('%s test -f %s && test -r %s' % (gplib.ssh_prefix(host=host), file, file))
        return ok

#############                       
def directory_exists(dir, host=None):
    if not host or host == 'localhost':
        return os.path.isdir(dir)
    else:
        (ok, out) = run('%s test -d %s' % (gplib.ssh_prefix(host=host), dir))
        return ok

#############
def directory_writable(dir, host=None):
    f = None
    file = os.path.join(dir, 'tmp_file_test')
    if not host or host == 'localhost':
        try:
            try:
                f = open(file, 'w')
                f.close()
            except IOError, e:
                fatal('write file %s error' % file)
        finally:
            f.close()
            os.remove(file)
        
    else:
        gphome = os.environ.get('GPHOME')
        cmd = makeCommand('''python -c \\"import sys, os; sys.path.extend(['%s', '%s']); import gpmlib; gpmlib.directory_writable('%s')\\"''' %
                          (os.path.join(gphome, 'bin', 'lib'), os.path.join(gphome, 'lib', 'python'), dir))
        (ok, out) = run('''%s "%s"''' % (gplib.ssh_prefix(host=host), cmd))
        if not ok:
            fatal('write file %s error' % file)
                        
    return True
    

#############
class Env:
    def __init__(self):
        self.GPHOME = None
        self.USER = None

        # mirror type
        self.MIRROR_NULL_TYPE = 0
        self.MIRROR_SINGLE_HOME_GROUP_TYPE = 1
        self.MIRROR_SINGLE_HOME_SPREAD_TYPE = 2
        self.MIRROR_MULTI_HOME_GROUP_TYPE = 3
        self.MIRROR_MULTI_HOME_SPREAD_TYPE = 4

        self.DBNAME = 'template1'
        self.GP_PG_VIEW = '''(SELECT l.dbid, l.isprimary, l.content, l."valid",
                              l.definedprimary FROM gp_pgdatabase() l(dbid smallint,
                              isprimary boolean, content smallint, "valid" boolean,
                              definedprimary boolean))'''

        self.AWK = findCmdInPath('awk')
        self.BASENAME = findCmdInPath('basename')
        self.CAT = findCmdInPath('cat')
        self.CLEAR = findCmdInPath('clear')
        self.CKSUM = findCmdInPath('cksum')
        self.CUT = findCmdInPath('cut')
        self.DATE = findCmdInPath('date')
        self.DD = findCmdInPath('dd')
        self.DIRNAME = findCmdInPath('dirname')
        self.DF = findCmdInPath('df')
        self.DU = findCmdInPath('du')
        self.ECHO = findCmdInPath('echo')
        self.EXPR = findCmdInPath('expr')
        self.FIND = findCmdInPath('find')

        self.GP_MOUNT_AGENT = findCmdInPath_noerror('gp_mount_agent') # GPDB supplied, but only required for SAN.

        self.GREP = findCmdInPath('grep')
        self.GZIP = findCmdInPath('gzip')
        self.EGREP = findCmdInPath('egrep')
        self.HEAD = findCmdInPath('head')
        self.HOSTNAME = findCmdInPath('hostname')

        self.INQ = findCmdInPath_noerror('inq') # SAN-specific not available on every system.

        self.IPCS = findCmdInPath('ipcs')
        self.IFCONFIG = findCmdInPath('ifconfig')
        self.KILL = findCmdInPath('kill')
        self.LS = findCmdInPath('ls')
        self.LOCALE = findCmdInPath('locale')
        self.MV = findCmdInPath('mv')
        self.MORE = findCmdInPath('more')

        self.MOUNT = findCmdInPath('mount')

        self.MKDIR = findCmdInPath('mkdir')
        self.MKFIFO = findCmdInPath('mkfifo')

        self.NAVISECCLI = findCmdInPath_noerror('naviseccli') # SAN-specific not available on every system.

        self.NETSTAT = findCmdInPath('netstat')
        self.PING = findCmdInPath('ping')

        self.POWERMT = findCmdInPath_noerror('powermt') # SAN-specific not available on every system.

        self.PS = findCmdInPath('ps')
        self.RM = findCmdInPath('rm')
        self.SCP = findCmdInPath('scp')
        self.SED = findCmdInPath('sed')
        self.SLEEP = findCmdInPath('sleep')
        self.SORT = findCmdInPath('sort')
        self.SPLIT = findCmdInPath('split')
        self.SSH = findCmdInPath('ssh')

        self.STAT = findCmdInPath_noerror('stat') # Only required for SAN.

        self.TAIL = findCmdInPath('tail')
        self.TAR = findCmdInPath('tar')
        self.TEE = findCmdInPath('tee')
        self.TOUCH = findCmdInPath('touch')
        self.TR = findCmdInPath('tr')
        self.WC = findCmdInPath('wc')
        self.WHICH = findCmdInPath('which')
        self.WHOAMI = findCmdInPath('whoami')
        self.ZCAT = findCmdInPath('zcat')

        plat = platform.uname()
        self.SYSTEM = plat[0].lower()

        if self.SYSTEM == 'sunos':
            self.IFCONFIG_TXT='-a inet'
            self.PS_TXT='ef'
            self.LIB_TYPE='LD_LIBRARY_PATH'
            self.ZCAT='gzcat'
            self.PG_METHOD='trust'
            self.HOST_ARCH_TYPE='uname -i'
            self.NOLINE_ECHO='/usr/bin/echo'
            self.DEFAULT_LOCALE_SETTING='en_US.UTF-8'
            self.MAIL='/bin/mailx'
            self.PING_TIME='1'
            self.DF=findCmdInPath('df')
            self.DU_TXT='-s'
            self.GTAR = findCmdInPath('gtar')            
        elif self.SYSTEM == 'linux':
            self.IFCONFIG_TXT=''
            self.PS_TXT='ax'
            self.LIB_TYPE='LD_LIBRARY_PATH'
            self.PG_METHOD='ident'
            self.HOST_ARCH_TYPE='uname -i'
            self.NOLINE_ECHO='%s -e' % self.ECHO
            self.DEFAULT_LOCALE_SETTING='en_US.utf8'
            self.PING_TIME='-c 1'
            self.DF='%s -P' % findCmdInPath('df')
            self.DU_TXT='c'
            self.GTAR = findCmdInPath('tar')            
        elif self.SYSTEM == 'darwin':
            self.IFCONFIG_TXT=''
            self.PS_TXT='ax'
            self.LIB_TYPE='DYLD_LIBRARY_PATH'
            self.PG_METHOD='ident'
            self.HOST_ARCH_TYPE='uname -m'
            self.NOLINE_ECHO= self.ECHO
            self.DEFAULT_LOCALE_SETTING='en_US.utf-8'
            self.PING_TIME='-c 1'
            self.DF='%s -P' % findCmdInPath('df')
            self.DU_TXT='-c'
            self.GTAR = findCmdInPath('tar')
        elif self.SYSTEM == 'freebsd':
            self.IFCONFIG_TXT=''
            self.PS_TXT='ax'
            self.LIB_TYPE='LD_LIBRARY_PATH'
            self.PG_METHOD='ident'
            self.HOST_ARCH_TYPE='uname -m'
            self.NOLINE_ECHO='%s -e' % self.ECHO
            self.DEFAULT_LOCALE_SETTING='en_US.utf-8'
            self.PING_TIME='-c 1'
            self.DF='%s -P' % findCmdInPath('df')
            self.DU_TXT='-c'
            self.GTAR = findCmdInPath('tar')
        else:
            fatal('platform not supported')

    def chk_environ(self):
        self.GPHOME=os.getenv('GPHOME')
        if not self.GPHOME:
            fatal('GPHOME not found')
        self.USER =os.getenv('USER') or os.getenv('LOGNAME')
        if not self.USER:
            fatal('USER not found')

        LIB_PATH=os.getenv(self.LIB_TYPE)
        if not LIB_PATH:
            LIB_PATH='.'

        PATH=os.getenv('PATH')
        if not PATH:
            PATH='.'

        os.environ[self.LIB_TYPE]='%s/lib:%s/ext/python/lib:%s' % (self.GPHOME, self.GPHOME, LIB_PATH)
        os.environ['PATH']='%s/bin:%s/ext/python/bin:%s' % (self.GPHOME, self.GPHOME, PATH)
        


ENV = Env()

#############
def ping_host(host, fail_exit=False):
    if ENV.SYSTEM == 'darwin':
        (ok, out) = run('%s %s %s > /dev/null 2>&1' % (ENV.PING, ENV.PING_TIME, host))
    else:
        (ok, out) = run('%s %s %s > /dev/null 2>&1' % (ENV.PING, host, ENV.PING_TIME))

    if ok:
        log_info('%s contact established' % host)
    else:
        if fail_exit:
            fatal('Unable to contact %s' % host)
        else:
            log_warn('Unable to contact %s' % host)

    return ok

def chk_postgres_version(host):
    cmd = makeCommand('%s --gp-version' % os.path.join(os.environ.get('GPHOME'), 'bin/postgres'))
    (ok, out) = run(cmd)
    if not ok:
        log_error('Unable to get Greenplum database version')
        return False
    current = out[0].strip()

    (ok, out) = run('ssh %s "%s"' % (host, cmd))
    if not ok:
        log_error('Unable to get Greenplum database version on remote host %s' % host)
        return False
    remote = out[0].strip()
    
    if current != remote:
        log_error('Greenplum database version does not match. %s != %s' % (current, remote))
        return False
    return True
    

#############
def postgres_active(port, host=None):
    ret = 0
    pg_lock_file = '/tmp/.s.PGSQL.%d.lock' % port
    pg_lock_netstat = 0
    pid = 0

    # ping host if present
    if host:
        ok = ping_host(host)
        if not ok:
            return (ok, pid)

    # netstat to check the port number
    (ok, out) = run('%s %s -an 2> /dev/null | %s ".s.PGSQL.%d" | %s \'{print $NF}\'| %s -F"." \'{print $NF}\' | %s -u' % 
                    (host and gplib.ssh_prefix(host=host) or '', ENV.NETSTAT, ENV.GREP, port, ENV.AWK, ENV.AWK, ENV.SORT))
    if not ok:
        return (ok, pid)

    for p_chk in out:
        p = int(p_chk)
        if p == port:
            pg_lock_netstat = port
    pg_lock_tmp = False
    if file_exists(file=pg_lock_file, host=host):
        pg_lock_tmp = True
            
    if pg_lock_netstat == 0 and pg_lock_tmp == False:
        ret = 1
        pid = 0
        log_info('No socket connection or lock file in /tmp found for %s port=%d' % (host and host or '', port))
    else:
        if not pg_lock_tmp and pg_lock_netstat != 0:
            log_warn('No lock file %s but process running on %s port %d' % (pg_lock_file, host and host or '', port))
            ret = 1

        if pg_lock_tmp and pg_lock_netstat == 0:
            if file_exists(file=pg_lock_file, host=host):
                (ok, out) = run('%s %s %s | %s -1 | %s \'{print $1}\'' % (host and gplib.ssh_prefix(host=host) or '', ENV.CAT, pg_lock_file, ENV.HEAD, ENV.AWK))
                if not ok:
                    return (ok, pid)
                pid = int(out[0])
            else:
                log_warn('Unable to access %s' % pg_lock_file)
            log_warn('Have lock file %s but no process running on %s port %d' % (pg_lock_file, host and host or '', port))
            ret = 1
        if pg_lock_tmp and pg_lock_netstat != 0:
            if file_exists(file=pg_lock_file, host=host):
                (ok, out) = run('%s %s %s | %s -1 | %s \'{print $1}\'' % (host and gplib.ssh_prefix(host=host) or '', ENV.CAT, pg_lock_file, ENV.HEAD, ENV.AWK))
                if not ok:
                    return (ok, pid)
                pid = int(out[0])
            else:
                log_warn('Unable to access %s' % pg_lock_file)
                ret = 1
            #log_info('Have lock file %s and a process running on %s port %d' % (pg_lock_file, host and host or '', port))

    return (not ret, pid)

#############
def get_master_port(master_data_dir):
    master_port = 0
    if not os.path.exists(master_data_dir):
        fatal('No %s directory' % master_data_dir)
    if os.path.exists(os.path.join(master_data_dir, 'postgresql.conf')):
        (ok, out) = run('%s \'split($0,a,"#")>0 && split(a[1],b,"=")>1 {print b[1] " " b[2]}\' %s/postgresql.conf | %s \'$1=="port" {print $2}\' | %s -1' % (ENV.AWK, master_data_dir, ENV.AWK, ENV.TAIL))
        if not ok or out[0].strip() == '':
            fatal('Failed to obtain master port number from %s/postgresql.conf' % master_data_dir)
        master_port = int(out[0])
    else:
        fatal('Do not have access to %s/postgresql.conf' % master_data_dir)

    return master_port
            
#############
def chk_db_running(db, chk_dispatch_access):
    master_data_directory = os.getenv('MASTER_DATA_DIRECTORY')
    if not directory_exists(master_data_directory):
        fatal('No Master %s directory' % master_data_directory)
    if not file_exists('%s/postgresql.conf' % master_data_directory):
        fatal('No %s/postgresql.conf file' % master_data_directory)
    port = get_master_port(master_data_directory)
    
    try:
        db.execute('''select d.datname as "Name", r.rolname as "Owner", 
pg_catalog.pg_encoding_to_char(d.encoding) as "Encoding" 
FROM pg_catalog.pg_database d JOIN pg_catalog.pg_authid r ON d.datdba = r.oid ORDER BY 1''')
        if chk_dispatch_access:
            pass
    except pg8000.errors.InterfaceError, e:
        return False

    return True

    
#############
def unique(alist):
    set = {}
    map(set.__setitem__, alist, [])
    return set.keys()

#############
def chk_multi_home(hostlist, full=True):
    hosts = full and hostlist or hostlist[0:2]
    hostnames=[]
    for host in hosts:
        (ok, out) = run('%s %s' % (gplib.ssh_prefix(host=host), ENV.HOSTNAME))
        if not ok:
            error('failed to run hostname on remote host')
        hostnames.append(out[0].strip())

    hostnames_uniq = unique(hostnames)
    return len(hostnames_uniq) != len(hostnames)

    
#############
def get_qe_details(db, order_by = "dbid"):
    db.execute('''
select a.hostname, fse.fselocation as datadir, a.port, b.valid, 
       b.definedprimary, a.dbid, a.content 
  from gp_segment_configuration a, gp_pgdatabase b, pg_filespace_entry fse 
 where a.dbid=b.dbid and fsedbid=a.dbid 
   and fsefsoid = (select oid from pg_filespace where fsname='pg_system') 
   and a.content <> -1 order by a.%s''' % order_by)
    rows = [ r for r in db.iterate_dict() ]
    return rows

    
#############
def chk_mirrors_configured(db):

    db.execute('select count(dbid)/count(distinct(content)) from gp_segment_configuration where content<>-1')
    r = db.read_tuple()
    return r[0] == 2

#############
def get_mirror_type(db, multi_home):
    if not chk_mirrors_configured(db):
        mir_type = 'No Mirror'
        mir_type_num = ENV.MIRROR_NULL_TYPE
    else:
        db.execute("select count(distinct hostname)/(select count(distinct hostname) from gp_segment_configuration where preferred_role='p' and content<>-1) from gp_segment_configuration where content<>-1")
        r = db.read_tuple()
        sep_count = r[0]
        if sep_count == 2:
            sep_text = '[Separate array]'
        else:
            sep_text = '[Shared array]'

        # get number of primary hosts
        db.execute("select count(distinct(hostname)) from gp_segment_configuration where content<>-1")
        r = db.read_tuple()
        num_seg_hosts = r[0]
        # get the primary and mirror hostnames for the first segment instance host
        db.execute("select hostname, content, preferred_role='p' as definedprimary from gp_segment_configuration where content>-1 and content<(select max(port)-min(port)+1 from gp_segment_configuration where content<>-1 and preferred_role='p') order by content,dbid")
        first_pri_mir_array = [ x for x in db.iterate_dict() ]
        first_pri_array = filter(lambda (x): x['definedprimary'] == True, first_pri_mir_array)
        first_mir_array = filter(lambda (x): x['definedprimary'] == False, first_pri_mir_array)
        seg_per_host = len(first_pri_mir_array) / 2
        if not multi_home:
            hosts = [ x['hostname'] for x in first_pri_mir_array ]
            if len(unique(hosts)) == 2 or num_seg_hosts == 1:
                mir_type = 'Group [Single-home] %s' % sep_text
                mir_type_num = ENV.MIRROR_SINGLE_HOME_GROUP_TYPE
            else:
                mir_type = 'Spread [Single-home] %s' % sep_text
                mir_type_num = ENV.MIRROR_SINGLE_HOME_SPREAD_TYPE
        else:
            mir_array = []
            for i in range(seg_per_host):
                pri_host = first_pri_array[i]['hostname']
                db.execute("select hostname from gp_segment_configuration where preferred_role = 'm' and content = %d" % (i))
                r = db.read_tuple()
                mir_host = r[0]
                (ok, out) = run('%s "hostname"' % (gplib.ssh_prefix(host=mir_host)))
                if not ok:
                    error('hostname on %s failed' % mir_host)
                mir_array.append(out[0])

            uniq_cnt = len(unique(mir_array))
            if uniq_cnt == 1:
                mir_type = 'Group [Multi-home] %s' % sep_text
                mir_type_num = ENV.MIRROR_MULTI_HOME_GROUP_TYPE
            else:
                mir_type = 'Spread [Mutli-home] %s' % sep_text
                mir_type_num = ENV.MIRROR_MULTI_HOME_SPREAD_TYPE
                
    return (mir_type_num, mir_type)
                



#############
def get_ipaddr(host):
    (ok, out) = run('%s "%s %s | %s \\"inet\\"| %s -v \\"127.0.0\\"| %s -v \\"inet6\\""' %
                    (gplib.ssh_prefix(host=host),  ENV.IFCONFIG, 
                     ENV.IFCONFIG_TXT, ENV.GREP, ENV.GREP, ENV.GREP))

    if not ok:
        return None

    addr = []
    for l in out:
        x = l.strip().split()
        ip = x[1].split(':')
        addr.append(len(ip) == 2 and ip[1] or ip[0])
        
    return addr

#############
def get_ipaddr_to_stdout(host):
    addr = get_ipaddr(host)
    if addr:
        for a in addr:
            sys.stdout.write(a)
            sys.stdout.write(' ')
        sys.stdout.write('\n')
    else:
        sys.exit(1)


#############
def edit_file(host, file, search_txt, sub_txt, append=False):
    if host != 'localhost':
        gphome = os.environ.get('GPHOME')
        cmd = makeCommand('''python -c \\"import sys; sys.path.extend(['%s', '%s']); import gpmlib; gpmlib.edit_file('localhost', '%s', '%s', '%s', %s)\\"''' % 
                          (os.path.join(gphome, 'bin', 'lib'),
                           os.path.join(gphome, 'lib', 'python'),
                           file,
                           search_txt,
                           sub_txt,
                           str(append)))

        (ok, out) = run2('''%s "%s"''' % (gplib.ssh_prefix(host=host), cmd), True)
        return ok
    else:

        f = None
        tmpf = None
        tmpfile = '%s.tmp' % file
        try:
            try:
                f = open(file, 'r')
                tmpf = open(tmpfile, 'w')
                for line in f:
                    if line.find(search_txt) != -1:
                        tmpf.write(sub_txt)
                        if append:
                            tmpf.write(' # ')
                            tmpf.write(line)
                        else:
                            tmpf.write('\n')
                    else:
                        tmpf.write(line)
            except IOError, e:
                log_error(str(e))
                return False
        finally:
            if f: f.close()
            if tmpf: tmpf.close()

        try:
            os.rename(tmpfile, file)
        except OSError, e:
            log_error('rename file from %s to %s failed' % (tmpfile, file))
            return False


        return True


class _SyncProgress:
    
    markLock = threading.Lock()
    
    def __init__(self, logfunc, dst_cfg):
        self.logfunc = logfunc
        self.dst_host = dst_cfg["hostname"]
        self.dst_dir = dst_cfg["datadir"]
        self.dbid = dst_cfg["dbid"] if dst_cfg.has_key("dbid") else None
        self.content = dst_cfg["content"] if dst_cfg.has_key("content") else None
        self._tag = "%s:dbid=%s" % (self.dst_host, self.dbid) if self.dbid != None else "%s:%s" % (self.dst_host, self.dst_dir)
    
    def mark_sync_progress(self, message):
        with _SyncProgress.markLock:
            self.logfunc("%s %s" % (self._tag, message))


#############
def sync_segment_datadir(src_host, src_dir, dst_cfg, logfile, 
                         force_stop=True, setpid_callback=None, err_is_fatal=True, 
                         verbose=False, progressBytes=None, progressTime=None):
    
    if setpid_callback:
        log_warn('Use of sync_segment_datadir setpid_callback keyword is deprecated')

    if directory_exists(dst_cfg['datadir'], dst_cfg['hostname']):
        log_info('Data Directory %s exists' % dst_cfg['datadir'])
    else:
        # make mirror data directory
        log_info('Make directory %s' % dst_cfg['datadir'])
        (ok, out) = run('%s "%s %s && chmod 700 %s"' % 
                        (gplib.ssh_prefix(host=dst_cfg['hostname']),
                         ENV.MKDIR,
                         dst_cfg['datadir'],
                         dst_cfg['datadir']))
        if not ok:
            fatal('mkdir directory %s host %s failed' % (dst_cfg['datadir'], dst_cfg['hostname']))


    # Call PysyncProxy to initiate the sync operation with progress feedback
    pysyncOptions = ["--delete", "-x", "db_dumps", "-x", "pg_log"]
    sync = pysync.PysyncProxy(src_host, src_dir, dst_cfg['hostname'], dst_cfg['datadir'], 
                              pysyncOptions, verbose=verbose, 
                              progressBytes=progressBytes, progressTime=progressTime, 
                              recordProgressCallback=_SyncProgress(log_info, dst_cfg).mark_sync_progress)
    code = sync.run()
    ok = (code == 0)

    if not ok:
        log_warn('-----------------------------------------------------')
        log_warn('Command Failed: %s' % sync.cmd)
        log_warn('Return code: %d' % code)
        log_warn('Exit status: %d' % os.WEXITSTATUS(sync.returncode))
        if sync.stdout:
            log_warn('Standard output:')
            for l in sync.stdout:
                log_warn('\t %s' % l.strip())
        else:
            log_warn('Standard output:  None')

        if sync.stderr:
            log_warn('Standard error:')
            for l in sync.stderr:
                log_warn('\t %s' % l.strip())
        else:
            log_warn('Standard error:   None')
        log_warn('-----------------------------------------------------')

        if err_is_fatal:
            fatal('failed to synchronize data from primary segment %s:%s to mirror segment %s:%s' % (src_host, src_dir, 
                                                                                                 dst_cfg['hostname'], dst_cfg['datadir']))
        else:
            log_error('failed to synchronize data from primary segment %s:%s to mirror segment %s:%s' % (src_host, src_dir, 
                                                                                                 dst_cfg['hostname'], dst_cfg['datadir']))
            return False

    # delete postmaster.pid in case active segment is still running
    postmaster_pid = os.path.join(dst_cfg['datadir'], 'postmaster.pid')
    (ok, out) = run('%s "if [ -f %s ] ; then rm -rf %s; fi"' %
                    (gplib.ssh_prefix(host=dst_cfg['hostname']),
                     postmaster_pid,
                     postmaster_pid))

    # delete postmaster.opts in case active segment is still running
    postmaster_opts = os.path.join(dst_cfg['datadir'], 'postmaster.opts')
    (ok, out) = run('%s "if [ -f %s ] ; then rm -rf %s; fi"' %
                    (gplib.ssh_prefix(host=dst_cfg['hostname']),
                     postmaster_opts,
                     postmaster_opts))
                     

    # change port number in postgresql.conf
    sub_txt = 'port=%d' % dst_cfg['port']
    ok = edit_file(dst_cfg['hostname'], os.path.join(dst_cfg['datadir'], 'postgresql.conf'), 
                   'port=', sub_txt, True)
    if not ok:
        fatal('failed to edit postgresql.conf')

    # start mirror segment in admin mode
    log_info('create pg_log directory')
    cmd = makeCommand('mkdir %s/pg_log' % dst_cfg['datadir'])
    (ok, out) = run_warn('%s "%s"' % (gplib.ssh_prefix(host=dst_cfg['hostname']),cmd))


    log_info('sync segment datadir=%s port=%d on host %s succeeded' % (dst_cfg['datadir'], dst_cfg['port'], dst_cfg['hostname']))
    return True    
    
    log_info('start segment datadir=%s port=%d on %s' % (dst_cfg['datadir'], dst_cfg['port'], dst_cfg['hostname']))
    cmd = makeCommand('env PGOPTIONS=\\"-c gp_session_role=utility\\" %s -w -D %s -l %s/pg_log/startup.log -o \\"-i -p %d \\" start 2>&1' %
                      (os.path.join(ENV.GPHOME, 'bin/pg_ctl'), 
                      dst_cfg['datadir'], dst_cfg['datadir'], dst_cfg['port']))    
    (ok, out) = run_warn('%s "%s"' % 
                         (gplib.ssh_prefix(host=dst_cfg['hostname']),
                          cmd))
    if not ok:
        fatal('failed to start mirror segment %s:%s in admin mode' % (dst_cfg['hostname'], dst_cfg['datadir']))

    if force_stop:
        # stop mirror segment
        log_info('stop segment datadir=%s port=%d on %s' % (dst_cfg['datadir'], dst_cfg['port'], dst_cfg['hostname'])) 
        cmd = makeCommand('env PGOPTIONS=\\"-c gp_session_role=utility\\" %s -w stop -D %s -m smart -l %s.log' % 
                          (os.path.join(ENV.GPHOME, 'bin/pg_ctl'), 
                           dst_cfg['datadir'], dst_cfg['datadir']))
        (ok, out) = run_warn('%s "%s" >> %s 2>&1' % 
                             (gplib.ssh_prefix(host=dst_cfg['hostname']),
                              cmd,
                              logfile))        
        if not ok:
            fatal('failed to stop mirror segment %s:%s' % (dst_cfg['hostname'], dst_cfg['datadir']))
    

    log_info('sync segment datadir=%s port=%d on host %s succeeded' % (dst_cfg['datadir'], dst_cfg['port'], dst_cfg['hostname']))
    return True

#############
def append_pg_hba(host, datadir, new_addr):
    pg_hba = os.path.join(datadir, 'pg_hba.conf')
    for addr in new_addr:
        (ok, out) = run_warn('%s "echo host all all %s/32 trust >> %s"' % 
                        (gplib.ssh_prefix(host=host), 
                         addr, pg_hba))
        if not ok:
            fatal('update %s:%s failed' % (host, pg_hba))

    return True
                        

#############
def get_gp_prefix(datadir):
    base = os.path.basename(datadir)
    idx = base.rfind('-1')
    if idx == -1:
        return None
    return base[0:idx]

##################
def chk_on_passive_standby(master_port):
    gpsync_count = 0
    (ok, pid) = postgres_active(master_port)
    postmaster_opts = os.path.join(os.environ.get('MASTER_DATA_DIRECTORY'), 'postmaster.opts')
    if os.path.isfile(postmaster_opts):
        (okok, out) = run('%s -c gpsync %s' % (ENV.GREP, postmaster_opts))
        gpsync_count = int(out[0])

    if ok and gpsync_count != 0:
        log_fatal('Cannot run this script on a passive standby instance')
        log_fatal('where there is a conflict with the current value of')
        log_fatal('the MASTER_DATA_DIRECTORY environment variable setting.')
        return True

    if gpsync_count != 0:
        log_fatal('Cannont run this script on the standby instance')
        log_fatal('Status indicates that standby instance process not running')
        log_fatal('Check standby process status via gpstate -f on Master instance')
        return True

    return False



def get_copy_filter(host, port):
    # pg8000 connects to master in utility mode
    db = None
    databases = None
    filters = []

    try:
        db = pg8000.Connection(host=host, user=ENV.USER, database='template1', port=port, options='-c gp_session_role=utility')
        
        # get list of user databases
        # select oid, datname from pg_database where oid >= 16384;
        db.execute('select oid, datname from pg_database where oid >= 16384')
#        db.execute('select oid, datname from pg_database')
        databases = [ r for r in db.iterate_tuple() ]

    except Exception, e:
        print str(e)
        return None
    finally:
        if db:
            db.close()
            db = None

    # foreach database
    for d in databases:
        try:
            oid = d[0]
            datname = d[1]
            db = pg8000.Connection(host=host, user=ENV.USER, database=datname, port=port,  options='-c gp_session_role=utility')

            # get user table filter (user table, TOAST table, sequences)
            # select oid from pg_class where old >= 16384 and relkind='r';
            db.execute("select oid from pg_class where oid >= 16384 and relkind='r'")
            table_filter = [ r[0] for r in db.iterate_tuple() ] 
            
            # get user TOAST table filter
            # select oid from pg_class where old >= 16384 and relkind='t';    
            db.execute("select oid from pg_class where oid >= 16384 and relkind='t'")
            toast_filter = [ r[0] for r in db.iterate_tuple() ] 

            # get sequences filter
            # select oid from pg_class where old >= 16384 and relkind='S';
            db.execute("select oid from pg_class where oid >= 16384 and relkind='S'")
            sequence_filter = [ r[0] for r in db.iterate_tuple() ] 

            # get index filter
            # select oid from pg_class where old >= 16384 and relkind='i';    
            db.execute("select oid from pg_class where oid >= 16384 and relkind='i'")
            index_filter = [ r[0] for r in db.iterate_tuple() ] 

            filters.append((oid, table_filter, toast_filter, sequence_filter, index_filter))
        except Exception, e:
            print str(e)
            continue
        finally:
            if db:
                db.close()
                db = None

    return filters

