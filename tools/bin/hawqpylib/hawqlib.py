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

import os, sys
import subprocess
import threading
import Queue
from xml.dom import minidom
from xml.etree.ElementTree import ElementTree
import shutil
from gppylib.db import dbconn
from gppylib.commands.base import WorkerPool, REMOTE
from gppylib.commands.unix import Echo
import re


class HawqCommands(object):
    def __init__(self, function_list=None, name='HAWQ', action_name = 'execute', logger = None):
        self.function_list = function_list
        self.name = name 
        self.action_name = action_name 
        self.return_flag = 0
        self.thread_list = []
        if logger:
            self.logger = logger

    def get_function_list(self, function_list):
        self.function_list = function_list

    def exec_function(self, func, *args, **kwargs):
        result = func(*args, **kwargs)
        if result != 0 and self.logger and func.__name__ == 'remote_ssh':
            self.logger.error("%s %s failed on %s" % (self.name, self.action_name, args[1]))
        self.return_flag += result

    def start(self):
        self.thread_list = []
        self.return_flag = 0
        for func_dict in self.function_list:
            if func_dict["args"]:
                new_arg_list = []
                new_arg_list.append(func_dict["func"])
                for arg in func_dict["args"]:
                    new_arg_list.append(arg)
                new_arg_tuple = tuple(new_arg_list)
                t = threading.Thread(target=self.exec_function, args=new_arg_tuple, name=self.name)
            else:
                t = threading.Thread(target=self.exec_function, args=(func_dict["func"],), name=self.name)
            self.thread_list.append(t)

        for thread_instance in self.thread_list:
            thread_instance.start()
            #print threading.enumerate()

        for thread_instance in self.thread_list:
            thread_instance.join()

    def batch_result(self):
        return self.return_flag


class threads_with_return(object):
    def __init__(self, function_list=None, name='HAWQ', action_name = 'execute', logger = None, return_values = None):
        self.function_list = function_list
        self.name = name
        self.action_name = action_name
        self.return_values = return_values
        self.thread_list = []
        self.logger = logger

    def get_function_list(self, function_list):
        self.function_list = function_list

    def exec_function(self, func, *args, **kwargs):
        result = func(*args, **kwargs)
        if result != 0 and self.logger and func.__name__ == 'remote_ssh':
            self.logger.error("%s %s failed on %s" % (self.name, self.action_name, args[1]))
        self.return_values.put(result)

    def start(self):
        self.thread_list = []
        for func_dict in self.function_list:
            if func_dict["args"]:
                new_arg_list = []
                new_arg_list.append(func_dict["func"])
                for arg in func_dict["args"]:
                    new_arg_list.append(arg)
                new_arg_tuple = tuple(new_arg_list)
                t = threading.Thread(target=self.exec_function, args=new_arg_tuple, name=self.name)
            else:
                t = threading.Thread(target=self.exec_function, args=(func_dict["func"],), name=self.name)
            self.thread_list.append(t)

        for thread_instance in self.thread_list:
            thread_instance.start()
            #print threading.enumerate()

        for thread_instance in self.thread_list:
            thread_instance.join()

    def batch_result(self):
        return self.return_values


def check_property_exist_xml(xml_file, property_name):
    property_exist = False
    property_value = ''
    with open(xml_file) as f:
        xmldoc = minidom.parse(f)
    for node in xmldoc.getElementsByTagName('property'):
        name, value = (node.getElementsByTagName('name')[0].childNodes[0].data,
                       node.getElementsByTagName('value')[0].childNodes[0].data)
        if name == property_name:
            property_exist = True
            property_value = value
    return property_exist, property_name, property_value


def get_xml_values(xmlfile):
    xml_dict = {}
    with open(xmlfile) as f:
        xmldoc = minidom.parse(f)

    for node in xmldoc.getElementsByTagName('property'):
        name = node.getElementsByTagName('name')[0].childNodes[0].data.encode('ascii')

        try:
            value = node.getElementsByTagName('value')[0].childNodes[0].data.encode('ascii')
        except:
            value = None

        xml_dict[name] = value

    return xml_dict


class HawqXMLParser:
    def __init__(self, GPHOME):
        self.GPHOME = GPHOME
        self.xml_file = "%s/etc/hawq-site.xml" % GPHOME
        self.hawq_dict = {}
        self.propertyValue = ""

    def get_value_from_name(self, property_name):
        with open(self.xml_file) as f:
            xmldoc = minidom.parse(f)
        for node in xmldoc.getElementsByTagName('property'):
            name = node.getElementsByTagName('name')[0].childNodes[0].data.encode('ascii')
            try:
                value = node.getElementsByTagName('value')[0].childNodes[0].data.encode('ascii')
            except:
                value = ''

            if name == property_name:
                self.propertyValue = value
        return self.propertyValue

    def get_all_values(self):
        with open(self.xml_file) as f:
            xmldoc = minidom.parse(f)

        for node in xmldoc.getElementsByTagName('property'):
            name = node.getElementsByTagName('name')[0].childNodes[0].data.encode('ascii')

            try:
                value = node.getElementsByTagName('value')[0].childNodes[0].data.encode('ascii')
            except:
                value = ''

            if value == '':
                value == 'None'
            self.hawq_dict[name] = value

        if 'hawq_standby_address_host' in self.hawq_dict:
            if self.hawq_dict['hawq_standby_address_host'].lower() in ['none', '', 'localhost']:
                del self.hawq_dict['hawq_standby_address_host']

        return None

    def get_xml_doc(self):
        with open(self.xml_file) as f:
            xmldoc = minidom.parse(f)
        return xmldoc


def check_hostname_equal(remote_host, user = ""):
    cmd = "hostname"
    result_local, local_hostname, stderr_remote  = local_ssh_output(cmd)
    result_remote, remote_hostname, stderr_remote = remote_ssh_output(cmd, remote_host, user)
    if local_hostname.strip() == remote_hostname.strip():
        return True
    else:
        return False


def check_hawq_running(host, data_directory, port, user = '', logger = None):

    hawq_running = True
    hawq_pid_file_path = data_directory + '/postmaster.pid'

    if check_file_exist(hawq_pid_file_path, host, logger):
        if not check_postgres_running(data_directory, user, host, logger):
            if logger:
                logger.warning("Have a postmaster.pid file but no hawq process running")

            lockfile="/tmp/.s.PGSQL.%s" % port
            if logger:
                logger.info("Clearing hawq instance lock files and pid file")
            cmd = "rm -rf %s %s" % (lockfile, hawq_pid_file_path)
            remote_ssh(cmd, host, user)
            hawq_running = False
        else:
            hawq_running = True

    else:
        if check_postgres_running(data_directory, user, host, logger):
            if logger:
                logger.warning("postmaster.pid file does not exist, but hawq process is running.")
            hawq_running = True
        else:
            if logger:
                logger.warning("HAWQ process is not running on %s, skip" % host)
            hawq_running = False

    return host, hawq_running


def local_ssh(cmd, logger = None, warning = False):
    result = subprocess.Popen(cmd, shell=True, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    stdout,stderr = result.communicate()
    if logger:
        if stdout != '':
            logger.info(stdout.strip())
        if stderr != '':
            if not warning:
                logger.error(stderr.strip())
            else:
                logger.warn(stderr.strip())
    return result.returncode


def local_ssh_output(cmd):
    result = subprocess.Popen(cmd, shell=True, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    stdout,stderr = result.communicate()

    return (result.returncode, str(stdout.strip()), str(stderr.strip()))


def remote_ssh(cmd, host, user):

    if user == "":
        remote_cmd_str = "ssh -o 'StrictHostKeyChecking no' %s \"%s\"" % (host, cmd)
    else:
        remote_cmd_str = "ssh -o 'StrictHostKeyChecking no' %s@%s \"%s\"" % (user, host, cmd)
    try:
        result = subprocess.Popen(remote_cmd_str, shell=True).wait()
    except subprocess.CalledProcessError:
        print "Execute shell command on %s failed" % host
        pass

    return result


def remote_ssh_output(cmd, host, user):

    if user == "":
        remote_cmd_str = "ssh -o 'StrictHostKeyChecking no' %s \"%s\"" % (host, cmd)
    else:
        remote_cmd_str = "ssh -o 'StrictHostKeyChecking no' %s@%s \"%s\"" % (user, host, cmd)

    try:
        result = subprocess.Popen(remote_cmd_str, shell=True, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        stdout,stderr = result.communicate()
    except subprocess.CalledProcessError:
        print "Execute shell command on %s failed" % host
        pass

    return (result.returncode, str(stdout.strip()), str(stderr.strip()))


def is_node_alive(host, user = '', logger = None):
    result = remote_ssh('true', host, user)
    if result != 0:
        if logger:
            logger.info("node %s is not alive" % host)
        return False
    else:
        return True


def check_return_code(result, logger = None,  error_msg = None, info_msg = None, exit_true = False):
    '''Check shell command exit code.'''
    if result != 0:
        if error_msg and logger:
            logger.error(error_msg)
        sys.exit(1)
    else:
        if info_msg and logger:
            logger.info(info_msg)
        if exit_true:
            sys.exit(0)
    return result


def check_postgres_running(data_directory, user, host = 'localhost', logger = None):
    cmd='ps -ef | grep postgres | grep %s | grep -v grep > /dev/null || exit 1;' % data_directory
    result = remote_ssh(cmd, host, user)
    if result == 0:
        return True
    else:
        if logger:
            logger.debug("postgres process is not running on %s" % host)
        return False


def check_syncmaster_running(data_directory, user, host = 'localhost', logger = None):
    cmd='ps -ef | grep gpsyncmaster | grep %s | grep -v grep > /dev/null || exit 1;' % data_directory
    result = remote_ssh(cmd, host, user)
    if result == 0:
        return True
    else:
        if logger:
            logger.debug("syncmaster process is not running on %s" % host)
        return False


def check_file_exist(file_path, host = 'localhost', logger = None):
    cmd = "if [ -f %s ]; then exit 0; else exit 1;fi" % file_path
    result = remote_ssh(cmd, host, '')
    if result == 0:
        return True
    else:
        if logger:
            logger.debug("%s not exist on %s." % (file_path, host))
        return False


def check_file_exist_list(file_path, hostlist, user):
    if user == "":
        user = os.getenv('USER')
    file_exist_host_list = {}
    for host in hostlist:
        result = remote_ssh("test -f %s;" % file_path, host, user)
        if result == 0:
            file_exist_host_list[host] = 'exist'
    return file_exist_host_list


def check_directory_exist(directory_path, host, user):
    if user == "":
        user = os.getenv('USER')
    cmd = "if [ ! -d %s ]; then mkdir -p %s; fi;" % (directory_path, directory_path)
    result = remote_ssh("if [ ! -d %s ]; then mkdir -p %s; fi;" % (directory_path, directory_path), host, user)
    if result == 0:
        file_exist = True
    else:
        file_exist = False
    return host, file_exist


def create_cluster_directory(directory_path, hostlist, user = '', logger = None):
    if user == "":
        user = os.getenv('USER')

    create_success_host = []
    create_failed_host = []
    work_list = []
    q = Queue.Queue()
    for host in hostlist:
        work_list.append({"func":check_directory_exist,"args":(directory_path, host, user)})

    dir_creator = threads_with_return(name = 'HAWQ', action_name = 'create', logger = logger, return_values = q)
    dir_creator.get_function_list(work_list)
    dir_creator.start()

    while not q.empty():
        item = q.get()
        if item[1] == True:
            create_success_host.append(item[0])
        else:
            create_failed_host.append(item[0])

    return create_success_host, create_failed_host


def parse_hosts_file(GPHOME):
    host_file = "%s/etc/slaves" % GPHOME
    host_list = list()
    with open(host_file) as f:
        hosts = f.readlines()
    for host in hosts:
        host = host.split("#",1)[0].strip()
        if host:
            host_list.append(host)
    return host_list


def update_xml_property(xmlfile, property_name, property_value):
    file_path, filename = os.path.split(xmlfile)
    xmlfile_backup = os.path.join(file_path, '.bak.' + filename)
    xmlfile_swap = os.path.join(file_path, '.swp.' + filename)

    # Backup current xmlfile
    shutil.copyfile(xmlfile, xmlfile_backup)

    f_tmp = open(xmlfile_swap, 'w')

    with open(xmlfile) as f:
        xmldoc = minidom.parse(f)

    with open(xmlfile) as f:
        while 1:
            line = f.readline()
            if not line:
                break
            m = re.match('.*<configuration>.*', line)
            if m:
                line_1 = line.split('<configuration>')[0] + '<configuration>\n'
                f_tmp.write(line_1)
                break
            else:
                f_tmp.write(line)

    count_num = 0

    for node in xmldoc.getElementsByTagName('property'):

        name = node.getElementsByTagName('name')[0].childNodes[0].data.encode('ascii')

        try:
            value = node.getElementsByTagName('value')[0].childNodes[0].data.encode('ascii')
        except:
            value = ''

        try:
            description = node.getElementsByTagName('description')[0].childNodes[0].data.encode('ascii')
        except:
            description = ''

        if name == property_name:
            value = property_value
            count_num += 1

        f_tmp.write("        <property>\n")
        f_tmp.write("                <name>%s</name>\n" % name)
        f_tmp.write("                <value>%s</value>\n" % value)
        if description:
            f_tmp.write("                <description>%s</description>\n" % description)
        f_tmp.write("        </property>\n\n")

    if count_num == 0:
        f_tmp.write("        <property>\n")
        f_tmp.write("                <name>%s</name>\n" % property_name)
        f_tmp.write("                <value>%s</value>\n" % property_value)
        f_tmp.write("        </property>\n\n")
        f_tmp.write("</configuration>\n")
    else:
        f_tmp.write("</configuration>\n")

    f_tmp.close

    shutil.move(xmlfile_swap, xmlfile)


def remove_property_xml(property_name, xmlfile, quiet = False):
    file_path, filename = os.path.split(xmlfile)
    xmlfile_backup = os.path.join(file_path, '.bak.' + filename)
    xmlfile_swap = os.path.join(file_path, '.swp.' + filename)

    # Backup current xmlfile
    shutil.copyfile(xmlfile, xmlfile_backup)

    f_tmp = open(xmlfile_swap, 'w')

    with open(xmlfile) as f:
        xmldoc = minidom.parse(f)

    with open(xmlfile) as f:
        while 1:
            line = f.readline()
            if not line:
                break
            m = re.match('.*<configuration>.*', line)
            if m:
                line_1 = line.split('<configuration>')[0] + '<configuration>\n'
                f_tmp.write(line_1)
                break
            else:
                f_tmp.write(line)

    for node in xmldoc.getElementsByTagName('property'):

        name = node.getElementsByTagName('name')[0].childNodes[0].data.encode('ascii')

        try:
            value = node.getElementsByTagName('value')[0].childNodes[0].data.encode('ascii')
        except:
            value = ''

        try:
            description = node.getElementsByTagName('description')[0].childNodes[0].data.encode('ascii')
        except:
            description = ''

        if name == property_name:
            if not quiet:
                print "Remove property %s" % property_name
        else:
            f_tmp.write("        <property>\n")
            f_tmp.write("                <name>%s</name>\n" % name)
            f_tmp.write("                <value>%s</value>\n" % value)
            if description:
                f_tmp.write("                <description>%s</description>\n" % description)
            f_tmp.write("        </property>\n\n")

    f_tmp.write("</configuration>\n")

    f_tmp.close

    shutil.move(xmlfile_swap, xmlfile)


def sync_hawq_site(GPHOME, host_list):
    for node in host_list:
        try:
            # Print "Sync hawq-site.xml to %s." % node
            os.system("scp %s/etc/hawq-site.xml %s:%s/etc/hawq-site.xml > /dev/null 2>&1" % (GPHOME, node, GPHOME))
        except:
            print ""
            sys.exit("sync to node %s failed." % node)
    return None

def get_hawq_hostname_all(master_port):
    try:
        dburl = dbconn.DbURL(port=master_port, dbname='template1')
        conn = dbconn.connect(dburl, True)
        query = "select role, status, port, hostname, address from gp_segment_configuration;"
        rows = dbconn.execSQL(conn, query)
        conn.close()
    except DatabaseError, ex:
        print "Failed to connect to database, this script can only be run when the database is up."
        sys.exit(1)

    seg_host_list = {}
    master_host = ''
    master_status = ''
    standby_host = ''
    standby_status = ''
    for row in rows:
        if row[0] == 'm':
            master_host = row[3]
            master_status = 'u'
        elif row[0] == 's':
            standby_host = row[3]
            if row[1] == "u":
                standby_status = "u"
            else:
                standby_status = "Unknown"
        elif row[0] == 'p':
            seg_host_list[row[3]] = row[1]

    hawq_host_array = {'master': {master_host: master_status}, 'standby': {standby_host: standby_status}, 'segment': seg_host_list} 
    return hawq_host_array

def get_host_status(hostlist):
    """
    Test if SSH command works on a host and return a dictionary
    Return Ex: {host1: True, host2: False}
    where True represents SSH command success and False represents failure
    """
    if not isinstance(hostlist, list):
        raise Exception("Input parameter should be of type list")

    pool = WorkerPool(min(len(hostlist), 16))

    for host in hostlist:
        cmd = Echo('ssh test', '', ctxt=REMOTE, remoteHost=host)
        pool.addCommand(cmd)

    pool.join()
    pool.haltWork()

    host_status_dict = {}
    for cmd in pool.getCompletedItems():
        if not cmd.get_results().wasSuccessful():
            host_status_dict[cmd.remoteHost] = False
        else:
            host_status_dict[cmd.remoteHost] = True

    return host_status_dict


def exclude_bad_hosts(host_list):
    """
    Split Hosts on which SSH works vs node on which it fails
    """
    host_status_dict = get_host_status(host_list)
    working_hosts = [host for host in host_status_dict.keys() if host_status_dict[host]]
    bad_hosts = list(set(host_list) - set(working_hosts))
    return working_hosts, bad_hosts
