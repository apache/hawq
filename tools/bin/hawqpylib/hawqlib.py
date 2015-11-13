#!/usr/bin/env python

import os, sys
import subprocess
import threading
from xml.dom import minidom
from xml.etree.ElementTree import ElementTree
from gppylib.db import dbconn


class HawqCommands(object):
    def __init__(self, function_list=None, name='HAWQ'):
        self.function_list = function_list
        self.name = name 
        self.return_flag = 0
        self.thread_list = []

    def get_function_list(self, function_list):
        self.function_list = function_list

    def exec_function(self, func, *args, **kwargs):
        result = func(*args, **kwargs)
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
            name, value = (node.getElementsByTagName('name')[0].childNodes[0].data,
                           node.getElementsByTagName('value')[0].childNodes[0].data)
            if name == property_name:
                self.propertyValue = value
        return self.propertyValue

    def get_all_values(self):
        with open(self.xml_file) as f:
            xmldoc = minidom.parse(f)
        for node in xmldoc.getElementsByTagName('property'):
            name, value = (node.getElementsByTagName('name')[0].childNodes[0].data.encode('ascii'),
                           node.getElementsByTagName('value')[0].childNodes[0].data.encode('ascii'))
            if value == '':
                value == 'None'
            self.hawq_dict[name] = value
        return None

    def get_xml_doc(self):
        with open(self.xml_file) as f:
            xmldoc = minidom.parse(f)
        return xmldoc


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


def remote_ssh(cmd, host, user):
    if user == "":
        remote_cmd_str = "ssh -o 'StrictHostKeyChecking no' %s \"%s\"" % (host, cmd)
    else:
        remote_cmd_str = "ssh -o 'StrictHostKeyChecking no' %s@%s \"%s\"" % (user, host, cmd)
    result = subprocess.Popen(remote_cmd_str, shell=True).wait()
    return result

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


def check_file_exist(file_path, hostlist, user):
    if user == "":
        user = os.getenv('USER')
    file_exist_host_list = {}
    for host in hostlist:
        result = remote_ssh("test -f %s;" % file_path, host, user)
        if result == 0:
            file_exist_host_list[host] = 'exist'
    return file_exist_host_list


def remove_property_xml(property_name, org_config_file):
    tree = ElementTree()
    tree.parse(org_config_file)
    root = tree.getroot()
    for child in root:
        for subet in child:
            if subet.text == property_name:
                print "Remove property %s." % subet.text
                root.remove(child)
    tree.write(org_config_file, encoding="utf-8")


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
