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
# encoding: utf-8
'''
This file contains all API for calling hdfs related commands directly
'''

import logging
import os, sys, subprocess
import re

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.DEBUG)

class HDFS_Cmd(object):
    def __init__(self, hadoop_dir=None):
        self.hadoop_dir = hadoop_dir;

    def run_shell_cmd(self, cmd, isRaiseException=True, isReturnRC=False):
        logger.debug('shell command is: %s', (cmd))
        p = subprocess.Popen(cmd, shell=True,
                                close_fds=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        result = p.communicate()
        if isReturnRC:
            return p.returncode
        if p.returncode != 0:
            logger.debug(result[0])
            if isRaiseException:
                logger.error("Error at execute: %s %s %s" % (str(cmd), result[0], result[1]))
                raise Exception(result[1])
        return result[0]

    def check_hdfs(self):
        if self.hadoop_dir==None:
            self.hadoop_dir = os.getenv('HADOOP_HOME')
        if self.hadoop_dir==None:
            try:
                self.hadoop_dir = self.run_shell_cmd('which hadoop').rsplit('/', 2)[0]
            except Exception, e:
                logger.fatal('which hadoop failure: ' + str(e))
                raise  Exception("Can not find the path for hadoop command from env $HADOOP_HOME or $PATH")
        if self.hadoop_dir!=None:
            self.hdfs_path = os.path.join(self.hadoop_dir, 'bin')
            if (not os.path.exists(self.hdfs_path )):
                logger.error("Can not find the path: %s." % str(self.hdfs_path ))
                raise Exception("Can not find the path: %s." % str(self.hdfs_path ))

    def parse_command_output(self,inputs, regexps):
        '''
        inputs:
            inputs: it can be a list with a nested list (like a table: row, column),
                or a string includes all lines, which can be transformed as a table with one column
            regexps: it can be a list with format [[input_col_id, regexp, output_group_num], ...] ,
                or it can be a string which can be transformed as [[0, regexps, 1]]

        Parse the inputs string with regular expression matched on each line, and fetch the groups of matched part as colums of one row
        '''
        result=[]
        line_id = 0
        p=[]

        # if it is a string
        if not hasattr(regexps, '__iter__') or isinstance(regexps, basestring):
            regexps = [[0, regexps, 1]]
        for rei in regexps:
            if(len(rei)!=3):
                logger.error("Format error for : %s" % (rei))
                raise Exception("Format error for : %s" % (rei))
            p.append(re.compile(rei[1]))

        # if inputs is a non-string sequence and is iterable, then it should a list, each item stands for a line
        if hasattr(inputs, '__iter__') and not isinstance(inputs, basestring):
            content = inputs
        else:
            content = inputs.splitlines()

        while(line_id<len(content)):
            row = []
            m = []
            is_all_matched = True
            j=-1
            for rei in regexps:
                j+=1
                input_col_id = rei[0]
                if input_col_id==-1 or content != inputs: #the whole item
                    line = content[line_id]
                else:
                    line = content[line_id][input_col_id]

                mi = p[j].match(line)
                if mi:
                    m.append(mi)
                else:
                    is_all_matched = False
                    break

            if is_all_matched:
                i=-1
                for rei in regexps:
                    i+=1
                    group_num = rei[2]
                    for gid in range(1, group_num+1):
                        row.append(m[i].group(gid));
                result.append(row)

            line_id+=1;
            
        return result

    def run_hdfs_command(self,cmd, isDfs=True, isRaiseException=True, isReturnRC=False):

        _cmd = os.path.join(self.hdfs_path, 'hdfs')
        if (not os.path.exists(_cmd)):
            logger.error("Can not find the path: %s." % str(_cmd))
            raise Exception("Can not find the path: %s." % str(_cmd))
        if isDfs:
            _cmd += ' dfs -'
        else:
            _cmd +=' '

        _cmd += '%s' % (cmd)
        return self.run_shell_cmd(_cmd,isRaiseException,isReturnRC)


    def run_hdfs_ls(self, path, isRecurs=False):
        """
        Return [['d', 'dir_path1'], ['-', 'file_path1'], ...]
        """
        cmd = 'ls '
        if isRecurs:
            cmd += '-R '
        cmd += path

        output = self.run_hdfs_command(cmd)
        result = self.parse_command_output(output,[[0,'^([d|-])[r|w|x|-]{9,9}\s+[-|\d]\s+\w+\s+\w+\s+\d+\s+[\d|-]+\s+[\d|:]+\s+(.*)', 2]])
        return result

    def run_hdfs_mkdir(self, path, isRecurs=False):
        cmd = 'mkdir '
        if isRecurs:
            cmd += '-p '
        cmd += path

        self.run_hdfs_command(cmd)

    def run_hdfs_rm(self, path, isRecurs=False):
        cmd = 'rm '
        if isRecurs:
            cmd += '-R '
        cmd += path

        self.run_hdfs_command(cmd)

    def run_hdfs_mv(self, path, new_path):
        cmd = 'mv %s %s'% (path, new_path)
        self.run_hdfs_command(cmd)

    def run_hdfs_test(self, path, flag):
        cmd = 'test %s %s' % (flag, path)
        return self.run_hdfs_command(cmd, isReturnRC=True)

    def run_hdfs_leave_safemode(self):
        # force to leave safe mode
        cmd = 'dfsadmin -safemode leave'
        self.run_hdfs_command(cmd, isDfs=False, isRaiseException=True)

    def run_hdfs_create_snapshot(self,path, snapshotname):
        # add permission
        cmd = 'dfsadmin -allowSnapshot %s' % path;
        self.run_hdfs_command(cmd, isDfs=False)

        cmd = 'createSnapshot %s '% (path)
        if snapshotname:
            cmd += snapshotname
        self.run_hdfs_command(cmd)

    def run_hdfs_rename_snapshot(self, path, oldname, newname):
        cmd = 'renameSnapshot %s %s %s'% (path, oldname, newname)
        self.run_hdfs_command(cmd)

    def run_hdfs_delete_snapshot(self, path, snapshotname):
        cmd = 'deleteSnapshot %s %s'% (path, snapshotname)
        self.run_hdfs_command(cmd)


    def run_hdfs_restore_snapshot(self, path, snapshotname):
        ss_path = os.path.join(path, '.snapshot', snapshotname)
        result = self.run_hdfs_ls(ss_path, True)
        for dir, path1 in result:
            if dir!='d':
                path2 = path1.replace("/.snapshot/%s"%snapshotname, "")
                cmd = 'cp -f -p %s %s' % (path1, path2)
                self.run_hdfs_command(cmd)
        # successfully restored snapshot, we can delete the snapshot now
        self.run_hdfs_delete_snapshot(path, snapshotname)

if __name__ == '__main__':
    # firstly test gdb exists
    #gdb_path = spawn.find_executable("hdfs")
    #if not gdb_path:
        #logger.info("hadoop hdfs command doesn't exists, please make sure you have installed hadoop!")
        #exit(1);
    hdfs_cmd = HDFS_Cmd('/Users/gpadmin/workspace/install/hadoop-2.4.1-gphd-3.2.0.0')
    hdfs_cmd.check_hdfs()
    result = hdfs_cmd.run_hdfs_ls('/hawq1/gpseg0', True)
    print "result: %s" % result
    result1 = hdfs_cmd.run_hdfs_ls('/hawq1/', False)
    print "result1: %s" % result1
    result2 = hdfs_cmd.parse_command_output(result1, [[0, '^(d)$', 1], [1, '^(.*/gpseg\d+)$', 1]])
    print "result2: %s" % result2
    result3 = hdfs_cmd.parse_command_output(result1, [[0, '^d$', 0], [1, '^(.*/gpseg\d+)$', 1]])
    print "result3: %s" % result3
    result4 = hdfs_cmd.run_hdfs_test('hdfs://localhost:9000/hawq1/gpseg0/.snapshot/.gpmigrator_orig', '-e');
    print "result4: %s" % result4
    result4 = hdfs_cmd.run_hdfs_test('hdfs://localhost:9000/hawq1/gpseg0/.snapshot/.gpmigrator_orig1', '-e');
    print "result4: %s" % result4

    hdfs_cmd = HDFS_Cmd()
    hdfs_cmd.check_hdfs()
    result = hdfs_cmd.run_hdfs_ls('/hawq1/gpseg1', True)
    print "result: %s" % result

