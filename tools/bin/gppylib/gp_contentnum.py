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
# Line too long - pylint: disable=C0301
# Invalid name  - pylint: disable=C0103

"""
  gp_contentnum.py
"""

import re
import os, stat

class DummyLogger:
    def info(self, msg):  pass
    def debug(self, msg): pass

CONTENTNUM_RE         = re.compile(r"content_num\s*=\s*(\d+)")

class GpContentnumFile:
    """
    Used by gpstart, gpinitstandby, and gpactivatestandby to
    manage the gp_contentnum file.
    """

    def __init__(self, datadir, do_read=False, logger=None):
        """
        Initialize path to gp_contentnum file and reset values.
        Log subsequent activity using specified logger and
        if do_read is True, immediately attempt to read values.
        """
        self.datadir      = datadir
        self.logger       = logger or DummyLogger()
        self.filepath     = os.path.join(self.datadir, 'gp_contentnum')
        self.contentnum   = None

        if do_read:
            self.read_gp_contentnum()


    def read_gp_contentnum(self):
        """
        Open the gp_contentnum file and parse its contents.
        """
        INFO = self.logger.info
        INFO('%s - read_gp_contentnum' % self.filepath)

        with open(self.filepath) as f:
            self.parse(f)


    def parse(self, f):
        """
        Parse f, looking for matching contentnum expressions and
        ignoring all other lines.  Assigns contentnum to observed 
        values, converting matched values from strings to integers.  
        """
        INFO  = self.logger.info
        DEBUG = self.logger.debug

        self.contentnum  = None
        for line in f:
            line = line.strip()
            DEBUG('parse: %s' % line)

            m = re.match(CONTENTNUM_RE, line)
            if m:
                self.contentnum = int(m.group(1))
                INFO('match contentnum: %d' % self.contentnum)

        assert self.contentnum is not None


    def format(self, f):
        """
        Generate gp_contentnum contents based on contentnum values
        """
        INFO  = self.logger.info

        f.write("# Greenplum Database content num for master/standby.\n")
        f.write("# Do not change the contents of this file.\n")
        f.write('contentnum = %d\n' % self.contentnum)
        INFO('wrote contentnum: %d' % self.contentnum)
 

    def write_gp_contentnum(self):
        """
        Create or replace gp_contentnum file with current values, changing
        permissions of the new file when done and verifying by re-reading
        the file contents and checking the values read match desired values
        """
        INFO  = self.logger.info
        INFO('%s - write_gp_contentnum' % self.filepath)

        if os.path.exists(self.filepath):
            INFO('found existing file')

            os.remove(self.filepath)
            INFO('removed existing file')

        self.logger.info('opening new file')
        with open(self.filepath, 'w') as f:
            self.format(f)

        INFO('setting read only')
        os.chmod(self.filepath, stat.S_IRUSR)  # user read permissions (0400)

        INFO('verifying file')
        v = GpContentnumFile(self.datadir, do_read=True)
        assert self.contentnum == v.contentnum


def writeGpContentnumFile(directory, contentnum, logger=None):
    """
    Writes the gp_contentnum file to the given directory, marking it as for the given contentnum.
    This method may be deprecating.  See comments in CR-2806.
    """
    d = GpContentnumFile(directory, logger=logger)
    d.contentnum = contentnum
    d.write_gp_contentnum()




#
# trivial unit test
#
if __name__ == '__main__':
    import copy, shutil
    import unittest2 as unittest

    TESTDIR = 'test_gp_contentnum1'

    class MyTestCase(unittest.TestCase):
        def test1(self):
            d = GpContentnumFile(TESTDIR)
            d2 = copy.copy(d)
            d.contentnum = 10
            d.write_gp_contentnum()
            d2.read_gp_contentnum()
            assert d.contentnum == d2.contentnum
       
    if os.path.exists(TESTDIR): shutil.rmtree(TESTDIR)
    os.mkdir(TESTDIR)
    unittest.main()
    shutil.rmtree(TESTDIR)
