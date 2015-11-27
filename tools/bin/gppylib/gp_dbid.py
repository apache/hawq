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
  gp_dbid.py
"""

import re
import os, stat

class DummyLogger:
    def info(self, msg):  pass
    def debug(self, msg): pass

DBID_RE         = re.compile(r"dbid\s*=\s*(\d+)")
STANDBY_DBID_RE = re.compile(r"standby_dbid\s*=\s*(\d+)")

class GpDbidFile:
    """
    Used by gpstart, gpinitstandby, gpactivatestandby and indirectly
    by gpmigrator via gpsetdbid.py to manage the gp_dbid file.
    """

    def __init__(self, datadir, do_read=False, logger=None):
        """
        Initialize path to gp_dbid file and reset values.
        Log subsequent activity using specified logger and
        if do_read is True, immediately attempt to read values.
        """
        self.datadir      = datadir
        self.logger       = logger or DummyLogger()
        self.filepath     = os.path.join(self.datadir, 'gp_dbid')
        self.dbid         = None
        self.standby_dbid = None

        if do_read:
            self.read_gp_dbid()


    def read_gp_dbid(self):
        """
        Open the gp_dbid file and parse its contents.
        """
        INFO = self.logger.info
        INFO('%s - read_gp_dbid' % self.filepath)

        with open(self.filepath) as f:
            self.parse(f)


    def parse(self, f):
        """
        Parse f, looking for matching dbid and standby_dbid expressions and
        ignoring all other lines.  Assigns dbid and/or standby_dbid to observed 
        values, converting matched values from strings to integers.  
        """
        INFO  = self.logger.info
        DEBUG = self.logger.debug

        self.dbid         = None
        self.standby_dbid = None
        for line in f:
            line = line.strip()
            DEBUG('parse: %s' % line)

            m = re.match(DBID_RE, line)
            if m:
                self.dbid = int(m.group(1))
                INFO('match dbid: %d' % self.dbid)

            m = re.match(STANDBY_DBID_RE, line)
            if m:
                self.standby_dbid = int(m.group(1))
                INFO('match standby_dbid: %d' % self.standby_dbid)

        assert self.dbid is not None


    def format(self, f):
        """
        Generate gp_dbid contents based on dbid and standby_dbid values
        """
        INFO  = self.logger.info

        f.write("# Greenplum Database identifier for this master/segment.\n")
        f.write("# Do not change the contents of this file.\n")
        f.write('dbid = %d\n' % self.dbid)
        INFO('wrote dbid: %d' % self.dbid)

        if self.standby_dbid:
            f.write('standby_dbid = %d\n' % self.standby_dbid)
            INFO('wrote standby_dbid: %d' % self.standby_dbid)

 
    def write_gp_dbid(self):
        """
        Create or replace gp_dbid file with current values, changing
        permissions of the new file when done and verifying by re-reading
        the file contents and checking the values read match desired values
        """
        INFO  = self.logger.info
        INFO('%s - write_gp_dbid' % self.filepath)

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
        v = GpDbidFile(self.datadir, do_read=True)
        assert self.dbid == v.dbid
        assert self.standby_dbid == v.standby_dbid


def writeGpDbidFile(directory, dbid, logger=None):
    """
    Writes the gp_dbid file to the given directory, marking it as for the given dbid.
    This method may be deprecating.  See comments in CR-2806.
    """
    d = GpDbidFile(directory, logger=logger)
    d.dbid = dbid
    d.write_gp_dbid()




#
# trivial unit test
#
if __name__ == '__main__':
    import copy, shutil
    import unittest2 as unittest

    TESTDIR = 'test_gp_dbid1'

    class MyTestCase(unittest.TestCase):
        def test1(self):
            d = GpDbidFile(TESTDIR)
            d2 = copy.copy(d)
            d.dbid = 10
            d.write_gp_dbid()
            d2.read_gp_dbid()
            assert d.dbid == d2.dbid
            assert d.standby_dbid == d2.standby_dbid
       
    if os.path.exists(TESTDIR): shutil.rmtree(TESTDIR)
    os.mkdir(TESTDIR)
    unittest.main()
    shutil.rmtree(TESTDIR)
