#!/usr/bin/env python
#
# Copyright (c) Greenplum Inc 2008. All Rights Reserved. 
#

import unittest
import tempfile
from gppylib.operations.restore import *

class restoreTestCase(unittest.TestCase):
    def test_GetDbName_1(self):
        """ Basic test """
        with tempfile.NamedTemporaryFile() as f:
            f.write("""
--
-- Database creation
--

CREATE DATABASE monkey WITH TEMPLATE = template0 ENCODING = 'UTF8' OWNER = thisguy;
""")
            f.flush()
            self.assertTrue(GetDbName(f.name).run() == "monkey")

    def test_GetDbName_2(self):
        """ Verify that GetDbName looks no further than 50 lines. """
        with tempfile.NamedTemporaryFile() as f:
            for i in range(0, 50):
                f.write("crap\n")
            f.write("CREATE DATABASE monkey")            
            f.flush()
            try:
                GetDbName(f.name).run()
            except GetDbName.DbNameGiveUp, e:
                return
            self.fail("DbNameGiveUp should have been raised.")

    def test_GetDbName_3(self):
        """ Verify that GetDbName fails  when cdatabase file ends prematurely. """
        with tempfile.NamedTemporaryFile() as f:
            f.write("this is the whole file")
            f.flush()
            try:
                GetDbName(f.name).run()
            except GetDbName.DbNameNotFound, e:
                return
            self.fail("DbNameNotFound should have been raised.")

if __name__ == '__main__':
    unittest.main()
