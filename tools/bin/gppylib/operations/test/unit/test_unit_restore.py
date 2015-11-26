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
