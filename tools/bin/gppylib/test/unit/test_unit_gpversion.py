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

""" 
Unit testing for gpversion module
"""
import unittest

import gpversion
from gpversion import *

class GpVersionTestCase(unittest.TestCase):
    def setUp(self):
        pass
        
    def test_case_1(self):
        vMain = GpVersion("main")

        self.assertTrue(vMain.isVersionCurrentRelease())
        self.assertTrue(vMain.getVersionBuild() == 'dev')
        self.assertTrue(str(vMain) == 'main build dev')

    def test_case_2(self):
        gpversion.MAIN_VERSION = [1,2,99,99]
        vTuple = GpVersion([3,3,0,0])

        self.assertTrue(vTuple.isVersionCurrentRelease() == False )
        self.assertTrue(vTuple.getVersionBuild() == 'dev')
        self.assertTrue(vTuple.getVersionRelease() == "3.3")
        self.assertTrue(vTuple.isVersionRelease("3.3"))
        self.assertTrue(vTuple.isVersionRelease("3.2") == False)
        self.assertTrue(vTuple > "3.2.0.5")
        self.assertTrue(vTuple < "4.0")

    def test_case_3(self):
        gpversion.MAIN_VERSION = [1,2,99,99]
        v33 = GpVersion("3.3 build dev")

        self.assertTrue(v33.isVersionCurrentRelease() == False )
        self.assertTrue(v33.getVersionBuild() == 'dev')
        self.assertTrue(v33.getVersionRelease() == "3.3")
        self.assertTrue(v33.isVersionRelease("3.3"))
        self.assertTrue(v33.isVersionRelease("3.2") == False)
        self.assertTrue(v33 > "3.2.0.5")
        self.assertTrue(v33 < "4.0")

    def test_case_4(self):
        gpversion.MAIN_VERSION = [1,2,99,99]
        v34 = GpVersion("3.4.filerep")

        self.assertTrue(v34.isVersionCurrentRelease() == False )
        self.assertTrue(v34.getVersionBuild() == 'filerep')
        self.assertTrue(v34.getVersionRelease() == "3.4")
        self.assertTrue(v34.isVersionRelease("3.4"))
        self.assertTrue(v34.isVersionRelease("3.2") == False)
        self.assertTrue(v34 > "3.2.0.5")
        self.assertTrue(v34 < "4.0")

    def test_case_5(self):
        gpversion.MAIN_VERSION = [1,2,99,99]
        vShort = GpVersion("postgres (HAWQ) 1.0.0.3 build dev")

        self.assertTrue(vShort.isVersionCurrentRelease() == False )
        self.assertTrue(vShort.getVersionBuild() == 'dev')
        self.assertTrue(vShort.getVersionRelease() == "1.0")
        self.assertTrue(vShort.isVersionRelease("1.0"))
        self.assertTrue(vShort.isVersionRelease("1.1") == False)
        self.assertTrue(vShort > "1.0.0.2")
        self.assertTrue(vShort < "1.1")


    def test_case_6(self):
        gpversion.MAIN_VERSION = [1,1,99,99]
        vLong = GpVersion("PostgreSQL 8.2.15 (Greenplum Database 4.2.0 build 1) (HAWQ 1.1.0.0 build dev) on i386-apple-darwin12.2.1, compiled by GCC gcc (GCC) 4.4.2 compiled on May 10 2013 11:31:49 (with assert checking)")
    
        self.assertTrue(vLong.isVersionCurrentRelease())
        self.assertTrue(vLong.getVersionBuild() == 'dev')
        self.assertTrue(vLong.getVersionRelease() == "1.1")
        self.assertTrue(vLong.isVersionRelease("1.1"))
        self.assertFalse(vLong.isVersionRelease("1.0"))
        self.assertTrue(vLong > "1.0.0.3")
        self.assertTrue(vLong < "1.2")




#------------------------------- Mainline --------------------------------
if __name__ == '__main__':
    unittest.main()
