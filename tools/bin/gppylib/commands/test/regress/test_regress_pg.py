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
# Unit Testing of pg commands
#

import os
import unittest
import tempfile

from gppylib.db import dbconn
from gppylib.db.test import skipIfDatabaseDown
from gppylib import gplog
from gppylib.commands import pg
from gppylib.gparray import GpArray

logger = gplog.get_default_logger()
gplog.enable_verbose_logging()

@skipIfDatabaseDown()
class PgCommandsTestCase(unittest.TestCase):
    def setUp(self):
         pass
     
    def tearDown(self):
        pass
    
    def testReadPostmasterTempFile(self):
        logger.info("testReadPostmasterTempFile")
        url = dbconn.DbURL()
        gpdb = GpArray.initFromCatalog(url)
        
        logger.info("Search for valid master port: %s" % gpdb.master.port)
        cmd = pg.ReadPostmasterTempFile.local('test pg tempfile read',gpdb.master.port)        
        (exists,PID,datadir)=cmd.getResults()
        logger.info("exists:=%s PID=%d datadir='%s'" % (exists,PID,datadir))                
        self.assertTrue(exists)
        self.assertTrue(PID > 0)
        self.assertEquals(datadir,gpdb.master.datadir)
        
        
        gpdb.master.port=4000
        logger.info("Search for bogus master port: %s" % gpdb.master.port)        
        cmd = pg.ReadPostmasterTempFile.local('test pg tempfile read',gpdb.master.port)        
        (exists,PID,datadir)=cmd.getResults()
        logger.info("exists:=%s PID=%d datadir='%s'" % (exists,PID,datadir))        
        self.assertFalse(exists)
        
        
    
            



#------------------------------- Mainline --------------------------------
if __name__ == '__main__': 
    unittest.main()    
