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
# Unit Testing of catalog module.
#

import unittest2 as unittest



from gppylib import gplog
from gppylib.db import dbconn
from gppylib.db import catalog
from gppylib.db.test import skipIfDatabaseDown


logger=gplog.get_default_logger()


@skipIfDatabaseDown()
class catalogTestCase(unittest.TestCase):
    
    def setUp(self):
        self.dburl=dbconn.DbURL()
        self.conn = dbconn.connect(self.dburl)
        
    
    def tearDown(self):
        self.conn.close()
        pass

    def test_vacuumcatalog(self):
        logger.info("test_vacuumcatalog")
        catalog.vacuum_catalog(self.dburl,self.conn)
        catalog.vacuum_catalog(self.dburl,self.conn,full=True)
    

#------------------------------- Helper --------------------------------
    def validate_gpid_table(self,orig_gpid_table,test_gpid_table):
        self.assertEquals(orig_gpid_table.gpname,test_gpid_table.gpname)
        self.assertEquals(orig_gpid_table.numsegments ,test_gpid_table.numsegments)
        self.assertEquals(orig_gpid_table.dbid ,test_gpid_table.dbid)
        self.assertEquals(orig_gpid_table.content ,test_gpid_table.content)
    
    def validate_versionatinitdb_table(self,orig_table,test_table):
        self.assertEquals(orig_table.schemaversion,test_table.schemaversion)
        self.assertEquals(orig_table.productversion,orig_table.productversion)
        



#------------------------------- Mainline --------------------------------
if __name__ == '__main__':
    unittest.main()    
