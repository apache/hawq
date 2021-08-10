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

""" Unittesting for pg8000 module
"""
import logging
import unittest

try: 
    from pg8000 import *
except ImportError, e:
    sys.exit('Error: unable to import module: ' + str(e))



from gppylib import gplog
from gppylib.db.dbconn import *
from gppylib.db.test import skipIfDatabaseDown

logger=gplog.get_unittest_logger()
gplog.enable_verbose_logging()


@skipIfDatabaseDown()
class pg8000TestCase(unittest.TestCase):

    def test_connect(self):
        conn=None        
        try:
            #def connect(user, host=None, unix_sock=None, port=5432, database=None, password=None, socket_timeout=60, ssl=False, options=None):
            dburl = DbURL()        
            self.failUnlessRaises(TypeError,DBAPI.connect)        
            self.failUnlessRaises(ProgrammingError,DBAPI.connect,dburl.pguser)        
            conn=DBAPI.connect(dburl.pguser,host=dburl.pghost,database=dburl.pgdb)
        finally:
            if conn: conn.close()
        
    def test_nonexist_objects(self):
        conn=None
        try:
            dburl=DbURL()
            conn=DBAPI.connect(dburl.pguser,host=dburl.pghost,database=dburl.pgdb)
            curs=conn.cursor()
            self.assertRaises(ProgrammingError,curs.execute,"DROP TABLE notable")
        finally:
            if conn: conn.close()
         
    def test_datatypes(self):
        conn=None
        try:
            
            dburl=DbURL()
            conn=DBAPI.connect(dburl.pguser,host=dburl.pghost,database=dburl.pgdb)
            curs=conn.cursor()
            curs.execute("CREATE TABLE tab1 ( a int )")
            self.assertRaises(ProgrammingError,curs.execute,"INSERT INTO tqb1 VALUES 1")
            self.assertRaises(ProgrammingError,curs.execute,"DROP TABLE tab1")
            self.assertRaises(ProgrammingError,conn.commit)
        finally:
            if conn: conn.close()
    
            

#------------------------------- non-test helper --------------------------------

#------------------------------- Mainline --------------------------------
if __name__ == '__main__':
    unittest.main()

