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

import os
import subprocess
from pygresql.pgdb import DatabaseError
from pygresql import pg

class GpsqlUtil:
    '''
    This class is used to provide Postgres database connection by applying PyGreSQL, and default arguments are already provided.

    Therefore, make sure PyGreSQL is installed before using.
    '''
    def __init__(self, dbName = "postgres", dbIP = "127.0.0.1", dbPort = 5432, dbUser = "gpadmin", dbPassword = ""):
        self.db_name = dbName
        self.db_ip = dbIP
        self.db_port = dbPort
        self.db_user = dbUser
        self.db_password = dbPassword
        self.db_connection = None

    def performQuery(self, queryStatement = "", operationType = 1):
        '''
        Put SQL statement into queryStatement.

        By default, operationType = 1, and it means that there is a return value.

        Set operationType = 0, if there is no return value.
        '''
        rtnVal = None
        try:
            self.db_connection = pg.DB(dbname = self.db_name, host = self.db_ip, port = self.db_port, user = self.db_user, passwd = self.db_password)
            if operationType == 1:
                rtnVal = self.db_connection.query(queryStatement).dictresult()
            else:
                rtnVal = self.db_connection.query(queryStatement)
        except Exception as e:
            print("*---------- Fail to connect to database or perform query statement. ----------*")
            print("------------Query statement is: %s" % queryStatement)
            print("------------Error message: %s" % str(e))
            print("*-----------------------------------------------------------------------------*")
        finally:
            if self.db_connection is not None:
                self.db_connection.close()
            return rtnVal
