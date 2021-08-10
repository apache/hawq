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

from gppylib import gplog, pgconf
from gppylib.commands import gp
from gppylib.db import catalog, dbconn
from gppylib.utils import toNonNoneString, checkNotNone

logger = gplog.get_default_logger()

class GpMasterEnvironment:
    """

    Encapsulates information about the environment in which the script is running AND about the
       master database.

    In the future we should make it possible to build this object on segments, or when the master data directory
       has not been built.

    """

    def __init__(self, masterDataDir, readFromMasterCatalog, timeout=None, retries=None):
        """
        masterDataDir: if None then we try to find it from the system environment
        readFromMasterCatalog: if True then we will connect to the master in utility mode and fetch some more
                               data from there (like collation settings)

        """
        if masterDataDir is None:
            self.__masterDataDir = gp.get_masterdatadir()
        else: self.__masterDataDir = masterDataDir

        logger.debug("Obtaining master's port from master data directory")
        pgconf_dict = pgconf.readfile(self.__masterDataDir + "/postgresql.conf")
        self.__masterPort = pgconf_dict.int('port')
        logger.debug("Read from postgresql.conf port=%s" % self.__masterPort)
        self.__masterMaxConnections = pgconf_dict.int('max_connections')
        logger.debug("Read from postgresql.conf max_connections=%s" % self.__masterMaxConnections)

        self.__gpHome = gp.get_gphome()
        self.__gpVersion = gp.GpVersion.local('local GP software version check',self.__gpHome)
        logger.info("local HAWQ Version: '%s'" % self.__gpVersion)

        # read collation settings from master
        if readFromMasterCatalog:
            dbUrl = dbconn.DbURL(port=self.__masterPort, dbname='template1', timeout=timeout, retries=retries)
            conn = dbconn.connect(dbUrl, utility=True)
            (self.__lcCollate, self.__lcMonetary, self.__lcNumeric) = catalog.getCollationSettings(conn)

            # MPP-13807, read/show the master's database version too
            self.__pgVersion = dbconn.execSQLForSingletonRow(conn, "select version();")[0]
            logger.info("master HAWQ Version: '%s'" % self.__pgVersion)
            conn.close()

            checkNotNone("lc_collate", self.__lcCollate)
            checkNotNone("lc_monetary", self.__lcMonetary)
            checkNotNone("lc_numeric", self.__lcNumeric)
        else:
            self.__lcCollate = None
            self.__lcMonetary = None
            self.__lcNumeric = None
            self.__pgVersion = None


    def getGpHome(self): return self.__gpHome
    def getGpVersion(self): return self.__gpVersion
    def getPgVersion(self): return self.__pgVersion
    def getLcCollate(self):
        checkNotNone("lc_collate", self.__lcCollate) # make sure we were initialized with "readFromMasterCatalog"
        return self.__lcCollate

    def getLcMonetary(self):
        checkNotNone("lc_monetary", self.__lcMonetary) # make sure we were initialized with "readFromMasterCatalog"
        return self.__lcMonetary

    def getLcNumeric(self):
        checkNotNone("lc_numeric", self.__lcNumeric) # make sure we were initialized with "readFromMasterCatalog"
        return self.__lcNumeric

    def getLocaleData(self):
        checkNotNone("lc_numeric", self.__lcNumeric) # make sure we were initialized with "readFromMasterCatalog"
        return ":".join([self.__lcCollate, self.__lcMonetary, self.__lcNumeric])

    def getMasterDataDir(self): return self.__masterDataDir
    def getMasterMaxConnections(self) : return self.__masterMaxConnections
    def getMasterPort(self) : return self.__masterPort
