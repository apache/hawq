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

Note that this does NOT pay attention to the current pause state in the db.  It can't do that right now
  anyway because gp_fts_probe_pause does not reflect the shared memory variable?

"""
import os
import time
import re

from gppylib.gplog import *
from gppylib.utils import checkNotNone
from gppylib.system.faultProberInterface import GpFaultProber
from gppylib.db import dbconn, catalog

logger = get_default_logger()

class GpFaultProberImplGpdb(GpFaultProber):
    def __init__(self):
        self.__isPaused = False
        self.__masterDbUrl = None
        self.__conn = None

    #
    # returns self
    #
    def initializeProber( self, masterPort ) :
        self.__masterDbUrl = dbconn.DbURL(port=masterPort, dbname='template1')
        return self

    def pauseFaultProber(self):
        assert not self.__isPaused
        assert self.__masterDbUrl is not None # must be initialized
        assert self.__conn is None

        logger.debug("Pausing fault prober")
        
        self.__conn = dbconn.connect(self.__masterDbUrl, True) # use utility mode so we don't do any segment connection stuff
        dbconn.execSQL( self.__conn, "set gp_fts_probe_pause = on")

        self.__isPaused = True

    def unpauseFaultProber(self):
        assert self.__isPaused
        assert self.__masterDbUrl is not None  # must be initialized
        assert self.__conn is not None

        logger.debug("Unpausing fault prober")

        self.__conn.close() # closing connection will rollback GUC setting and so unpause prober
        self.__conn = None
        self.__isPaused = False

    def isFaultProberPaused(self):
        return self.__isPaused

    def getFaultProberInterval(self):
        probe_interval_re = re.compile(r'(?P<val>\d+)(?P<unit>[a-zA-Z]*)')
        probe_interval_secs = 60
        
        conn = None
        
        try:
            conn = dbconn.connect(self.__masterDbUrl, True)
            fts_probe_interval_value = catalog.getSessionGUC(conn, 'gp_fts_probe_interval')
            m = probe_interval_re.match(fts_probe_interval_value)
            if m.group('unit') == 'min':
                probe_interval_secs = int(m.group('val')) * 60
            else:
                probe_interval_secs = int(m.group('val'))
        except:
            raise
        finally:
            if conn:
                conn.close()
                
        return probe_interval_secs
