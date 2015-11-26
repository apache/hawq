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

This file defines the interface that can be used to
   fetch and update system configuration information,
   as well as the data object returned by the

"""
import os

from gppylib.gplog import *
from gppylib.utils import checkNotNone

logger = get_default_logger()

class GpConfigurationProvider :
    """
    An implementation of GpConfigurationProvider will provide functionality
    to fetch and update gpdb system configuration information (as stored in the database)
    """

    def __init__(self):
        pass

    def initializeProvider( self, masterPort ) :
        """
        Initialize the provider to get information from the given master db, if it chooses to
            get its data from the database

        returns self
        """

        return self

    def loadSystemConfig( self, useUtilityMode ) :
        """
        Load all segment information from the configuration source.
        
        Returns a new GpArray object
        """
        pass

    def sendPgElogFromMaster( self, msg, sendAlerts):
        """
        Send a message from the master database using select pg_elog ... 
        """
        pass

    def updateSystemConfig( self, systemConfiguration, textForConfigTable, dbIdToForceMirrorRemoveAdd, useUtilityMode ) :
        """

        Update the configuration for the given segments in the underlying configuration store
            to match the current values

        Also resets any dirty bits on saved/updated objects

        """
        pass

"""
Management of registered configuration provider.  Right now it
   is a singleton, so initializeDatabase calls _could_ mess up
   the singleton for other parts of code.  Perhaps switch later
   to a factory
"""
gConfigurationProvider = None
def registerConfigurationProvider(provider):
    global gConfigurationProvider
    gConfigurationProvider = checkNotNone("New global configuration provider", provider)

def getConfigurationProvider():
    global gConfigurationProvider
    return checkNotNone("Global configuration provider", gConfigurationProvider)
