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

#
# An implementation of GpOsProvider will provide functionality to do native/os commands
#  (like sleeping)
#
class GpOsProvider :
    def __init__(self):
        pass

    def destroy(self):
        pass

    #
    # Sleep for the given number of seconds (as though calling python's sleep)
    #
    def sleep( self, sleepTime ) :
        raise Exception("not implemented")


#
# Management of registered configuration provider.  Right now it
#   is a singleton, so initializeDatabase calls _could_ mess up
#   the singleton for other parts of code.  Perhaps switch later
#   to a factory 
#
gProvider = None
def registerOsProvider(provider):
    global gProvider
    checkNotNone("New global osProvider", provider)

    if gProvider is not None:
        gProvider.destroy()

    gProvider = provider

def getOsProvider():
    global gProvider
    return checkNotNone("Global osProvider", gProvider)
