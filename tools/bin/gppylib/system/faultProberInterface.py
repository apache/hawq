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
   interface with the fault prober

"""
import os

from gppylib.gplog import *
from gppylib.utils import checkNotNone

logger = get_default_logger()

class GpFaultProber:
    def __init__(self):
        pass
    #
    # returns self
    #
    def initializeProber( self, masterPort ) :
        return self

    def pauseFaultProber(self):
        pass

    def unpauseFaultProber(self):
        pass

    def isFaultProberPaused(self):
        pass
    
    def getFaultProberInterval(self):
        pass

#
# Management of registered fault prober.  Right now it
#   is a singleton.  Perhaps switch later to a factory.
#
gFaultProber = None
def registerFaultProber(prober):
    global gFaultProber
    gFaultProber = checkNotNone("New global fault prober interface", prober)

def getFaultProber():
    global gFaultProber
    return checkNotNone("Global fault prober interface", gFaultProber)
