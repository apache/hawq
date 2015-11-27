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
"""
import os

from gppylib.testold.testUtils import *
from gppylib.gplog import *
from gppylib.utils import checkNotNone
from gppylib.system.faultProberInterface import GpFaultProber

logger = get_default_logger()

class GpFaultProberImplForTest(GpFaultProber):
    def __init__(self):
        self.__isPaused = False

    #
    # returns self
    #
    def initializeProber( self, masterPort ) :
        return self

    def pauseFaultProber(self):
        assert not self.__isPaused
        self.__isPaused = True
        testOutput("pausing fault prober")

    def unpauseFaultProber(self):
        assert self.__isPaused
        self.__isPaused = False
        testOutput("unpausing fault prober")

    def isFaultProberPaused(self):
        return self.__isPaused
    
    def getFaultProberInterval(self):
        return 60
