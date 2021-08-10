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
Module for commands that are DCA specific
"""

import os 
from gppylib.gplog import get_default_logger
from base import Command, LOCAL, REMOTE

logger = get_default_logger()

# NOTE THIS IS A CHECK FOR 1040 or later appliance
def is_dca_appliance():
    try:
        if os.path.isfile('/opt/dca/bin/dca_gpdb_initialized'):
            return True
    except:
        pass

    return False

#-----------------------------------------------
class DcaGpdbInitialized(Command):
    def __init__(self, name, ctxt=LOCAL, remoteHost=None):
        self.cmdStr="/opt/dca/bin/dca_gpdb_initialized"
        Command.__init__(self, name, self.cmdStr, ctxt, remoteHost)

    @staticmethod
    def local():
        try:
            cmd=DcaGpdbInitialized('dcainit')
            cmd.run(validateAfter=True)
        except Exception, e:
            logger.error(e.__str__())
            logger.error("Exception running dca initialization")
        except:
            logger.error("Exception running dca initialization")

#-----------------------------------------------
class DcaGpdbStopped(Command):
    def __init__(self, name, ctxt=LOCAL, remoteHost=None):
        self.cmdStr="/opt/dca/bin/dca_gpdb_stopped"
        Command.__init__(self, name, self.cmdStr, ctxt, remoteHost)

    @staticmethod
    def local():
        try:
            cmd=DcaGpdbStopped('dcastop')
            cmd.run(validateAfter=True)
        except Exception, e:
            logger.error(e.__str__())
            logger.error("Exception running dca de-initialization")
        except:
            logger.error("Exception running dca de-initialization")

