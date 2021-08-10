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

# Greenplum SAN related utility commands

from gppylib.commands.base import *
from gppylib.util.san_utils import SAN_CMDS

#----------mount ---------------
class Mount(Command):
    """Retrieves mount points."""
    
    def __init__(self, name, devfirst=True, ctxt=LOCAL, remoteHost=None):
        cmdStr = SAN_CMDS.MOUNT
        self.devfirst = devfirst
        print cmdStr
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)
        
    @staticmethod
    def remote(name, hostname, devfirst=True):
        cmd = Mount(name, devfirst, ctxt=REMOTE, remoteHost=hostname)
        cmd.run(validateAfter=True)
        return cmd
    
    def get_mount_points(self):
        mps = []
        out = self.get_results().stdout.strip()
        for l in out.split('\n'):
            x = l.strip().split()
            if self.devfirst:
                thismp = (x[0], x[2])
            else:
                thismp = (x[2], x[0])
            mps.append(thismp)
        return mps
    
#----------inq ---------------
class Inq(Command):
    def __init__(self, name, ctxt=LOCAL, remoteHost=None):
        cmdStr = '%s -clar_wwn -no_dots' % SAN_CMDS.INQ
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

    @staticmethod
    def remote(name, hostname):
        cmd = Inq(name, ctxt=REMOTE, remoteHost=hostname)
        cmd.run(validateAfter=True)
        return cmd
    
#----------gp_mount_agent ---------------
class GpMountAgentStorageGroup(Command):
    def __init__(self, name, ctxt=LOCAL, remoteHost=None):
        cmdStr = '%s --storagegroup %s' % (SAN_CMDS.GP_MOUNT_AGENT, SAN_CMDS.POWERMT)
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)
    
    @staticmethod
    def remote(name, hostname):
        cmd = GpMountAgentStorageGroup(name, ctxt=REMOTE, remoteHost=hostname)
        cmd.run(validateAfter=True)
        return cmd
    
