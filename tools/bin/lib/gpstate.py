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
#
# Recovers Greenplum segment instances that are marked as invalid, if
# mirroring is configured and operational
#


#
# THIS IMPORT MUST COME FIRST
#
# import mainUtils FIRST to get python version check
from gppylib.mainUtils import *
from gppylib.programs.clsSystemState import *

#-------------------------------------------------------------------------
if __name__ == '__main__':
    options = {}
    options["programNameOverride"] = "gpstate" # for now since we are invoked from the real gpstate
    options["suppressStartupLogMessage"] = False # since we are called from another program it's funny to print what we were called with
    simple_main( GpSystemStateProgram.createParser, GpSystemStateProgram.createProgram, options)

