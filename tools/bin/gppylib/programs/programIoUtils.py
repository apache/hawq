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

from gppylib import gparray, gplog, pgconf, userinput

logger = gplog.get_default_logger()

def appendSegmentInfoForOutput(instanceType, gpArray, seg, tableLogger):
    """
    @param tableLogger an instance of utils.TableLogger
    """

    tableLogger.info(['%s instance host' % instanceType, "= " + seg.getSegmentHostName()])
    tableLogger.info(['%s instance address' % instanceType, "= " + seg.getSegmentAddress()])
    tableLogger.info(['%s instance directory' % instanceType, "= " + seg.getSegmentDataDirectory()])
    tableLogger.info(['%s instance port' % instanceType, "= " + str(seg.getSegmentPort())])
    tableLogger.info(['%s instance replication port' % instanceType, "= " + str(seg.getSegmentReplicationPort())])

    for fs in gpArray.getFilespaces(False):
        path = seg.getSegmentFilespaces()[fs.getOid()]
        tableLogger.info(['%s instance %s directory' % (instanceType, fs.getName()), "= " + path])



