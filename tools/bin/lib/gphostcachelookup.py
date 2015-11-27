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
'''
gphostcachelookup.py -- look up the hostname for a list of interfaces

Usage: gphostcachelookup.py interface-name
Input is taken from stdin. Each line from stdin is considered as interface name.
Output is the hostname, gets printed to stdout.

'''

import sys
from gppylib.gphostcache import GpInterfaceToHostNameCache
from gppylib.commands import base

#-------------------------------------------------------------------------
if __name__ == '__main__':

    pool = base.WorkerPool(1)
    retCode = 0

    try:
        interfaces = []
        hostNames = []
        for line in sys.stdin:
            interfaces.append(line.strip())
            hostNames.append(None)

        lookup = GpInterfaceToHostNameCache(pool, interfaces, hostNames)

        for interface in interfaces:
            hostname = lookup.getHostName(interface)
            if hostname is None:
                sys.stdout.write("__lookup_of_hostname_failed__\n")
            else:
                sys.stdout.write(hostname)
                sys.stdout.write("\n")
    except Exception, e:
        sys.stderr.write("Exception converting hostname from cache: %s" % e.__str__())
        sys.stderr.write("\n")
        retCode = 1
    except:
        sys.stderr.write("Exception found converting interface to hostname")
        sys.stderr.write("\n")
        retCode = 1
    finally:
        sys.exit(retCode)


