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

import sys, os, pwd
import unittest2 as unittest

try:
    gphome = os.environ.get('GPHOME')
    if not gphome:
        raise Exception("GPHOME not set")
    location = "%s/bin/lib" % gphome
    sys.path.append(location)
    from gppylib.util.ssh_utils import HostList, Session
except Exception as e:
    print "PYTHON PATH: %s" % ":".join(sys.path)
    print str(e)
    raise

class SshUtilsTestCase(unittest.TestCase):

    def test00_test_filterMultiHomedHosts(self):
        """ 
            filterMultiHomedHosts should deduplicate hostnames
        """

        hostlist = HostList()
        hostlist.add('localhost')
        hostlist.add('localhost')
        hostlist.add('localhost')
        hostlist.filterMultiHomedHosts()
        self.assertEqual(len(hostlist.get()), 1, 
                         "There should be only 1 host in the hostlist after calling filterMultiHomedHosts")

    def test01_test_SessionLogin(self):
        """ 
            Session.login test, one success and one failure
        """

        uname = pwd.getpwuid(os.getuid()).pw_name

        s = Session()
        s.login(['localhost', 'fakehost'], uname)


 
if __name__ == "__main__":
    unittest.main()
