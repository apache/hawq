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

""" Unittesting for gphostcachelookup module
"""
import os, sys
import unittest2 as unittest
from gppylib import gplog
from gppylib.commands.base import Command, ExecutionError

FILEDIR=os.path.expanduser("~")
FILENAME=".gphostcache"
CACHEFILE = os.path.join(FILEDIR, FILENAME)

logger = gplog.get_unittest_logger()

try:
    if os.getenv('GPHOME') is '':
        raise Exception("Environment variable GPHOME not set")
except Exception, e:
    sys.stderr.write("Exception found: %s \n" % str(e))

GPHOSTLOOKUP = os.path.join(os.getenv('GPHOME'), 'bin/lib/', 'gphostcachelookup.py')

class GpHostCacheLookupTestCase(unittest.TestCase):

    LOOKUP_FAIL_MSG = '__lookup_of_hostname_failed__'

    def test01_gphost_lookup(self):
        """ Test gphostcachelookup for known interface (present in host cache)"""

        self._remove_old_cache_entries()
        test_interface = "localhost"
        hostname = os.uname()[1]

        result = self._run_gphost_lookup(test_interface)

        self.assertEqual(hostname, result)


    def test02_gphost_lookup(self):
        """Test gphostcachelookup for unknown interface (not present in host cache)"""

        test_interface = "foo"

        result = self._run_gphost_lookup(test_interface)

        self.assertIn(self.LOOKUP_FAIL_MSG, result)


    def test03_gphost_lookup(self):
        """Test gphostcachelookup for empty interface name """

        test_interface = ""

        result = self._run_gphost_lookup(test_interface)

        self.assertIn(self.LOOKUP_FAIL_MSG, result)


#-------------------------------- Non-test Helper -----------------------

    def _run_gphost_lookup(self, test_interface):
        """This is a helper function to simply run 'gphostcachelookup.py' for given interface"""

        
        cmd = Command("Running host lookup for given interface",
                        "echo %s | python %s" % (test_interface, GPHOSTLOOKUP))

        try:
            cmd.run(validateAfter = True)
            
        except ExecutionError, e:
            self.fail("ExecutionError %s" % str(e))

        result = cmd.get_results().stdout.strip()
        return result



    def _remove_old_cache_entries(self):
        """ This function will be used to remove already present 'localhost' entries from gphostcache file """

        with open(CACHEFILE, "r") as fr:
            lines = fr.readlines()

        with open(CACHEFILE, "w") as fw:
            for line in lines:
                if "localhost" not in line:
                    fw.write(line)


#------------------------------- Mainline --------------------------------
if __name__ == '__main__':
    unittest.main()
