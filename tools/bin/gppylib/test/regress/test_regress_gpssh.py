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

import os, signal, time, re
import unittest2 as unittest
import psutil, subprocess

class GpsshTestCase(unittest.TestCase):

    # return count of stranded ssh processes 
    def searchForProcessOrChildren(self):

        euid = os.getuid()
        count = 0

        for p in psutil.process.ProcessTable().values():
            if p.euid != euid:
                continue
            if not re.search('ssh', p.command):
                continue
            if p.ppid != 1:
                continue

            count += 1

        return count


    def test00_gpssh_sighup(self):
        """Verify that gppsh handles sighup 
           and terminates cleanly.
        """

        before_count = self.searchForProcessOrChildren()

        p = subprocess.Popen("gpssh -h localhost", shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        pid = p.pid

        time.sleep(3)

        try:
            os.kill(int(pid), signal.SIGHUP)
        except Exception:
            pass

        max_attempts = 6
        for i in range(max_attempts):
            after_count = self.searchForProcessOrChildren()
            error_count = after_count - before_count
            if error_count:
                if (i + 1) == max_attempts:
                    self.fail("Found %d new stranded gpssh processes after issuing sig HUP" % error_count)
                time.sleep(.5)
 
if __name__ == "__main__":
    unittest.main()
