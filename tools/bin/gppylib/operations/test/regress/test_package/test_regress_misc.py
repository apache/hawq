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

import os
import shutil

from gppylib.operations.test.regress.test_package import GppkgTestCase, unittest, run_command, GPHOME

class MiscTestCase(GppkgTestCase):
    """Covers all test cases which do not fall under any specific category"""
    @unittest.expectedFailure
    def test00_MPP_14671(self):
        """
        This test ensures that gppkg does not use the package name
        to discern information about the package. 
        """
        gppkg_file = self.build(self.alpha_spec, self.A_spec)

        dummy_file = "dummy-x.y.z-abcd-efgh.gppkg"
        shutil.copy(gppkg_file, dummy_file)
        self.extra_clean.add(dummy_file)

        self.install(dummy_file)
        self.assertTrue(os.path.exists(os.path.join(GPHOME, 'share', 'packages', 'archive', dummy_file)))

        self.check_rpm_install(self.A_spec.get_package_name())

        self.assertTrue(self.check_install(gppkg_file))

        results = run_command("gppkg -q --all")
        self.assertTrue(''.join(gppkg_file.split('-')[:1]) in results)

if __name__ == "__main__":
    unittest.main()
