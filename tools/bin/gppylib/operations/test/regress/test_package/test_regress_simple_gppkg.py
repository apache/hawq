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

import unittest2 as unittest

from gppylib.operations.test.regress.test_package import GppkgTestCase, GppkgSpec, BuildGppkg, RPMSpec, BuildRPM, run_command, run_remote_command

class SimpleGppkgTestCase(GppkgTestCase):
    """Covers simple build/install/remove/update test cases"""
    def test00_simple_build(self):
        self.build(self.alpha_spec, self.A_spec)

    def test01_simple_install(self):
        gppkg_file = self.alpha_spec.get_filename() 
        self.install(gppkg_file)

        #Check RPM database
        self.check_rpm_install(self.A_spec.get_package_name())

    def test02_simple_update(self):
        gppkg_file = self.alpha_spec.get_filename() 
        self.install(gppkg_file)

        update_rpm_spec = RPMSpec("A", "1", "2")
        update_gppkg_spec = GppkgSpec("alpha", "1.1")
        update_gppkg_file = self.build(update_gppkg_spec, update_rpm_spec)
   
        self.update(update_gppkg_file)

        #Check for the packages
        self.check_rpm_install(update_rpm_spec.get_package_name())

    def test03_simple_uninstall(self):
        gppkg_file = self.alpha_spec.get_filename()

        self.install(gppkg_file)
        self.remove(gppkg_file)

        results = run_command("gppkg -q --all")
        results = results.split('\n')[self.start_output:self.end_output]
        
        self.assertEqual(results, [])
       
    def test04_help(self):
        help_options = ["--help", "-h", "-?"] 

        for opt in help_options:
            results = run_command("gppkg " + opt)
            self.assertNotEqual(results, "")

    def test05_version(self):
        results = run_command("gppkg --version")
        self.assertNotEqual(results, "")

if __name__ == "__main__":
    unittest.main()
