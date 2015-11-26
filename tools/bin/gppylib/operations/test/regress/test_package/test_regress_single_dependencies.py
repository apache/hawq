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

from gppylib.commands.base import ExecutionError
from gppylib.operations.test.regress.test_package import GppkgTestCase, GppkgSpec, RPMSpec, unittest, run_command

class SingleDependenciesTestCase(GppkgTestCase):
    """Covers install/update/remove tests of gppkgs which have a single dependency"""
    def setUp(self):
        super(SingleDependenciesTestCase, self).cleanup()
        self.A_spec = RPMSpec("A", "1", "1", ["B = 1-1"])
        self.B_spec = RPMSpec("B", "1", "1")
        self.alpha_spec = GppkgSpec("alpha", "1.0")

    def test00_build(self):
        self.build(self.alpha_spec, self.A_spec, [self.B_spec])
       
    def test01_install(self):
        gppkg_file = self.build(self.alpha_spec, self.A_spec, [self.B_spec])
        self.install(gppkg_file)

    def test02_update_gppkg_higher(self):
        """
        This test tries to update a gppkg with a higher version 
        and the main and dependent rpms with a higher version.
        """
        #Use gppkg from previous test
        self.install(self.alpha_spec.get_filename())
        
        #New gppkg with higher gppkg, main and deps version
        update_main_rpm_spec = RPMSpec("A", "1", "2", ["B = 1-2"])
        update_dep_rpm_spec = RPMSpec("B", "1", "2")
        update_gppkg_spec = GppkgSpec("alpha", "1.1")
        update_gppkg_file = self.build(update_gppkg_spec, update_main_rpm_spec, [update_dep_rpm_spec]) 
        
        self.update(update_gppkg_file) 
   
    @unittest.expectedFailure 
    def test03_update_gppkg_lower(self):
        """
        This test tries to update a gppkg with a lower version 
        and the main and dependent rpms with a lower version.
        """
        #Use the gppkg from previous test
        update_main_rpm_spec = RPMSpec("A", "1", "2", ["B = 1-2"])
        update_dep_rpm_spec = RPMSpec("B", "1", "2")
        update_gppkg_spec = GppkgSpec("alpha", "1.1")
        self.install(update_gppkg_spec.get_filename())
    
        #Original gppkg with a lower gppkg, main and deps version
        with self.assertRaisesRegexp(ExecutionError, "A newer version of %s is already installed" % self.alpha_spec.get_filename()):
            self.update(self.alpha_spec.get_filename())

    def test04_uninstall(self):
        self.install(self.alpha_spec.get_filename())
        self.remove(self.alpha_spec.get_filename())

if __name__ == "__main__":
    unittest.main()
