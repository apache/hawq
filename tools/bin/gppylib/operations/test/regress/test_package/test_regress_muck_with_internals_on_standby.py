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
import tarfile

from contextlib import closing
from gppylib.commands.base import ExecutionError
from gppylib.commands.unix import Scp
from gppylib.operations.package import GpScp
from gppylib.operations.test.regress.test_package import GppkgTestCase, unittest, skipIfNoStandby, get_host_list, ARCHIVE_PATH, run_command
from gppylib.operations.unix import RemoveRemoteFile

@skipIfNoStandby()
class MuckWithInternalsOnStandby(GppkgTestCase):            
    @unittest.expectedFailure
    def test00_delete_package_from_archive_on_standby_and_install(self):
        gppkg_file = self.build(self.alpha_spec, self.A_spec)
        archive_file = os.path.join(ARCHIVE_PATH, gppkg_file)
        self.install(gppkg_file)

        #Remove package from standby
        standby = get_host_list()[0] 
        RemoveRemoteFile(os.path.join(ARCHIVE_PATH, gppkg_file), standby).run()

        try:
            self.install(gppkg_file)
        except ExecutionError, e:
            Scp(name = "copy gppkg to standby",
                srcFile = gppkg_file,
                dstFile = archive_file,
                srcHost = None,
                dstHost = standby).run(validateAfter = True)
            self.fail("ExecutionError %s" % e)

    @unittest.expectedFailure
    def test01_delete_package_from_archive_on_standby_and_uninstall(self):
        """
            Known issue: MPP-15737
        """
        gppkg_file = self.alpha_spec.get_filename()
        archive_file = os.path.join(ARCHIVE_PATH, gppkg_file)
        self.install(gppkg_file)

        #Remove package from standby
        standby = get_host_list()[0] 
        RemoveRemoteFile(os.path.join(ARCHIVE_PATH, gppkg_file), standby).run()

        try:
            self.remove(gppkg_file)
        except ExecutionError, e:
            GpScp(source_path = gppkg_file,
                  target_path = archive_file,
                  host_list = get_host_list()[1]).run()
            Scp(name = "copy gppkg to standby",
                srcFile = gppkg_file,
                dstFile = archive_file, 
                srcHost = None, 
                dstHost = standby).run(validateAfter = True)
            self.fail("ExecutionError %s" % e)

    @unittest.expectedFailure
    def test02_uninstall_rpm_on_standby_and_install(self):
        #Use gppkg from previous test
        gppkg_file = self.alpha_spec.get_filename()
        self.install(gppkg_file)
   
        standby = get_host_list()[0]
 
        self.uninstall_rpm_remotely(self.A_spec.get_filename(), standby)
        try:
            self.install(gppkg_file) 
        except ExecutionError, e:
            #Install the rpm 
            with closing(tarfile.open(self.alpha_spec.get_filename())) as tf:
                tf.extract(self.A_spec.get_filename())
            Scp(name = "copy rpm to standby", 
                srcFile = self.A_spec.get_filename(), 
                dstFile = self.A_spec.get_filename(), 
                srcHost = None,
                dstHost = standby).run(validateAfter = True)
            self.install_rpm_remotely(self.A_spec.get_filename(), standby)
            os.remove(self.A_spec.get_filename())
            RemoveRemoteFile(self.A_spec.get_filename(), standby).run()
            self.fail("ExecutionError %s" % e)

    def test03_uninstall_rpm_on_standby_and_uninstall(self):
        #Use gppkg from previous test
        gppkg_file = self.alpha_spec.get_filename()
        self.install(gppkg_file)
   
        standby = get_host_list()[0]
 
        self.uninstall_rpm_remotely(self.A_spec.get_filename(), standby)
        self.remove(gppkg_file) 

    def test04_install_rpm_on_standby_and_install(self):
        gppkg_file = self.alpha_spec.get_filename()
 
        standby = get_host_list()[0]
 
        #Install the rpm 
        with closing(tarfile.open(self.alpha_spec.get_filename())) as tf:
            tf.extract(self.A_spec.get_filename())
        Scp(name = "copy the rpm to standby",
            srcFile = self.A_spec.get_filename(),
            dstFile = self.A_spec.get_filename(), 
            srcHost = None, 
            dstHost = standby).run(validateAfter = True)

        self.install_rpm_remotely(self.A_spec.get_filename(), standby)
        self.install(gppkg_file) 

    @unittest.expectedFailure
    def test05_install_rpm_on_standby_and_uninstall(self):
        gppkg_file = self.alpha_spec.get_filename()
        
        standby = get_host_list()[0]

        #Install the rpm
        with closing(tarfile.open(self.alpha_spec.get_filename())) as tf:
            tf.extract(self.A_spec.get_filename())
        Scp(name = "copy rpm to standby",
            srcFile = self.A_spec.get_filename(),
            dstFile = self.A_spec.get_filename(), 
            srcHost = None,
            dstHost = standby).run(validateAfter = True)

        self.install_rpm_remotely(self.A_spec.get_filename(), standby)

        try:
            self.remove(gppkg_file)
        except ExecutionError, e:
            self.uninstall_rpm_remotely(self.A_spec.get_filename(), standby)
            os.remove(self.A_spec.get_filename())
            RemoveRemoteFile(self.A_spec.get_filename(), standby).run()
            self.fail("ExecutionError %s" % e)

if __name__ == "__main__":
    unittest.main()
