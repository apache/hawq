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

import sys, os, getpass
import unittest2 as unittest
from gppylib.userinput import ask_create_password


class GppylibUserInputTestCase(unittest.TestCase):

    @staticmethod
    def mock_get_pass_start(values):
        def mock_get_pass(prompt):
            mock_get_pass.counter += 1
            return values[mock_get_pass.counter]
        mock_get_pass.counter = -1
        GppylibUserInputTestCase.old_std_method = getpass.getpass
        getpass.getpass = mock_get_pass

    @staticmethod
    def mock_get_pass_stop():
        getpass.getpass = GppylibUserInputTestCase.old_std_method

    def test00_verify_too_short_password(self):
        """Verify too short password is rejected"""

        stdinvals = ['abc', 'abc', 'foobar', 'foobar']
        GppylibUserInputTestCase.mock_get_pass_start(stdinvals)

        result = ask_create_password()

        GppylibUserInputTestCase.mock_get_pass_stop()

        self.assertEquals(result, 'foobar')

    def test01_verify_too_short_password(self):
        """Verify non-matching password is rejected"""

        stdinvals = ['abcdef', 'ABCDEF', 'coolio', 'coolio']
        GppylibUserInputTestCase.mock_get_pass_start(stdinvals)

        result = ask_create_password()

        GppylibUserInputTestCase.mock_get_pass_stop()

        self.assertEquals(result, 'coolio')
 
    def test02_verify_max_attempts(self):
        """Verify max number of attempts to get password results in failure"""

        stdinvals = ['a', 'a', 'b', 'b', 'c', 'c', 'd', 'd']
        GppylibUserInputTestCase.mock_get_pass_start(stdinvals)

        result = ask_create_password(max_attempts=3)

        GppylibUserInputTestCase.mock_get_pass_stop()

        self.assertIsNone(result)

    def test03_verify_min_length(self):
        """Verify minimum password length"""

        stdinvals = ['a', 'a', 'bb', 'bb', 'ccc', 'ccc', 'dddd', 'dddd']
        GppylibUserInputTestCase.mock_get_pass_start(stdinvals)

        result = ask_create_password(max_attempts=10, min_length=3)

        GppylibUserInputTestCase.mock_get_pass_stop()

        self.assertEquals(result, 'ccc')
 
 
if __name__ == "__main__":
    unittest.main()
