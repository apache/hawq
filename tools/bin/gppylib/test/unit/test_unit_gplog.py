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

""" Unittesting for gplog module
"""
import logging
import unittest

import gplog

class gplogTestCase(unittest.TestCase):
    
    def test_basics(self):
        logger = gplog.get_default_logger()
        self.assertTrue(logger is not None)

    def test_set_loglevels(self):
        logger = gplog.get_default_logger()
        self.assertLogLevel(logger, logging.INFO)
        gplog.enable_verbose_logging()
        self.assertLogLevel(logger, logging.DEBUG)        
        

#------------------------------- non-test helper --------------------------------
    def assertLogLevel(self, logger, level): 
        self.assertTrue(logger.getEffectiveLevel() == level)

#------------------------------- Mainline --------------------------------
if __name__ == '__main__':
    unittest.main()
