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
import time

class GpMgmtTestRunner(unittest.TextTestRunner):
    def _makeResult(self):
        return GpMgmtTextTestResult(self.stream, self.descriptions, self.verbosity)

class GpMgmtTextTestResult(unittest.TextTestResult):

    def __init__(self, stream, descriptions, verbosity):
        super(GpMgmtTextTestResult, self).__init__(stream, descriptions, verbosity)
        self.verbosity = verbosity
        self.startTime = 0

    def getDescription(self, test):
        case_name, full_name = test.__str__().split()
        suite_name, class_name = full_name.strip('()').rsplit('.',1)
        if self.verbosity > 1:
            if test.shortDescription():
                return 'Test Suite Name|%s|Test Case Name|%s|Test Details|%s' % (suite_name, case_name, test.shortDescription())
            else:
                return 'Test Suite Name|%s|Test Case Name|%s|Test Details|' % (suite_name, case_name)

    def startTest(self, test):
        super(GpMgmtTextTestResult, self).startTest(test)
        self.startTime = test.start_time = time.time()

    def addSuccess(self, test):
        test.end_time = time.time()
        self._show_run_time()
        self.stream.write('|Test Status|')
        super(GpMgmtTextTestResult, self).addSuccess(test)

    def addError(self, test, err):
        test.end_time = time.time()
        self._show_run_time()
        self.stream.write('|Test Status|')
        super(GpMgmtTextTestResult, self).addError(test, err)

    def addFailure(self, test, err):
        test.end_time = time.time()
        self._show_run_time()
        self.stream.write('|Test Status|')
        super(GpMgmtTextTestResult, self).addFailure(test, err)

    def addSkip(self, test, err):
        self._show_run_time()
        self.stream.write('|Test Status|')
        super(GpMgmtTextTestResult, self).addSkip(test, err)

    def addExpectedFailure(self, test, err):
        self.end_time = time.time()
        self._show_run_time()
        self.stream.write('|Test Status|')
        super(GpMgmtTextTestResult, self).addExpectedFailure(test, err)

    def _show_run_time(self):
        etime = time.time()
        elapsed = etime - self.startTime
        self.stream.write('(%4.2f ms)' % (elapsed*1000))
