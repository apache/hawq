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
"""
figleafsections plugin for nose.

Automatically records coverage info for Python tests and connects it with
with test was being run at the time.  Can be used to produce a "barcode"
of code execution.
"""

DEFAULT_COVERAGE_FILE='.figleaf_sections'
import pkg_resources

try:
    pkg_resources.require('figleaf>=0.6.1')
    import figleaf
except ImportError:
    figleaf = None

import sys
err = sys.stderr

import nose.case
from nose.plugins.base import Plugin

import logging
import os

log = logging.getLogger(__name__)

def calc_testname(test):
    """
    Build a reasonably human-readable testname from each test.
    """
    name = str(test)
    if ' ' in name:
        name = name.split(' ')[1]

    return name

class FigleafSections(Plugin):
    def __init__(self):
        self.name = 'figleafsections'
        Plugin.__init__(self)
        self.testname = None

    def add_options(self, parser, env=os.environ):
        env_opt = 'NOSE_WITH_%s' % self.name.upper()
        env_opt.replace('-', '_')
        parser.add_option("--with-%s" % self.name,
                          action="store_true",
                          dest=self.enableOpt,
                          default=env.get(env_opt),
                          help="Enable plugin %s: %s [%s]" %
                          (self.__class__.__name__, self.help(), env_opt))

        parser.add_option("--figleaf-file",
                          action="store",
                          dest="figleaf_file",
                          default=None,
                          help="Store figleaf section coverage in this file")
        
    def configure(self, options, config):
        """
        Configure: enable plugin?  And if so, where should the coverage
        info be placed?
        """
        self.conf = config

        # enable?
        if hasattr(options, self.enableOpt):
            self.enabled = getattr(options, self.enableOpt)

        ### save coverage file name, if given.
        if options.figleaf_file:
            self.figleaf_file = options.figleaf_file
        else:
            self.figleaf_file = DEFAULT_COVERAGE_FILE

        if self.enabled and figleaf is None:
                raise Exception("You must install figleaf 0.6.1 before you can use the figleafsections plugin! See http://darcs.idyll.org/~t/projects/figleaf/doc/")

    def begin(self):
        """
        Initialize: start recording coverage info.
        """
        figleaf.start()

    def finalize(self, result):
        """
        Finalize: stop recording coverage info, save & exit.
        """
        figleaf.stop()
        
        fp = open(self.figleaf_file, 'w')
        figleaf.dump_pickled_coverage(fp)
        fp.close()

    def startTest(self, test):
        """
        Run at the beginning of each test, before per-test fixtures.

        One weakness is that this is only run for specific kinds of
        nose testcases.
        """
        if isinstance(test, nose.case.Test):
           
            self.testname = calc_testname(test)
            assert self.testname

            figleaf.start_section(self.testname)

    def stopTest(self, test):
        """
        Run at the end of each test, after per-test fixtures.
        """
        if self.testname:
            figleaf.stop_section()
            self.testname = None
