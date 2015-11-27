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
import sys
import pickle
import traceback 

class NullDevice():
    def write(self, s):
        pass

# Prevent use of stdout, as it disrupts pickling mechanism
old_stdout = sys.stdout
sys.stdout = NullDevice()

# log initialization must be done only AFTER rerouting stdout
from gppylib import gplog
from gppylib.mainUtils import getProgramName
from gppylib.commands import unix
hostname = unix.getLocalHostname()
username = unix.getUserName()
execname = pickle.load(sys.stdin)
gplog.setup_tool_logging(execname, hostname, username)
logger = gplog.get_default_logger()

operation = pickle.load(sys.stdin)

from gppylib.gpcoverage import GpCoverage
coverage = GpCoverage()
coverage.start()

try:
    ret = operation.run()
except Exception, e:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    tb_list = traceback.extract_tb(exc_traceback)
    try:
        # TODO: Build an ExceptionCapsule that can return the traceback
        # to RemoteOperation as well. See Pyro.

        # logger.exception(e)               # logging 'e' could be necessary for traceback

        pickled_ret = pickle.dumps(e)       # Pickle exception for stdout transmission
    except Exception, f:
        # logger.exception(f)               # 'f' is not important to us, except for debugging perhaps

        # No hope of pickling a precise Exception back to RemoteOperation.
        # So, provide meaningful trace as text and provide a non-zero return code
        # to signal to RemoteOperation that its Command invocation of gpoperation.py has failed.
        pretty_trace = str(e) + "\n"
        pretty_trace += 'Traceback (most recent call last):\n'
        pretty_trace += ''.join(traceback.format_list(tb_list))
        logger.critical(pretty_trace)
        print >> sys.stderr, pretty_trace
        sys.exit(2)                         # signal that gpoperation.py has hit unexpected error
else:
    pickled_ret = pickle.dumps(ret)         # Pickle return data for stdout transmission
finally:
    coverage.stop()
    coverage.generate_report()

sys.stdout = old_stdout
print pickled_ret
sys.exit(0)
