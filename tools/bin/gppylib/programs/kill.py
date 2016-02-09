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

# Line too long - pylint: disable=C0301

from gppylib import gplog
from gppylib.commands import gp
from optparse import OptionGroup
from gppylib.gpparseopts import OptParser, OptChecker
from gppylib.mainUtils import addStandardLoggingAndHelpOptions, ProgramArgumentValidationException
from gppylib.commands.unix import kill_sequence, check_pid 
from gppylib.operations.package import dereference_symlink
from psutil import Process, NoSuchProcessError

logger = gplog.get_default_logger()

class KillError(Exception): pass

class KillProgram:
    def __init__(self, options, args):
        self.check = options.check
        self.pid_list = args

    @staticmethod
    def create_parser():
        """Create the command line parser object for gpkill"""

        help = []
        parser = OptParser(option_class=OptChecker,
                    description='Check or Terminate a Greenplum Database process.',
                    version='%prog version $Revision: #1 $')
        parser.setHelp(help)

        addStandardLoggingAndHelpOptions(parser, True)

        parser.remove_option('-l')
        parser.remove_option('-a')
 
        addTo = OptionGroup(parser, 'Check Options') 
        parser.add_option_group(addTo)
        addTo.add_option('--check', metavar='pid', help='Only returns status 0 if pid may be killed without gpkill, status 1 otherwise.', action='store_true')
        
        return parser

    @staticmethod
    def create_program(options, args):
        return KillProgram(options, args)

    def cleanup(self):  pass

    def run(self):

        self.validate_arguments()
        if self.check:
            for pid in self.pid_list:
                self.validate_attempt(pid)
        else:
            for pid in self.pid_list:
                self.terminate_process(pid)
        
        return 0

    def validate_arguments(self):
       
        if len(self.pid_list) < 1:
            raise KillError('No pid specified')
 
        int_pid_list = []
        try:
            for x in self.pid_list:
                int_pid_list.append(int(x))
        except ValueError, e:
            raise KillError('Invalid pid specified (%s)' % x)
        
        self.pid_list = int_pid_list

    def validate_attempt(self, pid):
        """ 
            Checks if we can kill the process
        """  
    
        command = self.examine_process(pid)

        critical_process_prefix = ['postgres', gp.get_gphome(), dereference_symlink(gp.get_gphome())]

        for prefix in critical_process_prefix:
            if command.startswith(prefix):
                raise KillError('process %s may not be killed' % pid)

        if not command.startswith('python ' + gp.get_gphome()):
            raise KillError('process %s ignored by gpkill as it is not a greenplum process' % pid)

    def examine_process(self, pid):

        logger.info('Examining process: pid(%s)' % pid)
        
        try:    
            proc = Process(pid=pid)
        except NoSuchProcessError, e:
            raise KillError('Process with pid(%s) does not exist' % pid)

        logger.info('process %s is %s' % (pid, proc.command.strip()))
        
        return proc.command.strip()

    def terminate_process(self, pid):
       
        self.validate_attempt(pid)
      
        logger.warning('Confirm [N/y]:')  

        confirmation = raw_input().strip().lower()
        if confirmation not in ['y', 'ye', 'yes']:
            raise KillError('operation aborted')

        kill_sequence(pid)

        if check_pid(pid):
            raise KillError('Failed to kill process %s' % pid)
