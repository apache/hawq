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

'''
base.py 

common base for the commands execution framework.  Units of work are defined as Operations
as found in other modules like unix.py.  These units of work are then packaged up and executed
within a GpCommand.  A GpCommand is just a common infrastructure for executing an Operation.

The general idea is that the application developer breaks the problem down into a set of 
GpCommands that need to be executed.  This class also provides a queue and set of workers
for executing this set of commands.  

                
'''

from Queue import  Queue,Empty
from threading import Thread


import os
import signal
import subprocess
import sys
import time

from gppylib import gplog
from gppylib import gpsubprocess
#from pg import DB

# paramiko prints deprecation warnings which are ugly to the end-user
import warnings
warnings.simplefilter('ignore', DeprecationWarning)
import paramiko, getpass


logger=gplog.get_default_logger()

GPHOME=os.environ.get('GPHOME')
SRC_GPPATH=". %s/greenplum_path.sh;" % GPHOME

# Maximum retries if sshd rejects the connection due to too many
# unauthenticated connections.
SSH_MAX_RETRY=10
# Delay before retrying ssh connection, in seconds
SSH_RETRY_DELAY=.5


class WorkerPool(object):
    """TODO:"""
    
    def __init__(self,numWorkers=16,items=None):        
        self.workers=[]
        self.work_queue=Queue()
        self.completed_queue=Queue()
        self.num_assigned=0
        if items is not None:
            for item in items:                
                self.work_queue.put(item)
                self.num_assigned += 1
            
        for i in range(0,numWorkers):
            w = Worker("worker%d" % i,self)
            self.workers.append(w)
            w.start()
        self.numWorkers = numWorkers
        self.logger = logger
     
    ###
    def getNumWorkers(self):
       return self.numWorkers

    def getNextWorkItem(self,timeout=None):
        return self.work_queue.get(block=True,timeout=timeout)
    
    def addFinishedWorkItem(self,command):
        self.completed_queue.put(command)    
        self.work_queue.task_done()
    
    
    def addCommand(self,cmd):   
        self.logger.debug("Adding cmd to work_queue: %s" % cmd.cmdStr) 
        self.work_queue.put(cmd)
        self.num_assigned += 1
    
    def wait_and_printdots(self,command_count,quiet=True):
        while self.completed_queue.qsize() < command_count:
            time.sleep(1)
            
            if not quiet:
                sys.stdout.write(".")
                sys.stdout.flush()
        if not quiet:
            print " "    
        self.join()
        
        
    
    def join(self):
        self.work_queue.join()
        return True
    
    def joinWorkers(self):
        for w in self.workers:
            w.join()

    def getCompletedItems(self):
        completedList=[]
        try:
            while True:
                item=self.completed_queue.get(False)
                if item is not None:
                    completedList.append(item)                           
        except Empty:
            return completedList
        return completedList  #just to be sure
    

    def check_results(self):
        """ goes through all items in the completed_queue and throws an exception at the
            first one that didn't execute successfully
            
            throws ExecutionError
        """        
        try:
            while True:
                item=self.completed_queue.get(False)
                if not item.get_results().wasSuccessful():
                    raise  ExecutionError("Error Executing Command: ",item)           
        except Empty:
            return      
    
    def empty_completed_items(self):
        while not self.completed_queue.empty():
            self.completed_queue.get(False)         
        

    def isDone(self):
        #TODO: not sure that qsize() is safe
        return (self.num_assigned == self.completed_queue.qsize())
        
    
    def haltWork(self):
        self.logger.debug("WorkerPool haltWork()")
        for w in self.workers:
            w.haltWork()    


class OperationWorkerPool(WorkerPool):
    """ TODO: This is a hack! In reality, the WorkerPool should work with Operations, and
        Command should be a subclass of Operation. Till then, we'll spoof the necessary Command
        functionality within Operation. """
    def __init__(self, numWorkers=16, operations=None):
        if operations is not None:
            for operation in operations:
                self._spoof_operation(operation)
        super(OperationWorkerPool, self).__init__(numWorkers, operations)
    def check_results(self):
        raise NotImplementedError("OperationWorkerPool has no means of verifying success.")
    def _spoof_operation(self, operation):
        operation.cmdStr = str(operation)


class Worker(Thread):
    """TODO:"""
    pool=None
    shouldStop=False
    cmd=None
    name=None
    logger=None
    
    def __init__(self,name,pool,timeout=5):
        self.name=name
        self.pool=pool
        self.timeout=timeout
        self.logger=logger
        Thread.__init__(self)
    
    
    def run(self):
        try_count = 0
        while True:
            try:
                if try_count == 5:
                    self.logger.debug("[%s] try and get work from queue..." % self.name)
                    try_count = 0
                
                if self.shouldStop:
                    self.logger.debug('[%s] stopping' % self.name)
                    return
                    
                try:
                    self.cmd = self.pool.getNextWorkItem(timeout=self.timeout)
                except TypeError:
                    # misleading exception raised during interpreter shutdown
                    return
                
                if self.cmd is not None and not self.shouldStop:
                    self.logger.debug("[%s] got cmd: %s" % (self.name,self.cmd.cmdStr))
                    self.cmd.run()
                    self.logger.debug("[%s] finished cmd: %s" % (self.name, self.cmd))
                    self.pool.addFinishedWorkItem(self.cmd)
                    self.cmd=None
                    try_count = 0
                else:
                    try_count += 1
                    if self.shouldStop:
                        self.logger.debug("[%s] stopping" % self.name)
                        return
                      
            except Empty:                
                if self.shouldStop:
                    self.logger.debug("[%s] stopping" % self.name)
                    return
            except Exception,e:
                self.logger.exception(e)
                if self.cmd:
                    self.logger.debug("[%s] finished cmd with exception: %s" % (self.name, self.cmd))
                    self.pool.addFinishedWorkItem(self.cmd)
                    self.cmd=None
                    try_count = 0
                    
                    
    
    def haltWork(self):
        self.logger.debug("[%s] haltWork" % self.name)
        self.shouldStop=True

        # this was originally coded as
        # 
        #    if self.cmd is not None:
        #        self.cmd.interrupt()
        #        self.cmd.cancel()
        #
        # but as observed in MPP-13808, the worker thread's run() loop may set self.cmd to None
        # past the point where the calling thread checks self.cmd for None, leading to a curious 
        # "'NoneType' object has no attribute 'cancel' exception" which may prevent the worker pool's
        # haltWorkers() from actually halting all the workers.
        #
        c = self.cmd
        if c is not None:
            c.interrupt()
            c.cancel()
    
    def signalPassiveStop(self):
        self.shouldStop=True
        



"""
TODO: consider just having a single interface that needs to be implemented for
      describing work to allow the Workers to use it.  This would allow the user
      to better provide logic necessary.  i.e.  even though the user wants to 
      execute a unix command... how the results are interpretted are highly
      application specific.   So we should have a separate level of abstraction
      for executing UnixCommands and DatabaseCommands from this one.
      
      other things to think about:
      -- how to support cancel
      -- how to support progress
      -- undo?
      -- blocking vs. unblocking
      
"""

        
        
   
        
   



#--------------------------------NEW WORLD-----------------------------------

class CommandResult():
    """ Used as a way to package up the results from a GpCommand
    
    """
    
    #rc,stdout,stderr,completed,halt
    
    def __init__(self,rc,stdout,stderr,completed,halt):
        self.rc=rc
        self.stdout=stdout
        self.stderr=stderr
        self.completed=completed
        self.halt=halt
        pass

    def printResult(self):
        res = "cmd had rc=%d completed=%s halted=%s\n  stdout='%s'\n  " \
            "stderr='%s'" % (self.rc,str(self.completed), str(self.halt), self.stdout, self.stderr)
        return res
    
    def wasSuccessful(self):
        if self.halt:
            return False
        if not self.completed:
            return False
        if self.rc != 0:
            return False
        return True

    def __str__(self):
        return self.printResult()
        
    def split_stdout(self, how=':'):
        """ 
        TODO: AK: This doesn't belong here if it pertains only to pg_controldata.

        MPP-16318: Skip over discrepancies in the pg_controldata stdout, as it's
        not this code's responsibility to judge the pg_controldata stdout. This is
        especially true for 'immediate' shutdown, in which case, we won't even
        care for WARNINGs or other pg_controldata discrepancies.
        """
        for line in self.stdout.split('\n'):
            ret = line.split(how, 1)
            if len(ret) == 2:
                yield ret


class ExecutionError(Exception): 
    def __init__(self,summary,cmd):
        self.summary=summary
        self.cmd=cmd
    def __str__(self):
        #TODO: improve dumping of self.cmd
        return "ExecutionError: '%s' occured.  Details: '%s'  %s" %\
             (self.summary,self.cmd.cmdStr,self.cmd.get_results().printResult())



#specify types of execution contexts.
LOCAL=1
REMOTE=2
RMI=3
NAKED=4

gExecutionContextFactory = None

#
# @param factory needs to have a createExecutionContext(self, execution_context_id, remoteHost, stdin, nakedExecutionInfo) function
#
def setExecutionContextFactory(factory):
    global gExecutionContextFactory
    gExecutionContextFactory = factory

def createExecutionContext(execution_context_id,remoteHost,stdin, nakedExecutionInfo=None):
    if gExecutionContextFactory is not None:
        return gExecutionContextFactory.createExecutionContext(execution_context_id, remoteHost, stdin)
    elif execution_context_id == LOCAL:
        return LocalExecutionContext(stdin)
    elif execution_context_id == REMOTE:
        if remoteHost is None:
            raise Exception("Programmer Error.  Specified REMOTE execution context but didn't provide a remoteHost")
        return RemoteExecutionContext(remoteHost,stdin)
    elif execution_context_id == RMI:
        return RMIExecutionContext()
    elif execution_context_id == NAKED:
        if remoteHost is None:
            raise Exception("Programmer Error.  Specified NAKED execution context but didn't provide a remoteHost")
        if nakedExecutionInfo is None:
            raise Exception("Programmer Error.  Specified NAKED execution context but didn't provide a NakedExecutionInfo")
        return NakedExecutionContext(remoteHost, stdin, nakedExecutionInfo)

        
class ExecutionContext():
    """ An ExecutionContext defines where and how to execute the Command and how to 
    gather up information that are the results of the command.  
    
    """

    propagate_env_map = {}
    """ 
    Dict. mapping environment variables to their values. See gpcoverage.py for example usage.
    """

    def __init__(self):
        pass
    
    def execute(self,cmd):
        pass

    def interrupt(self,cmd):
        pass
    
    def cancel(self,cmd):
        pass


class LocalExecutionContext(ExecutionContext):
    proc=None
    halt=False
    completed=False

    def __init__(self,stdin):
        ExecutionContext.__init__(self)
        self.stdin = stdin
        pass
    
    def execute(self,cmd):
        # prepend env. variables from ExcecutionContext.propagate_env_map
        # e.g. Given {'FOO': 1, 'BAR': 2}, we'll produce "FOO=1 BAR=2 ..."
        for k, v in self.__class__.propagate_env_map.iteritems():
            cmd.cmdStr = "%s=%s %s" % (k, v, cmd.cmdStr)
        
        # also propagate env from command instance specific map
        for k, v in cmd.propagate_env_map.iteritems():
            cmd.cmdStr = "%s=%s %s" % (k, v, cmd.cmdStr)

        # executable='/bin/bash' is to ensure the shell is bash.  bash isn't the
        # actual command executed, but the shell that command string runs under.
        self.proc = gpsubprocess.Popen(cmd.cmdStr, env=None, shell=True, 
                                       executable='/bin/bash',
                                       stdin=subprocess.PIPE,
                                       stderr=subprocess.PIPE, 
                                       stdout=subprocess.PIPE, close_fds=True)
        
        (rc,stdout_value,stderr_value)=self.proc.communicate2(input=self.stdin)
        self.completed=True
         
        cmd.set_results(CommandResult(rc,"".join(stdout_value),"".join(stderr_value),self.completed,self.halt))

    def cancel(self,cmd):
        if self.proc:
            try:
                os.kill(self.proc.pid, signal.SIGTERM)
            except OSError:
                pass

    def interrupt(self,cmd):
        self.halt=True
        if self.proc:
            self.proc.cancel()
    
##########################################################################
# Naked Execution is used to run commands where ssh keys are not exchanged
class NakedExecutionInfo:

    SFTP_NONE = 0
    SFTP_PUT  = 1
    SFTP_GET  = 2

    def __init__(self, passwordMap, sftp_operation = SFTP_NONE, sftp_remote = None, sftp_local = None):
        self.passwordMap = passwordMap
        self.sftp_operation = sftp_operation
        self.sftp_remote = sftp_remote
        self.sftp_local = sftp_local


class NakedExecutionPasswordMap:
    def __init__(self, hostlist):
        self.hostlist = hostlist
        self.mapping = dict()
        self.unique_passwords = set()
        self.complete = False

    # this method throws exceptions on error to create a valid list
    def discover(self):

        for host in self.hostlist:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # TRY NO PASSWORD
            try:
                client.connect(host)
                self.mapping[host] = None
                client.close()
                continue # next host
            except Exception, e:
                pass

            try:
                client.close()
            except Exception, e:
                pass

            # TRY EXISTING PASSWORDS
            foundit = False
            for passwd in self.unique_passwords:
                try:
                    client.connect(host, password=passwd)
                    foundit = True
                    self.mapping[host] = passwd
                    break
                except Exception, e:
                    pass

            if foundit:
                continue

            # ASK USER
            foundit = False
            for attempt in range(5):
                try:
                    passwd = getpass.getpass('  *** Enter password for %s: ' % (host), sys.stderr)
                    client.connect(host, password=passwd)
                    foundit = True
                    self.mapping[host] = passwd
                    if passwd not in self.unique_passwords:
                        self.unique_passwords.add(passwd)
                    break
                except Exception, e:
                    pass

            try:
                client.close()
            except Exception, e:
                pass

            if not foundit:
                raise Exception("Did not get a valid password for host " + host)

        if len(self.mapping.keys()) == len(self.hostlist) and len(self.hostlist) > 0:
            self.complete = True



class NakedExecutionContext(LocalExecutionContext):

    def __init__(self,targetHost,stdin, nakedCommandInfo):

        LocalExecutionContext.__init__(self, stdin)
        self.targetHost=targetHost
        self.passwordMap = nakedCommandInfo.passwordMap
        self.sftp_operation = nakedCommandInfo.sftp_operation
        self.sftp_remote = nakedCommandInfo.sftp_remote
        self.sftp_local = nakedCommandInfo.sftp_local
        self.client = None

    def execute(self,cmd):

        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            self.client.connect(self.targetHost, password=self.passwordMap.mapping[self.targetHost])
        except paramiko.AuthenticationException:
            self.client.close()
            cmd.set_results(CommandResult(1,"","password validation on %s failed" % self.targetHost,False, False))
            return
        except Exception, e:
            cmd.set_results(CommandResult(1,"","conection to host " + self.targetHost + " failed: " + e.__str__(),False, False))
            return

        if self.sftp_operation == NakedExecutionInfo.SFTP_NONE:
            self.execute_ssh(cmd)
        elif self.sftp_operation == NakedExecutionInfo.SFTP_PUT:
            self.execute_sftp_put(cmd)
        elif self.sftp_operation == NakedExecutionInfo.SFTP_GET:
            self.execute_sftp_get(cmd)
        else:
            raise Exception("bad NakedExecutionInfo.sftp_operation")

    def execute_ssh(self,cmd):

        try:
            stdin, stdout, stderr = self.client.exec_command(cmd.cmdStr)
            rc  = stdout.channel.recv_exit_status() 
            self.completed=True
            cmd.set_results(CommandResult(rc,stdout.readlines(),stderr.readlines(),self.completed, self.halt))
            stdin.close()
            stdout.close()
            stderr.close()
        except Exception, e:
            cmd.set_results(CommandResult(1,"",e.__str__(),False, False))
        finally:
            self.client.close()

    def execute_sftp_put(self, cmd):

        ftp = None
        try:
            ftp = self.client.open_sftp()
            ftp.put(self.sftp_local, self.sftp_remote)
            self.completed=True
            cmd.set_results(CommandResult(0,"","",self.completed, self.halt))
        except Exception, e:
            cmd.set_results(CommandResult(1,"",e.__str__(),False, False))
        finally:
            ftp.close()
            self.client.close()

    def execute_sftp_get(self, cmd):

        ftp = None
        try:
            ftp = self.client.open_sftp()
            ftp.get(self.sftp_remote, self.sftp_local)
            self.completed=True
            cmd.set_results(CommandResult(0,"","",self.completed, self.halt))
        except Exception, e:
            cmd.set_results(CommandResult(1,"",e.__str__(),False, False))
        finally:
            ftp.close()
            self.client.close()

    def interrupt(self, cmd):
        self.halt=True
        self.client.close()
        cmd.set_results(CommandResult(1,"","command on host " + self.targetHost + " interrupted ", False, False))
    
    def cancel(self, cmd):
        self.client.close()
        cmd.set_results(CommandResult(1,"","command on host " + self.targetHost + " canceled ", False, False))


    
class RemoteExecutionContext(LocalExecutionContext):

    trail = set()
    """
    Leaves a trail of hosts to which we've ssh'ed, during the life of a particular interpreter.
    """
    
    def __init__(self,targetHost,stdin):
        LocalExecutionContext.__init__(self, stdin)
        self.targetHost=targetHost
        pass
    
    def execute(self,cmd):
        # prepend env. variables from ExcecutionContext.propagate_env_map
        # e.g. Given {'FOO': 1, 'BAR': 2}, we'll produce "FOO=1 BAR=2 ..."
        for k, v in self.__class__.propagate_env_map.iteritems():
            cmd.cmdStr = "%s=%s %s" % (k, v, cmd.cmdStr)
        self.__class__.trail.add(self.targetHost)

        # also propagate env from command instance specific map
        for k, v in cmd.propagate_env_map.iteritems():
            cmd.cmdStr = "%s=%s %s" % (k, v, cmd.cmdStr)

        # Escape " for remote execution otherwise it interferes with ssh
        cmd.cmdStr = cmd.cmdStr.replace('"', '\\"')
        cmd.cmdStr="ssh -o 'StrictHostKeyChecking no' %s \"%s %s\"" % (self.targetHost,SRC_GPPATH,cmd.cmdStr)
        LocalExecutionContext.execute(self,cmd)
        if (cmd.get_results().stderr.startswith('ssh_exchange_identification: Connection closed by remote host')):
            self.__retry(cmd)
        pass
    
    def __retry(self, cmd, count=0):
        if count == SSH_MAX_RETRY:
            return
        time.sleep(SSH_RETRY_DELAY)
        LocalExecutionContext.execute(self, cmd)
        if (cmd.get_results().stderr.startswith('ssh_exchange_identification: Connection closed by remote host')):
            self.__retry(cmd, count + 1)
        
        
class RMIExecutionContext(ExecutionContext):
    """ Leave this as a big old TODO: for now.  see agent.py for some more details"""
    def __init__(self):
        ExecutionContext.__init__(self)
        raise Exception("RMIExecutionContext - Not implemented")
        pass


class Command:
    """ TODO: 
    """
    name=None
    cmdStr=None
    results=None
    exec_context=None
    propagate_env_map={} #  specific environment variables for this command instance

    def __init__(self,name,cmdStr,ctxt=LOCAL,remoteHost=None,stdin=None,nakedExecutionInfo=None):
        self.name=name
        self.cmdStr=cmdStr        
        self.exec_context=createExecutionContext(ctxt,remoteHost,stdin=stdin,nakedExecutionInfo=nakedExecutionInfo)
        self.remoteHost=remoteHost
        
    def __str__(self):
        if self.results:
            return "%s cmdStr='%s'  had result: %s" % (self.name,self.cmdStr,self.results)
        else:
            return "%s cmdStr='%s'" % (self.name,self.cmdStr)
        

    def run(self,validateAfter=False):
        faultPoint = os.getenv('GP_COMMAND_FAULT_POINT')
        if not faultPoint or (self.name and not self.name.startswith(faultPoint)):
            self.exec_context.execute(self)
        else:
            # simulate error
            self.results = CommandResult(1,'Fault Injection','Fault Injection' ,False,True)

        if validateAfter:
            self.validate()
        pass
    
    
    
    def set_results(self,results):
        self.results=results
    
        
    def get_results(self):
        return self.results
    
    def get_stdout_lines(self):
        return self.results.stdout.splitlines()

    def get_stderr_lines(self):
        return self.results.stderr.splitlines()

    
    def cancel(self):
        self.exec_context.cancel(self)    
    
    def interrupt(self):
        self.exec_context.interrupt(self)
    
    def was_successful(self):
        if self.results is None:
            return False
        else:
            return self.results.wasSuccessful()
    
        
    def validate(self,expected_rc=0):
        """Plain vanilla validation which expects a 0 return code."""        
        if self.results.rc != expected_rc:
            raise ExecutionError("non-zero rc: %d" % self.results.rc, self)
        

class SQLCommand(Command):
    """Base class for commands that execute SQL statements.  Classes
    that inherit from SQLCOmmand should set cancel_conn to the pygresql 
    connection they wish to cancel and check self.cancel_flag."""
    
    def __init__(self,name):
        Command.__init__(self, name, cmdStr=None)
        self.cancel_flag = False
        self.cancel_conn = None

    
    def run(self,validateAfter=False):
        raise ExecutionError("programmer error.  implementors of SQLCommand must implement run()", self)
    
    def interrupt(self):
        # No execution context for SQLCommands
        pass
    
    def cancel(self):
        # assignment is an atomic operation in python
        self.cancel_flag = True
        
        # if self.conn is not set we cannot cancel.
        if self.cancel_conn:
            DB(self.cancel_conn).cancel()
    
    
def run_remote_commands(name, commands):
    """
    """
    cmds = {}
    pool = WorkerPool()
    for host, cmdStr in commands.items():
        cmd = Command(name=name, cmdStr=cmdStr, ctxt=REMOTE, remoteHost=host)
        pool.addCommand(cmd)
        cmds[host] = cmd
    pool.join()
    pool.check_results()
    return cmds
