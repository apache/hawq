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
#
# This file contains ssh Session class and support functions/classes.

import sys
import os
import cmd
import threading
from gppylib.commands.base import WorkerPool, REMOTE, ExecutionError
from gppylib.commands.unix import Hostname, Echo

sys.path.append(sys.path[0] + '/lib')
import pxssh
import pexpect
import socket


class HostNameError(Exception):
    def __init__(self, msg, lineno = 0):
        if lineno: self.msg = ('%s at line %d' % (msg, lineno))
        else: self.msg = msg
    def __str__(self):
        return self.msg

class SSHError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

# Utility Functions
def ssh_prefix(host):
    ssh = 'ssh -o "BatchMode yes" -o "StrictHostKeyChecking no" ' + host
    return ssh

def get_hosts(hostsfile):
    hostlist = HostList()
    hostlist.parseFile(hostsfile)
    return hostlist.get()


class HostList():
    
    def __init__(self): 
        self.list = []

    def get(self): 
        return self.list

    def addHostNameAlternatives(self):
        ''' Add short name and FQDN for each host to the host list '''

        for host in self.list:
            try:
                fqdn = socket.getfqdn(host)
                hostname = fqdn.partition('.')[0]
                ''' Add fully qualified domain names '''
                if fqdn not in self.list:
                    self.list.append(fqdn)

                ''' Add short names '''
                if hostname not in self.list:
                    self.list.append(hostname)
            except socket.error, e:
                print "Error while including hostname alternatives"

    def add(self, host, lineno=0):
        '''Add a host to the hostlist.'''

        # we don't allow the user@ syntax here
        if host.find('@') >= 0:
            raise HostNameError(host, lineno)

        # MPP-13617 - check for ipv6
        if host.find(':') >= 0:
            try:
                socket.inet_pton(socket.AF_INET6, host)
            except socket.error, e:
                raise HostNameError(str(e), lineno)

        # MPP-13617 - check for ipv4
        if host.find('.') >= 0:
            octs = host.split('.')
            if len(octs) == 4 and False not in [o.isdigit() for o in octs]:
                try:
                    socket.inet_pton(socket.AF_INET, host)
                except socket.error, e:
                    raise HostNameError(str(e), lineno)

        self.list.append(host)
        return self.list

    def parseFile(self, path):
        '''Add lines in a file to the hostlist.'''
        with open(path) as fp:
            for i, line in enumerate(fp):
                line = line.strip()
                if not line or line[0] == '#': 
                    continue
                self.add(line, i+1)
        return self.list

    def checkSSH(self):
        '''Check that ssh to hostlist is okay.'''

        pool = WorkerPool()

        for h in self.list:
            cmd = Echo('ssh test', '', ctxt=REMOTE, remoteHost=h)
            pool.addCommand(cmd)
            
        pool.join()
        pool.haltWork()  


        for cmd in pool.getCompletedItems():
            if not cmd.get_results().wasSuccessful():
                raise SSHError("Unable to ssh to '%s'" % cmd.remoteHost)

        return True

    def filterMultiHomedHosts(self):
        '''For multiple host that is of the same node, keep only one in the hostlist.'''
        unique = {}

        pool = WorkerPool()
        for h in self.list:
            cmd = Hostname('hostname', ctxt=REMOTE, remoteHost=h)
            pool.addCommand(cmd)
            
        pool.join()
        pool.haltWork()
        
        for finished_cmd in pool.getCompletedItems():
            hostname = finished_cmd.get_hostname()
            if (not hostname):
                unique[finished_cmd.remoteHost] = finished_cmd.remoteHost
            elif not unique.get(hostname):
                unique[hostname] = finished_cmd.remoteHost
            elif hostname == finished_cmd.remoteHost:
                unique[hostname] = finished_cmd.remoteHost

        self.list = unique.values()
        
        return self.list

# Session is a command session, derived from a base class cmd.Cmd
class Session(cmd.Cmd):
    '''Implements a list of open ssh sessions ready to execute commands'''
    verbose=False
    hostList=[]
    userName=None
    echoCommand=False
    class SessionError(StandardError): pass
    class SessionCmdExit(StandardError): pass

    def __init__(self, hostList=None, userName=None):
        cmd.Cmd.__init__(self)
        self.pxssh = []
        self.prompt = '=> '
        self.peerStringFormatRaw = None
        if hostList:
            for host in hostList:
                self.hostList.append(host)
        if userName: self.userName=userName

    def peerStringFormat(self):
        if self.peerStringFormatRaw: return self.peerStringFormatRaw
        cnt = 0
        for p in self.pxssh:
            if cnt < len(p.x_peer): cnt = len(p.x_peer)
        self.peerStringFormatRaw = "[%%%ds]" % cnt
        return self.peerStringFormatRaw

    def login(self, hostList=None, userName=None):
        '''This is the normal entry point used to add host names to the object and log in to each of them'''
        if self.verbose: print '\n[Reset ...]'
        if not (self.hostList or hostList):
            raise self.SessionError('No host list available to Login method')
        if not (self.userName or userName):
            raise self.SessionError('No user name available to Login method')
        
        #Cleanup    
        self.clean()
    
        if hostList: #We have a new hostlist to use, initialize it
            self.hostList=[]
            for host in hostList:
                self.hostList.append(host)
        if userName: self.userName=userName  #We have a new userName to use

        # MPP-6583.  Save off term type and set to nothing before creating ssh process
        origTERM = os.getenv('TERM', None)
        os.putenv('TERM', '')
        
        for host in hostList:
            self.hostList.append(host)
            p = pxssh.pxssh()
            p.loginAsync(host, self.userName)
            p.x_peer = host
            p.x_pid = p.pid
            self.pxssh.append(p)
            
        # Restore terminal type
        if origTERM:
            os.putenv('TERM', origTERM)

        some_errors = False
        good_list = []
        for p in self.pxssh:
            
            success_login = False
            if self.verbose: print '[INFO] login %s' % p.x_peer
            try:
                success_login = p.loginWait(set_term_dumb=True)
            except Exception as e:
                pass

            if success_login:
                good_list.append(p)
            else:
                some_errors = True
                print '[ERROR] unable to login to %s' % p.x_peer

        if some_errors:
            print 'hint: use gpssh-exkeys to setup public-key authentication between hosts'

        self.pxssh = good_list

    def close(self):
        return self.clean()
    
    def reset(self):
        '''reads from all the ssh connections to make sure we dont have any pending cruft'''
        for s in self.pxssh:
            s.readlines()
                
    def clean(self):
        net_return_code = self.closePxsshList(self.pxssh)
        self.pxssh = []
        return net_return_code

    def emptyline(self):
        pass

    def escapeLine(self,line):
        '''Escape occurrences of \ and $ as needed and package the line as an "eval" shell command'''
        line = line.strip()
        if line == 'EOF' or line == 'exit' or line == 'quit':
            raise self.SessionCmdExit()
        line = line.split('\\')
        line = '\\\\'.join(line)
        line = line.split('"')
        line = '\\"'.join(line)
        line = line.split('$')
        line = '\\$'.join(line)
        line = 'eval "' + line + '" < /dev/null'
        
        return line

    def executeCommand(self,command):
        commandoutput=[]
        
        if self.echoCommand:
            escapedCommand = command.replace('"', '\\"')
            command = 'echo "%s"; %s' % (escapedCommand, command)
            
        #Execute the command in all of the ssh sessions
        for s in self.pxssh:
            s.sendline(command)
            s.flush()
            
        #Wait for each command and retrieve the output
        for s in self.pxssh:
            #Wait for each command to finish
            #!! TODO verify that this is a tight wait loop and find another way to do this
            while not s.prompt(120) and s.isalive() and not s.eof(): pass
            
        for s in self.pxssh:
            #Split the output into an array of lines so that we can add text to the beginning of
            #    each line
            output = s.before.split('\n')
            output = output[1:-1]
                
            commandoutput.append(output)
            
        return commandoutput.__iter__()

# Interactive command line handler
#    Override of base class, handles commands that aren't recognized as part of a predefined set
#    The "command" argument is a command line to be executed on all available command sessions
#    The output of the command execution is printed to the standard output, prepended with
#        the hostname of each machine from which the output came
    def default(self, command):
    
        line = self.escapeLine(command)
    
        if self.verbose: print command
        
    #Execute the command on our ssh sessions
        commandoutput=self.executeCommand(command)
        self.writeCommandOutput(commandoutput)

    def writeCommandOutput(self,commandoutput):
        '''Takes a list of output lists as an iterator and writes them to standard output,
        formatted with the hostname from which each output array was obtained'''
        for s in self.pxssh:
            output = commandoutput.next()
            #Write the output
            if len(output) == 0:
                print (self.peerStringFormat() % s.x_peer)
            else:
                for line in output:
                    print (self.peerStringFormat() % s.x_peer), line
    
    def closePxsshList(self,list):
        lock = threading.Lock()
        return_codes = [0]
        def closePxsshOne(p, return_codes): 
            p.logout()
            p.close()
            with lock:
                return_codes.append(p.exitstatus)
        th = []
        for p in list:
            t = threading.Thread(target=closePxsshOne, args=(p, return_codes))
            t.start()
            th.append(t)
        for t in th:
            t.join() 
        return max(return_codes)
