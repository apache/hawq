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
from __future__ import with_statement 
import os, sys, subprocess

sys.path.append(sys.path[0] + '/lib')
import cmd, signal, atexit, threading
import pxssh, pexpect

#disable deprecationwarnings
import warnings
warnings.simplefilter('ignore', DeprecationWarning)


progname = os.path.split(sys.argv[0])[-1]

class VersionError(Exception):
    pass

if sys.version_info < (2, 5, 0):
    raise VersionError(
'''Error: %s is supported on Python versions 2.5 or greater
Please upgrade python installed on this machine.''' % progname)


################
def cli_help():
    help_path = os.path.join(sys.path[0], '..', 'docs', 'cli_help', progname + '_help');
    f = None
    try:
        try:
            f = open(help_path);
            return f.read(-1)
        except:
            return ''
    finally:
        if f: f.close()


################
def parse_port(val): 
    try: return int(val)
    except ValueError: return 0


#############
def ssh_prefix(host):
    ssh = 'ssh -o "BatchMode yes" -o "StrictHostKeyChecking no" ' + host
    return ssh


#############
def scp_prefix():
    scp = 'scp -o "BatchMode yes" -o "StrictHostKeyChecking no"'
    return scp

#############
class Error(Exception):
    '''Base class for exceptions in this module.'''
    pass


#############
class HostNameError(Error):
    def __init__(self, msg, lineno = 0):
        if lineno: self.msg = ('%s at line %d' % (value, lineno))
        else: self.msg = msg
    def __str__(self):
        return self.msg


#############
class SSHError(Error):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

#############
def ssh(host, cmd):
    s = 'ssh -o "BatchMode yes" -o "StrictHostKeyChecking no" %s %s 2>& 1' % (host, cmd)
    p = os.popen(s)
    if not p:
        raise SSHError('unable to ssh to %s' % host)
    yield p
    out = p.read(-1)
    rc = p.close() or 0 
    yield (rc, out)


#############
class HostList():
    
    def __init__(self): 
        self.list = []

    def get(self): 
        return self.list

    def add(self, host):
        '''Add a host to the hostlist.'''
        if host.find(':') >= 0 or host.find('@') >= 0:
            raise HostNameError(host)
        self.list.append(host)
        return self.list

    def parseFile(self, path):
        '''Add lines in a file to the hostlist.'''
        with open(path) as fp:
            lineno = 0
            for line in fp:
                lineno = lineno + 1
                line = line.strip()
                if not line: continue;
                if line[0] == '#': continue
                if not self.add(line): 
                    raise HostNameError(line, lineno)
        return self.list

    def batch(self, size=32):
        '''Return list of hostlists, each is of 'size' elements'''
        cnt = int((len(self.list) - 1) / size) + 1
        b = []
        for i in xrange(cnt):
            start = i * size
            end = start + size
            if end > len(self.list): 
                b.append(self.list[start:])
            else:
                b.append(self.list[start:end])
        return b

    def checkSSH(self):
        '''Check that ssh to hostlist is okay.'''

        #TODO: Eliminate this function - it's redundant to the Session class

        for hlist in self.batch():
            slist = []
            for h in hlist:
                s = ssh(h, 'echo')
                s.next()
                slist.append((h, s))
            for (h, s) in slist:
                (rc, out) = s.next()
                if out and out.find('Permission denied') >= 0: 
                    raise SSHError("Permission denied while ssh to '%s'" % h)
                if rc: 
                    raise SSHError("Unable to ssh to '%s'" % h)
        return True

    def filterMultiHomedHosts(self):
        '''For multiple host that is of the same node, keep only one in the hostlist.'''
        unique = {}
        #TODO: use a Session object to do this work so that it runs in parallel
        
        for hlist in self.batch():
            slist = []
            for h in hlist:
                s = ssh(h, 'hostname')
                s.next()
                slist.append((h, s))
            for (h, s) in slist:
                (rc, hname) = s.next()
                if rc: 
                    raise SSHError('Cannot execute hostname on host %s' % h)
                hname = hname.strip()
                if not unique.get(hname):
                    unique[hname] = h
                elif hname == h:
                    unique[hname] = h

        self.list = unique.values()
        return self.list

#############
# Session is a command session, derived from a base class cmd.Cmd
class Session(cmd.Cmd):
    '''Implements a list of open ssh sessions ready to execute commands'''
    verbose=False
    hostList=[]
    userName=None
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

        for p in self.pxssh:
            if self.verbose: print '[INFO] login %s' % p.x_peer
            if not p.loginWait(set_term_dumb=True):
                print '[ERROR] unable to login to %s' % p.x_peer
                print
                print 'hint: use gpssh-exkeys to setup public-key authentication between hosts'
                print
                raise self.SessionError('Error logging into host %s' % p.x_peer)

    def close(self):
        self.clean()
    
    def reset(self):
        '''reads from all the ssh connections to make sure we dont have any pending cruft'''
        for s in self.pxssh:
            s.readlines()
                
    def clean(self):
        self.closePxsshList(self.pxssh)
        self.pxssh = []

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
        def closePxsshOne(p): p.close(force = True)
        th = []
        for p in list:
            t = threading.Thread(target=closePxsshOne, args=(p,))
            t.start()
            th.append(t)
        for t in th:
            t.join() 
