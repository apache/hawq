# The MIT License
#
# Copyright (C) 2009 Floris Bruynooghe
#
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Helper module for running test appllications"""


import os
import signal
import sys

try:
    import subprocess
except ImportError:
    HAVE_SUBPROCESS = False
    import popen2
    import select
else:
    HAVE_SUBPROCESS = True


__all__ = ['APP32', 'APP64', 'pscmd', 'TestApp']


def can_run(file):
    """Check if we can run a test application

    Returns the filename if so, False otherwise.
    """
    if not os.path.exists(file):
        return False
    if HAVE_SUBPROCESS:
        try:
            p = subprocess.Popen([file], stdout=subprocess.PIPE)
            p.stdout.read(1)
        except OSError:
            pass
        else:
            os.kill(p.pid, signal.SIGTERM)
            p.wait()
            return file
    else:
        p = popen2.Popen3(file, True)
        rlist, wlist, xlist = select.select([p.fromchild, p.childerr], [], [])
        if p.poll() == -1:
            os.kill(p.pid, signal.SIGTERM)
            p.wait()
        stdout = p.fromchild.read()
        stderr = p.childerr.read()
        if stdout:
            return file
#     sys.stdout.write('%s not runnable, some tests will be skipped'
#                      % os.path.basename(file))
    return False


APP32 = can_run(os.path.join(os.path.dirname(__file__), 'app32'))
APP64 = can_run(os.path.join(os.path.dirname(__file__), 'app64'))


def run(cmd):
    """Invoke a command, return stdout

    This is a small helper that runs on all supported Python versions.
    `cmd` is a list of the command line arguments.
    """
    if HAVE_SUBPROCESS:
        val = subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]
    else:
        val = os.popen(' '.join(cmdl)).read()
    val = val.decode()
    return val.strip()


# Find the ps command.
if os.path.exists('/bin/ps'):
    PSCMD = '/bin/ps'
elif os.path.exists('/usr/bin/ps'):
    PSCMD = '/usr/bin/ps'
else:
    PSCMD = None


def pscmd(item, pid=os.getpid()):
    """Invoke ps -o %(item)s -p %(pid)d and return the result"""
    pscmd = PSCMD
    if item == 'sid' and os.uname()[0] == 'AIX':
        pscmd = '/usr/sysv/bin/ps'
    if item == 'sid' and os.uname()[0] == 'Darwin':
        item = 'sess'
    assert pscmd, 'ps command not found (%s), can not run test' % pscmd
    if item == 'ni' and os.uname()[0] == 'SunOS':
        item = 'nice'
    if item == 'rssize' and os.uname()[0] in ['SunOS', 'Darwin']:
        item = 'rss'
    if item == 'pgrp' and os.uname()[0] in ['SunOS', 'AIX', 'Darwin']:
        item = 'pgid'
    cmdl = [pscmd, '-o', item, '-p', str(pid)]
    if HAVE_SUBPROCESS:
        val = subprocess.Popen(cmdl, stdout=subprocess.PIPE).communicate()[0]
    else:
        val = os.popen(' '.join(cmdl)).read()
    val = val.decode()
    val = val.strip().split()[-1]
    if item == 'sess' and os.uname()[0] == 'Darwin':
        # 'ps -o sess' on Darwin returns a hex value
        val = int(val, 16)
    return val


class TestApp:
    """Simple class to run test apps as subprocesses

    This will work on all supported python versions (as opposed to the
    subprocess module).  It will also ensure that the process is
    actually running when the constructor returns.
    """
    def __init__(self, args, env=None):
        """Create the instance

        args:: Argument list as a Python list.
        env:: Environment to run in.
        """
        self.args = args
        self.env = env
        self.pid = None
        if HAVE_SUBPROCESS:
            self.app = self._run_subprocess()
        else:
            self.app = self._run_fork()

    def kill(self):
        """Kill the process

        Calling this more then once or on an already killed process
        does not do any harm.
        """
        self.kill_to_zombie()
        self.wait()

    def kill_to_zombie(self):
        """Kill the process, leaving it in a zombie state

        This does not guarantee the process is in zombie state, it
        might have been waited on somewhere else.
        """
        try:
            os.kill(self.pid, signal.SIGTERM)
        except OSError:
            e = sys.exc_info()[1]
            if not e.errno == 3:
                raise

    def wait(self):
        """Wait for the process

        If the process in no longer waitable, i.e. does no longer
        exist, this will return immediately.
        """
        try:
            if hasattr(self.app, 'wait'):
                self.app.wait()
            else:
                os.waitpid(self.pid, 0)
        except OSError:
            e = sys.exc_info()[1]
            if not e.errno == 10:
                raise
        self.pid = None

    def _run_subprocess(self):
        app =  subprocess.Popen(self.args, env=self.env, stdout=subprocess.PIPE)
        self.pid = app.pid
        app.stdout.read(1)
        return app

    def _run_fork(self):
        # Based on popen2.Popen3 but simplified.
        try:
            MAXFD = os.sysconf('SC_OPEN_MAX')
        except (AttributeError, ValueError):
            MAXFD = 256
        c2pread, c2pwrite = os.pipe()
        pid = os.fork()
        if pid == 0:            # child
            os.dup2(c2pwrite, 1)
            for i in xrange(3, MAXFD):
                try:
                    os.close(i)
                except OSError:
                    pass
            try:
                if self.env is not None:
                    os.execve(self.args[0], self.args, self.env)
                else:
                    os.execv(self.args[0], self.args)
            finally:
                os._exit(1)
        else:                   # parent
            self.pid = pid
            os.close(c2pwrite)
            fromchild = os.fdopen(c2pread, 'r')
            fromchild.read(1)
            return None
