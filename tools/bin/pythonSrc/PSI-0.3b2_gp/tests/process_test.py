# The MIT License
#
# Copyright (C) 2007 Chris Miles
#
# Copyright (C) 2008-2009 Floris Bruynooghe
#
# Copyright (C) 2008-2009 Abilisoft Ltd.
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

"""Tests for the psi.process.Process class"""


import grp
import math
import os
import pwd
import signal
import sys
import time
import unittest
import warnings

from apphelper import *

try:
    import subprocess
except ImportError:
    HAVE_SUBPROCESS = False
else:
    HAVE_SUBPROCESS = True

import psi
import psi.arch
import psi.process


# Ignore FutureWarnings, we test those bits
if sys.hexversion >= 0x02030000: # >= 2.3
    warnings.simplefilter('ignore', FutureWarning)


# Patch old versions of unittest
if not hasattr(unittest.TestCase, 'assertTrue'):
    unittest.TestCase.assertTrue = unittest.TestCase.failUnless
if not hasattr(unittest.TestCase, 'assertFalse'):
    unittest.TestCase.assertFalse = unittest.TestCase.failIf
if not hasattr(unittest.TestCase, 'assertAlmostEqual'):
    def assertAlmostEqual(self, first, second, places=7, msg=None):
        """Stolen from Python 2.5"""
        if round(second-first, places) != 0:
            raise self.failureException(
                msg or '%r != %r within %r places'
                % (first, second, places))
    unittest.TestCase.assertAlmostEqual = assertAlmostEqual


def do_work():
    """Make sure total CPU time of our process increases"""
    t = 0
    e = time.clock() + 0.05
    if isinstance(psi.arch.arch_type(), psi.arch.ArchAIX):
        e += 0.8
    while t < e:
        for i in range(100):
            math.sin(i)
        t = time.clock()


class ExceptionTest(unittest.TestCase):
    def test_class(self):
        self.assert_(issubclass(psi.process.NoSuchProcessError,
                                psi.MissingResourceError))

    def test_instance(self):
        np = psi.process.NoSuchProcessError('foo')
        self.assert_(isinstance(np, psi.MissingResourceError))


class ProcessInitTest(unittest.TestCase):
    def setUp(self):
        self.pid = os.getpid()
        self.p = psi.process.Process(self.pid)

    def test_type(self):
        self.assert_(isinstance(self.p, psi.process.Process))

    def test_bad_kw_arg(self):
        self.assertRaises(TypeError, psi.process.Process, foo=1)

    def test_bad_pos_arg(self):
        self.assertRaises(TypeError, psi.process.Process, 'foo')

    def test_no_such_pid(self):
        # may not work in rare circumstances ... see how we go
        self.assertRaises(psi.process.NoSuchProcessError,
                          psi.process.Process, pid=-1)

    def test_pid(self):
        self.assertEqual(self.p.pid, self.pid)


class ProcessSpecialMethods(unittest.TestCase):
    def setUp(self):
        self.pid = os.getpid()
        self.p = psi.process.Process(self.pid)

    def test_repr(self):
        self.assertEqual('psi.process.Process(pid=%d)'%self.pid, repr(self.p))

    def test_hash_works(self):
        self.assertTrue(hash(self.p))

    def test_hash_compare(self):
        p = psi.process.Process(self.pid)
        self.assertEqual(hash(self.p), hash(p))


class RichCompareTest(unittest.TestCase):
    def setUp(self):
        self.pid = os.getpid()
        self.p = psi.process.Process(self.pid)
        self.init = psi.process.Process(1)

    def test_eq(self):
        self.assertEqual(self.p, self.p)
        self.assertEqual(self.p, psi.process.Process(self.pid))

    def test_ne(self):
        self.assertNotEqual(self.p, self.pid)
        self.assertNotEqual(self.p, self.init)

    def test_lt(self):
        self.assertTrue(self.init < self.p)
        self.assertFalse(self.p < self.p)
        self.assertFalse(self.p < self.init)

    def test_le(self):
        self.assertTrue(self.init <= self.p)
        self.assertTrue(self.p <= self.p)
        self.assertFalse(self.p <= self.init)

    def test_gt(self):
        self.assertFalse(self.init > self.p)
        self.assertFalse(self.p > self.p)
        self.assertTrue(self.p > self.init)

    def test_ge(self):
        self.assertFalse(self.init >= self.p)
        self.assertTrue(self.p >= self.p)
        self.assertTrue(self.p >= self.init)


class ProcessExeArgsEnvTest(unittest.TestCase):
    def setUp(self):
        self.pid = os.getpid()
        self.p = psi.process.Process(self.pid)
        self.arch = psi.arch.arch_type()
        self.args_short = ['abcdefghijklmnopqrtstuvw']
        self.args_long = 50 * self.args_short
        self.env = {'foo': 'fooenv',
                    'bar': 'barenv',
                    'baz': 'bazenv'}
        self.app = None

    def tearDown(self):
        if self.app is not None:
            self.app.kill()

    def test_name(self):
        # Lets assume that 'python' will at least appear inside our name
        if isinstance(self.arch, psi.arch.ArchDarwin):
            python_name = 'Python'  # OS X
        else:
            python_name = 'python'
        self.assert_(self.p.name.find(python_name) >= 0)

    def test_exe(self):
        if (isinstance(self.arch, psi.arch.ArchLinux) or
            (isinstance(self.arch, psi.arch.ArchSunOS) and
             self.arch.release_info >= (5, 10))):
            self.assertTrue(os.path.isabs(self.p.exe), self.p.exe)
        elif isinstance(self.arch, psi.arch.ArchDarwin):
            self.assertTrue(self.p.exe.endswith('Python'))
        else:
            self.assertTrue(self.p.exe.find('python') >= 0)

    def test_args_simple(self):
        self.assert_('python' ' '.join(self.p.args).lower())

    def test_argc_simple(self):
        self.assertTrue(self.p.argc > len(sys.argv))

    def test_command(self):
        calc_comm = ' '.join(self.p.args)
        psi_comm = self.p.command
        self.assertEqual(psi_comm, calc_comm[:len(psi_comm)])

    def test_env_simple(self):
        self.assertEqual(self.p.env['HOME'], os.path.expanduser('~'))


    if APP32:
        def test_env_32bit(self):
            self.app = TestApp([APP32], env=self.env)
            p = psi.process.Process(self.app.pid)
            self.assertEqual(p.env, self.env)

        def test_args_32bit_short(self):
            self.app = TestApp([APP32] + self.args_short)
            p = psi.process.Process(self.app.pid)
            self.assertEqual(p.args, tuple([APP32] + self.args_short))

        def test_args_32bit_long(self):
            self.app = TestApp([APP32] + self.args_long)
            p = psi.process.Process(self.app.pid)
            self.assertEqual(p.args, tuple([APP32] + self.args_long))

        def test_args_32bit_longarg(self):
            args = [APP32, 'arg_longer_then_fifty_characters'*2]
            self.app = TestApp(args)
            p = psi.process.Process(self.app.pid)
            self.assertEqual(p.args[1], args[1])

        def test_argc_32bit(self):
            self.app = TestApp([APP32, 'foo', 'bar'])
            p = psi.process.Process(self.app.pid)
            self.assertEqual(p.argc, 3)

    if APP64:
        def test_env_64bit(self):
            self.app = TestApp([APP64], env=self.env)
            p = psi.process.Process(self.app.pid)
            self.assertEqual(p.env, self.env)

        def test_args_64bit_short(self):
            self.app = TestApp([APP64] + self.args_short)
            p = psi.process.Process(self.app.pid)
            self.assertEqual(p.args, tuple([APP64] + self.args_short))

        def test_args_64bit_long(self):
            self.app = TestApp([APP64] + self.args_long)
            p = psi.process.Process(self.app.pid)
            self.assertEqual(p.args, tuple([APP64] + self.args_long))

        def test_args_64bit_longarg(self):
            args = [APP64, 'arg_longer_then_fifty_characters'*2]
            self.app = TestApp(args)
            p = psi.process.Process(self.app.pid)
            self.assertEqual(p.args[1], args[1])

        def test_argc_64bit(self):
            self.app = TestApp([APP64] + ['foo', 'bar'])
            p = psi.process.Process(self.app.pid)
            self.assertEqual(p.argc, 3)


class ProcessIdsTest(unittest.TestCase):
    def setUp(self):
        self.pid = os.getpid()
        self.p = psi.process.Process(self.pid)

    def test_euid(self):
        self.assertEqual(self.p.euid, os.geteuid())

    def test_egid(self):
        self.assertEqual(self.p.egid, os.getegid())

    def test_ruid(self):
        self.assertEqual(self.p.ruid, os.getuid())

    def test_rgid(self):
        self.assertEqual(self.p.rgid, os.getgid())


class ProcessAttrsTest(unittest.TestCase):
    def setUp(self):
        self.arch = psi.arch.arch_type()
        self.pid = os.getpid()
        self.p = psi.process.Process(self.pid)

    def test_cwd(self):
        if isinstance(self.arch, psi.arch.ArchDarwin):
            self.assertRaises(psi.AttrNotAvailableError, getattr, self.p, 'cwd')
        else:
            self.assertEqual(os.path.normpath(self.p.cwd), os.getcwd())

    def test_ppid(self):
        self.assert_(self.p.ppid > 1)
        self.assertEqual(type(self.p.ppid), type(self.p.pid))

    def test_pgrp(self):
        pgrp = pscmd('pgrp')
        self.assertEqual(self.p.pgrp, int(pgrp))

    def test_sid(self):
        sid = pscmd('sid')
        self.assertEqual(self.p.sid, int(sid))

    def test_nthreads(self):
        if isinstance(self.arch, psi.arch.ArchLinux) \
                and self.arch.release[:3] == '2.4':
            return
        if isinstance(self.arch, psi.arch.ArchSunOS):
            nlwp = pscmd('nlwp')
            self.assertEqual(self.p.nthreads, int(nlwp))
        else:
            self.assertEqual(self.p.nthreads, 1)

    def test_terminal(self):
        if isinstance(self.arch, psi.arch.ArchSunOS) \
                and self.arch.release in ['5.8', '5.9']:
            return
        if not HAVE_SUBPROCESS:
            return

        terminal = subprocess.Popen(['/usr/bin/tty'],
                                    stdout=subprocess.PIPE).communicate()[0]
        terminal = terminal.decode()
        self.assertEqual(self.p.terminal, terminal.strip())

    def test_status(self):
        if isinstance(self.arch, psi.arch.ArchLinux):
            self.assertEqual(self.p.status, psi.process.PROC_STATUS_RUNNING)
        elif isinstance(self.arch, psi.arch.ArchSunOS):
            if hasattr(psi.process, 'PROC_STATUS_SONPROC'):
                self.assertEqual(self.p.status, psi.process.PROC_STATUS_SONPROC)
            else:
                self.assertEqual(self.p.status, psi.process.PROC_STATUS_SRUN)
        elif isinstance(self.arch, psi.arch.ArchAIX):
            self.assertEqual(self.p.status, psi.process.PROC_STATUS_SACTIVE)

    def test_nice(self):
        # XXX Also need to test non-default nice values.
        ni = pscmd('ni')
        self.assertEqual(self.p.nice, int(ni))


class ProcessPriorityTest(unittest.TestCase):
    if os.uname()[0] == 'AIX':
        def test_range(self):
            p = psi.process.Process(os.getpid())
            self.assertTrue(0 <= p.priority <= 255)

    # XXX Add tests_range like tests for Linux and SunOS.


class ProcessTimeTest(unittest.TestCase):
    def setUp(self):
        self.pid = os.getpid()
        self.p = psi.process.Process(self.pid)
        self.arch = psi.arch.arch_type()

    def test_start_time(self):
        self.assert_(isinstance(self.p.start_time, psi.TimeSpec))

        # Due to rounding errors and CPUs being able to do lots within
        # just one jiffie/tick it can appear that we started after
        # now, at least on Linux.
        # By assuming we haven't been running for more then 20 minutes
        # we can more accurately detect if the timezone handling is correct

        now = time.time() + 1
        before_start = now - 20*60
        self.assert_(before_start < self.p.start_time.timestamp() <= now,
                     '%s < %s <= %s' %
                     (before_start, self.p.start_time.timestamp(), now))

        localnow = time.time() + time.timezone + 1
        if time.daylight:
            localnow += time.altzone
        before_start = localnow - 20*60
        self.assert_(before_start < self.p.start_time.mktime() <= localnow,
                     '%s < %s <= %s' %
                     (before_start, self.p.start_time.mktime(), localnow))

    def test_jiffies(self):
        if isinstance(self.arch, psi.arch.ArchLinux):
            f = open('/proc/%d/stat' % self.pid)
            try:
                stat = f.read().split()
            finally:
                f.close()
            self.assertEqual(self.p.jiffies, int(stat[21]))


class ProcessCpuTest(unittest.TestCase):
    def setUp(self):
        self.pid = os.getpid()
        self.p = psi.process.Process(self.pid)

    def test_cputime(self):
        self.assert_(isinstance(self.p.cputime, psi.TimeSpec))
        self.assert_(self.p.cputime >= (0, 0))

    def test_utime(self):
        self.assert_(isinstance(self.p.utime, psi.TimeSpec))
        self.assert_(self.p.utime >= (0, 0))

    def test_stime(self):
        self.assert_(isinstance(self.p.stime, psi.TimeSpec))
        self.assert_(self.p.stime >= (0, 0))

    def test_cputime_sum(self):
        variance = psi.TimeSpec(1, 0)
        min = self.p.cputime - variance
        max = self.p.cputime + variance
        self.assert_(min < self.p.utime+self.p.stime < max,
                     '%s < %s < %s' % (min, self.p.utime+self.p.stime, max))

    def test_cputime_increases(self):
        do_work()
        p = psi.process.Process(self.pid)
        assert self.p.cputime < p.cputime


    if hasattr(psi.process.Process, 'pcpu'):
        def test_pcpu(self):
            self.assertTrue(0 <= self.p.pcpu <= 100)


class ProcessMemTest(unittest.TestCase):
    def setUp(self):
        self.pid = os.getpid()
        self.p = psi.process.Process(self.pid)
        self.arch = psi.arch.arch_type()

    def test_rss(self):
        self.assert_(self.p.rss > 0)

    def test_vsz(self):
        self.assert_(self.p.vsz > 0)

    def test_rss_vs_vsz(self):
        if isinstance(self.arch, psi.arch.ArchAIX):
            # On AIX the VSZ is only the data section of the virtual
            # size since that's what ps(1) does there.  This means
            # that often (but not always) the RSS value will be larger
            # then the VSZ value, depending on the amount of data in
            # the shared libraries.
            return
        self.assert_(self.p.rss < self.p.vsz,
                     "%d < %d" % (self.p.rss, self.p.vsz))

    def test_rss_ps(self):
        rss = int(pscmd('rssize'))
        rss_min = rss - rss*0.1
        rss_max = rss + rss*0.1
        self.assert_(rss_min < self.p.rss/1024 < rss_max,
                     '%s < %s < %s' % (rss_min, self.p.rss/1024, rss_max))

    def test_vsz_ps(self):
        vsz = int(pscmd('vsz'))
        self.assert_(vsz*0.8 < self.p.vsz/1024 < vsz*1.2,
                     '%s < %s < %s' % (vsz*0.8, self.p.vsz/1024, vsz*1.2))


class ConstantsTest(unittest.TestCase):
    def test_status_codes(self):
        stat_names = [s for s in dir(psi.process)
                      if s.startswith('PROC_STATUS_')]
        self.assert_(len(stat_names) > 0)
        for name in stat_names:
            attr = getattr(psi.process, name)
            self.assert_(isinstance(attr, int))


class ProcessPrivsTest(unittest.TestCase):
    def test_init_works(self):
        p = psi.process.Process(1)
        self.assertEqual(p.euid, 0)


if os.geteuid() == 0 and (APP32 or APP64):
    class ProcessPriorityTest(unittest.TestCase):
        def setUp(self):
            self.appname = APP32 or APP64
            self.app = None

        def tearDown(self):
            if self.app is not None:
                self.app.kill()

        if os.uname()[0] == 'Linux':
            def test_sched_other(self):
                self.app = TestApp(['/usr/bin/chrt',
                                    '--other', '0', self.appname])
                p = psi.process.Process(self.app.pid)
                self.assertEqual(p.priority, 0)

            def test_sched_fifo(self):
                self.app = TestApp(['/usr/bin/chrt',
                                    '--fifo', '42', self.appname])
                p = psi.process.Process(self.app.pid)
                self.assertEqual(p.priority, 42)

            def test_sched_rr(self):
                self.app = TestApp(['/usr/bin/chrt',
                                    '--rr', '42', self.appname])
                p = psi.process.Process(self.app.pid)
                self.assertEqual(p.priority, 42)

        if os.uname()[0] == 'SunOS':
            def test_class_ts(self):
                # This is a very bad class to test, we can't really
                # assert anything since psi gets the real priority and
                # not the user priority.  But on SunOS 8 the FX class
                # is not available, so better have this test then none.
                self.app = TestApp(['/usr/bin/priocntl', '-e',
                                    '-c', 'TS', self.appname])
                p = psi.process.Process(self.app.pid)
                self.assertTrue(-60 <= p.priority <= 60,
                                '-60 <= %d <= 60' % p.priority)

            if psi.arch.arch_type().release_info > (5,8):
                def test_class_fx(self):
                    self.app = TestApp(['/usr/bin/priocntl', '-e',
                                        '-c', 'FX', '-m', '60',
                                        '-p', '42', self.appname])
                    p = psi.process.Process(self.app.pid)
                    self.assertEqual(p.priority, 42)

        if os.uname()[0] == 'AIX':
            def test_aixapp(self):
                # aixapp runs under SCHED_RR at priority 42.
                aixapp = os.path.join(os.path.dirname(__file__), 'aixapp')
                self.app = TestApp([aixapp])
                p = psi.process.Process(self.app.pid)
                self.assertEqual(p.priority, 42)


class RefreshTests(unittest.TestCase):
    def test_refresh(self):
        p = psi.process.Process(os.getpid())
        pid = p.pid
        ct = p.cputime
        do_work()
        p.refresh()
        self.assertEqual(pid, p.pid)
        self.assert_(ct < p.cputime, '%s < %s' % (ct, p.cputime))

    def test_proc_gone(self):
        app = TestApp(APP32 or APP64)
        try:
            p = psi.process.Process(app.pid)
            app.kill()
            app = None
            self.assertRaises(psi.process.NoSuchProcessError, p.refresh)
        finally:
            if app is not None:
                app.kill()


class ChildrenTests(unittest.TestCase):
    def setUp(self):
        self.p = psi.process.Process(os.getpid())

    def test_no_child(self):
        children = self.p.children()
        self.assertEqual(len(children), 0)

    def test_one_child(self):
        app = TestApp(APP32 or APP64)
        try:
            child = psi.process.Process(app.pid)
            children = self.p.children()
            self.assertEqual(len(children), 1)
            self.assertEqual(children[0], child)
        finally:
            app.kill()


class ExistsTests(unittest.TestCase):
    def test_exists(self):
        p = psi.process.Process(os.getpid())
        self.assertEqual(p.exists(), True)

    def test_dead(self):
        app = TestApp(APP32 or APP64)
        try:
            p = psi.process.Process(app.pid)
            app.kill()
            app = None
            self.assertEqual(p.exists(), False)
        finally:
            if app is not None:
                app.kill()


class KillTests(unittest.TestCase):
    def setUp(self):
        self.app = TestApp(APP32 or APP64)
        self.p = psi.process.Process(self.app.pid)

    def tearDown(self):
        if self.app.pid:
            self.app.kill()

    def test_default(self):
        self.p.kill()
        t = 5.0
        while t > 0:
            os.waitpid(self.p.pid, os.WNOHANG)
            if not self.p.exists():
                break
            time.sleep(0.05)
            t -= 0.05
        self.assertEqual(self.p.exists(), False)

    def test_stop(self):
        if isinstance(psi.arch.arch_type(), psi.arch.ArchLinux):
            stop_status = psi.process.PROC_STATUS_STOPPED
        else:
            stop_status = psi.process.PROC_STATUS_SSTOP
        self.p.kill(signal.SIGSTOP)
        t = 5.0
        while t > 0:
            self.p.refresh()
            if self.p.status == stop_status:
                break
            time.sleep(0.05)
            t -= 0.05
        self.assertEqual(self.p.status, stop_status)
        self.p.kill(signal.SIGCONT)
        self.p.refresh()
        self.assertNotEqual(self.p.status, stop_status)
        self.assertEqual(self.p.exists(), True)


class ZombieTests(unittest.TestCase):
    def setUp(self):
        self.app = TestApp(APP32 or APP64)
        self.app.kill_to_zombie()
        a = psi.arch.arch_type()
        if isinstance(a, psi.arch.ArchLinux):
            self.zombie_stat = psi.process.PROC_STATUS_ZOMBIE
        else:
            self.zombie_stat = psi.process.PROC_STATUS_SZOMB

    def tearDown(self):
        if self.app.pid:
            self.app.kill()

    def test_zombie_no_err(self):
        # Ensure no error during init when looking at a zombie
        if isinstance(psi.arch.arch_type(), psi.arch.ArchDarwin):
            # pause to allow process status to update
            time.sleep(1)
        p = psi.process.Process(self.app.pid)
        self.assertEqual(p.status, self.zombie_stat)


if __name__ == '__main__':
    unittest.main()
