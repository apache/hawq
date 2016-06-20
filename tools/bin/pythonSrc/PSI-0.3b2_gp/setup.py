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

"""
-------------------------
Python System Information
-------------------------

``psi`` is a Python module providing direct access to real-time system
and process information.  It is made of of several sub-modules.

The ``arch`` module gives some information about the system such as
the sytem name and version, the machine architecture etc.  It has a
class representing each system and a factory function that will return
an instance of the class which ``psi`` is running on currently.

The experimental ``mount`` module gives information about the various
mounted filesystems on the system.  It has a class representing local
or remote filesystems.

The ``process`` module provides an interface to information about
processes currently running on the system.  Each process is
represented as an instance of the ``Process`` class and additionally
there is a ``ProcessTable`` class which is a dictionary of all running
processes.  To know exactly what attributes are available and what
they mean you should look at the docstrings and examples in the
``REAME`` file and ``examples/`` directory, but important to note is
that all the information is collected at instatiation time.  So the
contents of ``ProcessTable`` and ``Process`` instances are really
snapshots and will still contain all information even after the actual
process has gone.

Lastly there are some general functions available directly under the
``psi`` namespace such as ``loadavg()``, ``getzoneid()`` etc.  Once
more see the docstrings for detailed information.

Some information may not be available on all platforms, rather then
trying to emulate this information these parts of the API just don't
exist on those platforms.  Examples of these are:
``psi.process.Process.pcpu`` which is not available on Linux,
``psi.getzoneid()`` which is only available on SunOS 10 and above etc.


Supported Platforms
===================

Python: 2.2 and above, including 3.x.

Linux: all 2.4 and 2.6 kernels.

SunOS: Solaris 8 and above, including OpenSolaris (SunOS 11).

AIX: 5.3

Darwin: 10.3 and above.


Documentation
=============

Care is taken to provide complete and accurate docstrings, so use
Python's ``pydoc`` tool and the interactive prompt should get you on
your way.

We also have a wiki (http://bitbucket.org/chrismiles/psi/wiki/Home)
and a mailing list (http://groups.google.com/group/psi-discuss
psi-discuss@googlegroups.com).  Don't hesitate to ask questions or
give feedback.


Bugs
====

Please use our issue tracker:
http://bitbucket.org/chrismiles/psi/issues


Extra setup.py features
=======================

New ``build_ext`` option: ``--devel``.  This uses ``-Werror`` and
enables many more warnings as well as disables optimisation.

Using ``--undef PYMALLOC`` or ``-U PYMALLOC`` to ``build_ext`` will
use libc's memory heap for allocation instead of Python's.


The ``test`` command will run the testsuite.  Some tests will only be
run when running the test suite as root.  Currently these are the
tests that try to run a simple test application under specific
schedulers and priorities to assert psi detects these process
attributes correctly.


The ``valgrind`` command does run the testsuite under the valgrind
memory checker.  For this you need to have a specially compiled
python::

  ./configure --with-pydebug --without-pymalloc --prefix=/opt/pydebug
  make
  make install


The ``tags`` command will build an emacs TAGS file using ``grind``
(which is a binary of the python grin_ package).

.. _grin: http://pypi.python.org/pypi/grin
"""


# Note: We can't use the platform module as that breaks Python 2.2
#       compatibility.  Use os.uname() instead.


import distutils.dep_util
import glob
import os
import os.path
import shutil
import sys
import unittest
from distutils import ccompiler
from distutils import sysconfig
from distutils.core import setup, Extension, Command
from distutils.command.clean import clean
from distutils.command.build_ext import build_ext
from distutils.errors import LinkError


try:
    import subprocess
except ImportError:
    HAVE_SUBPROCESS = False
else:
    HAVE_SUBPROCESS = True

try:
    import distutils.log
except ImportError:
    class MyLog:
        def info(self, arg):
            sys.stdout.write(arg)
            sys.stdout.write('\n')

        def warn(self, arg):
            sys.stdout.write(arg)
            sys.stdout.write('\n')

    import distutils
    distutils.log = MyLog()

try:
    from distutils.command.build_py import build_py_2to3
except ImportError:
    pass


# CPP macros we want defined, use --undef or -U if you want to disable
# any of these.
MACROS = [('PYMALLOC', None)]

# Define macros describing the system we're compiling on.
# e.g.:
# - Linux: LINUX, LINUX2_6, LINUX2_6_27, LINUX2_6_MINOR=27
# - Solaris: SUNOS, SUNOS5_10, SUNOS5_MINOR=10
# - AIX: AIX, AIX5, AIX5_3, AIX_MINOR=3
SYSTEM = os.uname()[0].upper()
MACROS.append((SYSTEM, None))
if SYSTEM in ['LINUX', 'SUNOS']:
    RELEASE = os.uname()[2].split('-')[0].replace('.', '_')
    MACROS.append((SYSTEM+RELEASE, None))
if SYSTEM == 'LINUX':
    maj = '_'.join(RELEASE.split('_')[:2])
    MACROS.append((SYSTEM+maj, None))
    MACROS.append((SYSTEM+maj+'_MINOR', RELEASE.split('_')[2]))
if SYSTEM == 'SUNOS':
    maj = RELEASE.split('_')[0]
    MACROS.append((SYSTEM+maj, None))
    MACROS.append((SYSTEM+maj+'_MINOR', RELEASE.split('_')[1]))
if SYSTEM == 'AIX':
    major = os.uname()[3]
    minor = os.uname()[2]
    MACROS.append((SYSTEM+major, None))
    MACROS.append((SYSTEM+major+'_'+minor, None))
    MACROS.append((SYSTEM+major+'_MINOR', minor))


# These are used to locate the correct source files.
PLATFORM = os.uname()[0].lower()


_psi_headers = ['include/psi.h',
                'include/psifuncs.h',
                'include/posix_utils.h']
_psi_sources = ['src/util.c',
                'src/_psimodule.c',
                'src/timespec.c',
                'src/arch/posix_utils.c',
                'src/arch/%s_psi.c' % PLATFORM]
_psi_libraries = []
if PLATFORM == 'aix':
    _psi_sources.append('src/arch/aix_psi.c')
    _psi_libraries.append('perfstat')
if PLATFORM == 'linux':
    _psi_headers.append('include/linux_utils.h')
    _psi_sources.append('src/arch/linux_utils.c')
if PLATFORM != 'aix':
    _psi_sources.append('src/arch/getloadavg.c')

arch_headers = ['include/psi.h',
                'include/arch.h']
arch_sources = ['src/util.c',
                'src/archmodule.c',
                'src/arch.c',
                'src/arch/posix_arch.c']

mount_headers = ['include/psi.h',
                 'include/mount.h',
                 'include/posix_mount.h']
mount_sources = ['src/util.c',
                 'src/mountmodule.c',
                 'src/mount.c',
                 'src/arch/posix_mount.c',
                 'src/arch/%s_mount.c' % PLATFORM]

process_headers = ['include/psi.h',
                   'include/process.h',
                   'include/posix_utils.h',
                   'include/procfs_utils.h']
process_sources = ['src/util.c',
                   'src/processmodule.c',
                   'src/process.c',
                   'src/processtable.c',
                   'src/arch/posix_utils.c',
                   'src/arch/procfs_utils.c',
                   'src/arch/%s_process.c' % PLATFORM]
if PLATFORM == 'linux':
    process_headers.append('include/linux_utils.h')
    process_sources.append('src/arch/linux_utils.c')
if PLATFORM == 'darwin':
    process_sources.append('src/arch/darwin_processtable.c')
else:
    process_sources.append('src/arch/procfs_processtable.c')

_psi = Extension('psi._psi',
                 define_macros = MACROS,
                 include_dirs = ['include'],
                 sources = _psi_sources,
                 libraries = _psi_libraries,
                 depends = _psi_headers)

arch = Extension('psi.arch',
                 define_macros = MACROS,
                 include_dirs = ['include'],
                 sources = arch_sources,
                 depends = arch_headers)

mount = Extension('psi.mount',
                  define_macros = MACROS,
                  include_dirs = ['include'],
                  sources = mount_sources,
                  depends = mount_headers)

process = Extension('psi.process',
                    define_macros = MACROS,
                    include_dirs = ['include'],
                    sources = process_sources,
                    depends = process_headers)


class BuildExtCommand(build_ext):
    """Extend the build_ext command

    We add an option to enable extra compilation flags during
    development.  We also compile the helper application "sargs64.so"
    on Solaris here.
    """
    user_options = build_ext.user_options + [
        ('devel', None,
         'use extra-safe compilation options, e.g. -Werror etc.')]
    boolean_options = build_ext.boolean_options + ['devel']
    extra_cflags = ['-O0',
                    '-Wall',
                    '-Werror',
                    '-Wdeclaration-after-statement',
                    '-Wno-pointer-sign',
                    '-Wcast-align',
                    '-Wbad-function-cast',
                    '-Wmissing-declarations',
                    '-Winline',
                    '-Wsign-compare',
                    '-Wmissing-prototypes',
                    '-Wnested-externs',
                    '-Wpointer-arith',
                    '-Wredundant-decls',
                    '-Wformat-security',
                    '-Wswitch-enum',
                    '-Winit-self',
                    '-Wmissing-include-dirs']
    def initialize_options(self):
        build_ext.initialize_options(self)
        self.devel = False
        if PLATFORM == 'sunos':
            # XXX This is rubbish, should check the compiler instead. --flub
            self.extra_cflags.remove('-Wno-pointer-sign')
            self.extra_cflags.remove('-Wmissing-include-dirs')
            self.extra_cflags.remove('-Wredundant-decls')

    def finalize_options(self):
        build_ext.finalize_options(self)
        if self.devel:
            for ext in self.extensions:
                if hasattr(ext, 'extra_compile_args'):
                    ext.extra_compile_args.extend(self.extra_cflags)
                else:
                    ext.extra_compile_args = self.extra_cflags

    def run(self):
        build_ext.run(self)
        if PLATFORM == 'sunos':
            src = os.path.join('src', 'arch', 'sargs64.c')
            destdir = os.path.join(self.build_lib, 'psi')
            if self.devel:
                postargs = self.extra_cflags
            else:
                postargs = []
            try:
                self.compiler.link_executable([src],
                                              'sargs64.so',
                                              output_dir=destdir,
                                              extra_preargs=['-m64'],
                                              extra_postargs=postargs)
            except LinkError:
                distutils.log.warn(
                    'sargs64.so not built, this is normal on 32-bit platforms')


class TestCommand(Command):
    description = "Runs the test suite on the files in build/"
    user_options = []
    def initialize_options(self):
        self._dir = os.getcwd()

    def finalize_options(self):
        build = self.get_finalized_command('build')
        self.build_purelib = build.build_purelib
        self.build_platlib = build.build_platlib

    def run(self):
        """Finds all the tests modules in tests/, and runs them"""
        self.run_command('build')
        self.cc = ccompiler.new_compiler()
        sysconfig.customize_compiler(self.cc)
        self.compile_ctests()
        if os.uname()[0] == 'AIX':
            self.compile_aix_ctest()
        sys.path.insert(0, self.build_purelib)
        sys.path.insert(0, self.build_platlib)
        tdir = os.path.join(self._dir, 'tests')
        sys.path.insert(0, tdir)
        testfiles = []
        for t in glob.glob(os.path.join(tdir, '*_test.py')):
            testfiles.append(os.path.splitext(os.path.basename(t))[0])
        tests = unittest.TestLoader().loadTestsFromNames(testfiles)
        t = unittest.TextTestRunner(verbosity=2)
        r = t.run(tests)
        if not r.wasSuccessful():
            sys.exit(1)

    def compile_ctests(self):
        """Compile helper executables from C

        Currently a very simple app is compiled in both 32- and 64-bit
        mode.  This is used to test argv & envv on AIX and SunOS
        """
        # If one of app32 or app64 exists we assume the other can't be
        # compiled.
        targets = []
        if os.path.exists('tests/app32'):
            targets.append('tests/app32')
        if os.path.exists('tests/app64'):
            targets.append('tests/app64')
        build = True
        for target in targets:
            build = False
            if distutils.dep_util.newer('tests/app.c', target):
                build = True
                break
        if not build:
            distutils.log.info('helper apps are up-to-date')
            return

        # We skip the explicity compiling to object files, the command
        # line invoked by link_executable() will also work from source
        # files directly.
        distutils.log.info('compiling helper apps for tests')
        m32 = '-m32'
        m64 = '-m64'
        if sys.platform == 'aix5':
            m32 = '-maix32'
            m64 = '-maix64'
        try:
            self.cc.link_executable(['tests/app.c'],
                                    'app32',
                                    output_dir='tests',
                                    extra_preargs=[m32])
        except LinkError:
            distutils.log.warn('app32 not built, some tests will be skipped')
        try:
            self.cc.link_executable(['tests/app.c'],
                                    'app64',
                                    output_dir='tests',
                                    extra_preargs=[m64])
        except LinkError:
            distutils.log.warn('app64 not built, some tests will be skipped')

    def compile_aix_ctest(self):
        """Compile AIX specific helper executables from C"""
        if not distutils.dep_util.newer('tests/aixapp.c', 'tests/aixapp'):
            distutils.log.info('AIX helper app is up-to-date')
            return
        distutils.log.info('compiling AIX helper app for tests')
        self.cc.link_executable(['tests/aixapp.c'], 'tests/aixapp')


class ValgrindCommand(Command):
    description = "runs valgrind on test suite using /opt/pydebug/bin/python"
    user_options = []
    def initialize_options(self):
        if not HAVE_SUBPROCESS:
            sys.exit('This command requires the subprocess module')
        self._pydbg = os.path.join('/opt', 'pydebug', 'bin', 'python')
        if not os.path.isfile(self._pydbg):
            sys.exit('%s not found' % self._pydbg)
        #self._curdir = os.path.dirname(__file__)
        self._curdir = os.getcwd() # XXX: Why doesn't the line above work?
        self._suppr = os.path.join(self._curdir, 'misc', 'valgrind-python.supp')
        if not os.path.isfile(self._suppr):
            sys.exit('%s not found' % self._suppr)

    def finalize_options(self):
        pass

    def run(self):
        subprocess.call('%s setup.py build_ext --devel'%self._pydbg, shell=True)
        subprocess.call('%s setup.py build'%self._pydbg, shell=True)
        subprocess.call('valgrind --tool=memcheck --suppressions=%(suppr)s '
                        '--leak-check=full %(pydbg)s -E -tt setup.py test'
                        % {'suppr': self._suppr, 'pydbg': self._pydbg},
                        shell=True)


class CleanCommand(clean):
    """Extend the clean command

    We also want to remove all .pyc files and test applications.
    """
    def initialize_options(self):
        self._clean_me = []
        clean.initialize_options(self)

    def finalize_options(self):
        if hasattr(os, 'walk'):
            for root, dirs, files in os.walk('.'):
                for f in files:
                    if f.endswith('.pyc'):
                        self._clean_me.append(os.path.join(root, f))
        if os.path.exists(os.path.join('tests', 'app32')):
            self._clean_me.append(os.path.join('tests', 'app32'))
        if os.path.exists(os.path.join('tests', 'app64')):
            self._clean_me.append(os.path.join('tests', 'app64'))
        clean.finalize_options(self)

    def run(self):
        for clean_me in self._clean_me:
            distutils.log.info("removing '%s'" % clean_me)
            try:
                os.unlink(clean_me)
            except:
                pass
        clean.run(self)


class TagsCommand(Command):
    description = "generate a TAGS file for use with Emacs (uses `grind')"
    user_options = []
    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        subprocess.call('grind --dirs src/ include/ | xargs etags', shell=True)


COMMANDS = {'build_ext': BuildExtCommand,
            'test': TestCommand,
            'valgrind': ValgrindCommand,
            'clean': CleanCommand,
            'tags': TagsCommand}
if sys.version_info[0] == 3:
    COMMANDS['build_py'] = build_py_2to3


try:
    psidir = os.path.dirname(__file__)
except NameError:
    # Python <= 2.2: Assume we are $(pwd)/setup.py
    psidir = os.getcwd()
    if not os.path.exists(os.path.join(psidir, 'setup.py')):
        sys.exit('Current directory needs to be the root of the PSI package')
sys.path.insert(0, os.path.join(psidir, 'psi'))
import _version


setup(name = 'PSI',
      version = _version.version,
      author = _version.author,
      author_email = 'psi-discuss@googlegroups.com',
      license = _version.license,
      url = 'http://bitbucket.org/chrismiles/psi',
      download_url='http://pypi.python.org/pypi/PSI/',
      description = 'Python System Information',
      long_description = __doc__,
      packages = ['psi'],
      ext_modules = [_psi, arch, mount, process],
      classifiers = [
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: AIX',
        'Operating System :: POSIX :: Linux',
        'Operating System :: POSIX :: SunOS/Solaris',
        'Programming Language :: C',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Operating System Kernels',
        'Topic :: System :: Systems Administration'],
      cmdclass = COMMANDS)

