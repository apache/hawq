#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-

"""
Python distutils setup script for pychecker.

This code was originally contributed by Nicolas Chauvat, and has been rewritten
to generalize it so it works properly with --prefix and other distutils
options.

This install script needs to customize two distutils behaviors: it needs to
install the documentation to the configured package install directory, and it
needs to create a pychecker (or pychecker.bat) script containing the correct
path to the installed checker.py module and the Python interpreter.

Nicolas' original attempt at this worked fine in the normal case (i.e. a root
user install).  However, because it assumed that the package directory was
always within sysconfig.get_python_lib(), it broke when users wanted to specify
a prefix or a home directory install to, etc.

After some research, I've decided that the best way to make this work is to
customize (override) some of the distutils action classes.  This way, we get
access to the distutils configuration and can "do the right thing" when options
are specified.  

@author: Kenneth J. Pronovici <pronovic@ieee.org>, after Nicolas Chauvat.
"""

###################
# Imported modules
###################

import sys
import os
from distutils import core
from distutils.util import execute
from distutils.command.bdist import bdist
from distutils.command.bdist_dumb import bdist_dumb
from distutils.command.bdist_rpm import bdist_rpm
from distutils.command.bdist_wininst import bdist_wininst
from distutils.command.install_data import install_data
from distutils.command.install_scripts import install_scripts
from distutils.command.build_scripts import build_scripts


###############################
# Overridden distutils actions
###############################

using_bdist = 0   # assume we're not using bdist

class my_bdist(bdist):
   """Customized action which flags that a binary distribution is being produced."""
   def run(self):
      global using_bdist
      using_bdist = 1
      bdist.run(self)

class my_bdist_dumb(bdist_dumb):
   """Customized action which flags that a binary distribution is being produced."""
   def run(self):
      global using_bdist
      using_bdist = 1
      bdist_dumb.run(self)

class my_bdist_rpm(bdist_rpm):
   """Customized action which flags that a binary distribution is being produced."""
   def run(self):
      global using_bdist
      using_bdist = 1
      bdist_rpm.run(self)

class my_bdist_wininst(bdist_wininst):
   """Customized action which flags that a binary distribution is being produced."""
   def run(self):
      global using_bdist
      using_bdist = 1
      bdist_wininst.run(self)

class my_install_data(install_data):
   """
   Customized install_data distutils action.

   This customized action forces all data files to all be installed relative to
   the normal library install directory (within site-packages) rather than in
   the standard location.

   This action does not obey any --install-data flag that the user specifies.
   Distutils apparently does not provide a way to tell whether the install data
   directory on the Distribution class is "standard" or has already been
   overrwritten.  This means that we don't have a way to say "change this only
   if it hasn't been changed already".  All we can do is override the location
   all of the time.

   Note: If you want your files to go in the "pychecker" package directory,
   make sure that you specify "pychecker" as the prefix in the setup target.
   """
   def finalize_options(self):
      self.set_undefined_options('install', ('install_lib', 'install_dir'))
      install_data.finalize_options(self) # invoke "standard" action

class my_install_scripts(install_scripts):
   """
   Customized install_scripts distutils action.

   This customized action fills in the pychecker script (either Windows or
   shell style, depending on platform) using the proper path to the checker.py
   file and Python interpreter.  This all is done through the execute() method,
   so that the action obeys the --dry-run flag, etc.  

   The pychecker script must be built here, in the install action, because the
   proper path to checker.py cannot be known before this point.

   If this action is invoked directly from the command-line, it won't have
   access to certain configuration, such as the list of scripts.  Instead of
   trying to work around all that (it's a pain), I've just disallowed the
   action if it doesn't have the information it needs.

   We have some special behavior around binary distributions.  If we're
   building a binary distibution, then we don't try to figure out where
   checker.py will be installed "for real".  We just assume it will be put in
   the standard site-packages location.
   """
   def run(self):
      global using_bdist
      if not using_bdist:
         install_lib = self.distribution.get_command_obj("install").install_lib
      else:
         install_lib = get_site_packages_path()
      scripts = self.distribution.get_command_obj("build_scripts").scripts
      if install_lib is None or scripts is None or self.build_dir is None:
         print "note: install_scripts can only be invoked by install"
      else:
         script_path = get_script_path(self.build_dir)
         if script_path in scripts:
            package_path = os.path.join(install_lib, "pychecker")
            # if a staging root is used, we don't want that path to end up
            # in the install_lib path
            root = self.distribution.get_command_obj("install").root
            if root:
               package_path = package_path[len(root):]
            self.execute(func=create_script, 
                         args=[script_path, package_path], 
                         msg="filling in script %s" % script_path)
         install_scripts.run(self) # invoke "standard" action

class my_build_scripts(build_scripts):
   """
   Customized build_scripts distutils action.

   This action looks through the configured scripts list, and if "pychecker" is
   in the list, replaces that entry with the real name of the script to be
   created within the build directory (including the .bat extension if needed).
   Then, it creates an empty script file at that path to make distutils happy.
   This is all done through the execute() method, so that the action obeys the
   --dry-run flag, etc.

   It might be surprising that we don't fill in the pychecker script here.
   This is because in order to fill in the script, we need to know the
   installed location of checker.py.  This information isn't available until
   the install action is executed.

   This action completely ignores any scripts other than "pychecker" which are
   listed in the setup configuration, and it only does anything if "pychecker"
   is listed in the first place.  This way, new scripts with constant contents
   (if any) can be added to the setup configuration without writing any new
   code.  It also helps OS package maintainers (like for the Debian package)
   because they can still avoid installing scripts just by commenting out the
   scripts line in configuration, just like usual.
   """
   def run(self):
      if self.scripts is not None and "pychecker" in self.scripts:
         script_path = get_script_path(self.build_dir)
         self.scripts.remove("pychecker")
         self.scripts.append(script_path)
         self.mkpath(self.build_dir)
         self.execute(func=open, args=[script_path, "w"], msg="creating empty script %s" % script_path)
      build_scripts.run(self) # invoke "standard" action

def get_script_path(build_dir):
   """
   Returns the platform-specific path to the pychecker script within the build dir.
   """
   if sys.platform == "win32":
      return os.path.join(build_dir, "pychecker.bat")
   else:
      return os.path.join(build_dir, "pychecker")

def get_site_packages_path():
   """
   Returns the platform-specific location of the site-packages directory.
   This directory is usually something like /usr/share/python2.3/site-packages
   on UNIX platforms and /Python23/Lib/site-packages on Windows platforms.
   """
   if sys.platform.lower().startswith('win'):
      return os.path.join(sys.prefix, "Lib", "site-packages")
   else:
      return os.path.join(sys.prefix, "lib", 
                          "python%s" % sys.version[:3], 
                          "site-packages")

def create_script(script_path, package_path):
   """
   Creates the pychecker script at the indicated path.

   The pychecker script will be created to point to checker.py in the package
   directory, using the Python executable specified in sys.executable.  If the
   platform is Windows, a batch-style script will be created.  Otherwise, a
   Bourne-shell script will be created.  Note that we don't worry about what
   permissions mode the created file will have because the distutils install
   process takes care of that for us.

   @param script_path: Path to the script to be created
   @param package_path: Path to the package that checker.py can be found within

   @raise Exception: If script cannot be created on disk.
   """
   try:
      checker_path = os.path.join(package_path, "checker.py")
      if sys.platform == "win32":
         script_str = "%s %s %%*\n" % (sys.executable, checker_path)
      else:
         script_str = '#! /bin/sh\n\n%s %s "$@"\n' % (sys.executable, checker_path)
      open(script_path, "w").write(script_str)
   except Exception, e:
      print "ERROR: Unable to create %s: %s" % (script_path, e)
      raise e


######################
# Setup configuration
######################

CUSTOMIZED_ACTIONS = { 'build_scripts'  : my_build_scripts, 
                       'install_scripts': my_install_scripts, 
                       'install_data'   : my_install_data,
                       'bdist'          : my_bdist,
                       'bdist_dumb'     : my_bdist_dumb,
                       'bdist_rpm'      : my_bdist_rpm,
                       'bdist_wininst'  : my_bdist_wininst,
                     }

DATA_FILES = [ 'COPYRIGHT', 'README', 'VERSION', 'ChangeLog', 'NEWS', 
                'KNOWN_BUGS', 'MAINTAINERS', 'TODO', 
             ]

LONG_DESCRIPTION = """
PyChecker is a tool for finding bugs in python source code.  It finds problems
that are typically caught by a compiler for less dynamic languages, like C and
C++. Because of the dynamic nature of Python, some warnings may be incorrect;
however, spurious warnings should be fairly infrequent.
"""

kw =  { 'name'             : "PyChecker",
        'version'          : "0.8.18",
        'license'          : "BSD-like",
        'description'      : "Python source code checking tool",
        'author'           : "Neal Norwitz",
        'author_email'     : "nnorwitz@gmail.com",
        'url'              : "http://pychecker.sourceforge.net/",
        'packages'         : [ 'pychecker', ],
        'scripts'          : [ "pychecker" ],   # note: will be replaced by customized action
        'data_files'       : [ ( "pychecker", DATA_FILES, ) ], 
        'long_description' : LONG_DESCRIPTION,
        'cmdclass'         : CUSTOMIZED_ACTIONS, 
      }

if hasattr(core, 'setup_keywords') and 'classifiers' in core.setup_keywords:
   kw['classifiers'] =  [ 'Development Status :: 4 - Beta',
                          'Environment :: Console',
                          'Intended Audience :: Developers',
                          'License :: OSI Approved :: BSD License',
                          'Operating System :: OS Independent',
                          'Programming Language :: Python',
                          'Topic :: Software Development :: Debuggers',
                          'Topic :: Software Development :: Quality Assurance',
                          'Topic :: Software Development :: Testing',
                        ]

if __name__ == '__main__' :
   core.setup(**kw)
