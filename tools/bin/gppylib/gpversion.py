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

"""
  gpversion.py:

  Contains GpVersion class for handling version comparisons.
"""

# ===========================================================
import sys, os, re

# Python version 2.6.2 is expected, must be between 2.5-3.0
if sys.version_info < (2, 5, 0) or sys.version_info >= (3, 0, 0):
    sys.stderr.write("Error: %s is supported on Python versions 2.5 or greater\n" 
                     "Please upgrade python installed on this machine." 
                     % os.path.split(__file__)[-1])
    sys.exit(1)


MAIN_VERSION = [2,0,99,99]    # version number for main


#============================================================
class GpVersion:
    '''
    The gpversion class is an abstraction of a given Greenplum release 
    version.  It exists in order to facilitate version comparisons,
    formating, printing, etc.
    
      x = GpVersion([3,2,0,4]) => Greenplum 3.2.0.4
      x = GpVersion('3.2')     => Greenplum 3.2 dev
      x = GpVersion('3.2 build dev') => Greenplum 3.2 dev
      x = GpVersion('main')    => Greenplum main 
      x.major()                => Major release, eg "3.2"
      x.isrelease('3.2')       => Major version comparison
    '''

    #------------------------------------------------------------
    def __init__(self, version):
        '''
        The initializer for GpVersion is complicated so that it can handle
        creation via several methods:
           x = GpVersion('3.2 dev')  => via string
           x = GpVersion([3,2])  => via tuple
           y = GpVersion(x)      => copy constructor

        If the input datatype is not recognised then it is first cast to
        a string and then converted.
        '''
        try:
            self.version = None
            self.build   = None

            # Local copy that we can safely manipulate
            v = version

            # Copy constructor
            if isinstance(v, GpVersion):
                self.version = v.version
                self.build   = v.build
                return

            # if version isn't a type we recognise then convert to a string
            # first
            if not (isinstance(v, str) or
                    isinstance(v, list) or
                    isinstance(v, tuple)):
                v = str(v)
            
            # Convert a string into the version components.
            # 
            # There are several version formats that we anticipate receiving:
            #
            # Versions from "postgres --gp-version":
            #    ".* (Greenplum Database) <VERSION> build <BUILD>"
            #
            # Version from sql "select version()"
            #    ".* (Greenplum Database <VERSION> build <BUILD>) .*"
            #
            # Versions from python code:
            #    "<VERSION>"
            #    "<VERSION> <BUILD>"
            #
            if isinstance(v, str):

                # See if it matches one of the two the long formats
                regex = r"\(HAWQ\)? ([^ ]+) build ([^ )]+)"
                m = re.search(regex, v)
                if m:
                    (v, self.build) = m.groups()   # (version, build)
                
                # Remove any surplus whitespace, if present
                v = v.strip()

                # We should have either "<VERSION> <BUILD>" or "VERSION" so 
                # split on whitespace.
                vlist = v.split(' ')
                if len(vlist) == 2:
                    (v, self.build) = vlist
                elif len(vlist) == 3 and vlist[1] == 'build':
                    (v, _, self.build) = vlist
                elif len(vlist) > 2:
                    raise StandardError("too many tokens in version")
                
                # We should now just have "<VERSION>"
                if v == 'main':
                    self.version = MAIN_VERSION
                    if not self.build:
                        self.build = 'dev'
                    return
                
                # Check if version contains any "special build" tokens
                # e.g. "3.4.0.0_EAP1" or "3.4.filerep".  
                #
                # For special builds we use the special value for <BUILD>
                # rather than any value calculated above.
                
                # <VERSION> consists of:
                #    2 digits for major version
                #    optionally another 2 digits for minor version
                #    optionally a string specifiying a "special build", eg:
                #        
                #        we ignore the usual build version and use the special
                #        vilue for "<BUILD>" instead.
                regex = r"[0123456789.]+\d"
                m = re.search(regex, v)
                if not m:
                    raise StandardError("unable to coerce to version")
                if m.end() < len(v):
                    self.build = v[m.end()+1:]
                v = v[m.start():m.end()]
                
                # version is now just the digits, split on '.' and fall
                # into the default handling of a list argument.
                v = v.split('.')

            # Convert a tuple to a list so that extend and slicing will work 
            # nicely
            if isinstance(v, tuple):
                v = list(v)

            # Any input we received should have been 
            if not isinstance(v, list):
                raise StandardError("Internal coding error")

            # Convert a list into a real version
            if len(v) < 2:
                raise StandardError("Version too short")
            if len(v) > 4:
                raise StandardError("Version too long")
            if len(v) < 4:
                v.extend([99,99])

            v = map(int, v)  # Convert to integers
            self.version = v[:4]  # If we extended beyond 4 cut it back down
            if not self.build:
                self.build = 'dev'


        # If part of the conversion process above failed, throw an error,
        except:
            raise StandardError("Unrecognised Greenplum Version '%s'" % 
                                str(version))

    #------------------------------------------------------------
    def __cmp__(self, other):
        '''
        One of the main reasons for this class is so that we can safely compare
        versions with each other.  This needs to be pairwise integer comparison
        of the tuples, not a string comparison, which is why we maintain the
        internal version as a list.       
        '''
        if isinstance(other, GpVersion):
            return cmp(self.version, other.version)
        else:
            return cmp(self, GpVersion(other))
    
    #------------------------------------------------------------
    def __str__(self):
        ''' 
        The other main reason for this class is that the display version is
        not the same as the internal version for main and development releases.
        '''
        if self.version == MAIN_VERSION:
            v = 'main'
        elif self.version[2] == 99 and self.version[3] == 99: 
            v = '.'.join(map(str,self.version[:2]))
        else:
            v = '.'.join(map(str,self.version[:4]))

        if self.build:
            return "%s build %s" % (v, self.build)
        else:
            return v

    #------------------------------------------------------------
    def getVersionBuild(self):
        '''
        Returns the build number portion of the version.
        '''
        return self.build

    #------------------------------------------------------------
    def getVersionRelease(self):
        '''
        Returns the major (first 2 values) portion of the version.
        '''
        return '.'.join(map(str,self.version[:2]))

    #------------------------------------------------------------
    def isVersionRelease(self, version):
        '''
        Returns true if the version matches a particular major release.
        '''
        other = GpVersion(version)
        return self.version[0:2] == other.version[0:2]

    #------------------------------------------------------------
    def isVersionCurrentRelease(self):
        '''
        Returns true if the version matches the current MAIN_VERSION
        '''
        return self.isVersionRelease(MAIN_VERSION)

    
