#!/usr/bin/env python

# The MIT License
#
# Copyright (C) 2007  Chris Miles
#
# Copyright (C) 2009  Floris Bruynooghe
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


"""process_details.py

Display information about a running process.
"""


__author__ = 'Chris Miles'
__copyright__ = """\
Copyright (C) 2007  Chris Miles
Copyright (C) 2009  Floris Bruynooghe
"""
__version__ = '0.2'


import grp
import optparse
import os
import pwd
import sys
from datetime import datetime

import psi.arch
import psi.process


help_message = 'Display information about a running process'


def run(pid):
    a = psi.arch.arch_type()
    print "Python %s / PSI %s / %s %s" % (
        '.'.join([str(v) for v in sys.version_info[:3]]),
        psi.__version__,
        a.sysname,
        a.release
    )
    
    try:
        p = psi.process.Process(pid)
    except ValueError, e:
        sys.stderr.write("%s\n"%e)
        return 1
    
    print "\nProcess information:"
    fmt = "  %-20s: %s"
    print fmt %('Process ID', p.pid)
    print fmt %('Parent Process ID', p.ppid)
    print fmt %('Executable', getattr(p, 'exe', 'N/A'))
    print fmt %('Command', p.command)

    try:
        v = ' '.join(p.args)
    except psi.AttrInsufficientPrivsError, e:
        v = str(e)
    print fmt %('Full Command', v)

    print fmt %('Effective user', '%s (%d)'
                % (pwd.getpwuid(p.euid).pw_name, p.euid))
    print fmt %('Real user', '%s (%d)'
                % (pwd.getpwuid(p.ruid).pw_name, p.ruid))
    print fmt %('Effective group', '%s (%d)'
                %(grp.getgrgid(p.egid).gr_name, p.egid))
    print fmt %('Real group', '%s (%d)'
                % (grp.getgrgid(p.rgid).gr_name, p.rgid))
    print fmt %('Process group', p.pgrp)
    print fmt %('Number of threads', getattr(p, 'nthreads', 'N/A'))
    print fmt %('Started', str(p.start_time.datetime()))
    
    try:
        status = p.status
    except AttributeError, e:
        value = str(e)
    else:
        all_status = [(s, getattr(psi.process, s)) for s in dir(psi.process)
                      if s.startswith('PROC_STATUS_')]
        proc_status = [s[0] for s in all_status if status == s[1]]
        value = ', '.join(proc_status)
    print fmt %('Status', value)

    print fmt %('Terminal device', getattr(p, 'terminal', 'N/A'))
    print fmt %('Environment', getattr(p, 'env', 'N/A'))

    print "\nCPU Use:"
    print fmt %('CPU seconds used', str(p.cputime.timedelta()))
    try:
        print ' '*24 + '(User: %s   System: %s)' % (p.utime.timedelta(),
                                                    p.stime.timedelta())
    except AttributeError:
        pass
    print fmt %('Recent CPU used', getattr(p, 'pcpu', 'N/A'))
    print fmt %('Nice level', getattr(p, 'nice', 'N/A'))
    print fmt %('Priority', getattr(p, 'priority', 'N/A'))
    
    print "\nMemory Use:"
    print fmt %('Resident memory used', p.rss)
    print fmt %('Virtual memory used', p.vsz)


def main(argv=None):
    if argv is None:
        argv = sys.argv
    
    # define usage and version messages
    usageMsg = "usage: %s [options] pid" % sys.argv[0]
    versionMsg = """%s %s""" % (os.path.basename(argv[0]), __version__)
    
    # get a parser object and define our options
    parser = optparse.OptionParser(usage=usageMsg, version=versionMsg)
    
    parser.add_option('-S', '--self', dest='self',
            action='store_true', default=False,
            help="Show information about itself")
    
    # Parse options & arguments
    (options, args) = parser.parse_args()
    
    if len(args) < 1 and not options.self:
        parser.error("A process ID must be specified.")
    if len(args) > 1:
        parser.error("Only one process ID can be specified.")
    
    if options.self:
        # introspect itself
        pid = os.getpid()
    else:
        try:
            pid = int(args[0])
        except:
            parser.error("Invalid process ID.")
    
    return run(pid)


if __name__ == "__main__":
    sys.exit(main())
