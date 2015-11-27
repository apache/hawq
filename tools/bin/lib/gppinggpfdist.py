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

import sys, httplib, getopt, socket

def usage(exitarg):
    print 'usage: %s [-q] host:port' % sys.argv[0]
    print 
    print '   -q: quiet mode'
    print
    print 'e.g. %s localhost:8080' % sys.argv[0]
    print
    sys.exit(exitarg)

gpfdist = ''
quiet = 0
uri = ''

try:
    (options, args) = getopt.getopt(sys.argv[1:], 'q')
except Exception, e:
    usage('Error: ' + str(e))

for (switch, val) in options:
    if (switch == '-q'):  quiet = 1

if len(args) != 1:
    usage('Error: please specify uri.')

host_port = args[0]

try:
    conn = httplib.HTTPConnection(host_port)
    conn.request('GET', '/')
    r = conn.getresponse()
    gpfdist = r.getheader('X-GPFDIST-VERSION', '')
except socket.error:
    if not quiet:
	print 'Error: gpfdist is not running (reason: socket error)'
	print 'Exit: 1'
    sys.exit(1)
     
if not gpfdist:
    if not quiet:
	print 'Error: gpfdist port is taken by some other programs'
	print 'Exit: 2'
    sys.exit(2)

if not quiet:
    print 'Okay, gpfdist version "%s" is running on %s.' % (gpfdist, host_port)
sys.exit(0)
