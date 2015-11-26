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
gpgetconfig -- obtain gp_configuration

Usage: gpgetconfig [-f] [-u user] -d master_data_directory

	-f : if necessary, force start up and shutdown of DB to obtain configuration

Exit: 0 - no error
      1 - misc error
      2 - unable to connect to database

'''

import os, sys
os.putenv('PGHOST', '')
os.putenv("PGOPTIONS", '-c gp_session_role=utility')
os.putenv('PGDATABASE', 'template1')

class __globals__:
    opt = {}
    for o in 'ud': opt['-'+o] = ''
    for o in 'f': opt['-'+o] = False

GV = __globals__()


############
def usage(exitarg):
    print __doc__
    sys.exit(exitarg)


############
def parseCommandLine():
    import getopt
    try:
	(options, args) = getopt.getopt(sys.argv[1:], '?fu:d:')
    except Exception, e:
	sys.stderr.write('Error: %s\n' % str(e))
	usage(1)

    for (switch, val) in options:
	if switch == '-?': usage(0)
	elif switch[1] in 'ud': GV.opt[switch] = val
	elif switch[1] in 'f': GV.opt[switch] = True

    if not GV.opt['-d']:
	usage('Error: missing -d param')


############
def setPort():
    port = 0
    f = None
    try:
	f = open(os.path.join(GV.opt['-d'], 'postgresql.conf'))
        lines = f.readlines()
        lines = map(lambda x: x.strip().split('='), lines)
        lines = filter(lambda x: len(x) and x[0] == 'port', lines)
	port = int( (lines[0][1].split()) [0])
    except Exception, e: 
	pass
    finally: 
	if f: f.close()

    if port == 0:
	sys.stderr.write('Error: unable to read port number from %s/postgresql.conf' % 
			GV.opt['-d'])
	sys.exit(1)

    os.putenv('PGPORT', str(port))


############
def getConfiguration():
    CMD = """psql -At -q -c "select content, preferred_role='p' as definedprimary, dbid, role = 'p' as isprimary, 't' as valid, hostname, port, fse\
location as datadir from gp_segment_configuration join pg_filespace_entry on (dbid = fsedbid) where fsefsoid = 3052" 2> /dev/null"""
    p = os.popen(CMD)
    out = p.readlines()
    rc = p.close()
    return (rc, out)
    

############
def main():
    parseCommandLine()
    if GV.opt['-u']:
        os.putenv('PGUSER', GV.opt['-u'])
    os.putenv('MASTER_DATA_DIRECTORY', GV.opt['-d'])
    setPort()
    
    (rc, out) = getConfiguration()
    if rc:
	if not GV.opt['-f']: 
	    sys.stderr.write('Error: psql unable to connect\n')
	    sys.exit(2)

        os.putenv('GPSTART_INTERNAL_MASTER_ONLY', '1')
	p = os.popen("gpstart -m")
	p.readlines()
	p.close()
	(rc, out) = getConfiguration()
	p = os.popen("gpstop -m")
	p.readlines()
	p.close()

        if rc:
	    sys.stderr.write('Error: psql still unable to connect after bouncing\n')
	    sys.exit(1)

    out = filter(lambda x: x, map(lambda x: x.strip(), out))
    for line in out:
	print '[gpgetconfig]',line
    

if __name__ == '__main__':
    main()
