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

# Line too long - pylint: disable=C0301
# Invalid name  - pylint: disable=C0103

"""
  gpstandbywatch.py
  Copyright (c) EMC/Greenplum Inc 2011. All Rights Reserved.

  Check actual contents and process state of syncmaster
  in order to properly return accurate information back to
  gpinitstandby via gpstart.
"""

import os
import sys
import glob
import time

from gppylib.gplog    import setup_tool_logging, get_default_logger
from gppylib.commands import gp, unix


def matching_files(pglogdir, ignore=None, setlimit=False):
    """
    Generate a series of file names corresponding to files
    in 'pglogdir' which are not in the specified 'ignore' map.

    Note that startup.log is always returned if present.

    If 'setlimit' is not false, files whose modification time
    exceeds the ctime of startup.log will also be ignored.
    """
    mlimit = None
    pattern = os.path.join(pglogdir, 'startup.log')
    for path in glob.glob(pattern):
        if setlimit:
            mlimit = os.stat(path).st_ctime
        yield path

    home_directory = os.path.expanduser("~")
    pattern = os.path.join('%s/hawqAdminLogs/' % home_directory, 'startup.log')
    for path in glob.glob(pattern):
        if setlimit:
            mlimit = os.stat(path).st_ctime
        yield path

    pattern = os.path.join(pglogdir, '*.csv')
    for path in glob.glob(pattern):
        if ignore is not None and path in ignore:
            continue
        if mlimit is not None and os.stat(path).st_mtime >= mlimit:
            continue
        yield path


def updated_files(pglogdir, ignore, prev):
    """
    Generate a series of (time, path) tuples corresponding to files
    in 'pglogdir' and not in 'ignore' which were also modified
    after 'prev' (or all files if 'prev' is None).
    """
    for path in matching_files(pglogdir, ignore):
        ts = os.stat(path).st_mtime
        if prev is None or prev < ts:
            yield (ts, path)


def updated_handles(pglogdir, ignore, prev, handles):
    """
    Generate a series of (time, handle) tuples corresponding to files
    in 'pglogdir' and not in 'ignore' modified after 'prev'
    (or all files if 'prev' is None).
    """
    for ts, path in updated_files(pglogdir, ignore, prev):
        h = handles.get(path, None)
        if h is None:
            h = open(path, 'r')
            handles[path] = h
        yield (ts, h)



class SyncmasterWatcher:
    """
    Watch changes to files in the pg_log directory recorded by the gpsyncmaster.
    """

    def __init__(self, datadir):
        """
        Build a map containing the existing contents of the pg_log
        directory so that we can avoid getting confused by them
        after we start the syncmaster.
        """
        self.datadir         = datadir
        self.pglogdir        = os.path.join(self.datadir, 'pg_log')

        # note use of setlimit=True here to prevent any files created
        # after startup.log from becoming ignored.
        self.ignore          = {}
        for path in matching_files( self.pglogdir, setlimit=True ):
            self.ignore[path] = True

        self.handles         = {}
        self.maxlines        = 1000
        self.timelimit       = 3
        self.delay           = 0.1


    def tail_briefly(self):
        """
        Generate lines recently added to log files in the pg_log directory
        updated after our __init__ constructor was called.
        """
        start   = time.time()                       # starting time
        elapsed = 0                                 # time elapsed so far
        count   = 0                                 # number of lines we've seen
        tp      = None

        # until we're out of time or have returned enough lines
        while elapsed < self.timelimit and count < self.maxlines:

            # for each file modified since we last checked
            tn = None
            for ts, h in updated_handles(self.pglogdir, self.ignore, tp, self.handles):

                # track the last file modification time
                if tn is None or tn < ts:
                    tn = ts

                # yield the new lines to the caller
                while count < self.maxlines:
                    line = h.readline()
                    if not line:
                        break
                    yield line
                    count += 1

            # update the elapsed time
            elapsed = time.time() - start

            # if any new lines, update prev and keep checking for more
            if tn is not None:
                tp = tn
                continue

            # if we get here it means none of the files were updated in
            # our last iteration. sleep a moment before checking for
            # more updates
            time.sleep(self.delay)



    def monitor_logs(self):
        """
        Read the syncmaster log files for a few seconds, looking for
        potential problems.

        Returns 0 if no problems were seen or or 1 if the startup log
        contained an error or if the gpsyncmaster process exited before
        we were done watching.
        """
        logger.info("Monitoring logs")

        # now scan some of the syncmaster output for a moment
        for line in self.tail_briefly():

            if line.startswith('Traceback'):        # gpsyncmaster traceback recorded
                logger.warning(line)
                return 1

            # MPP-13212 - since the syncmaster reports rejected client connections
            #   as 'FATAL' errors, the presence of a 'FATAL' error need not indicate
            #   a problem in the syncmaster so we comment out the following logic:
            #
            # if line.find('FATAL') >= 0:             # fatal error recorded
            #     logger.warning(line)
            #     return 1
            #
            # This is especially important for health monitoring clients which may
            # rely on the difference between a rejected connection and a TCP failure.

            if line.find('could not bind IPv4 socket') >= 0: # syncmaster used IPv6 by mistake
                logger.warning(line)
                return 1

            if line.find('QDSYNC: scan forward') >= 0: # syncmaster appears to be working
                logger.info(line)
                break

        logger.info("checking if syncmaster is running")
        count = 0
        counter = 20
        while True:
            pid = gp.getSyncmasterPID('localhost', self.datadir)
            if not pid > 0:
                if count >= counter:
                    logger.error("Standby master start timeout")
                    return 1
                else:
                    logger.warning("syncmaster not running, waiting...")
            else:
                break
            count += 1
            time.sleep(3)

        # syncmaster is running and there are no obvious errors in the log
        logger.info("syncmaster appears ok, pid %s" % pid)
        return 0


    def close(self):
        """
        Closes all handles to the logs we're watching.
        """
        for h in self.handles.values():
            h.close()
        self.handles = {}



if __name__ == '__main__':

    # setup gpAdminLogs logging
    execname = os.path.split(sys.argv[0])[-1]
    hostname = unix.getLocalHostname()
    username = unix.getUserName()
    setup_tool_logging(execname, hostname, username)
    logger = get_default_logger()

    # watch syncmaster logs
    if len(sys.argv) > 2 and sys.argv[2] == 'debug':
        logger.info("Checking standby master status")
    watcher = SyncmasterWatcher( sys.argv[1] )
    rc = watcher.monitor_logs()
    watcher.close()

    # report final status
    # logger.info("exiting with %s" % rc)
    sys.exit( rc )
