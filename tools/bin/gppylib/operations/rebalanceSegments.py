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
from gppylib.gparray import GpArray
from gppylib.db import dbconn
from gppylib.commands.gp import GpSegStopCmd, GpRecoverseg
from gppylib.commands.base import WorkerPool, SQLCommand, REMOTE
from gppylib import gplog
import signal

logger = gplog.get_default_logger()


class ReconfigDetectionSQLQueryCommand(SQLCommand):
    """A distributed query that will cause the system to detect
    the reconfiguration of the system"""
    
    query = "SELECT * FROM gp_dist_random('gp_id')"
    
    def __init__(self, conn):
        SQLCommand.__init__(self, "Reconfig detection sql query")
        self.cancel_conn = conn

    def run(self):
        dbconn.execSQL(self.cancel_conn, self.query)


class GpSegmentRebalanceOperation:
    def __init__(self, gpEnv, gpArray):
        self.gpEnv = gpEnv
        self.gpArray = gpArray
    
    def rebalance(self):
        # Get the unbalanced primary segments grouped by hostname
        # These segments are what we will shutdown.
        logger.info("Getting unbalanced segments")
        unbalanced_primary_segs = GpArray.getSegmentsByHostName(self.gpArray.get_unbalanced_primary_segdbs())
        pool = WorkerPool()
        
        count = 0

        try:        
            # Disable ctrl-c
            signal.signal(signal.SIGINT,signal.SIG_IGN)
            
            logger.info("Stopping unbalanced primary segments...")
            for hostname in unbalanced_primary_segs.keys():
                cmd = GpSegStopCmd("stop unbalanced primary segs",
                                   self.gpEnv.getGpHome(),
                                   self.gpEnv.getGpVersion(),
                                   'fast',
                                   unbalanced_primary_segs[hostname],
                                   ctxt=REMOTE,
                                   remoteHost=hostname,
                                   timeout=600)
                pool.addCommand(cmd)
                count+=1
                
            pool.wait_and_printdots(count, False)
            
            failed_count = 0
            completed = pool.getCompletedItems()
            for res in completed:
                if not res.get_results().wasSuccessful():
                    failed_count+=1
                    
            if failed_count > 0:
                logger.warn("%d segments failed to stop.  A full rebalance of the")
                logger.warn("system is not possible at this time.  Please check the")
                logger.warn("log files, correct the problem, and run gprecoverseg -r")
                logger.warn("again.")
                logger.info("gprecoverseg will continue with a partial rebalance.")
            
            pool.empty_completed_items()
            # issue a distributed query to make sure we pick up the fault
            # that we just caused by shutting down segments
            conn = None
            try:
                logger.info("Triggering segment reconfiguration")
                dburl = dbconn.DbURL()
                conn = dbconn.connect(dburl)
                cmd = ReconfigDetectionSQLQueryCommand(conn)
                pool.addCommand(cmd)
                pool.wait_and_printdots(1, False)
            except Exception:
                # This exception is expected
                pass
            finally:
                if conn:
                    conn.close()

            # Final step is to issue a recoverseg operation to resync segments
            logger.info("Starting segment synchronization")
            cmd = GpRecoverseg("rebalance recoverseg")
            pool.addCommand(cmd)
            pool.wait_and_printdots(1, False)
        except Exception, ex:
            raise ex
        finally:
            signal.signal(signal.SIGINT,signal.default_int_handler)
        
        # check that recoverseg was successful
        completed = pool.getCompletedItems()
        if not completed[0].get_results().wasSuccessful():
            logger.error("Failed to start the synchronization step of the segment rebalance.")
            logger.error("Check the gprecoverseg log file, correct any problems, and re-run")
            logger.error("'gprecoverseg -a'.")        
            logger.error(completed[0].get_results())
            raise Exception("Error synchronizing.")
            
        
        
