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
import sys

from gppylib.gplog import *
from gppylib.system.configurationInterface import *
from gppylib.system import configurationImplTest, fileSystemImplTest, fileSystemInterface, osInterface, osImplTest, \
        faultProberInterface, faultProberImplTest
from gppylib.gparray import GpDB, FAULT_STRATEGY_NONE, FAULT_STRATEGY_FILE_REPLICATION

logger = get_default_logger()

class TestDriver:

    def __init__(self):
        self.__configurationProvider = None
        self.__fileSystemProvider = None
        pass

    #
    #
    # To get segmentData you can run this query against a 3.4 database:
    #
    # SELECT dbid, content, role, preferred_role, mode, status, hostname, address, port,
    #                         fselocation AS datadir, replication_port
    #                     FROM pg_catalog.gp_segment_configuration
    #                         JOIN pg_catalog.pg_filespace_entry ON (dbid = fsedbid)
    #                         JOIN pg_catalog.pg_filespace fs ON (fsefsoid = fs.oid AND fsname='pg_system')
    #                    ORDER BY content, preferred_role DESC
    #
    # Include the header in what you paste!
    #
    #
    def setSegments(self, segmentData, faultStrategy):
        lines = segmentData.strip().split("\n")
        
        assert len(lines[1].split("+")) == len(lines[0].split("|")) # verify header is listed

        self.__configurationProvider = configurationImplTest.GpConfigurationProviderForTesting()
        self.__configurationProvider.setFaultStrategy(faultStrategy)
        for line in lines[2:len(lines)]:
            row = [s.strip() for s in line.strip().split("|")]


            dbId = int(row[0])
            contentId = int(row[1])
            role = row[2]
            preferredRole = row[3]
            mode = row[4]
            status = row[5]
            hostName = row[6]
            address = row[7]
            port = int(row[8])
            dataDirectory = row[9]
            replicationPort = None if row[10] == "" else int(row[10])

            segment = GpDB(content=contentId,
                           preferred_role=preferredRole,
                           dbid=dbId,
                           role=role,
                           mode=mode,
                           status=status,
                           hostname=hostName,
                           address=address,
                           port=port,
                           datadir=dataDirectory,
                           replicationPort=replicationPort)

            self.__configurationProvider.addTestSegment(segment)

        registerConfigurationProvider( self.__configurationProvider )

        self.__fileSystemProvider = fileSystemImplTest.GpFileSystemProviderForTest()
        fileSystemInterface.registerFileSystemProvider(self.__fileSystemProvider)
        osInterface.registerOsProvider(osImplTest.GpOsProviderForTest())
        faultProberInterface.registerFaultProber(faultProberImplTest.GpFaultProberImplForTest())
        pass

    def getFileSystem(self):
        return self.__fileSystemProvider

    def getConfiguration(self):
        return self.__configurationProvider

        
    def initOneHostConfiguration(self):
        configStr = \
            """
             dbid | content | role | preferred_role | mode | status |      hostname      |      address       | port  |                     datadir                     | replication_port
            ------+---------+------+----------------+------+--------+--------------------+--------------------+-------+-------------------------------------------------+------------------
                1 |      -1 | p    | p              | s    | u      | this-is-my-host | this-is-my-host |  5432 |/datadirpathdbmaster/gp-1 |
                2 |       0 | p    | p              | s    | u      | this-is-my-host | this-is-my-host | 50001 |/datadirpathdbfast1/gp0   |            55001
                4 |       0 | m    | m              | s    | u      | this-is-my-host | this-is-my-host | 60001 |/datadirpathdbfast3/gp0   |            65001
                3 |       1 | p    | p              | s    | u      | this-is-my-host | this-is-my-host | 50002 |/datadirpathdbfast2/gp1   |            55002
                5 |       1 | m    | m              | s    | u      | this-is-my-host | this-is-my-host | 60002 |/datadirpathdbfast4/gp1   |            65002
                
        """

        self.setSegments(configStr, FAULT_STRATEGY_FILE_REPLICATION)
        return self

    def initTwoSegmentOneFailedMirrorConfiguration(self):
        configStr = \
            """
             dbid | content | role | preferred_role | mode | status |      hostname      |      address       | port  |                     datadir                     | replication_port
            ------+---------+------+----------------+------+--------+--------------------+--------------------+-------+-------------------------------------------------+------------------
                1 |      -1 | p    | p              | s    | u      | master-host | primary-host |  5432 |/datadirpathdbmaster/gp-1 |
                2 |       0 | p    | p              | s    | u      | first-host | first-host | 50001 |/datadirpathdbfast1/gp0   |            55001
                7 |       0 | m    | m              | s    | u      | second-host | second-host | 40001 |/second/datadirpathdbfast3/gp0   |            45001
                3 |       1 | p    | p              | s    | u      | first-host | first-host | 50002 |/datadirpathdbfast2/gp1   |            55002
                9 |       1 | m    | m              | s    | u      | second-host | second-host | 40002 |/second/datadirpathdbfast4/gp1   |            45002
                4 |       2 | m    | m              | s    | u      | first-host | first-host | 60001 |/datadirpathdbfast3/gp0   |            65001
                6 |       2 | p    | p              | s    | u      | second-host | second-host | 30001 |/second/datadirpathdbfast1/gp0   |            35001
                5 |       3 | m    | m              | c    | d      | first-host | first-host | 60002 |/datadirpathdbfast4/gp1   |            65002
                8 |       3 | p    | p              | c    | u      | second-host | second-host | 30002 |/second/datadirpathdbfast2/gp1   |            35002

        """

        self.setSegments(configStr, FAULT_STRATEGY_FILE_REPLICATION)
        return self

    def initThreeHostMultiHomeNoMirrors(self):
        configStr = \
            """
             dbid | content | role | preferred_role | mode | status |      hostname      |      address       | port  |                     datadir                     | replication_port
            ------+---------+------+----------------+------+--------+--------------------+--------------------+-------+-------------------------------------------------+------------------
                1 |      -1 | p    | p              | s    | u      | master-host        | primary-host       |  5432 |/datadirpathdbmaster/gp-1                        |
                2 |       0 | p    | p              | s    | u      | first-host         | first-host-1       | 50001 |/first/datadirpathdbfast1/gp0                    |
                3 |       1 | p    | p              | s    | u      | first-host         | first-host-2       | 50002 |/first/datadirpathdbfast2/gp1                    |
                4 |       2 | p    | p              | s    | u      | second-host        | second-host-1      | 50001 |/second/datadirpathdbfast1/gp2                   |
                5 |       3 | p    | p              | s    | u      | second-host        | second-host-2      | 50002 |/second/datadirpathdbfast2/gp3                   |
                6 |       4 | p    | p              | s    | u      | third-host         | third-host-1       | 50001 |/third/datadirpathdbfast2/gp4                    |
                7 |       5 | p    | p              | s    | u      | third-host         | third-host-2       | 50002 |/third/datadirpathdbfast2/gp5                    |

        """

        self.setSegments(configStr, FAULT_STRATEGY_NONE)
        return self




