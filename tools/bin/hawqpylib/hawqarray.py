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
  hawqarray.py:

    Contains three classes representing configuration information of a
    Greenplum array:

      HAWQArray - The primary interface - collection of all HAWQDB within an array
      HAWQDB    - represents configuration information for a single registration_order
      Segment   - collection of all HAWQDB with the same registration_order
"""

# ============================================================================
from datetime import date
import copy
import traceback

from gppylib.utils import checkNotNone, checkIsInt
from gppylib    import gplog
from gppylib.db import dbconn
from gppylib.gpversion import GpVersion
from gppylib.commands.unix import *
from hawqpylib.hawqlib import HawqXMLParser


SYSTEM_FILESPACE = 3052        # oid of the system filespace

logger = gplog.get_default_logger()

DESTINATION_FILE_SPACES_DIRECTORY = "fs_directory"

ROLE_MASTER  = 'm'
ROLE_STANDBY = 's'
ROLE_PRIMARY = 'p'
VALID_ROLES  = [ROLE_MASTER, ROLE_STANDBY, ROLE_PRIMARY]


STATUS_UP    = 'u'
STATUS_DOWN  = 'd'
VALID_STATUS = [STATUS_UP, STATUS_DOWN]


# SegmentState values returned from gp_primarymirror.
SEGMENT_STATE_NOT_INITIALIZED               = "NotInitialized"
SEGMENT_STATE_INITIALIZATION                = "Initialization"
SEGMENT_STATE_IN_CHANGE_TRACKING_TRANSITION = "InChangeTrackingTransition"
SEGMENT_STATE_IN_RESYNCTRANSITION           = "InResyncTransition"                       
SEGMENT_STATE_IN_SYNC_TRANSITION            = "InSyncTransition"
SEGMENT_STATE_READY                         = "Ready"
SEGMENT_STATE_CHANGE_TRACKING_DISABLED      = "ChangeTrackingDisabled"
SEGMENT_STATE_FAULT                         = "Fault"
SEGMENT_STATE_SHUTDOWN_BACKENDS             = "ShutdownBackends"
SEGMENT_STATE_SHUTDOWN                      = "Shutdown"
SEGMENT_STATE_IMMEDIATE_SHUTDOWN            = "ImmediateShutdown"


MASTER_REGISTRATION_ORDER = 0

class InvalidSegmentConfiguration(Exception):
    """Exception raised when an invalid hawqarray configuration is
    read from gp_segment_configuration or an attempt to save an 
    invalid hawqarray configuration is made."""
    def __init__(self, array):
        self.array = array
        
    def __str__(self):
        return "Invalid HAWQArray: %s" % self.array

# ============================================================================
# ============================================================================
class HAWQDB:
    """
    HAWQDB class representing configuration information for a single
    registration_order within a HAWQ cluster.
    """

    # --------------------------------------------------------------------
    def __init__(self, registration_order, role, status,
                 hostname, address, port, datadir):

        self.registration_order = registration_order
        self.role = role
        self.status = status
        self.hostname = hostname
        self.address = address
        self.port = port
        self.datadir = datadir
        self.catdir = datadir

        # Filespace mappings for a HAWQ DB
        self.filespaces = None

        # Pending filespace creation
        self.pending_filespace = None

        # Check if the status is 'u' up, 'd' for down
        self.valid = (status == 'u')
        
    # --------------------------------------------------------------------
    def __str__(self):
        """
        Construct a printable string representation of a HAWQDB
        """
        return "%s:%s:registration_order=%s:status=%s" % (
            self.hostname,
            self.datadir,
            self.registration_order,
            self.status
            )

    #
    # Note that this is not an ideal comparison -- it uses the string representation
    #   for comparison
    #
    def __cmp__(self,other):
        left = repr(self)
        right = repr(other)
        if left < right: return -1
        elif left > right: return 1
        else: return 0

    def equalIgnoringStatus(self, other):
        """
        Return true if none of the "core" attributes (e.g. filespace) 
          of two segments differ, false otherwise.

        This method is used by updateSystemConfig() to know when a catalog
        change will cause removing and re-adding a mirror segment.
        """
        firstStatus = self.getStatus()
        try:

            # make the elements we don't want to compare match and see if they are then equal
            self.setStatus(other.getStatus())

            return self == other
        finally:
            # restore mode and status after comaprison 
            self.setStatus(firstStatus)

    # --------------------------------------------------------------------
    @staticmethod
    def getDataDirPrefix(datadir):
        retValue = ""
        retValue = datadir[:datadir.rfind('/')]
        return retValue

    # --------------------------------------------------------------------
    @staticmethod
    def getFileSpaceDirsWithNewSuffix(fileSpaceDictionary, suffix, includeSystemFilespace = True):
        """
        This method will take the a dictionary of file spaces and return the same dictionary with the new sufix.
        """
        retValue = {}

        for entry in fileSpaceDictionary:
            if entry == SYSTEM_FILESPACE and includeSystemFilespace == False:
                continue
            newDir = HAWQDB.getDataDirPrefix(fileSpaceDictionary[entry])
            newDir = newDir + "/" + suffix
            retValue[entry] = newDir
        return retValue

    # --------------------------------------------------------------------
    def copy(self):
        """
        Creates a copy of the segment, shallow for everything except the filespaces map

        """
        res = copy.copy(self)
        res.filespaces = copy.copy(self.filespaces)
        return res

    # --------------------------------------------------------------------
    # Six simple helper functions to identify what role a segment plays:
    #  + QD (Query Dispatcher)
    #     + master
    #     + standby master
    #  + QE (Query Executor)
    #     + primary
    # --------------------------------------------------------------------    
    def isMaster(self):
        return self.role == ROLE_MASTER

    def isStandby(self):
        return self.role == ROLE_STANDBY

    def isSegment(self):
        return self.role == ROLE_PRIMARY

    def isUp(self):
        return self.status == STATUS_UP

    def isDown(self):
        return self.status == STATUS_DOWN

    # --------------------------------------------------------------------
    # getters
    # --------------------------------------------------------------------
    def getRegistrationOrder(self):
        return checkNotNone("registration_order", self.registration_order)

    def getRole(self):
        return checkNotNone("role", self.role)

    def getStatus(self):
        return checkNotNone("status", self.status)

    def getPort(self):
        """
        Returns the listening port for the postmaster for this segment.

        Note: With file replication the postmaster will not be active for
        mirrors so nothing will be listening on this port, instead the
        "replicationPort" is used for primary-mirror communication.
        """
        return checkNotNone("port", self.port)

    def getHostName(self):
        """
        Returns the actual `hostname` for the host

        Note: use getSegmentAddress for the network address to use
        """
        return self.hostname

    def getAddress(self):
        """
        Returns the network address to use to contact the segment (i.e. the NIC address).

        """
        return self.address

    def getDataDirectory(self):
        """
        Return the primary datadirectory location for the segment.

        Note: the datadirectory is just one of the filespace locations
        associated with the segment, calling code should be carefull not 
        to assume that this is the only directory location for this segment.

        Todo: evaluate callers of this function to see if they should really
        be dealing with a list of filespaces.
        """
        return checkNotNone("dataDirectory", self.datadir)

    def getFilespaces(self):
        """
        Returns the filespace dictionary of oid->path pairs
        """
        return self.filespaces        


    # --------------------------------------------------------------------
    # setters
    # --------------------------------------------------------------------
    def setRegistrationOrder(self, registration_order):
        checkNotNone("registration_order", registration_order)
        checkIsInt("registration_order", registration_order)
        self.registration_order = registration_order

    def setRole(self, role):
        checkNotNone("role", role)

        if role not in VALID_ROLES:
            raise Exception("Invalid role '%s'" % role)

        self.role = role

    def setStatus(self, status):
        checkNotNone("status", status)

        if status not in VALID_STATUS:
            raise Exception("Invalid status '%s'" % status)

        self.status = status

    def setPort(self, port):
        checkNotNone("port", port)
        checkIsInt("port", port)
        self.port = port

    def setHostName(self, hostName):
        # None is allowed -- don't check
        self.hostname = hostName

    def setAddress(self, address):
        # None is allowed -- don't check
        self.address = address

    def setDataDirectory(self, dataDirectory):
        checkNotNone("dataDirectory", dataDirectory)
        self.datadir = dataDirectory

    def addFilespace(self, oid, path):
        """
        Add a filespace path for this segment.
        
        Throws: 
           Exception - if a path has already been specified for this segment.
        """

        # gpfilespace adds a special filespace with oid=None to indicate
        # the filespace that it is currently building, since the filespace
        # does not yet exist there is no valid value that could be used.
        if oid == None:
            if self.pending_filespace:
                raise Exception("Duplicate filespace path for registration_order %d" % 
                                self.registration_order)
            self.pending_filespace = path
            return
        
        # oids should always be integer values > 0
        oid = int(oid)
        assert(oid > 0)

        # The more usual case just sets the filespace in the filespace 
        # dictionary
        if oid in self.filespaces:
            raise Exception("Duplicate filespace path for "
                            "registration_order %d filespace %d" % (self.registration_order, oid))
        self.filespaces[oid] = path

    def getPendingFilespace(self):
        """
        Returns the pending filespace location for this segment
        (called by gpfilespace)
        """
        return self.pending_filespace


class HAWQFilesystemObj:
    """
    List information for a filesystem, as stored in pg_filesystem
    """
    def __init__(self, oid, name, shared):
        self.__oid = oid
        self.__name = name
        self.__shared = shared

    def getOid(self):
        return self.__oid

    def getName(self):
        return self.__name

    def isShared(self):
        return self.__shared == True

    @staticmethod
    def getFilesystemObj(filesystemArr, fsoid):
        # local storage
        if fsoid == 0:
            return None
        # plugin storage
        for fsys in filesystemArr:
            if (fsys.getOid() == fsoid):
                return fsys
        raise Exception("Error: invalid file system oid %d" % (fsoid))

class HAWQFilespaceObj:
    """
    List information for a filespace, as stored in pg_filespace
    """
    def __init__(self, oid, name, fsys):
        self.__oid = oid
        self.__name = name
        self.__fsys = fsys

    def getOid(self):
        return self.__oid

    def getName(self):
        return self.__name

    def getFsys(self):
        return self.__fsys

    def isSystemFilespace(self):
        return self.__oid == SYSTEM_FILESPACE



class HAWQArray:
    """ 
    HAWQArray is a python class that describes a HAWQ array.

    A HAWQ array consists of:
      master         - The primary QD for the array
      standby master - The mirror QD for the array [optional]
      segment array  - an array of segments within the cluster

    Each segment is either a single HAWQDB object, or a primary/mirror pair.

    It can be initialized either from a database connection, in which case
    it discovers the configuration information by examining the catalog, or
    via a configuration file.
    """

    # --------------------------------------------------------------------
    def __init__(self, hawqdbs):
        """
        segmentsInDb is used only be the configurationImpl* providers; it is used to track the state of the
          segments in the database

        TODO:

        """

        self.master = None
        self.standbyMaster = None
        self.segments = []
        self.numSegments = 0
        self.version = None

        self.setFilespaces([])

        for hdb in hawqdbs:

            # Handle master
            if hdb.isMaster():
                if self.master != None:
                    logger.error("multiple master dbs defined")
                    raise Exception("HAWQArray - multiple master dbs defined")
                self.master = hdb

            # Handle standby
            elif hdb.isStandby():
                if self.standbyMaster != None:
                    logger.error("multiple standby master dbs defined")
                    raise Exception("HAWQArray - multiple standby master dbs defined")
                self.standbyMaster = hdb

            # Handle segments
            elif hdb.isSegment():
                self.addSegment(hdb)

            else:
                # Not a master, standbymaster, primary, or mirror?
                # shouldn't even be possible.
                logger.error("FATAL - invalid dbs defined")
                raise Exception("Error: HAWQArray() - invalid dbs defined")

        # Make sure HAWQ cluster has a master
        if self.master is None:
            logger.error("FATAL - no master defined!")
            raise Exception("Error: HAWQArray() - no master defined")

    def __str__(self):
        return "Master: %s\nStandby: %s\nSegments: %s" % (str(self.master),
                                                          str(self.standbyMaster) if self.standbyMaster else 'Not Configured',
                                                          "\n".join([str(seg) for seg in self.segments]))

    def addSegment(self, hdb):
        if hdb.isSegment():
            self.segments.append(hdb)
            self.numSegments += 1
        else:
            raise Exception("Error: adding invalid segment to HAWQArray")


    # --------------------------------------------------------------------
    @staticmethod
    def initFromCatalog(dbURL, utility=False, useAllSegmentFileSpaces=False):
        """
        Factory method, initializes a HAWQArray from provided database URL

        Please note that -
        useAllSegmentFilespaces when set to true makes this method add *all* filespaces
        to the segments of hawqarray. If false, only returns Master/Standby all filespaces
        This is *hacky* and we know that it is not the right way to design methods/interfaces
        We are doing this so that we do not affect behavior of existing tools like upgrade, gprecoverseg etc
        """

        conn = dbconn.connect(dbURL, utility)

        # Get the version from the database:
        version_str = None
        for row in dbconn.execSQL(conn, "SELECT version()"):
            version_str = row[0]
        version = GpVersion(version_str)

        # Only for HAWQ 2.0
        if version.getVersionRelease() in ("2.0"):

            hawq_site = HawqXMLParser(GPHOME)
            master_data_directory  = hawq_site.get_value_from_name('hawq_master_directory')
            segment_data_directory = hawq_site.get_value_from_name('hawq_segment_directory')

            # strategy_rows = dbconn.execSQL(conn, "show gp_fault_action")
            strategy_rows = []

            config_rows = dbconn.execSQL(conn, '''
                               SELECT sc.registration_order,
                                      sc.role,
                                      sc.status,
                                      sc.hostname,
                                      sc.address,
                                      sc.port,
                                      CASE
                                          WHEN sc.registration_order <= 0 THEN '%s'
                                          ELSE '%s'
                                      END AS datadir
                               FROM pg_catalog.gp_segment_configuration sc
                               ORDER BY sc.registration_order;''' %
                               (master_data_directory, segment_data_directory))

            # All of filesystem is shared storage
            filesystemRows = dbconn.execSQL(conn, '''
                SELECT oid, fsysname, true AS fsysshared
                FROM pg_filesystem
                ORDER BY fsysname
            ''')

            filesystemArr = [HAWQFilesystemObj(fsysRow[0], fsysRow[1], fsysRow[2]) for fsysRow in filesystemRows]

            filespaceRows = dbconn.execSQL(conn, '''
                SELECT oid, fsname, fsfsys AS fsoid
                FROM pg_filespace
                WHERE oid != %d
                ORDER BY fsname;
            ''' % (SYSTEM_FILESPACE))

            filespaceArr = [HAWQFilespaceObj(fsRow[0], fsRow[1], HAWQFilesystemObj.getFilesystemObj(filesystemArr, fsRow[2])) for fsRow in filespaceRows]

        else:
            raise Exception("HAWQ version is invalid: %s" % version)

        hawqdbs = []
        print "### initFromCatalog ###"
        hdb = None
        for row in config_rows:

            print row

            # Extract fields from the row
            (registration_order, role, status, hostname, 
             address, port, datadir) = row

            # In GPSQL, only master maintain the filespace information.
            # if registration_order != MASTER_REGISTRATION_ORDER and \
            #    fsoid != SYSTEM_FILESPACE and \
            #    not useAllSegmentFileSpaces:
            #    print "### initFromCatalog ... continue ###"
            #    continue

            # The query returns all the filespaces for a segment on separate
            # rows.  If this row is the same dbid as the previous row simply
            # add this filespace to the existing list, otherwise create a
            # new segment.
            # if seg and seg.getSegmentRegistrationOrder() == registration_order:
            #     seg.addSegmentFilespace(fsoid, fslocation)
            # else:
            #     seg = HAWQDB(registration_order, role, status, 
            #                 hostname, address, port, datadir)
            #    segments.append(seg)

            hdb = HAWQDB(registration_order, role, status, 
                         hostname, address, port, datadir)
            print "### initFromCatalog ... hdb ###"
            print hdb
            hawqdbs.append(hdb)
            print "### initFromCatalog ... hawqdbs ###"
            print hawqdbs
        
        conn.close()
        
        # origSegments = [seg.copy() for seg in segments]
        
        array = HAWQArray(hawqdbs)
        array.version = version
        array.setFilespaces(filespaceArr)
        array.setFilesystem(filesystemArr)
        
        return array

    # --------------------------------------------------------------------
    def is_array_valid(self):
        """Checks that each array is in a valid state"""

        if self.master.getStatus() != STATUS_UP:
            return False

        if self.standbyMaster and self.standbyMaster.getStatus() != STATUS_UP:
            return False

        for seg in self.segments:
            if not seg.status == STATUS_UP:
                return False
        return True

    # --------------------------------------------------------------------
    def setFilesystem(self, filesystemArr):
        """
        @param filesystemArr of GpFilesystemObj objects
        """
        self.filesystemArr = [fsys for fsys in filesystemArr]

    def getFilesystem(self):
        """
        @return a newly allocated list of GpFilespaceObj objects, will have been sorted by filesystem name
        """
        return [fsys for fsys in self.filesystemArr]

    def setFilespaces(self, filespaceArr):
        """
        @param filespaceArr of GpFilespaceObj objects
        """
        self.filespaceArr = [fs for fs in filespaceArr]

    def getFilespaces(self, includeSystemFilespace=True):
        """
        @return a newly allocated list of GpFilespaceObj objects, will have been sorted by filespace name
        """
        return [fs for fs in self.filespaceArr if fs.isSystemFilespace()]

    def getNonSystemFilespaces(self):
        """
        @return a newly allocated list of GpFilespaceObj objects, will have been sorted by filespace name
        """
        return [fs for fs in self.filespaceArr if not fs.isSystemFilespace()]

    def getAllFilespaces(self):
        """
        @return a newly allocated list of GpFilespaceObj objects, will have been sorted by filespace name
        """
        return [fs for fs in self.filespaceArr]

    # --------------------------------------------------------------
    def getFileSpaceName(self, filespaceOid):
        retValue = None
        
        if self.filespaceArr != None:
            for entry in self.filespaceArr:
                if entry.getOid() == filespaceOid:
                    retValue = entry.getName()
                    break
        return retValue

    # --------------------------------------------------------------
    def getFileSpaceOid(self, filespaceName):
        retValue = None
        
        if self.filespaceArr != None:
            for entry in self.filespaceArr:
                if entry.getName() == filespaceName:
                    retValue = entry.getOid()
                    break
        return retValue

    # --------------------------------------------------------------
    def isFileSpaceShared(self, filespaceOid):
        retValue = False
        
        if self.filespaceArr != None:
            for entry in self.filespaceArr:
                if entry.getOid() == filespaceOid:
                    retValue = entry.getFsys() != None and entry.getFsys().isShared()
                    break
        return retValue


    # --------------------------------------------------------------------
    def getDbList(self):
        """
        Return a list of all HAWQDB objects that make up the array
        """
        dbs=[]

        dbs.append(self.master)

        if self.standbyMaster:
            dbs.append(self.standbyMaster)

        dbs.extend(self.getSegDbList())

        return dbs

    # --------------------------------------------------------------------
    def getHostList(self, includeExpansionSegs = False):
        """
        Return a list of all Hosts that make up the array
        """
        hostList = []

        hostList.append(self.master.getSegmentHostName())

        if self.standbyMaster:
            hostList.append(self.standbyMaster.getSegmentHostName())
            
        dbList = self.getDbList()
        for db in dbList:
            if db.getSegmentHostName() in hostList:
                continue
            else:
                hostList.append(db.getSegmentHostName())

        return hostList

    def getSegDbList(self):
        """Return a list of all HAWQDB objects for all segments in the array"""
        dbs=[]

        for seg in self.segments:
            dbs.append(seg)

        return dbs

