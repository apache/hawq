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

# Line too long            - pylint: disable=C0301
# Invalid name             - pylint: disable=C0103
#
# Note: the option to recover to a new host is not very good if we have a multi-home configuration
#
# Options removed when 4.0 gprecoverseg was implemented:
#        --version
#       -S "Primary segment dbid to force recovery": I think this is done now by bringing the primary down, waiting for
#           failover, and then doing recover full
#       -z "Primary segment data dir and host to force recovery" see removed -S option for comment
#       -f        : force Greenplum Database instance shutdown and restart
#       -F (HAS BEEN CHANGED) -- used to mean "force recovery" and now means "full recovery)
# And a change to the -i input file: it now takes replicationPort in list of args (for failover target)
#
# import mainUtils FIRST to get python version check
# THIS IMPORT SHOULD COME FIRST
from gppylib.mainUtils import *

from optparse import Option, OptionGroup, OptionParser, OptionValueError, SUPPRESS_USAGE
import os, sys, getopt, socket, StringIO, signal
from gppylib import gparray, gplog, pgconf, userinput, utils 
from gppylib.util import gp_utils
from gppylib.commands import base, gp, pg, unix
from gppylib.db import catalog, dbconn
from gppylib.gpparseopts import OptParser, OptChecker
from gppylib.operations.startSegments import *
from gppylib.operations.buildMirrorSegments import *
from gppylib.operations.rebalanceSegments import GpSegmentRebalanceOperation
from gppylib.programs import programIoUtils
from gppylib.programs.clsAddMirrors import validateFlexibleHeadersListAllFilespaces
from gppylib.system import configurationInterface as configInterface
from gppylib.system.environment import GpMasterEnvironment
from gppylib.testold.testUtils import *
from gppylib.parseutils import line_reader, parse_filespace_order, parse_gprecoverseg_line, \
        canonicalize_address
from gppylib.utils import ParsedConfigFile, ParsedConfigFileRow, writeLinesToFile, \
        normalizeAndValidateInputPath, TableLogger
from gppylib.gphostcache import GpInterfaceToHostNameCache
from gppylib.operations.utils import ParallelOperation
from gppylib.operations.package import SyncPackages 

import gppylib.commands.gp

logger = gplog.get_default_logger()

class PortAssigner:
    """
    Used to assign new ports to segments on a host

    Note that this could be improved so that we re-use ports for segments that are being recovered but this
      does not seem necessary.

    """

    MAX_PORT_EXCLUSIVE=65536

    def __init__(self, gpArray):
        #
        # determine port information for recovering to a new host --
        #   we need to know the ports that are in use and the valid range of ports
        #
        segments = gpArray.getDbList()
        ports = [seg.getSegmentPort() for seg in segments if seg.isSegmentQE()]
        if len(ports) > 0:
            self.__minPort = min(ports)
        else:
            raise Exception("No segment ports found in array.")
        self.__usedPortsByHostName = {}

        byHost = GpArray.getSegmentsByHostName(segments)
        for hostName, segments in byHost.iteritems():
            usedPorts = self.__usedPortsByHostName[hostName] = {}
            for seg in segments:
                usedPorts[seg.getSegmentPort()] = True

    def findAndReservePort(self, hostName, address):
        """
        Find an unused port of the given type (normal postmaster or replication port)
        When found, add an entry:  usedPorts[port] = True   and return the port found
        Otherwise raise an exception labeled with the given address
        """
        if hostName not in self.__usedPortsByHostName:
            self.__usedPortsByHostName[hostName] = {}
        usedPorts = self.__usedPortsByHostName[hostName]

        minPort = self.__minPort
        for port in range(minPort, PortAssigner.MAX_PORT_EXCLUSIVE):
            if port not in usedPorts:
                usedPorts[port] = True
                return port
        raise Exception("Unable to assign port on %s" % address)


#-------------------------------------------------------------------------
class GpRecoverSegmentProgram:

    #
    # Constructor:
    #
    # @param options the options as returned by the options parser
    #
    def __init__(self, options):
        self.__options = options
        self.__pool = None
        self.logger = logger

    def outputToFile(self, mirrorBuilder, gpArray, fileName):
        lines = []

        #
        # first line is always the filespace order
        #
        filespaceArr = [fs for fs in gpArray.getFilespaces(False)]
        lines.append("filespaceOrder=" + (":".join([fs.getName() for fs in filespaceArr])))

        # now one for each failure
        for mirror in mirrorBuilder.getMirrorsToBuild():
            str = ""
            seg  = mirror.getFailedSegment()
            addr = canonicalize_address( seg.getSegmentAddress() )
            str += ('%s:%d:%s' % ( addr, seg.getSegmentPort(), canonicalize_address(seg.getSegmentDataDirectory())))

            seg = mirror.getFailoverSegment()
            if seg is not None:

                #
                # build up :path1:path2   for the mirror segment's filespace paths
                #
                segFilespaces = seg.getSegmentFilespaces()
                filespaceValues = []
                for fs in filespaceArr :
                    path = segFilespaces.get(fs.getOid())
                    assert path is not None   # checking consistency should have been done earlier, but doublecheck here
                    filespaceValues.append(":" + canonicalize_address(path))

                str += ' '
                addr = canonicalize_address( seg.getSegmentAddress() )
                str += ('%s:%d:%d:%s%s' % (addr, seg.getSegmentPort(), seg.getSegmentReplicationPort(), seg.getSegmentDataDirectory(),
                        "".join(filespaceValues)))

            lines.append(str)
        writeLinesToFile(fileName, lines)

    def getRecoveryActionsFromConfigFile(self, gpArray):
        """
        getRecoveryActionsFromConfigFile

        returns a GpMirrorListToBuild object
        """

        # create fileData object from config file
        #
        filename = self.__options.recoveryConfigFile
        fslist   = None
        rows     = []
        with open(filename) as f:
            for lineno, line in line_reader(f):
                if fslist is None:
                    fslist = parse_filespace_order(filename, lineno, line)
                else:
                    fixed, flexible = parse_gprecoverseg_line(filename, lineno, line, fslist)
                    rows.append( ParsedConfigFileRow(fixed, flexible, line) )
        fileData = ParsedConfigFile(fslist, rows)
        
        # validate fileData
        #
        validateFlexibleHeadersListAllFilespaces("Segment recovery config", gpArray, fileData)
        filespaceNameToFilespace = dict([ (fs.getName(), fs) for fs in gpArray.getFilespaces(False)])

        allAddresses = [row.getFixedValuesMap()["newAddress"] for row in fileData.getRows()
                                                if "newAddress" in row.getFixedValuesMap()]
        allNoneArr = [None for a in allAddresses]
        interfaceLookup = GpInterfaceToHostNameCache(self.__pool, allAddresses, allNoneArr)

        failedSegments = []
        failoverSegments = []
        for row in fileData.getRows():
            fixedValues = row.getFixedValuesMap()
            flexibleValues = row.getFlexibleValuesMap()

            # find the failed segment
            failedAddress = fixedValues['failedAddress']
            failedPort = fixedValues['failedPort']
            failedDataDirectory = normalizeAndValidateInputPath( fixedValues['failedDataDirectory'],
                                                                        "config file", row.getLine())
            failedSegment = None
            for segment in gpArray.getDbList():
                if segment.getSegmentAddress() == failedAddress and \
                            str(segment.getSegmentPort()) == failedPort and \
                            segment.getSegmentDataDirectory() == failedDataDirectory:

                    if failedSegment is not None:
                        #
                        # this could be an assertion -- configuration should not allow multiple entries!
                        #
                        raise Exception(("A segment to recover was found twice in configuration.  " \
                            "This segment is described by address:port:directory '%s:%s:%s' on the input line: %s") %
                            (failedAddress, failedPort, failedDataDirectory, row.getLine()))
                    failedSegment = segment

            if failedSegment is None:
                raise Exception("A segment to recover was not found in configuration.  " \
                            "This segment is described by address:port:directory '%s:%s:%s' on the input line: %s" %
                            (failedAddress, failedPort, failedDataDirectory, row.getLine()))

            failoverSegment = None
            if "newAddress" in fixedValues:
                """
                When the second set was passed, the caller is going to tell us to where we need to failover, so
                  build a failover segment
                """
                # these two lines make it so that failoverSegment points to the object that is registered in gparray
                failoverSegment = failedSegment
                failedSegment = failoverSegment.copy()

                address = fixedValues["newAddress"]
                try:
                    port = int(fixedValues["newPort"])
                    replicationPort = int(fixedValues["newReplicationPort"])
                except ValueError:
                    raise Exception( 'Config file format error, invalid number value in line: %s' % (row.getLine()))

                dataDirectory = normalizeAndValidateInputPath(fixedValues["newDataDirectory"], "config file", row.getLine())

                hostName = interfaceLookup.getHostName(address)
                if hostName is None:
                    raise Exception( 'Unable to find host name for address %s from line:%s' % (address, row.getLine()))

                filespaceOidToPathMap = {}
                for fsName, path in flexibleValues.iteritems():
                    path = normalizeAndValidateInputPath(path, "config file", row.getLine())
                    filespaceOidToPathMap[filespaceNameToFilespace[fsName].getOid()] = path

                # now update values in failover segment
                failoverSegment.setSegmentAddress( address )
                failoverSegment.setSegmentHostName( hostName )
                failoverSegment.setSegmentPort( port )
                failoverSegment.setSegmentReplicationPort( replicationPort )
                failoverSegment.setSegmentDataDirectory( dataDirectory )

                for fsOid, path in filespaceOidToPathMap.iteritems():
                    failoverSegment.getSegmentFilespaces()[fsOid] = path
                failoverSegment.getSegmentFilespaces()[gparray.SYSTEM_FILESPACE] = dataDirectory

            # this must come AFTER the if check above because failedSegment can be adjusted to
            #   point to a different object
            failedSegments.append(failedSegment)
            failoverSegments.append(failoverSegment)

        peersForFailedSegments = self.findAndValidatePeersForFailedSegments(gpArray, failedSegments)

        segs = []
        for i in range(len(failedSegments)):
            segs.append( GpMirrorToBuild(failedSegments[i], peersForFailedSegments[i], failoverSegments[i], \
                                self.__options.forceFullResynchronization))
        return GpMirrorListToBuild(segs, self.__pool, self.__options.quiet, self.__options.parallelDegree)

    def findAndValidatePeersForFailedSegments(self, gpArray, failedSegments):
        """
        findAndValidatePeersForFailedSegments will returns random segments for failed segments.
        """
        segs = [ seg for seg in gpArray.get_valid_segdbs() if not seg.isSegmentDown()]
        if len(segs) <= 0:
            raise Exception("No available segment in the cluster.")
        peersForFailedSegments = [ segs[0] for seg in failedSegments]
        for i in range(len(failedSegments)):
            peer = peersForFailedSegments[i]
            if peer is None:
                raise Exception("No peer found for dbid %s" % failedSegments[i].getSegmentDbId())
            elif peer.isSegmentDown():
                raise Exception("Both segments for content %s are down; Try restarting Greenplum DB and running %s again." %
                        (peer.getSegmentContentId(), getProgramName()))
        return peersForFailedSegments

    def __outputSpareDataDirectoryFile( self, gpEnv, gpArray, outputFile):
        lines = []
        for fs in gpArray.getFilespaces():
            if gpArray.isFileSpaceShared(fs.getOid()):
                lines.append(fs.getName() + "=sharedFilespaceCannotMove")
            else:
                lines.append(fs.getName() + "=enterFilespacePath")
        lines.sort()
        utils.writeLinesToFile(outputFile, lines)

        self.logger.info("Wrote sample configuration file %s" % outputFile)
        self.logger.info("MODIFY IT and then run with      gprecoverseg -s %s" % outputFile)

    def __readSpareDirectoryMap(self, gpArray, spareDataDirectoryFile):
        """
        Read filespaceName=path configuration from spareDataDirectoryFile

        File format should be in sync with format printed by __outputSpareDataDirectoryFile

        @return a dictionary mapping filespace oid to path
        """
        filespaceNameToFilespace = dict([ (fs.getName(), fs) for fs in gpArray.getFilespaces()])

        specifiedFilespaceNames = {}
        fsOidToPath = {}
        for line in utils.readAllLinesFromFile(spareDataDirectoryFile, skipEmptyLines=True, stripLines=True):
            arr = line.split("=")
            if len(arr) != 2:
                raise Exception("Invalid line in spare directory configuration file: %s" % line)
            fsName = arr[0]
            path = arr[1]

            if fsName in specifiedFilespaceNames:
                raise Exception("Filespace %s has multiple entries in spare directory configuration file." % fsName )
            specifiedFilespaceNames[fsName] = True

            if fsName not in filespaceNameToFilespace:
                raise Exception("Invalid filespace %s in spare directory configuration file." % fsName )
            oid = filespaceNameToFilespace[fsName].getOid()

            if gpArray.isFileSpaceShared(oid):
                path = None
            else:
                path = normalizeAndValidateInputPath(path, "config file" )

            fsOidToPath[oid] = path

        if len(fsOidToPath) != len(filespaceNameToFilespace):
            raise Exception("Filespace configuration file only lists %s of needed %s filespace directories.  "
                            "Use -S option to create sample input file." %
                            (len(fsOidToPath), len(filespaceNameToFilespace)))
        return fsOidToPath

    def __applySpareDirectoryMapToSegment( self, gpEnv, gpArray, spareDirectoryMap, segment):
        gpPrefix = gp_utils.get_gp_prefix(gpEnv.getMasterDataDir())
        if not gpPrefix:
            gpPrefix = 'gp'

        fsMap = segment.getSegmentFilespaces()
        for oid, path in spareDirectoryMap.iteritems():
            # Shared storage can not be relocated
            if (gpArray.isFileSpaceShared(oid)):
                continue
            newPath = utils.createSegmentSpecificPath( path, gpPrefix, segment)
            fsMap[oid] = newPath
            if oid == gparray.SYSTEM_FILESPACE:
                segment.setSegmentDataDirectory( newPath )

    def getRecoveryActionsFromConfiguration(self, gpEnv, gpArray):
        """
        getRecoveryActionsFromConfiguration

        returns a GpMirrorListToBuild object
        """
        segments = gpArray.getSegDbList()

        failedSegments = [ seg for seg in segments if seg.isSegmentDown() ]
        peersForFailedSegments = self.findAndValidatePeersForFailedSegments(gpArray, failedSegments)

        # Dictionaries used for building mapping to new hosts
        recoverAddressMap = {}
        recoverHostMap = {}
        interfaceHostnameWarnings = []
        
        # Check if the array is a "standard" array
        (isStandardArray, _ignore) = gpArray.isStandardArray()
        
        recoverHostIdx = 0
        
        if self.__options.newRecoverHosts and len(self.__options.newRecoverHosts) > 0:
            for seg in failedSegments:
                segAddress = seg.getSegmentAddress()
                segHostname = seg.getSegmentHostName()
                
                # Haven't seen this hostname before so we put it on a new host
                if not recoverHostMap.has_key(segHostname):
                    try:
                        recoverHostMap[segHostname] = self.__options.newRecoverHosts[recoverHostIdx]
                    except:
                        # If we get here, not enough hosts were specified in the -p option.  Need 1 new host
                        # per 1 failed host.
                        raise Exception('Not enough new recovery hosts given for recovery.')
                    recoverHostIdx += 1

                if isStandardArray:
                    # We have a standard array configuration, so we'll try to use the same
                    # interface naming convention.  If this doesn't work, we'll correct it
                    # below on name lookup
                    segInterface = segAddress[segAddress.rfind('-'):] 
                    destAddress = recoverHostMap[segHostname] + segInterface
                    destHostname = recoverHostMap[segHostname]
                else:
                    # Non standard configuration so we won't make assumptions on 
                    # naming.  Instead we'll use the hostname passed in for both 
                    # hostname and address and flag for warning later.
                    destAddress = recoverHostMap[segHostname]
                    destHostname = recoverHostMap[segHostname]

                # Save off the new host/address for this address.
                recoverAddressMap[segAddress] = (destHostname, destAddress)

            # Now that we've generated the mapping, look up all the addresses to make
            # sure they are resolvable.
            interfaces = [address for (_ignore, address) in recoverAddressMap.values()]
            interfaceLookup = GpInterfaceToHostNameCache(self.__pool, interfaces, [None] * len(interfaces))
            
            for key in recoverAddressMap.keys():
                (newHostname, newAddress) = recoverAddressMap[key]
                try:
                    addressHostnameLookup = interfaceLookup.getHostName(newAddress)
                    # Lookup failed so use hostname passed in for everything.
                    if addressHostnameLookup is None:
                        interfaceHostnameWarnings.append("Lookup of %s failed.  Using %s for both hostname and address." % (newAddress, newHostname))
                        newAddress = newHostname
                except:
                    # Catch all exceptions.  We will use hostname instead of address
                    # that we generated.
                    interfaceHostnameWarnings.append("Lookup of %s failed.  Using %s for both hostname and address." % (newAddress, newHostname))
                    newAddress = newHostname
                    
                # if we've updated the address to use the hostname because of lookup failure
                # make sure the hostname is resolvable and up
                if newHostname == newAddress:
                    try:
                        unix.Ping.local("ping new hostname", newHostname)
                    except:
                        raise Exception("Ping of host %s failed." % newHostname)
                    
                # Save changes in map
                recoverAddressMap[key] = (newHostname, newAddress)
                
            if len(self.__options.newRecoverHosts) != recoverHostIdx:
                interfaceHostnameWarnings.append("The following recovery hosts were not needed:")
                for h in self.__options.newRecoverHosts[recoverHostIdx:]:
                    interfaceHostnameWarnings.append("\t%s" % h)
        
        spareDirectoryMap = None
        if self.__options.spareDataDirectoryFile is not None:
            spareDirectoryMap = self.__readSpareDirectoryMap(gpArray, self.__options.spareDataDirectoryFile)

        portAssigner = PortAssigner(gpArray)

        forceFull = self.__options.forceFullResynchronization
        segs = []
        for i in range(len(failedSegments)):

            failoverSegment = None
            failedSegment = failedSegments[i]
            liveSegment = peersForFailedSegments[i]

            if self.__options.newRecoverHosts and len(self.__options.newRecoverHosts) > 0:
                (newRecoverHost, newRecoverAddress) = recoverAddressMap[failedSegment.getSegmentAddress()]
                # these two lines make it so that failoverSegment points to the object that is registered in gparray
                failoverSegment = failedSegment
                failedSegment = failoverSegment.copy()
                failoverSegment.setSegmentHostName( newRecoverHost )
                failoverSegment.setSegmentAddress( newRecoverAddress )
                port = portAssigner.findAndReservePort(newRecoverHost, newRecoverAddress )
                failoverSegment.setSegmentPort( port )

            if spareDirectoryMap is not None:
                #
                # these two lines make it so that failoverSegment points to the object that is registered in gparray
                failoverSegment = failedSegment
                failedSegment = failoverSegment.copy()
                self.__applySpareDirectoryMapToSegment( gpEnv, gpArray, spareDirectoryMap, failoverSegment)
                # we're failing over to different location on same host so we don't need to assign new ports

            segs.append( GpMirrorToBuild(failedSegment, liveSegment, failoverSegment, forceFull ))
        
        return GpMirrorListToBuild(segs, self.__pool, self.__options.quiet, self.__options.parallelDegree, interfaceHostnameWarnings)

    def GPSQLFailback(self, gpArray, gpEnv):
        if self.__options.outputSpareDataDirectoryFile is not None:
            self.__outputSpareDataDirectoryFile(gpEnv, gpArray, self.__options.outputSpareDataDirectoryFile)
            return 0

        if self.__options.newRecoverHosts is not None:
            try:
                uniqueHosts = []
                [uniqueHosts.append(h.strip()) for h in self.__options.newRecoverHosts.split(',') \
                    if h.strip() not in uniqueHosts ]
                self.__options.newRecoverHosts = uniqueHosts
            except Exception, ex:
                raise ProgramArgumentValidationException(\
                    "Invalid value for recover hosts: %s" % ex)
            # GPSQL can not fail to the host already exists in cluster. 
            segmentsHosts = []
            [ segmentsHosts.append(seg.getSegmentHostName()) for seg in gpArray.getSegDbList() if seg.isSegmentQE() ]
            for seg in segmentsHosts:
                if seg in uniqueHosts:
                    raise Exception("You can not choose the host \"%s\" which has segments running for recover host." % seg)

        # If it's a rebalance operation
        if self.__options.rebalanceSegments:
            raise Exception("GPSQL does not support rebalance.")

        # retain list of hosts that were existing in the system prior to getRecoverActions...
        # this will be needed for later calculations that determine whether
        # new hosts were added into the system
        existing_hosts = set(gpArray.getHostList())

        # figure out what needs to be done
        mirrorBuilder = self.getRecoveryActionsBasedOnOptions(gpEnv, gpArray)

        if self.__options.outputSampleConfigFile is not None:
            # just output config file and done
            self.outputToFile(mirrorBuilder, gpArray, self.__options.outputSampleConfigFile)
            self.logger.info('Configuration file output to %s successfully.' % self.__options.outputSampleConfigFile)            
        elif len(mirrorBuilder.getMirrorsToBuild()) == 0:
            self.logger.info('No segments to recover')
        else:
            mirrorBuilder.checkForPortAndDirectoryConflicts(gpArray)

            self.displayRecovery(mirrorBuilder, gpArray)
            self.__displayRecoveryWarnings(mirrorBuilder)

            if self.__options.interactive:
                if not userinput.ask_yesno(None, "\nContinue with segment recovery procedure", 'N'):
                    raise UserAbortedException()

            # sync packages
            current_hosts = set(gpArray.getHostList())
            new_hosts = current_hosts - existing_hosts
            if new_hosts:
                self.syncPackages(new_hosts)

            mirrorBuilder.buildMirrors("recover", gpEnv, gpArray )
            return 1
            confProvider.sendPgElogFromMaster("Recovery of %d segment(s) has been started." % \
                    len(mirrorBuilder.getMirrorsToBuild()), True)

            self.logger.info("******************************************************************")
            self.logger.info("Updating segments for resynchronization is completed.")
            self.logger.info("For segments updated successfully, resynchronization will continue in the background.")
            self.logger.info("")
            self.logger.info("Use  gpstate -s  to check the resynchronization progress.")
            self.logger.info("******************************************************************")

        return 0 # success -- exit code 0!

    # San-failback is handled separately from the Filerep-recovery operations.
    # 
    # I need to walk through results returned by shell commands digging for information
    # this isn't as easy as I'd like.
    def SanFailback(self, array_config, gpEnv):

        # Get the configuration information maps.
        (san_mounts, san_mount_by_dbid) = array_config.getSanConfigMaps()

        # 1 Get the failed segments
        bad_segs = {}
        for dbid, v in san_mount_by_dbid.iteritems():
            (status, content, mountlist) = v
            if status == 'd':
                self.logger.info('Bad segment with dbid %d' % dbid)
                bad_segs[dbid] = (status, content, mountlist)

        # 2 Get the failed mountpoints.
        bad_mounts = {}
        for mount_id, v in san_mounts.iteritems():
            if v['active'] == 'm':
                self.logger.info('Bad mountpoint with id %d' % mount_id)
                bad_mounts[mount_id] = v

        # 3 Verify that the required hosts are back up (this may reduce the number of recoverable segments)
        recoverable_mps = {}
        for mount_id, v in bad_mounts.iteritems():
            try:
                unix.Echo.remote('check host', 'Success', v['primaryhost'])
                recoverable_mps[mount_id] = v
            except:
                # Host not available, not added to recoverable_mps.  We'll ignore
                # because there may be others we can recover
                pass

        # 4
        # From the recoverable mountpoints, we should now be able to identify the
        # mountpoints required to recover for the segments. A segment is recoverable
        # if all of its mountpoints are recoverable.
        recoverable_segs = {}
        for dbid, v in bad_segs.iteritems():
            (status, content, mountlist) = v
            recoverable = True
            for mount_id in mountlist:
                if not recoverable_mps.has_key(mount_id):
                    recoverable = False
                    break
            if recoverable:
                recoverable_segs[dbid] = v
            else:
                self.logger.warning('Unrecoverable segment dbid %d' % (dbid))

        if len(recoverable_segs) == 0:
            raise Exception("Found no recoverable segments.")

        # 4 Stop GPDB.
        e = os.system('gpstop -aq -d %s' % (os.environ.get('MASTER_DATA_DIRECTORY')))
        ok = not e
        if not ok:
            self.logger.error('Failed to shutdown Greenplum Database: segment recovery cannot proceed.')
            raise Exception("Failed to shutdown GPDB. Segment recovery failed.")
        else:
            self.logger.info('Successfully shutdown the Greenplum Database')

        # 5 Move mountpoints
        #   For each recoverable seg, walk its mountlist.
        # 5a
        #   unmount on failover host.            
        # 5b
        #   reconnect to primary.
        # 5c
        #   mount on primary.
        mount_done = {}
        for dbid, v in recoverable_segs.iteritems():
            (status, content, mountlist) = v

            for mount_id in mountlist:
                if mount_done.has_key(mount_id):
                    continue # already moved this

                if self.SanFailback_mountpoint(mount_id, recoverable_mps[mount_id]) == 0:
                    # TODO: some kind of error handling here ??
                    mount_done[mount_id] = True
                else:
                    mount_done[mount_id] = False

        self.logger.debug('Completed mount-recovery:')
        for mount_id, v in mount_done.iteritems():
            if v:
                self.logger.debug('mount-id %d ---> TRUE' % mount_id)
            else:
                self.logger.debug('mount-id %d ---> FALSE' % mount_id)

        # N - 3
        # Start GPDB in admin-mode
        os.putenv('GPSTART_INTERNAL_MASTER_ONLY', '1')
        e = os.system('gpstart -m -d %s' % (os.environ.get('MASTER_DATA_DIRECTORY')))
        ok = not e
        if not ok:
            self.logger.error('Failed to bring Greenplum Database up in management mode: segment recovery failed')
            raise Exception("Failed to start GPDB in management mode.")
        else:
            self.logger.info('Greenplum Database restarted for configuration update')

        # N - 2
        # Update configuration

        # Open a connection to the DB.
        conn = None
        try:
            db_url = dbconn.DbURL(port=gpEnv.getMasterPort(), dbname='template1')

            conn = dbconn.connect(db_url, utility=True)

            dbconn.execSQL(conn, "BEGIN")

            self.logger.debug('Starting Transaction')

            # Update gp_san_configuration
            for mount_id, v in mount_done.iteritems():
                self.logger.debug('Checking Mount id %d' % mount_id)
                if v:
                    sql = 'UPDATE gp_san_configuration SET active_host=\'p\' WHERE mountid=%d' % mount_id
                    self.logger.debug('Issuing SQL [%s]' % sql)
                    dbconn.executeUpdateOrInsert(conn, sql, 1)

                    history_message = "GPRECOVERSEG: san-mount-id %d set active_host to primary" % (mount_id)
                    sql = 'INSERT INTO gp_configuration_history values (now(), -1, \'%s\')' % history_message
                    self.logger.debug('Issuing SQL [%s]' % sql)
                    dbconn.executeUpdateOrInsert(conn, sql, 1)

            # Update gp_segment_configuration
            for dbid, v in recoverable_segs.iteritems():
                (status, content, mountlist) = v

                self.logger.debug('Checking dbid id %d' % dbid)

                all_mountpoints = True
                for mount_id, v in mount_done.iteritems():
                    self.logger.debug('Scanning mountid %d in dbid id %d' % (mount_id, dbid))
                    if not v:
                        self.logger.debug('Mountid %d --> False' % mount_id)
                        all_mountpoints = False
                    else:
                        self.logger.debug('Mountid %d --> True' % mount_id)

                if all_mountpoints:
                    sql = 'UPDATE gp_segment_configuration SET status = \'u\' where dbid = %d' % dbid

                    self.logger.debug('Issuing SQL [%s]' % sql)
                    dbconn.executeUpdateOrInsert(conn, sql, 1)

                    sql = 'UPDATE gp_segment_configuration SET role = preferred_role where content = %d' % content
                    self.logger.debug('Issuing SQL [%s]' % sql)
                    dbconn.executeUpdateOrInsert(conn, sql, 2)

                    history_message = "GPRECOVERSEG: content %d, dbid %d moved to primary host" % (content, dbid)
                    sql = 'INSERT INTO gp_configuration_history values (now(), %d, \'%s\')' % (dbid, history_message)
                    self.logger.debug('Issuing SQL [%s]' % sql)
                    dbconn.executeUpdateOrInsert(conn, sql, 1)
                else:
                    self.logger.info('Failed to recover sufficient mountpoints for dbid %d' % dbid)

            self.logger.debug('Committing our updates.')
            dbconn.execSQL(conn, "COMMIT")
        finally:
            if conn:
                conn.close()

        # N - 1 
        # Stop GPDB-admin-mode
        e = os.system('gpstop -m -d %s' % (os.environ.get('MASTER_DATA_DIRECTORY')))
        ok = not e
        if not ok:
            self.logger.error('Failed to stop Greenplum Database up in management mode: segment recovery failed')
            raise Exception("Failed to stop GPDB, from management mode.")
        else:
            self.logger.info('Greenplum Database stopped, preparing for full restart.')

        # N Start GPDB
        e = os.system('gpstart -aq -d %s' % (os.environ.get('MASTER_DATA_DIRECTORY')))
        ok = not e
        if not ok:
            self.logger.error('Failed to restart Greenplum Database: segment recovery failed')
            raise Exception("Failed to restart GPDB.")
        else:
            self.logger.info('Successfully restarted the Greenplum Database')

        configInterface.getConfigurationProvider().sendPgElogFromMaster( "SAN recovery has completed.", True)

        return 0

    def SanFailback_mountpoint(self, mp_id, mp_dict):
        active = mp_dict['active']
        type = mp_dict['type']

        if active == 'm':
            oldhost = mp_dict['primaryhost']
            old_mp = mp_dict['primarymountpoint']

            newhost = mp_dict['mirrorhost']
            new_mp = mp_dict['mirrormountpoint']
        else:
            # failback unnecessary ?
            self.logger.info('Not failback required for mount id %d primary is active!' % mp_id)
            return 0

        # RUN GP_MOUNT_AGENT HERE ??

        command = 'gp_mount_agent --agent -u -t %c -a %c -p %s -d %s -m %s -q %s -e %s -n %s' % (mp_dict['type'], mp_dict['active'],
                                                                                                 mp_dict['primaryhost'], mp_dict['primarydevice'], mp_dict['primarymountpoint'],
                                                                                                 mp_dict['mirrorhost'], mp_dict['mirrordevice'], mp_dict['mirrormountpoint'])
                                                                                         
        self.logger.debug('gp_mount_agent command is \'%s\'' % command)

        e = os.system(command)
        ok = not e
        if not ok:
            self.logger.error('gp_mount_agent: failed to relocate mount point %d' % mp_id)
            raise Exception("Failed to relocate mountpoint for mount id %d" % mp_id)

        return 0

    def getRecoveryActionsBasedOnOptions(self, gpEnv, gpArray):
        if self.__options.rebalanceSegments:
            return GpSegmentRebalanceOperation(gpEnv, gpArray)
        elif self.__options.recoveryConfigFile is not None:
            return self.getRecoveryActionsFromConfigFile(gpArray)
        else:
            return self.getRecoveryActionsFromConfiguration(gpEnv, gpArray)

    def syncPackages(self, new_hosts):
        # The design decision here is to squash any exceptions resulting from the 
        # synchronization of packages. We should *not* disturb the user's attempts to recover.
        try:
            logger.info('Syncing Greenplum Database extensions')
            operations = [ SyncPackages(host) for host in new_hosts ]
            ParallelOperation(operations, self.__options.parallelDegree).run()
            # introspect outcomes
            for operation in operations:
                operation.get_ret()
        except Exception, e:
            logger.exception('Syncing of Greenplum Database extensions has failed.')
            logger.warning('Please run gppkg --clean after successful segment recovery.')


    def displayRecovery(self, mirrorBuilder, gpArray):
        self.logger.info('Greenplum instance recovery parameters')
        self.logger.info('---------------------------------------------------------')

        if self.__options.recoveryConfigFile:
            self.logger.info('Recovery from configuration -i option supplied')
        elif self.__options.newRecoverHosts is not None:
            self.logger.info('Recovery type              = Pool Host')
            for h in self.__options.newRecoverHosts:
                self.logger.info('Pool host for recovery     = %s' % h)
            type_text = 'Pool    '
        elif self.__options.spareDataDirectoryFile is not None:
            self.logger.info('Recovery type              = Pool Directory')
            self.logger.info('Mirror pool directory file = %s' % self.__options.spareDataDirectoryFile)
            type_text = 'Pool dir'
        elif self.__options.rebalanceSegments:
            self.logger.info('Recovery type              = Rebalance')
            type_text = 'Rebalance segments'
        else:
            self.logger.info('Recovery type              = Standard')
            type_text = 'Failed  '

        if self.__options.rebalanceSegments:
            i = 1
            total = len(gpArray.get_unbalanced_segdbs())
            for toRebalance in gpArray.get_unbalanced_segdbs():
                tabLog = TableLogger()
                self.logger.info('---------------------------------------------------------')
                self.logger.info('Unbalanced segment %d of %d' % (i, total))
                self.logger.info('---------------------------------------------------------')
                programIoUtils.appendSegmentInfoForOutput("Unbalanced", gpArray, toRebalance, tabLog)
                tabLog.info(["Balanced role", "= Primary" if toRebalance.preferred_role == 'p' else "= Mirror"])
                tabLog.info(["Current role", "= Primary" if toRebalance.role == 'p' else "= Mirror"])                
                tabLog.outputTable()
                i+=1                
        else:
            count = 0
            # self.logger.info('Recovery parallel batch value = %d' % opt['-B'])
            i = 0
            total = len(mirrorBuilder.getMirrorsToBuild())
            for toRecover in mirrorBuilder.getMirrorsToBuild():
                self.logger.info('---------------------------------------------------------')
                self.logger.info('Recovery %d of %d' % (i+1, total))
                self.logger.info('---------------------------------------------------------')
    
                tabLog = TableLogger()
    
                # syncMode = "Full" if toRecover.isFullSynchronization() else "Incremental"
                # tabLog.info(["Synchronization mode", "= " + syncMode])
                programIoUtils.appendSegmentInfoForOutput("Failed", gpArray, toRecover.getFailedSegment(), tabLog)
                # programIoUtils.appendSegmentInfoForOutput("Recovery Source", gpArray, toRecover.getLiveSegment(), tabLog)
    
                if toRecover.getFailoverSegment() is not None:
                    programIoUtils.appendSegmentInfoForOutput("Recovery Target", gpArray, toRecover.getFailoverSegment(), tabLog)
                else:
                    tabLog.info(["Recovery Target", "= in-place"])
                tabLog.outputTable()
    
                i = i + 1

        self.logger.info('---------------------------------------------------------')

    def __getSimpleSegmentLabel(self, seg):
        addr = canonicalize_address( seg.getSegmentAddress() )
        return "%s:%s" % ( addr, seg.getSegmentDataDirectory())

    def __displayRecoveryWarnings(self, mirrorBuilder):
        for warning in self._getRecoveryWarnings(mirrorBuilder):
            self.logger.warn(warning)

    def _getRecoveryWarnings(self, mirrorBuilder):
        """
        return an array of string warnings regarding the recovery
        """
        res = []
        for toRecover in mirrorBuilder.getMirrorsToBuild():

            if toRecover.getFailoverSegment() is not None:
                #
                # user specified a failover location -- warn if it's the same host as its primary
                #
                src = toRecover.getLiveSegment()
                dest = toRecover.getFailoverSegment()

                if src.getSegmentHostName() == dest.getSegmentHostName():
                    res.append("Segment is being recovered to the same host as its primary: "
                            "primary %s    failover target: %s"
                            % (self.__getSimpleSegmentLabel(src), self.__getSimpleSegmentLabel(dest)))
        
        for warning in mirrorBuilder.getAdditionalWarnings():
            res.append(warning)
        
        return res

    def run(self):
        if self.__options.parallelDegree < 1 or self.__options.parallelDegree > 64:
            raise ProgramArgumentValidationException("Invalid parallelDegree provided with -B argument: %d" % self.__options.parallelDegree)

        self.__pool = base.WorkerPool(self.__options.parallelDegree)
        gpEnv = GpMasterEnvironment(self.__options.masterDataDirectory, True)

        # verify "where to recover" options
        optionCnt = 0
        if self.__options.newRecoverHosts is not None:
            optionCnt += 1
        if self.__options.spareDataDirectoryFile is not None:
            optionCnt += 1
        if self.__options.recoveryConfigFile is not None:
            optionCnt += 1
        if self.__options.outputSpareDataDirectoryFile is not None:
            optionCnt += 1
        if self.__options.rebalanceSegments:
            optionCnt += 1
        if optionCnt > 1:
            raise ProgramArgumentValidationException(\
                    "Only one of -i, -p, -s, -r, and -S may be specified")

        faultProberInterface.getFaultProber().initializeProber(gpEnv.getMasterPort())

        confProvider = configInterface.getConfigurationProvider().initializeProvider(gpEnv.getMasterPort())
        gpArray = confProvider.loadSystemConfig(useUtilityMode=False) 

        # Make sure gpArray and segments are in agreement on current state of system.
        segmentList = gpArray.getSegDbList()
        getVersionCmds = {}
        for seg in segmentList:
            if seg.isSegmentQD() == True:
                continue
            if seg.isSegmentModeInChangeLogging() == False:
                continue
            cmd = gp.SendFilerepTransitionStatusMessage( name = "Get segment status information"
                                                         , msg = gp.SEGMENT_STATUS_GET_STATUS
                                                         , dataDir = seg.getSegmentDataDirectory()
                                                         , port = seg.getSegmentPort()
                                                         , ctxt = gp.REMOTE
                                                         , remoteHost = seg.getSegmentHostName()
                                                        )
            getVersionCmds[seg.getSegmentDbId()] = cmd
            self.__pool.addCommand(cmd)
        self.__pool.join()

        # We can not check to see if the command was successful or not, because gp_primarymirror always returns a non-zero result.
        # That is just the way gp_primarymirror was designed.

        dbsMap = gpArray.getSegDbMap()
        for dbid in getVersionCmds:
            cmd = getVersionCmds[dbid]
            mode         = None
            segmentState = None
            dataState    = None
            try:
                lines = str(cmd.get_results().stderr).split("\n")
                mode         = lines[0].split(": ")[1].strip()
                segmentState = lines[1].split(": ")[1].strip()
                dataState    = lines[2].split(": ")[1].strip()
            except Exception, e:
                self.logger.warning("Problem getting Segment state dbid = %s, results = %s." % (str(dbid), str(cmd.get_results().stderr)))
                continue

            db = dbsMap[dbid]
            if gparray.ROLE_TO_MODE_MAP[db.getSegmentRole()] != mode:
                raise Exception("Inconsistency in catalog and segment Role/Mode. Catalog Role = %s. Segment Mode = %s." % (db.getSegmentRole(), mode))
            if gparray.MODE_TO_DATA_STATE_MAP[db.getSegmentMode()] != dataState:
                raise Exception("Inconsistency in catalog and segment Mode/DataState. Catalog Mode = %s. Segment DataState = %s." % (db.getSegmentMode(), dataState))
            if segmentState != gparray.SEGMENT_STATE_READY and segmentState != gparray.SEGMENT_STATE_CHANGE_TRACKING_DISABLED:
                if segmentState == gparray.SEGMENT_STATE_INITIALIZATION or segmentState == gparray.SEGMENT_STATE_IN_CHANGE_TRACKING_TRANSITION:
                    raise Exception("Segment is not ready for recovery dbid = %s, segmentState = %s. Retry recovery in a few moments" % (str(db.getSegmentDbId()), segmentState))
                else:
                    raise Exception("Segment is in unexpected state. dbid = %s, segmentState = %s." % (str(db.getSegmentDbId()), segmentState))

        # check that we actually have mirrors
        if gpArray.getFaultStrategy() == gparray.FAULT_STRATEGY_SAN:
            self.SanFailback(gpArray, gpEnv)
            return 0
        elif gpArray.getFaultStrategy() == gparray.FAULT_STRATEGY_NONE:
            self.GPSQLFailback(gpArray, gpEnv)
            return 0
        elif gpArray.getFaultStrategy() != gparray.FAULT_STRATEGY_FILE_REPLICATION:
            raise ExceptionNoStackTraceNeeded( \
                    'GPDB Mirroring replication is not configured for this Greenplum Database instance.')

        # We have phys-rep/filerep mirrors.

        if self.__options.outputSpareDataDirectoryFile is not None:
            self.__outputSpareDataDirectoryFile(gpEnv, gpArray, self.__options.outputSpareDataDirectoryFile)
            return 0

        if self.__options.newRecoverHosts is not None:
            try:
                uniqueHosts = []
                [uniqueHosts.append(h.strip()) for h in self.__options.newRecoverHosts.split(',') \
                    if h.strip() not in uniqueHosts ]
                self.__options.newRecoverHosts = uniqueHosts
            except Exception, ex:
                raise ProgramArgumentValidationException(\
                    "Invalid value for recover hosts: %s" % ex)


        # If it's a rebalance operation, make sure we are in an acceptable state to do that
        # Acceptable state is:
        #    - No segments down
        #    - No segments in change tracking or unsynchronized state
        if self.__options.rebalanceSegments:
            if len(gpArray.get_invalid_segdbs()) > 0:
                raise Exception("Down segments still exist.  All segments must be up to rebalance.")
            if len(gpArray.get_synchronized_segdbs()) != len(gpArray.getSegDbList()):
                raise Exception("Some segments are not yet synchronized.  All segments must be synchronized to rebalance.")

        # retain list of hosts that were existing in the system prior to getRecoverActions...
        # this will be needed for later calculations that determine whether
        # new hosts were added into the system
        existing_hosts = set(gpArray.getHostList())

        # figure out what needs to be done
        mirrorBuilder = self.getRecoveryActionsBasedOnOptions(gpEnv, gpArray)

        if self.__options.outputSampleConfigFile is not None:
            # just output config file and done
            self.outputToFile(mirrorBuilder, gpArray, self.__options.outputSampleConfigFile)
            self.logger.info('Configuration file output to %s successfully.' % self.__options.outputSampleConfigFile)
        elif self.__options.rebalanceSegments:
            assert(isinstance(mirrorBuilder,GpSegmentRebalanceOperation))

            # Make sure we have work to do
            if len(gpArray.get_unbalanced_segdbs()) == 0:
                self.logger.info("No segments are running in their non-preferred role and need to be rebalanced.")
            else:           
                self.displayRecovery(mirrorBuilder, gpArray)
                
                if self.__options.interactive:
                    self.logger.warn("This operation will cancel queries that are currently executing.")
                    self.logger.warn("Connections to the database however will not be interrupted.")
                    if not userinput.ask_yesno(None, "\nContinue with segment rebalance procedure", 'N'):
                        raise UserAbortedException()
                
                mirrorBuilder.rebalance()
                
                self.logger.info("******************************************************************")
                self.logger.info("The rebalance operation has completed successfully.")
                self.logger.info("There is a resynchronization running in the background to bring all")
                self.logger.info("segments in sync.")
                self.logger.info("")
                self.logger.info("Use gpstate -s to check the resynchronization progress.")
                self.logger.info("******************************************************************")
            
        elif len(mirrorBuilder.getMirrorsToBuild()) == 0:
            self.logger.info('No segments to recover')
        else:
            mirrorBuilder.checkForPortAndDirectoryConflicts(gpArray)

            self.displayRecovery(mirrorBuilder, gpArray)
            self.__displayRecoveryWarnings(mirrorBuilder)

            if self.__options.interactive:
                if not userinput.ask_yesno(None, "\nContinue with segment recovery procedure", 'N'):
                    raise UserAbortedException()

            # sync packages
            current_hosts = set(gpArray.getHostList())
            new_hosts = current_hosts - existing_hosts
            if new_hosts:
                self.syncPackages(new_hosts)

            mirrorBuilder.buildMirrors("recover", gpEnv, gpArray )
            
            confProvider.sendPgElogFromMaster("Recovery of %d segment(s) has been started." % \
                    len(mirrorBuilder.getMirrorsToBuild()), True)

            self.logger.info("******************************************************************")
            self.logger.info("Updating segments for resynchronization is completed.")
            self.logger.info("For segments updated successfully, resynchronization will continue in the background.")
            self.logger.info("")
            self.logger.info("Use  gpstate -s  to check the resynchronization progress.")
            self.logger.info("******************************************************************")

        return 0 # success -- exit code 0!

    def cleanup(self):
        if self.__pool:
            self.__pool.haltWork()    # \  MPP-13489, CR-2572
            self.__pool.joinWorkers() #  > all three of these appear necessary 
            self.__pool.join()        # /  see MPP-12633, CR-2252 as well

    #-------------------------------------------------------------------------
    @staticmethod
    def createParser():

        description = ("Recover a failed segment")
        help = [""]

        parser = OptParser(option_class=OptChecker,
                    description=' '.join(description.split()),
                    version='%prog version $Revision$')
        parser.setHelp(help)

        addStandardLoggingAndHelpOptions(parser, True)

        addTo = OptionGroup(parser, "Connection Options")
        parser.add_option_group(addTo)
        addMasterDirectoryOptionForSingleClusterProgram(addTo)

        addTo = OptionGroup(parser, "Recovery Source Options")
        parser.add_option_group(addTo)
        addTo.add_option("-i", None, type="string",
                            dest="recoveryConfigFile", 
                            metavar="<configFile>",
                            help="Recovery configuration file")
        addTo.add_option("-o", None,
                            dest="outputSampleConfigFile",
                            metavar="<configFile>", type="string",
                            help="Sample configuration file name to output; "
                                "this file can be passed to a subsequent call using -i option")

        addTo = OptionGroup(parser, "Recovery Destination Options")
        parser.add_option_group(addTo)
        addTo.add_option("-p", None, type="string",
                            dest="newRecoverHosts",
                            metavar="<targetHosts>",
                            help="Spare new hosts to which to recover segments")
        addTo.add_option("-s", None, type="string",
                            dest="spareDataDirectoryFile",
                            metavar="<spareDataDirectoryFile>",
                            help="File listing spare data directories (in filespaceName=path format) on current hosts")
        addTo.add_option("-S", None, type="string",
                            dest="outputSpareDataDirectoryFile",
                            metavar="<outputSpareDataDirectoryFile>",
                            help="Write a sample file to be modified for use by -s <spareDirectoryFile> option")

        addTo = OptionGroup(parser, "Recovery Options")
        parser.add_option_group(addTo)
        addTo.add_option('-F', None, default=False, action='store_true',
                            dest="forceFullResynchronization",
                            metavar="<forceFullResynchronization>",
                            help="Force full segment resynchronization")
        addTo.add_option("-B", None, type="int", default=16,
                            dest="parallelDegree",
                            metavar="<parallelDegree>",
                            help="Max # of workers to use for building recovery segments.  [default: %default]")
        addTo.add_option("-r", None, default=False, action='store_true',
                         dest='rebalanceSegments', help='Rebalance synchronized segments.')

        parser.set_defaults()
        return parser

    @staticmethod
    def createProgram(options, args):
        if len(args) > 0 :
            raise ProgramArgumentValidationException(\
                            "too many arguments: only options may be specified", True)
        return GpRecoverSegmentProgram(options)
    
        
    @staticmethod
    def mainOptions():
        """
        The dictionary this method returns instructs the simple_main framework 
        to check for a gprecoverseg.pid file under MASTER_DATA_DIRECTORY to
        prevent the customer from trying to run more than one instance of
        gprecoverseg at the same time.
        """
        return {'pidfilename':'gprecoverseg.pid', 'parentpidvar':'GPRECOVERPID'}
