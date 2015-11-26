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
# import mainUtils FIRST to get python version check
# THIS IMPORT SHOULD COME FIRST
from gppylib.mainUtils import *

from optparse import Option, OptionGroup, OptionParser, OptionValueError, SUPPRESS_USAGE
import os, sys, getopt, socket, StringIO, signal, copy

from gppylib import gparray, gplog, pgconf, userinput, utils
from gppylib.util import gp_utils
from gppylib.commands import base, gp, pg, unix
from gppylib.db import catalog, dbconn
from gppylib.gpparseopts import OptParser, OptChecker
from gppylib.operations.startSegments import *
from gppylib.operations.buildMirrorSegments import *
from gppylib.programs import programIoUtils
from gppylib.system import configurationInterface as configInterface
from gppylib.system.environment import GpMasterEnvironment
from gppylib.testold.testUtils import *
from gppylib.parseutils import line_reader, parse_filespace_order, parse_gpaddmirrors_line, \
        canonicalize_address
from gppylib.utils import ParsedConfigFile, ParsedConfigFileRow, \
        writeLinesToFile, readAllLinesFromFile, TableLogger, \
        PathNormalizationException, normalizeAndValidateInputPath
from gppylib.gphostcache import GpInterfaceToHostNameCache
from gppylib.userinput import *

logger = gplog.get_default_logger()

def validateFlexibleHeadersListAllFilespaces(configFileLabel, gpArray, fileData):
    """
    Verify that every filespace in the gpArray has exactly one entry in the flexible headers of fileData
    """
    filespaceNameToFilespace = dict([ (fs.getName(), fs) for fs in gpArray.getFilespaces(False)])
    specifiedFilespaces = {}
    for fsName in fileData.getFlexibleHeaders():
        if fsName not in filespaceNameToFilespace:
            raise Exception('%s refers to filespace that does not exist: "%s"' % (configFileLabel, fsName))
        if fsName in specifiedFilespaces:
            raise Exception('%s refers to filespace multiple times: "%s"' % (configFileLabel, fsName))
        specifiedFilespaces[fsName] = True
    if len(fileData.getFlexibleHeaders()) != len(filespaceNameToFilespace):
        raise Exception('%s specified only %d of %d filespaces' %
            (configFileLabel, len(fileData.getFlexibleHeaders()), len(filespaceNameToFilespace)))


class GpMirrorBuildCalculator:
    """
    Create mirror segment (GpDB) objects for an existing array, using different strategies.

    This class should be used by constructing and then calling either getSpreadMirrors, getGroupMirrors, or call
       addMirror multiple times

    The class uses internal state for tracking so cannot be reused after calling getSpreadMirrors or getGroupMirrors
    """

    def __init__(self, gpArray, mirrorPortOffset, mirrorDataDirs, mirrorFilespaceOidToPathMaps):
        self.__gpArray = gpArray
        self.__primaries = [seg for seg in gpArray.getDbList() if seg.isSegmentPrimary(False)]
        self.__primariesByHost = GpArray.getSegmentsByHostName(self.__primaries)
        self.__nextDbId = max([seg.getSegmentDbId() for seg in gpArray.getDbList()]) + 1
        self.__minPrimaryPortOverall = min([seg.getSegmentPort() for seg in self.__primaries])

        def comparePorts(left, right):
            return cmp(left.getSegmentPort(), right.getSegmentPort())

        self.__mirrorsAddedByHost = {} # map hostname to the # of mirrors that have been added to that host
        self.__primariesUpdatedToHaveMirrorsByHost = {} # map hostname to the # of primaries that have been attached to mirrors for that host
        self.__primaryPortBaseByHost = {} # map hostname to the lowest port number in-use by a primary on that host
        for hostName, segments in self.__primariesByHost.iteritems():
            self.__primaryPortBaseByHost[hostName] = min([seg.getSegmentPort() for seg in segments])
            self.__mirrorsAddedByHost[hostName] = 0
            self.__primariesUpdatedToHaveMirrorsByHost[hostName] = 0
            segments.sort(comparePorts)

        self.__mirrorPortOffset = mirrorPortOffset
        self.__mirrorDataDirs = mirrorDataDirs
        self.__mirrorFilespaceOidToPathMaps = mirrorFilespaceOidToPathMaps

        standard, message = self.__gpArray.isStandardArray()
        if standard == False:
           logger.warn('The current system appears to be non-standard.')
           logger.warn(message)
           logger.warn('gpaddmirrors will not be able to symmetrically distribute the new mirrors.')
           logger.warn('It is recommended that you specify your own input file with appropriate values.')
           if not ask_yesno('', "Are you sure you want to continue with this gpaddmirrors session?",'N'):
              logger.info("User Aborted. Exiting...")
              sys.exit(0)
           self.__isStandard = False
        else:
           self.__isStandard = True

    def addMirror(self, resultOut, primary, targetHost, address, port, mirrorDataDir, replicationPort,
            primarySegmentReplicationPort, filespaceOidToPathMap):
        """
        Add a mirror to the gpArray backing this calculator, also update resultOut and do some other checking

        Unlike __addMirrorForTargetHost, this does not require that the segments be added to
               a host that already has a primary
        
        """
        if targetHost not in self.__mirrorsAddedByHost:
            self.__mirrorsAddedByHost[targetHost] = 0
            self.__primaryPortBaseByHost[targetHost] = self.__minPrimaryPortOverall
            self.__primariesUpdatedToHaveMirrorsByHost[targetHost] = 0
            
        mirrorIndexOnTargetHost = self.__mirrorsAddedByHost[targetHost]
        assert mirrorIndexOnTargetHost is not None

        mirror = gparray.GpDB(
                content = primary.getSegmentContentId(),
                preferred_role = gparray.ROLE_MIRROR,
                dbid = self.__nextDbId,
                role = gparray.ROLE_MIRROR,
                mode = gparray.MODE_RESYNCHRONIZATION,
                status = gparray.STATUS_UP,
                hostname = targetHost,
                address = address,
                port = port,
                datadir = mirrorDataDir,
                replicationPort = replicationPort)
        for fsOid, fsPath in filespaceOidToPathMap.iteritems():
            mirror.addSegmentFilespace( fsOid, fsPath )

        self.__gpArray.addSegmentDb(mirror)

        primary.setSegmentReplicationPort(primarySegmentReplicationPort)
        primary.setSegmentMode(gparray.MODE_RESYNCHRONIZATION)

        resultOut.append(GpMirrorToBuild(None, primary, mirror, True))

        self.__primariesUpdatedToHaveMirrorsByHost[primary.getSegmentHostName()] += 1
        self.__mirrorsAddedByHost[targetHost] = mirrorIndexOnTargetHost + 1
        self.__nextDbId += 1

    def __addMirrorForTargetHost(self, resultOut, primary, targetHost ):
        """
        Add a new mirror for the given primary to the targetHost.

        This code assumes that the mirror is added to a host that already has primaries

        Fetches directory path info from the various member variables
        """
        primaryHost = primary.getSegmentHostName()

        assert self.__nextDbId != -1

        primariesOnTargetHost = self.__primariesByHost[targetHost]
        assert primariesOnTargetHost is not None

        primariesOnPrimaryHost = self.__primariesByHost[primaryHost]
        assert primariesOnPrimaryHost is not None

        primaryHostAddressList = []
        for thePrimary in primariesOnPrimaryHost:
            address = thePrimary.getSegmentAddress()
            if address in primaryHostAddressList:
               continue
            primaryHostAddressList.append(address)
        primaryHostAddressList.sort()

        mirrorIndexOnTargetHost = self.__mirrorsAddedByHost[targetHost]
        assert mirrorIndexOnTargetHost is not None

        usedPrimaryIndexOnPrimaryHost = self.__primariesUpdatedToHaveMirrorsByHost[primaryHost]
        assert usedPrimaryIndexOnPrimaryHost is not None

        # find basePort for target host
        basePort = self.__primaryPortBaseByHost[targetHost]
        assert basePort is not None
        basePort += mirrorIndexOnTargetHost

        # find basePort for the primary host
        primaryHostBasePort = self.__primaryPortBaseByHost[primaryHost]
        assert primaryHostBasePort is not None
        primaryHostBasePort += usedPrimaryIndexOnPrimaryHost

        # assign new ports to be used
        port = basePort + self.__mirrorPortOffset
        replicationPort = basePort + self.__mirrorPortOffset * 2
        primarySegmentReplicationPort = primaryHostBasePort + self.__mirrorPortOffset * 3

        if mirrorIndexOnTargetHost >= len(self.__mirrorDataDirs):
            raise Exception("More mirrors targeted to host %s than there are mirror data directories" % targetHost )

        mirrorDataDir = self.__mirrorDataDirs[mirrorIndexOnTargetHost]
        filespaceOidToPathMap = self.__mirrorFilespaceOidToPathMaps[mirrorIndexOnTargetHost]

        # Shared storage path have not been set, do it here.
        for fsOid, fsPath in filespaceOidToPathMap.iteritems():
            if fsPath == None:
                filespaceOidToPathMap[fsOid] = primary.getSegmentFilespaces()[fsOid]

        #
        # We want to spread out use of addresses on a single host so go through primary addresses
        # for the target host to get the mirror address. We also want to put the primary and new
        # mirror on different subnets (i.e. addresses).
        #

        if self.__isStandard == False:
            address = primariesOnTargetHost[mirrorIndexOnTargetHost % len(primariesOnTargetHost)].getSegmentAddress()
        else:
            # This looks like a nice standard system, so we will attempt to distribute the mirrors appropriately.
            # Get a list of all the address on the primary and the mirror and sort them. Take the current primaries
            # index into the primary host list, and add one, and mod it with the number of address on the primary to
            # get the mirror address offset in a sorted list of addresses on the mirror host.

            primaryHostAddressList = []
            for thePrimary in primariesOnPrimaryHost:
                address = thePrimary.getSegmentAddress()
                if address in primaryHostAddressList:
                   continue
                primaryHostAddressList.append(address)
            primaryHostAddressList.sort()

            mirrorHostAddressList = []
            for thePrimary in primariesOnTargetHost:
                address = thePrimary.getSegmentAddress()
                if address in mirrorHostAddressList:
                   continue
                mirrorHostAddressList.append(address)
            mirrorHostAddressList.sort()

            primaryAddress = primary.getSegmentAddress()
            index = 0
            for address in primaryHostAddressList:
                if address == primaryAddress:
                   break
                index = index + 1
            index = (index + 1) % len(primaryHostAddressList)
            address = mirrorHostAddressList[index]

        self.addMirror( resultOut, primary, targetHost, address, port, mirrorDataDir, replicationPort,
                primarySegmentReplicationPort, filespaceOidToPathMap)

    def getGroupMirrors(self):
        """
         Side-effect: self.__gpArray and other fields are updated to contain the returned segments AND
                      to change the replication port values of the primaries as needed
        """

        hosts = self.__primariesByHost.keys()
        hosts.sort()

        result = []
        for hostIndex, primaryHostName in enumerate(hosts):
            primariesThisHost = self.__primariesByHost[primaryHostName]
            targetHost = hosts[(hostIndex + 1) % len(hosts)]

            # for the primary host, build mirrors on the target host
            for i in range(len(primariesThisHost)):
                self.__addMirrorForTargetHost( result, primariesThisHost[i], targetHost)

        return result

    def getSpreadMirrors(self):
        """
         Side-effect: self.__gpArray is updated to contain the returned segments AND to
                      change the replication port values of the primaries as needed
        """

        hosts = self.__primariesByHost.keys()
        hosts.sort()

        result = []
        for hostIndex, primaryHostName in enumerate(hosts):
            primariesThisHost = self.__primariesByHost[primaryHostName]

            hostOffset = 1 # hostOffset is used to put mirrors on primary+1,primary+2,primary+3,...
            for i in range(len(primariesThisHost)):
                targetHostIndex = (hostIndex + hostOffset) % len(hosts)
                if targetHostIndex == hostIndex:
                    hostOffset += 1
                    targetHostIndex = (hostIndex + hostOffset) % len(hosts)
                targetHost = hosts[targetHostIndex]

                self.__addMirrorForTargetHost( result, primariesThisHost[i], targetHost)

                hostOffset += 1
        return result

class GpAddMirrorsProgram:
    """
    The implementation of gpaddmirrors
    
    """
    def __init__(self, options):
        """
        Constructor:

        @param options the options as returned by the options parser

        """
        self.__options = options
        self.__pool = None

    def __getMirrorsToBuildFromConfigFile(self, gpArray):

        # create fileData object from config file
        #
        filename = self.__options.mirrorConfigFile
        fslist   = None
        rows     = []
        with open(filename) as f:
            for lineno, line in line_reader(f):
                if fslist is None:
                    fslist = parse_filespace_order(filename, lineno, line)
                else:
                    fixed, flexible = parse_gpaddmirrors_line(filename, lineno, line, fslist)
                    rows.append( ParsedConfigFileRow(fixed, flexible, line) )
        fileData = ParsedConfigFile(fslist, rows)

        # validate fileData
        #
        validateFlexibleHeadersListAllFilespaces("Mirror config", gpArray, fileData)
        filespaceNameToFilespace = dict([ (fs.getName(), fs) for fs in gpArray.getFilespaces(False)])

        allAddresses = [row.getFixedValuesMap()["address"] for row in fileData.getRows()]
        allNoneArr = [None for a in allAddresses]
        interfaceLookup = GpInterfaceToHostNameCache(self.__pool, allAddresses, allNoneArr)

        #
        # build up the output now
        #
        toBuild = []
        primaries = [seg for seg in gpArray.getDbList() if seg.isSegmentPrimary(current_role=False)]
        segsByContentId = GpArray.getSegmentsByContentId(primaries)

        # note: passed port offset in this call should not matter
        calc = GpMirrorBuildCalculator(gpArray, self.__options.mirrorOffset, [], [])
        
        for row in fileData.getRows():
            fixedValues = row.getFixedValuesMap()
            flexibleValues = row.getFlexibleValuesMap()

            contentId = int(fixedValues['contentId'])
            address = fixedValues['address']
            #
            # read the rest and add the mirror
            #
            port = int(fixedValues['port'])
            replicationPort = int(fixedValues['replicationPort'])
            primarySegmentReplicationPort = int(fixedValues['primarySegmentReplicationPort'])
            dataDir = normalizeAndValidateInputPath( fixedValues['dataDirectory'], "in config file", row.getLine())
            hostName = interfaceLookup.getHostName(address)
            if hostName is None:
                raise Exception("Segment Host Address %s is unreachable" % address)
            
            filespaceOidToPathMap = {}
            for fsName, path in flexibleValues.iteritems():
                path = normalizeAndValidateInputPath( path, "in config file", row.getLine())
                filespaceOidToPathMap[filespaceNameToFilespace[fsName].getOid()] = path

            primary = segsByContentId[contentId]
            if primary is None:
                raise Exception("Invalid content %d specified in input file" % contentId)
            primary = primary[0]

            calc.addMirror(toBuild, primary, hostName, address, port, dataDir, replicationPort, \
                primarySegmentReplicationPort, filespaceOidToPathMap )

        if len(toBuild) != len(primaries):
            raise Exception("Wrong number of mirrors specified (specified %s mirror(s) for %s primarie(s))" % \
                    (len(toBuild), len(primaries)))

        return GpMirrorListToBuild(toBuild, self.__pool, self.__options.quiet, self.__options.parallelDegree)

    def __outputToFile(self, mirrorBuilder, file, gpArray):
        """
        """
        lines = []

        #
        # first line is always the filespace order
        #
        filespaceArr = [fs for fs in gpArray.getFilespaces(False)]
        lines.append("filespaceOrder=" + (":".join([fs.getName() for fs in filespaceArr])))

        #
        # now a line for each mirror 
        #
        for i, toBuild in enumerate(mirrorBuilder.getMirrorsToBuild()):
            mirror = toBuild.getFailoverSegment()
            primary = toBuild.getLiveSegment()

            #
            # build up   :path1:path2   for the mirror segment's filespace paths
            #
            mirrorFilespaces = mirror.getSegmentFilespaces()
            filespaceValues = []
            for fs in filespaceArr :
                path = mirrorFilespaces.get(fs.getOid())
                assert path is not None   # checking consistency should have been done earlier, but doublecheck here
                filespaceValues.append(":" + canonicalize_address(path))

            line = 'mirror%d=%d:%s:%d:%d:%d:%s%s' % \
                    (i, \
                    mirror.getSegmentContentId(), \
                    canonicalize_address( mirror.getSegmentAddress() ), \
                    mirror.getSegmentPort(), \
                    mirror.getSegmentReplicationPort(), \
                    primary.getSegmentReplicationPort(), \
                    mirror.getSegmentDataDirectory(),
                    "".join(filespaceValues))

            lines.append(line)
        writeLinesToFile(self.__options.outputSampleConfigFile, lines)

    def __getDataDirectoriesForMirrors(self, maxPrimariesPerHost, gpArray):
        dirs = []
        filespaceOidToPathMaps = []
        while len(filespaceOidToPathMaps) < maxPrimariesPerHost:
            filespaceOidToPathMaps.append({})

        filespaceNameToOid = {}
        for fs in gpArray.getFilespaces(False):
            filespaceNameToOid[fs.getName()] = fs.getOid()

        configFile = self.__options.mirrorDataDirConfigFile
        if configFile is not None:

            #
            # load from config file
            #
            lines = readAllLinesFromFile(configFile, stripLines=True, skipEmptyLines=True)

            labelOfPathsBeingRead = "data"
            index = 0
            fsOid = gparray.SYSTEM_FILESPACE
            enteredFilespaces = {}
            for line in lines:
                if line.startswith("filespace "):
                    if index < maxPrimariesPerHost:
                        raise Exception('Number of %s directories must equal %d but %d were read from %s' % \
                            (labelOfPathsBeingRead, maxPrimariesPerHost, index, configFile))

                    fsName = line[len("filespace "):].strip()
                    labelOfPathsBeingRead = fsName

                    if fsName not in filespaceNameToOid:
                        raise Exception("Unknown filespace %s specified in input file %s" % \
                            (fsName, configFile))
                    fsOid = filespaceNameToOid[fsName]
                    
                    if gpArray.isFileSpaceShared(fsOid):
                        raise Exception("Shared filespace %s does not need to specify in input file %s" % \
                            (fsName, configFile))

                    if fsName in enteredFilespaces:
                        raise Exception( "Filespace %s specified twice in input file %s" % \
                            (fsName, configFile))
                    enteredFilespaces[fsName] = True

                    index = 0
                else:
                    if index == maxPrimariesPerHost:
                        raise Exception('Number of %s directories must equal %d but more were read from %s' % \
                            (labelOfPathsBeingRead, maxPrimariesPerHost, configFile))

                    path = normalizeAndValidateInputPath(line, "config file")
                    if fsOid == gparray.SYSTEM_FILESPACE:
                        dirs.append(path)
                    else:
                        filespaceOidToPathMaps[index][fsOid] = path
                    index += 1
            if index < maxPrimariesPerHost:
                raise Exception('Number of %s directories must equal %d but %d were read from %s' % \
                    (labelOfPathsBeingRead, maxPrimariesPerHost, index, configFile))

            # The shared filespace path will set later.
            for (fsName, fsOid) in filespaceNameToOid.iteritems():
                if fsName in enteredFilespaces:
                    continue
                if not gpArray.isFileSpaceShared(fsOid):
                    break
                for index in range(maxPrimariesPerHost):
                    filespaceOidToPathMaps[index][fsOid] = None
                enteredFilespaces[fsName] = True

            if len(enteredFilespaces) != len(filespaceNameToOid):
                raise Exception("Only read directories for %d of %d filespaces from %s" % \
                    (len(enteredFilespaces), len(filespaceNameToOid), configFile))
        else:

            #
            # get from stdin
            #
            while len(dirs) < maxPrimariesPerHost:
                print 'Enter mirror segment data directory location %d of %d >' % (len(dirs)+1, maxPrimariesPerHost)
                line = sys.stdin.readline().strip()
                if len(line) > 0:
                    try:
                        dirs.append(normalizeAndValidateInputPath(line))
                    except PathNormalizationException, e:
                        print "\n%s\n" % e


            for fs in gpArray.getFilespaces(False):
                # Shared storage path can only be set when we know the
                # relationship between the primary and mirror.
                fsys = fs.getFsys()
                delay_set = False
                if fsys != None and fsys.isShared():
                    delay_set = True
                    print 'Skip filespace "%s" stored on the shared filesystem "%s"' % (fs.getName(), fsys.getName())

                index = 0
                while index < maxPrimariesPerHost:
                    if delay_set:
                        filespaceOidToPathMaps[index][fs.getOid()] = None
                        index += 1
                        continue

                    print "Enter mirror filespace '%s' directory location %d of %d >" % \
                                (fs.getName(), index+1, maxPrimariesPerHost)
                    line = sys.stdin.readline().strip()
                    if len(line) > 0:
                        try:
                            filespaceOidToPathMaps[index][fs.getOid()] = normalizeAndValidateInputPath(line)
                            index += 1
                        except PathNormalizationException, e:
                            print "\n%s\n" % e

        return (dirs, filespaceOidToPathMaps)

    def __generateMirrorsToBuild(self, gpEnv, gpArray):
        toBuild = []

        maxPrimariesPerHost = 0
        segments = [seg for seg in gpArray.getDbList() if seg.isSegmentPrimary(False)]
        for hostName, hostSegments in GpArray.getSegmentsByHostName(segments).iteritems():
            if len(hostSegments) > maxPrimariesPerHost:
                maxPrimariesPerHost = len(hostSegments)

        (dataDirs, filespaceOidToPathMaps) = self.__getDataDirectoriesForMirrors(maxPrimariesPerHost, gpArray)
        calc = GpMirrorBuildCalculator(gpArray, self.__options.mirrorOffset, dataDirs, filespaceOidToPathMaps)
        if self.__options.spreadMirroring:
            toBuild = calc.getSpreadMirrors()
        else:
            toBuild = calc.getGroupMirrors()

        gpPrefix = gp_utils.get_gp_prefix(gpEnv.getMasterDataDir())
        if not gpPrefix:
            gpPrefix = 'gp'

        for mirToBuild in toBuild:
            # mirToBuild is a GpMirrorToBuild object
            mir = mirToBuild.getFailoverSegment()

            dataDir = utils.createSegmentSpecificPath(mir.getSegmentDataDirectory(), gpPrefix, mir)
            mir.setSegmentDataDirectory(dataDir)

            # Shard storage path cannot be changed!
            fsMap = mir.getSegmentFilespaces()
            for oid, path in copy.copy(fsMap).iteritems():
                if gpArray.isFileSpaceShared(oid):
                    fsMap[oid] = path
                else:
                    fsMap[oid] = utils.createSegmentSpecificPath( path, gpPrefix, mir)
            fsMap[gparray.SYSTEM_FILESPACE] = dataDir

        return GpMirrorListToBuild(toBuild, self.__pool, self.__options.quiet, self.__options.parallelDegree)

    def __getMirrorsToBuildBasedOnOptions(self, gpEnv, gpArray):
        """
        returns a GpMirrorListToBuild object
        """

        if self.__options.mirrorConfigFile is not None:
            return self.__getMirrorsToBuildFromConfigFile(gpArray)
        else:
            return self.__generateMirrorsToBuild(gpEnv, gpArray)

    def __displayAddMirrors(self, gpEnv, mirrorBuilder, gpArray):
        logger.info('Greenplum Add Mirrors Parameters')
        logger.info('---------------------------------------------------------')
        logger.info('Greenplum master data directory          = %s' % gpEnv.getMasterDataDir())
        logger.info('Greenplum master port                    = %d' % gpEnv.getMasterPort())
        logger.info('Parallel batch limit                     = %d' % self.__options.parallelDegree )

        total = len(mirrorBuilder.getMirrorsToBuild())
        for i, toRecover in enumerate(mirrorBuilder.getMirrorsToBuild()):
            logger.info('---------------------------------------------------------')
            logger.info('Mirror %d of %d' % (i+1, total))
            logger.info('---------------------------------------------------------')

            tabLog = TableLogger()
            programIoUtils.appendSegmentInfoForOutput("Primary", gpArray, toRecover.getLiveSegment(), tabLog)
            programIoUtils.appendSegmentInfoForOutput("Mirror", gpArray, toRecover.getFailoverSegment(), tabLog)
            tabLog.outputTable()

        logger.info('---------------------------------------------------------')

    def checkMirrorOffset(self, gpArray):
        """
        return an array of the ports to use to begin mirror port, replication port, and mirror replication port
        """

        maxAllowedPort = 61000
        minAllowedPort = 7000

        minPort = min([seg.getSegmentPort() for seg in gpArray.getDbList()])
        maxPort = max([seg.getSegmentPort() for seg in gpArray.getDbList()])

        if self.__options.mirrorOffset < 0:
            minPort = minPort + 3 * self.__options.mirrorOffset
            maxPort = maxPort + self.__options.mirrorOffset
        else:
            minPort = minPort + self.__options.mirrorOffset
            maxPort = maxPort + 3 * self.__options.mirrorOffset

        if maxPort > maxAllowedPort or minPort < minAllowedPort:
            raise ProgramArgumentValidationException( \
                'Value of port offset supplied via -p option produces ports outside of the valid range' \
                'Mirror port base range must be between %d and %d' % (minAllowedPort, maxAllowedPort))

    def run(self):
        if self.__options.parallelDegree < 1 or self.__options.parallelDegree > 64:
            raise ProgramArgumentValidationException(
                    "Invalid parallelDegree provided with -B argument: %d" % self.__options.parallelDegree)

        self.__pool = base.WorkerPool(self.__options.parallelDegree)
        gpEnv = GpMasterEnvironment(self.__options.masterDataDirectory, True)

        faultProberInterface.getFaultProber().initializeProber(gpEnv.getMasterPort())
        confProvider = configInterface.getConfigurationProvider().initializeProvider(gpEnv.getMasterPort())
        gpArray = confProvider.loadSystemConfig(useUtilityMode=False)

        # check that we actually have mirrors
        if gpArray.getFaultStrategy() != gparray.FAULT_STRATEGY_NONE:
            raise ExceptionNoStackTraceNeeded( \
                    "GPDB physical mirroring cannot be added.  The cluster is already configured with %s." % \
                    gparray.getFaultStrategyLabel(gpArray.getFaultStrategy()))

        # figure out what needs to be done
        mirrorBuilder = self.__getMirrorsToBuildBasedOnOptions(gpEnv, gpArray )
        mirrorBuilder.checkForPortAndDirectoryConflicts(gpArray)

        if self.__options.outputSampleConfigFile is not None:
          # just output config file and done
            self.__outputToFile(mirrorBuilder, self.__options.outputSampleConfigFile, gpArray)
            logger.info('Configuration file output to %s successfully.' % self.__options.outputSampleConfigFile)
        else:
            self.__displayAddMirrors(gpEnv, mirrorBuilder, gpArray )
            if self.__options.interactive:
                if not userinput.ask_yesno(None, "\nContinue with add mirrors procedure", 'N'):
                    raise UserAbortedException()

            gpArray.setFaultStrategy(gparray.FAULT_STRATEGY_FILE_REPLICATION)
            mirrorBuilder.buildMirrors("add", gpEnv, gpArray )

            logger.info("******************************************************************")
            logger.info("Mirror segments have been added; data synchronization is in progress.")
            logger.info("Data synchronization will continue in the background.")
            logger.info("")
            logger.info("Use  gpstate -s  to check the resynchronization progress.")
            logger.info("******************************************************************")
                
        return 0 # success -- exit code 0!

    def cleanup(self):
        if self.__pool:
            self.__pool.haltWork()

        #-------------------------------------------------------------------------
    @staticmethod
    def createParser():

        description = ("Add mirrors to a system")
        help = [""]

        parser = OptParser(option_class=OptChecker,
                           description=' '.join(description.split()),
                           version='%prog version $Revision$')
        parser.setHelp(help)

        addStandardLoggingAndHelpOptions(parser, True)

        addTo = OptionGroup(parser, "Connection Options")
        parser.add_option_group(addTo)
        addMasterDirectoryOptionForSingleClusterProgram(addTo)

        addTo = OptionGroup(parser, "Mirroring Options")
        parser.add_option_group(addTo)
        addTo.add_option("-i", None, type="string",
                         dest="mirrorConfigFile",
                         metavar="<configFile>",
                         help="Mirroring configuration file")

        addTo.add_option("-o", None,
                         dest="outputSampleConfigFile",
                         metavar="<configFile>", type="string",
                         help="Sample configuration file name to output; "
                         "this file can be passed to a subsequent call using -i option")
        
        addTo.add_option("-m", None, type="string",
                         dest="mirrorDataDirConfigFile",
                         metavar="<dataDirConfigFile>",
                         help="Mirroring data directory configuration file")

        addTo.add_option('-s', default=False, action='store_true',
                         dest="spreadMirroring" ,
                         help="use spread mirroring for placing mirrors on hosts")

        addTo.add_option("-p", None, type="int", default=1000,
                         dest="mirrorOffset",
                         metavar="<mirrorOffset>",
                         help="Mirror port offset.  The mirror port offset will be used multiple times "
                         "to derive three sets of ports [default: %default]")

        addTo.add_option("-B", None, type="int", default=16,
                         dest="parallelDegree",
                         metavar="<parallelDegree>",
                         help="Max # of workers to use for building recovery segments.  [default: %default]")

        parser.set_defaults()
        return parser

    @staticmethod
    def createProgram(options, args):
        raise ExceptionNoStackTraceNeeded("add mirrors NOT SUPPORTED YET IN GPSQL")
        if len(args) > 0 :
            raise ProgramArgumentValidationException("too many arguments: only options may be specified", True)
        return GpAddMirrorsProgram(options)


