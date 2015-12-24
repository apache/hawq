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
  ComputeCatalogUpdate.py

  Used by updateSystemConfig() to compare the db state and 
  goal state of a HAWQArray containing the Greenplum segment 
  configruation details and computes appropriate changes.
"""
import copy
from gppylib.gplog import *
from hawqpylib.hawqarray import ROLE_MASTER, ROLE_STANDBY, ROLE_PRIMARY
from hawqpylib.hawqarray import MASTER_REGISTRATION_ORDER
from hawqpylib import hawqarray

logger = get_default_logger()

class ComputeCatalogUpdate:
    """
    Helper class for GpConfigurationProvider.updateSystemConfig().
    
    This computes seven lists of GpDb objects (commonly referenced as 'seg') 
    from a HAWQArray, reflecting the logical changes that need to be made
    to the database catalog to make it match the system as defined.
    The names of the lists are reasonably descriptive:

    mirror_to_remove	     - to be removed (e.g. via gp_remove_segment_mirror())
    primary_to_remove	     - to be removed (e.g. via gp_remove_segment())
    primary_to_add	     - to be added (e.g. via gp_add_segment())
    mirror_to_add	     - to be added (e.g. via gp_add_segment_mirror())
    mirror_to_remove_and_add - change or force list requires this mirror
                                to be removed and then added back
    segment_to_update	     - to be updated (e.g. via SQL)
    segment_unchanged	     - needs no update (included for validation)
    """

    def __init__(self, hawqArray, forceMap, useUtilityMode, allowPrimary):
        """
        This class just holds lists of objects in the underlying hawqArray.  
        As such, it has no methods - the constructor does all the computation.

        @param hawqArray the array containing the goal and db segment states.
        @param forceMap a map of dbid->True for mirrors for which we should force updating via remove/add
        @param useUtilityMode True if the operations we're doing are expected to run via utility moed
        @param allowPrimary True if caller authorizes add/remove primary operations (e.g. gpexpand)
        """

        forceMap = forceMap or {}
        self.useUtilityMode = useUtilityMode
        self.allowPrimary = allowPrimary

        # 'dbsegmap' reflects the current state of the catalog
        self.dbsegmap    = dict([(seg.getSegmentDbId(), seg) for seg in hawqArray.getSegmentsAsLoadedFromDb()])

        # 'goalsegmap' reflects the desired state of the catalog
        self.goalsegmap  = dict([(seg.getSegmentDbId(), seg) for seg in hawqArray.getDbList(includeExpansionSegs=True)])

        # find mirrors and primaries to remove
        self.mirror_to_remove = [
            seg for seg in self.dbsegmap.values()		        # segment in database
                 if seg.isSegmentMirror()                               # segment is a mirror
                and (seg.getSegmentDbId() not in self.goalsegmap)       # but not in goal configuration
        ]
        self.debuglog("mirror_to_remove:          %s", self.mirror_to_remove)

        self.primary_to_remove = [
            seg for seg in self.dbsegmap.values()              		# segment is database
                 if seg.isSegmentPrimary()                      	# segment is a primary
                and (seg.getSegmentDbId() not in self.goalsegmap)       # but not in goal configuration
        ]
        self.debuglog("primary_to_remove:         %s", self.primary_to_remove)

        # find primaries and mirrors to add
        self.primary_to_add = [
            seg for seg in self.goalsegmap.values()			# segment in goal configuration
                 if seg.isSegmentPrimary()				# segment is a primary
                and (seg.getSegmentDbId() not in self.dbsegmap)		# but not in the database
        ]
        self.debuglog("primary_to_add:            %s", self.primary_to_add)

        self.mirror_to_add = [
            seg for seg in self.goalsegmap.values()			# segment in goal configuration
                 if seg.isSegmentMirror()				# segment is a mirror
                and (seg.getSegmentDbId() not in self.dbsegmap)		# but not in the database
        ]
        self.debuglog("mirror_to_add:             %s", self.mirror_to_add)

        # find segments to update
        initial_segment_to_update = [
            seg for seg in self.goalsegmap.values()			# segment in goal configuration
                 if (seg.getSegmentDbId() in self.dbsegmap)          	# and also in the database 
                and (seg != self.dbsegmap[ seg.getSegmentDbId() ])   	# but some attributes differ
        ]
        self.debuglog("initial_segment_to_update: %s", initial_segment_to_update)

        # create a map of the segments which we can't update in the 
        # ordinary way either because they were on the forceMap or
        # they differ in an attribute other than mode, status or replication port
        removeandaddmap = {}
        for seg in initial_segment_to_update:
            dbid = seg.getSegmentDbId()
            if dbid in forceMap:
                removeandaddmap[dbid] = seg
                continue
            # In GPSQL, no needs to remove the original segment, updating the hostname and address is enough.
            if hawqArray.getFaultStrategy() == hawqarray.FAULT_STRATEGY_NONE:
                continue
            if not seg.equalIgnoringStatus(self.dbsegmap[dbid]):
                removeandaddmap[dbid] = seg
                continue

        # create list of mirrors to update via remove/add
        self.mirror_to_remove_and_add = [seg for seg in removeandaddmap.values()]
        self.debuglog("mirror_to_remove_and_add:  %s", self.mirror_to_remove_and_add)

        # find segments to update in the ordinary way
        self.segment_to_update = [
            seg for seg in initial_segment_to_update			# segments to update
                 if seg.getSegmentDbId() not in removeandaddmap 	# that don't require remove/add
        ]
        self.debuglog("segment_to_update:         %s", self.segment_to_update)

        # find segments that don't need change
        self.segment_unchanged = [
            seg for seg in self.goalsegmap.values()			# segment in goal configuration
                 if (seg.getSegmentDbId() in self.dbsegmap)          	# and also in the database 
                and (seg == self.dbsegmap[ seg.getSegmentDbId() ])   	# and attribtutes are all the same
        ]
        self.debuglog("segment_unchanged:         %s", self.segment_unchanged)


                
    def final_segments(self):
        """
        Generate a series of segments appearing in the final configuration.
        """
        for seg in self.primary_to_add:
            yield seg
        for seg in self.mirror_to_add:
            yield seg
        for seg in self.mirror_to_remove_and_add:
            yield seg
        for seg in self.segment_to_update:
            yield seg
        for seg in self.segment_unchanged:
            yield seg


    def validate(self):
        """
        Check that the operation and new configuration is valid.
        """

        # Validate that we're not adding or removing primaries unless authorized
        #
        if not self.allowPrimary:
            if len(self.primary_to_add) > 0:
                p = self.primary_to_add[0]
                raise Exception("Internal error: Operation may not add primary: %s" % repr(p))

            if len(self.primary_to_remove) > 0:
                p = self.primary_to_remove[0]
                raise Exception("Internal error: Operation may not remove primary: %s" % repr(p))


        # Validate that operations do not result in a contentid with a pair of segments in same preferred role
        #
        final = { ROLE_PRIMARY:{}, ROLE_MIRROR:{} }
        for seg in self.final_segments():
            subset = final[ seg.getSegmentPreferredRole() ]
            other  = subset.get( seg.getSegmentContentId() )
            if other is not None:
                raise Exception("Segments sharing a content id may not have same preferred role: %s and %s" % (repr(seg), repr(other)))
            subset[ seg.getSegmentContentId() ] = seg


        # Validate that if we have any mirrors, that all primaries have mirrors
        # If there is only one standby in mirror, skip this check.
        check_mirror_sanity = len(final[ ROLE_MIRROR ]) > 0 and len([ contentId for contentId in final[ ROLE_MIRROR] if contentId != MASTER_CONTENT_ID ]) > 0
        if check_mirror_sanity:
            for contentId in final[ ROLE_PRIMARY ]:
                if contentId != MASTER_CONTENT_ID and final[ ROLE_MIRROR ].get( contentId ) is None:
                    seg = final[ ROLE_PRIMARY ][ contentId ]
                    raise Exception("Primary must have mirror when mirroring enabled: %s" % repr(seg))


        # Validate that the remove/add list contains only qualified mirrors.
        # In particular, we disallow remove/add of the master, standby or a primary.
        #
        for seg in self.mirror_to_remove_and_add:
            originalSeg = self.dbsegmap.get(seg.getSegmentDbId())

            # filespace and other core has changed, or it's a mirror and we are recovering full
            #     (in which case we want to call removeMirror and addMirror so we mark
            #      the primary as full-resyncing)
            #
            if seg.isSegmentMaster(current_role=True) or seg.isSegmentStandby():

                #
                # Assertion here -- user should not be allowed to change master/standby info.
                #
                raise Exception("Internal error: Can only change core details of segments, not masters" \
                                " (on segment %s) (seg %s vs original %s)" %
                                (seg.getSegmentDbId(), repr(seg), repr(originalSeg)))

            if not seg.isSegmentMirror(current_role=True):
                #
                # Assertion here -- user should not be allowed to change primary info.
                #
                return
                raise Exception("Internal error: Can only change core details of mirrors, not primaries" \
                                " (on segment %s) (seg %s vs original %s)" %
                                (seg.getSegmentDbId(), repr(seg), repr(originalSeg)))

            if self.useUtilityMode:
                raise Exception("Cannot change core details of mirrors in utility mode")



    def debuglog(self, msg, seglist):
        """
        Write debugging details about the specified segment list.
        """
        logger.debug(msg % ("%s segments" % len(seglist)))
        for seg in seglist:
            logger.debug(msg % repr(seg))




# minimal test framework when run from command line
#
if __name__ == '__main__':
    
    ROLE_PRIMARY = 'p'
    ROLE_MIRROR  = 'm'
    
    MODE_NOT_INITIALIZED = ''
    MODE_CHANGELOGGING = 'c'
    MODE_SYNCHRONIZED = 's'
    MODE_RESYNCHRONIZATION = 'r'

    class GpDb:
        def __init__(self,dbid,content,pref,mode='',curr=None,status='u',rport=0,misc=None):
            self.dbid           = dbid
            self.content        = content
            self.preferred_role = pref
            self.mode           = mode  
            self.role           = curr or pref
            self.status         = status
            self.rport          = rport 
            self.misc           = misc  
        def getSegmentDbId(self):                  return self.dbid
        def getSegmentContentId(self):             return self.content
        def getSegmentPreferredRole(self):         return self.preferred_role
        def getSegmentMode(self):                  return self.mode
        def getSegmentRole(self):                  return self.role
        def getSegmentStatus(self):                return self.status
        def getSegmentReplicationPort(self):       return self.rport
        def setSegmentMode(self,mode):             self.mode = mode
        def setSegmentStatus(self,status):         self.status = status
        def setSegmentReplicationPort(self,rport): self.rport = rport
        def isSegmentPrimary(self, current_role=False):
            role = self.role if current_role else self.preferred_role
            return self.content >= 0 and role == ROLE_PRIMARY
        def isSegmentMirror(self, current_role=False):
            role = self.role if current_role else self.preferred_role
            return self.content >= 0 and role == ROLE_MIRROR
        def isSegmentMaster(self, current_role=False):
            role = self.role if current_role else self.preferred_role
            return self.content < 0 and role == ROLE_PRIMARY
        def isSegmentStandby(self):
            role = self.role if current_role else self.preferred_role
            return self.content < 0 and role == ROLE_MIRROR
        def __cmp__(self,other):                   return cmp(repr(self), repr(other))
        def __repr__(s): 
            return '(%s,%s,%s,%s,%s,%s,%s,%s)' % (s.dbid, s.content, s.preferred_role, s.mode, s.role, s.status, s.rport, s.misc)
        def equalIgnoringStatus(self, other):
            tmp = copy.copy(self)
            tmp.setSegmentMode( other.getSegmentMode() )
            tmp.setSegmentStatus( other.getSegmentStatus() )
            tmp.setSegmentReplicationPort( other.getSegmentReplicationPort() )
            return tmp == other

    class xxx:
        def xxx():
            print dbsegmap
            print goalsegmap
            print 'db not goal', [seg for seg in dbsegmap.values()   if seg.getSegmentDbId() not in goalsegmap]
            print 'goal not db', [seg for seg in goalsegmap.values() if seg.getSegmentDbId() not in dbsegmap]
    
    class HAWQArray:
        def __init__(s, forceMap=None, useUtilityMode=False, allowPrimary=True):
            s.c = ComputeCatalogUpdate(s,forceMap,useUtilityMode,allowPrimary)
            s.dump()
        def dump(s):
            print s.__class__.__name__, s.__class__.__doc__
            s.c.validate()
            print " -m", s.c.mirror_to_remove,
            print " -p", s.c.primary_to_remove,
            print " +p", s.c.primary_to_add,
            print " +m", s.c.mirror_to_add,
            print " +/-m", s.c.mirror_to_remove_and_add,
            print " u",  s.c.segment_to_update,
            print " n",  s.c.segment_unchanged
        def __repr__(s):
            return '<%s,%s>' % (s.getDbList(), s.getSegmentsAsLoadedFromDb())
    
    class HAWQArrayBad(HAWQArray):
        def __init__(s, forceMap=None, useUtilityMode=False, allowPrimary=True):
            try:
                HAWQArray.__init__(s,forceMap,useUtilityMode,allowPrimary)
                print " ERROR: expected exception"
            except Exception, e:
                print " EXPECTED: ", str(e)

    class HAWQArray1(HAWQArray):
        "expect no change"
        def getDbList(self,includeExpansionSegs): return [GpDb(1,1,'p'), GpDb(2,1,'m')]
        def getSegmentsAsLoadedFromDb(self):      return [GpDb(1,1,'p'), GpDb(2,1,'m')]
    HAWQArray1()
    
    class HAWQArray1a(HAWQArray):
        "expect update of mirror"
        def getDbList(self,includeExpansionSegs): return [GpDb(1,1,'p'), GpDb(2,1,'m','a')]
        def getSegmentsAsLoadedFromDb(self):      return [GpDb(1,1,'p'), GpDb(2,1,'m')]
    HAWQArray1a()
    
    class HAWQArray2(HAWQArray):
        "expect add mirror"
        def getDbList(self,includeExpansionSegs): return [GpDb(1,1,'p','a'), GpDb(2,1,'m','a')]
        def getSegmentsAsLoadedFromDb(self):      return [GpDb(1,1,'p')]
    HAWQArray2()
    
    class HAWQArray3(HAWQArray):
        "expect remove mirror"
        def getDbList(self,includeExpansionSegs): return [GpDb(1,1,'p')]
        def getSegmentsAsLoadedFromDb(self):      return [GpDb(1,1,'p'), GpDb(2,1,'m','a')]
    HAWQArray3()
    
    class HAWQArray4(HAWQArray):
        "expect add primary and mirror"
        def getDbList(self,includeExpansionSegs): return [GpDb(1,1,'p'), GpDb(2,1,'m'), GpDb(3,2,'p'), GpDb(4,2,'m')]
        def getSegmentsAsLoadedFromDb(self):      return [GpDb(1,1,'p'), GpDb(2,1,'m')]
    HAWQArray4()

    class HAWQArray5(HAWQArray):
        "expect remove primary/mirror and add/primary mirror"
        def getDbList(self,includeExpansionSegs): return [GpDb(3,2,'p'), GpDb(4,2,'m')]
        def getSegmentsAsLoadedFromDb(self):      return [GpDb(1,1,'p'), GpDb(2,1,'m','a')]
    HAWQArray5()

    class HAWQArray6(HAWQArray):
        "expect update via add/remove"
        def getDbList(self,includeExpansionSegs): return [GpDb(1,1,'p'), GpDb(2,1,'m')]
        def getSegmentsAsLoadedFromDb(self):      return [GpDb(1,1,'p'), GpDb(2,1,'m',misc='x')]
    HAWQArray6()
    
    class HAWQArray7(HAWQArrayBad):
        "can't rely on remove/add for primary"
        def getDbList(self,includeExpansionSegs): return [GpDb(1,1,'p',misc='x'), GpDb(2,1,'m'), GpDb(3,2,'p'), GpDb(4,2,'m')]
        def getSegmentsAsLoadedFromDb(self):      return [GpDb(1,1,'p'), GpDb(2,1,'m')]
    HAWQArray7()

    class HAWQArray8(HAWQArrayBad):
        "can't rely on remove/add for master"
        def getDbList(self,includeExpansionSegs): return [GpDb(0,-1,'p',misc="x"), GpDb(1,1,'p'), GpDb(2,1,'m')]
        def getSegmentsAsLoadedFromDb(self):      return [GpDb(0,-1,'p'), GpDb(1,1,'p'), GpDb(2,1,'m')]
    HAWQArray8()

    class HAWQArray9(HAWQArrayBad):
        "can't rely on remove/add for master"
        def __init__(s): HAWQArrayBad.__init__(s,[0])
        def getDbList(self,includeExpansionSegs): return [GpDb(0,-1,'p',rport=2), GpDb(1,1,'p'), GpDb(2,1,'m')]
        def getSegmentsAsLoadedFromDb(self):      return [GpDb(0,-1,'p'), GpDb(1,1,'p'), GpDb(2,1,'m')]
    HAWQArray9()

    class HAWQArray10(HAWQArray):
        "expect update"
        def getDbList(self,includeExpansionSegs): return [GpDb(1,1,'p'), GpDb(2,1,'m',rport=2)]
        def getSegmentsAsLoadedFromDb(self):      return [GpDb(1,1,'p'), GpDb(2,1,'m')]
    HAWQArray10()

    class HAWQArray11(HAWQArray):
        "expect update via add/remove"
        def __init__(s): HAWQArray.__init__(s,[2])
        def getDbList(self,includeExpansionSegs): return [GpDb(1,1,'p'), GpDb(2,1,'m',rport=2)]
        def getSegmentsAsLoadedFromDb(self):      return [GpDb(1,1,'p'), GpDb(2,1,'m')]
    HAWQArray11()
    
    class HAWQArray12(HAWQArrayBad):
        "can't add primary"
        def __init__(s): HAWQArrayBad.__init__(s,allowPrimary=False)
        def getDbList(self,includeExpansionSegs): return [GpDb(1,1,'p'), GpDb(2,1,'m'), GpDb(3,2,'p'), GpDb(4,2,'m')]
        def getSegmentsAsLoadedFromDb(self):      return [GpDb(1,1,'p'), GpDb(2,1,'m')]
    HAWQArray12()
    
    class HAWQArray13(HAWQArrayBad):
        "can't remove primary"
        def __init__(s): HAWQArrayBad.__init__(s,allowPrimary=False)
        def getDbList(self,includeExpansionSegs): return [GpDb(2,1,'m')]
        def getSegmentsAsLoadedFromDb(self):      return [GpDb(1,1,'p'), GpDb(2,1,'m')]
    HAWQArray13()
    
    class HAWQArray14(HAWQArrayBad):
        "can't have pair in same preferred role"
        def __init__(s): HAWQArrayBad.__init__(s)
        def getDbList(self,includeExpansionSegs): return [GpDb(1,1,'p'), GpDb(2,1,'p')]
        def getSegmentsAsLoadedFromDb(self):      return [GpDb(1,1,'p'), GpDb(2,1,'m')]
    HAWQArray14()
    
    class HAWQArray15(HAWQArrayBad):
        "can't have pair in same preferred role"
        def __init__(s): HAWQArrayBad.__init__(s)
        def getDbList(self,includeExpansionSegs): return [GpDb(1,1,'m'), GpDb(2,1,'m')]
        def getSegmentsAsLoadedFromDb(self):      return [GpDb(1,1,'p'), GpDb(2,1,'m')]
    HAWQArray15()
    
    class HAWQArray16(HAWQArrayBad):
        "all primaries must have mirrors when mirroring"
        def __init__(s): HAWQArrayBad.__init__(s)
        def getDbList(self,includeExpansionSegs): return [GpDb(1,1,'p'), GpDb(2,1,'m'), GpDb(3,2,'p')]
        def getSegmentsAsLoadedFromDb(self):      return [GpDb(1,1,'p'), GpDb(2,1,'m')]
    HAWQArray16()
