/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*-------------------------------------------------------------------------
 *
 * cdbutil.h
 *	  Header file for routines in cdbutil.c and results returned by
 *	  those routines.
 *
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBUTIL_H
#define CDBUTIL_H

/*
 * Copied from geqo_random.h (deprecated)
 */

#include <math.h>

/* cdb_rand returns a random float value between 0 and 1 inclusive */
#define cdb_rand() ((double) random() / (double) MAX_RANDOM_VALUE)

/* cdb_randint returns integer value between lower and upper inclusive */
#define cdb_randint(upper,lower) \
	( (int) floor( cdb_rand()*(((upper)-(lower))+0.999999) ) + (lower) )


#include "c.h"
#include "nodes/pg_list.h"

/* --------------------------------------------------------------------------------------------------
 * Structure for MPP 2.0 database information
 *
 * The information contained in this structure represents logically a row from
 * gp_configuration.  It is used to describe either an entry
 * database or a segment database.
 *
 * Storage for instances of this structure are palloc'd.  Storage for the values
 * pointed to by non-NULL char*'s are also palloc'd.
 *
 */
#define COMPONENT_DBS_MAX_ADDRS (8)
typedef struct CdbComponentDatabaseInfo
{
	int         segindex;
	char		role;			/* primary, master, mirror, master-standby */
	char		status;

	char	   *hostname;		/* name or ip address of host machine */
	char	   *address;		/* ip address of host machine */

	char	   *hostip;			/* cached lookup of name */
	int32		port;			/* port that instance is listening on */

	char	   *hostaddrs[COMPONENT_DBS_MAX_ADDRS];	/* cached lookup of names */	
} CdbComponentDatabaseInfo;

#define SEGMENT_ROLE_PRIMARY 'p'
#define SEGMENT_ROLE_MIRROR 'r'

#define SEGMENT_ROLE_MASTER_CONFIG 'm'
#define SEGMENT_ROLE_STANDBY_CONFIG 's'


#define SEGMENT_IS_ACTIVE_MIRROR(p) \
	((p)->role == SEGMENT_ROLE_MIRROR ? true : false)
#define SEGMENT_IS_ACTIVE_PRIMARY(p) \
	((p)->role == SEGMENT_ROLE_PRIMARY ? true : false)

#define SEGMENT_IS_ALIVE(p) ((p)->status == 'u' ? true : false)


/* --------------------------------------------------------------------------------------------------
 * Structure for return value from getCdbSegmentDatabases()
 *
 * The storage for instances of this structure returned by getCdbSegmentInstances() is palloc'd.
 * freeCdbSegmentDatabases() can be used to release the structure.
 */
typedef struct CdbComponentDatabases
{
	CdbComponentDatabaseInfo *segment_db_info;	/* array of
												 * SegmentDatabaseInfo's for
												 * segment databases */
	int			total_segment_dbs;		/* count of the array  */
	CdbComponentDatabaseInfo *entry_db_info;	/* array of
												 * SegmentDatabaseInfo's for
												 * entry databases */
	int			total_entry_dbs;	/* count of the array  */
	int			total_segments; /* count of distinct segindexes */
} CdbComponentDatabases;

/*
 * Minimal data structure for a segment.
 */
typedef struct Segment {
	bool	master;
	bool	standby;

	char	*hostname;
	int32	port;

	/* Need host <=> ip module to remove this field. */
	char	*hostip;

	char* hdfsHostname;

	/* Need an in memory FTS to remove this field. */
	bool	alive;

	int		segindex;

	/* Global unique ID. */
	int		ID;
} Segment;

extern Segment *GetMasterSegment(void);
extern Segment *GetStandbySegment(void);
extern List *GetSegmentList(void);
extern void FreeSegment(Segment *segment);
extern Segment *CopySegment(Segment *segment, MemoryContext cxt);
extern List *GetVirtualSegmentList(void);

/*
 * performs all necessary setup required for initializing Greenplum Database components.
 *
 * This includes cdblink_setup() and initializing the Motion Layer.
 *
 */
extern void cdb_setup(void);


/*
 * performs all necessary cleanup required when cleaning up Greenplum Database components
 * when disabling Greenplum Database functionality.
 *
 */
extern void cdb_cleanup(int code, Datum arg  __attribute__((unused)) );


/*
 * getCdbComponentDatabases() returns a pointer to a CdbComponentDatabases
 * structure. Both the segment_db_info array and the entry_db_info_array are ordered by segindex,
 * isprimary desc.
 *
 * The CdbComponentDatabases structure, and the segment_db_info array and the entry_db_info_array
 * are contained in palloc'd storage allocated from the current storage context.
 * The same is true for pointer-based values in CdbComponentDatabaseInfo.  The caller is responsible
 * for setting the current storage context and releasing the storage occupied the returned values.
 *
 * The function freeCdbComponentDatabases() is used to release the structure
 * returned by getCdbComponentDatabases().
 *
 */
extern CdbComponentDatabases *getCdbComponentDatabases(void);

/*
 * freeCdbComponentDatabases() releases the palloc'd storage returned by
 * getCdbComponentDatabases().
 */
extern void freeCdbComponentDatabases(CdbComponentDatabases *pDBs);
extern void freeCdbComponentDatabaseInfo(CdbComponentDatabaseInfo *cdi);

// UNDONE: This was a private procedure... are there any issues in making it public???
/*
 * getCdbComponentInfo
 *
 *
 * Storage for the SegmentInstances block and all subsidiary
 * structures are allocated from the caller's context.
 */
extern CdbComponentDatabases *getCdbComponentInfo(bool DnsLookupFailureIsError);

/*
 * Given total number of primary segment databases and a number of segments
 * to "skip" - this routine creates a boolean map (array) the size of total
 * number of segments and randomly selects several entries (total number of
 * total_to_skip) to be marked as "skipped". This is used for external tables
 * with the 'gpfdist' protocol where we want to get a number of *random* segdbs
 * to connect to a gpfdist client.
 */
extern bool *makeRandomSegMap(int total_primaries, int total_to_skip);

/*
 * do a host:port to IP lookup, bypass caching.
 */
extern char *getDnsAddress(char *name, int port, int elevel);
extern CdbComponentDatabaseInfo *registration_order_get_dbinfo(int32 order);
extern CdbComponentDatabaseInfo *role_get_dbinfo(char role);
#endif   /* CDBUTIL_H */
