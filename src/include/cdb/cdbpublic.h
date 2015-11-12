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
 * cdbpublic.c
 *
 *		Greenplum specific structures and typedefs exposed to the
 *      outside world.
 *
 *      This is a non-ideal solution, consider alternatives before
 *      adding to this file.
 *
 *
 *-------------------------------------------------------------------------
 */

/* This file should not include any other files under "cdb" */

#ifndef CDBPUBLIC_H
#define CDBPUBLIC_H

#include "c.h"         /* DistributedTransactionId */
#include "postmaster/identity.h"

/* Things defined in this header */
typedef struct TMGXACT_LOG           TMGXACT_LOG;
typedef struct LocalDistribXactRef   LocalDistribXactRef;
typedef struct CdbExplain_Agg        CdbExplain_Agg;
typedef struct CdbCellBuf            CdbCellBuf;

/* From "cdb/cdbtm.h" */
struct TMGXACT_LOG
{
	char						gid[TMGIDSIZE];
	DistributedTransactionId	gxid;
};



typedef struct TMGXACT_CHECKPOINT
{
	int						committedCount;
	int						segmentCount;

    /* Array [0..committedCount-1] of TMGXACT_LOG structs begins here */
	TMGXACT_LOG				committedGxactArray[1];
}	TMGXACT_CHECKPOINT;

#define TMGXACT_CHECKPOINT_BYTES(committedCount) \
    (SIZEOF_VARSTRUCT(committedCount, TMGXACT_CHECKPOINT, committedGxactArray))



/* From "cdb/cdblocaldistribxact.h" */
#define LocalDistribXactRef_Eyecatcher "LDX"
#define LocalDistribXactRef_EyecatcherLen 4
#define LocalDistribXactRef_StaticInit {LocalDistribXactRef_Eyecatcher,-1}

struct LocalDistribXactRef
{
	char		eyecatcher[LocalDistribXactRef_EyecatcherLen];	
								/* 
								 * Used to validate this is a LocalDistribXact
								 * reference.  3 characters plus NUL.
								 */
	int			index;			/* Index to the element. */

};

/* From "cdb/cdbexplain.h" */
struct CdbExplain_Agg
{
    double      vmax;           /* maximum value of statistic */
    double      vlast;           /* maximum value of statistic */
    double      vsum;           /* sum of values */
    int         vcnt;           /* count of values > 0 */
    int         imax;           /* id of 1st observation having maximum value */
    int     	    ilast;           /* id of slowest */
    char        hostnamemax[SEGMENT_IDENTITY_NAME_LENGTH];    /*max row vs's hostname */
    char        hostnamelast[SEGMENT_IDENTITY_NAME_LENGTH];    /*slowest vs's hostname */
};

/* From "cdb/cdbcellbuf.h" */
/*
 * CdbCellBuf
 *   - A container for small fixed-size items.
 */
struct CdbCellBuf
{
    char           *cbeg;               /* first cell in active bunch */
    char           *cfree;              /* end of used cells in active bunch */
    char           *cend;               /* end of cells in active bunch */
    uint16          cellbytes;          /* size of one cell (bytes) */
    int16           offset_nearby;      /* distance in bytes from this struct to
                                         *   nearby cells provided by caller */
    int             nearby_bunch_len;   /* cellbytes * number of nearby cells */
    int             expand_bunch_len;   /* cellbytes * number of cells per
                                         *   expansion bunch */
    long            nfull_total;        /* total number of occupied cells */
    struct CdbCellBuf_Bumper   *head;   /* beginning of first expansion bunch */
    struct CdbCellBuf_Bumper   *tail;   /* beginning of last expansion bunch */
    MemoryContext   context;            /* memory context for expansion */
};


#endif

