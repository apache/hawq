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
 * cdbconn.h
 *
 * Functions returning results from a remote database
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBCONN_H
#define CDBCONN_H

#include "gp-libpq-fe.h"               /* prerequisite for libpq-int.h */
#include "gp-libpq-int.h"              /* PQExpBufferData */


/* --------------------------------------------------------------------------------------------------
 * Structure for segment database definition and working values
 */
typedef struct SegmentDatabaseDescriptor
{
	/* segment_database_info
	 *
	 * Points to the SegmentDatabaseInfo structure describing the
	 * parameters for this segment database.  Information in this structure is
	 * obtained from the Greenplum administrative schema tables.
	 */
	struct Segment		*segment;
	
    /* conn
	 *
	 * A non-NULL value points to the PGconn block of a successfully
	 * established connection to the segment database.
	 */
	PGconn				   *conn;		
	
	/*
	 * Error info saved when connection cannot be established.
	 */
    int                     errcode;        /* ERRCODE_xxx (sqlstate encoded as
                                             * an int) of first error, or 0.
                                             */
	PQExpBufferData         error_message;  /* message text; '\n' at end */

    /*
     * Connection info saved at most recent PQconnectdb.
     *
     * NB: Use malloc/free, not palloc/pfree, for the items below.
     */
    int4		            motionListener; /* interconnect listener port */
    int4					backendPid;
    char                   *whoami;         /* QE identifier for msgs */
} SegmentDatabaseDescriptor;


/* Initialize a segment descriptor in storage provided by the caller. */
void
cdbconn_initSegmentDescriptor(SegmentDatabaseDescriptor *segdbDesc,
                              struct Segment *segment);


/* Free all memory owned by a segment descriptor. */
void
cdbconn_termSegmentDescriptor(SegmentDatabaseDescriptor *segdbDesc);


/* Connect to a QE as a client via libpq. */
bool                            /* returns true if connected */
cdbconn_doConnect(SegmentDatabaseDescriptor    *segdbDesc,
                  const char                   *options);

/* Set the slice index for error messages related to this QE. */
bool
cdbconn_setSliceIndex(SegmentDatabaseDescriptor    *segdbDesc,
                      int                           sliceIndex);

#endif   /* CDBCONN_H */
