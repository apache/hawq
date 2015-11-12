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

/*--------------------------------------------------------------------------
*
* cdbcopy.h
*	 Definitions and API functions for cdbcopy.c
*	 These are functions that are used by the backend
*	 COPY command in Greenplum Database.
*
*--------------------------------------------------------------------------
*/
#ifndef CDBCOPY_H
#define CDBCOPY_H

#include "access/aosegfiles.h" /* for InvalidFileSegNumber const */ 
#include "lib/stringinfo.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbquerycontextdispatching.h"
#include "cdb/dispatcher.h"

struct DispatchExecutors;

#define COPYOUT_CHUNK_SIZE 16 * 1024

typedef enum SegDbState
{
	/*
	 * (it is best to avoid names like OUT that are likely to be #define'd or
	 * typedef'd in some platform-dependent runtime library header file)
	 */
	SEGDB_OUT,					/* Not participating in COPY (invalid etc...) */
	SEGDB_IDLE,					/* Participating but COPY not yet started */
	SEGDB_COPY,					/* COPY in progress */
	SEGDB_DONE					/* COPY completed (with or without errors) */
}	SegDbState;

typedef struct CdbCopy
{
	int			partition_num;		/* total number of partition for copy. */
	QueryResource		*resource;
	DispatchDataResult	executors;
	bool		copy_in;		/* direction: true for COPY FROM false for COPY TO */
	bool		remote_data_err;/* data error occurred on a remote COPY session */
	bool		io_errors;		/* true if any I/O error occurred trying to
								 * communicate with segDB's */
	SegDbState		**segdb_state;
	
	StringInfoData	err_msg;		/* error message for cdbcopy operations */
	StringInfoData  err_context; /* error context from QE error */
	StringInfoData	copy_out_buf;/* holds a chunk of data from the database */
		
	List			*outseglist;    /* segs that currently take part in copy out. 
									 * Once a segment gave away all it's data rows
									 * it is taken out of the list */
	PartitionNode *partitions;
	List		  *ao_segnos;
	HTAB		  *aotupcounts; /* hash of ao relation id to processed tuple count */
} CdbCopy;



/* global function declarations */
CdbCopy    *makeCdbCopy(bool copy_in, QueryResource *resource);
int			cdbCopyGetDbCount(int total_segs, int seg);
void		cdbCopyStart(CdbCopy *cdbCopy, char *copyCmd, Oid relid, Oid relerror, List *err_aosegnos);
void		cdbCopySendData(CdbCopy *c, int target_seg, const char *buffer, int nbytes);
void		cdbCopySendDataSingle(CdbCopy *c, int target_seg, const char *buffer, int nbytes);
bool		cdbCopyGetData(CdbCopy *c, bool cancel, uint64 *rows_processed);
int			cdbCopyEnd(CdbCopy *c);

#endif   /* CDBCOPY_H */
