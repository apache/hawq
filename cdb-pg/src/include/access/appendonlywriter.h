/*-------------------------------------------------------------------------
 *
 * appendonlywriter.h
 *	  Definitions for appendonlywriter.h
 *
 *
 * Copyright (c) 2006-2008, Greenplum inc.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef APPENDONLYWRITER_H
#define APPENDONLYWRITER_H

#include "access/aosegfiles.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "storage/lock.h"
#include "tcop/dest.h"

/*
 * Maximum concurrent number of writes into a single append only table.
 * TODO: may want to make this a guc instead (can only be set at gpinit time).
 */
#define MAX_AOREL_CONCURRENCY 128

/*
 * This segfile number may only be used for special case write operations.
 * These include operations that bypass the qd -OR- that need not worry about
 * concurrency. Currently these are:
 * 
 *   - UTILITY mode operations, for example, gp_restore.
 *   - Table rewrites that are sometimes done as a part of ALTER TABLE.
 *   - CTAS, which runs on an exclusively locked destination table.
 *   
 * Note that in some cases (such as CTAS) the aoseg table on the QD will not
 * be updated for segment entry 0 after the operation completes. This is ok
 * as the concurrency module never cares about the RESERVED segno anyway. When
 * wanting to sync the aoseg table on the master after such operations you need
 * to call gp_update_ao_master_stats(), which gp_restore does automatically.
 */
#define RESERVED_SEGNO 0

/*
 * GUC variables
 */
extern int	MaxAppendOnlyTables;	/* Max # of concurrently used AO rels */

/*
 * Describes the status of a particular file segment number accross an entire
 * AO relation. 
 *
 * This information is kept in a hash table and kept up to date. It represents
 * the global status of this segment number accross the whole array, for example
 * 'tupcount' for segno #4 will show the total rows in all file segments with
 * number 4 for this specific table. 
 * 
 * This data structure is accessible by the master node and it is used to make 
 * decisions regarding file segment allocations for data writes. Note that it 
 * is not used for reads. Durind reads each segdb scanner reads its local 
 * catalog.
 *
 * Note that 'isfull' tries to guess if any of the file segments is full. Since
 * there may be some skew in the data we use a threshold that is a bit lower
 * than the max tuples allowed per segment.
 * 
 */
typedef struct AOSegfileStatus
{
	int64			tupcount;  		/* Total tupcount for this segno across   * 
									 * all segdbs 							  */
	int64			tupsadded;		/* Num tuples added in this segno across  * 
									 * all segdbs in the current transaction  */
	TransactionId	xid;	   		/* the inserting transaction id 		  */
	TransactionId 	latestWriteXid; /* the latest committed inserting
												transaction id */
	bool			inuse;	   		/* if true - segno is assigned right now. *
									 * use another 							  */
	bool			isfull;	   		/* if true - never insert into this segno *
									 * anymore 								  */
	bool			needCheck;		/* need to check if the segfile contain unexpected garbage data */
} AOSegfileStatus;

/*
 * Describes the status of all file segments of an AO relation in the system.
 * This data structure is kept in a hash table on the master and kept up to 
 * date.
 * 
 * 'relid' stands for the AO relation this entry serves.
 * 'txns_using_rel' stands for the number of transactions that are currently
 * inserting into this relation. if equals zero it is safe to remove this 
 * entry from the hash table (when needed).
 */
typedef struct AORelHashEntryData
{
	Oid				relid;
	Oid				segrelid;
	int				txns_using_rel;
	char			tspPathPrefix[MAXPGPATH + 1];
	AOSegfileStatus relsegfiles[MAX_AOREL_CONCURRENCY];
	
} AORelHashEntryData;
typedef AORelHashEntryData	*AORelHashEntry;


typedef struct AppendOnlyWriterData
{
	int		num_existing_aorels; /* Current # of recorded entries for AO relations */
} AppendOnlyWriterData;
extern AppendOnlyWriterData	*AppendOnlyWriter;

/*
 * functions in appendonlywriter.c
 */
extern Size AppendOnlyWriterShmemSize(void);
extern void InitAppendOnlyWriter(void);
extern Size AppendOnlyWriterShmemSize(void);
extern bool TestCurrentTspSupportTruncate(Oid tsp);
extern int  SetSegnoForWrite(int existingsegno, Oid relid);
extern List *assignPerRelSegno(List *all_rels);
extern void UpdateMasterAosegTotals(Relation parentrel,
									int segno, 
									uint64 tupcount);
extern bool AORelRemoveHashEntry(Oid relid);
extern void AtCommit_AppendOnly(void);
extern void AtAbort_AppendOnly(bool isSubTransaction);
extern void AtEOXact_AppendOnly(void);

extern void ValidateAppendOnlyMetaDataSnapshot(
	Snapshot *appendOnlyMetaDataSnapshot);

#endif  /* APPENDONLYWRITER_H */
