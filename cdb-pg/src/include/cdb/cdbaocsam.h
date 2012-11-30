/*-------------------------------------------------------------------------
 *
 * cdbaocsam.h
 *	  append-only columnar relation access method definitions.
 *
 * Portions Copyright (c) 2009, Greenplum Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDB_AOCSAM_H
#define CDB_AOCSAM_H

#include "access/relscan.h"
#include "access/sdir.h"
#include "access/tupmacs.h"
#include "access/xlogutils.h"
#include "access/appendonlytid.h"
#include "executor/tuptable.h"
#include "nodes/primnodes.h"
#include "storage/block.h"
#include "storage/lmgr.h"
#include "utils/rel.h"
#include "utils/tqual.h"
#include "cdb/cdbappendonlyblockdirectory.h"
#include "cdb/cdbappendonlystoragelayer.h"
#include "cdb/cdbappendonlystorageread.h"
#include "cdb/cdbappendonlystoragewrite.h"
#include "utils/datumstream.h"

/*
 * AOCSInsertDescData is used for inserting data into append-only columnar
 * relations. It serves an equivalent purpose as AOCSScanDescData
 * only that the later is used for scanning append-only columnar
 * relations.
 */
struct DatumStream;
struct AOCSFileSegInfo;

typedef struct AOCSInsertDescData
{
	Relation	aoi_rel;
	Snapshot	appendOnlyMetaDataSnapshot;
	AOCSFileSegInfo *fsInfo;
	int64		insertCount;
	int64		varblockCount;
	int64		rowCount; /* total row count before insert */
	int64		numSequences; /* total number of available sequences */
	int64		lastSequence; /* last used sequence */
	int32		cur_segno;
	AppendOnlyEntry *aoEntry;

	char *compType;
	int32 compLevel;
	int32 blocksz;

	struct DatumStreamWrite **ds;

	AppendOnlyBlockDirectory blockDirectory;
} AOCSInsertDescData;

typedef AOCSInsertDescData *AOCSInsertDesc;

/*
 * used for scan of append only relations using BufferedRead and VarBlocks
 */
typedef struct AOCSScanDescData
{
	/* scan parameters */
	Relation	aos_rel;			/* target relation descriptor */
	Snapshot	appendOnlyMetaDataSnapshot;
	TupleDesc   relationTupleDesc;	/* tuple descriptor of table to scan.
									 *  Code should use this rather than aos_rel->rd_att,
									 *  as THEY MAY BE DIFFERENT.
									 *  See code in aocsam.c's aocs_beginscan for more info
 									 */

	Index aos_scanrelid; /* index */

	int total_seg;
	int cur_seg;

	char *compType;
	int32 compLevel;
	int32 blocksz;

	struct AOCSFileSegInfo **seginfo;
	struct DatumStreamRead **ds;
	bool *proj;

	/* synthetic system attributes */
	ItemPointerData cdb_fake_ctid;
	int64 total_row;
	int64 cur_seg_row;

	AppendOnlyEntry *aoEntry;

	/*
	 * The block directory info.
	 *
	 * For CO tables that are upgraded from pre-3.4 release, the block directory
	 * built during the first index creation.
	 */
	bool buildBlockDirectory;
	AppendOnlyBlockDirectory *blockDirectory;

}	AOCSScanDescData;

typedef AOCSScanDescData *AOCSScanDesc;

/*
 * Used for fetch individual tuples from specified by TID of append only relations
 * using the AO Block Directory.
 */
typedef struct AOCSFetchDescData
{
	Relation		relation;
	Snapshot		appendOnlyMetaDataSnapshot;

	MemoryContext	initContext;

	int				totalSegfiles;
	struct AOCSFileSegInfo **segmentFileInfo;
	bool			*proj;

	char			*segmentFileName;
	int				segmentFileNameMaxLen;
	char            *basepath;

	AppendOnlyBlockDirectory	blockDirectory;

	DatumStreamFetchDesc *datumStreamFetchDesc;

	AppendOnlyEntry *aoEntry;

	int64	skipBlockCount;

} AOCSFetchDescData;

typedef AOCSFetchDescData *AOCSFetchDesc;

/* ----------------
 *		function prototypes for appendonly access method
 * ----------------
 */

extern AOCSScanDesc aocs_beginscan(Relation relation, Snapshot appendOnlyMetaDataSnapshot, TupleDesc relationTupleDesc, bool *proj);
extern void aocs_rescan(AOCSScanDesc scan);
extern void aocs_endscan(AOCSScanDesc scan);
extern void aocs_getnext(AOCSScanDesc scan, ScanDirection direction, TupleTableSlot *slot);
extern AOCSInsertDesc aocs_insert_init(Relation rel, int segno);
extern Oid aocs_insert_values(AOCSInsertDesc idesc, Datum *d, bool *null, AOTupleId *aoTupleId);
static inline Oid aocs_insert(AOCSInsertDesc idesc, TupleTableSlot *slot)
{
	Oid oid;
	AOTupleId aotid;

	slot_getallattrs(slot);
	oid = aocs_insert_values(idesc, slot_get_values(slot), slot_get_isnull(slot), &aotid);
	slot_set_ctid(slot, (ItemPointer)&aotid);

	return oid;
}
extern void aocs_insert_finish(AOCSInsertDesc idesc);
extern AOCSFetchDesc aocs_fetch_init(Relation relation,
									 Snapshot snapshot,
									 bool *proj);
extern bool aocs_fetch(AOCSFetchDesc aocsFetchDesc,
					   AOTupleId *aoTupleId,
					   TupleTableSlot *slot);
extern void aocs_fetch_finish(AOCSFetchDesc aocsFetchDesc);

#endif   /* AOCSAM_H */
