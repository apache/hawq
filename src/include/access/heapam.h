/*-------------------------------------------------------------------------
 *
 * heapam.h
 *	  POSTGRES heap access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/access/heapam.h,v 1.117 2006/11/05 22:42:10 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef HEAPAM_H
#define HEAPAM_H

#include "access/htup.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/skey.h"
#include "access/tupmacs.h"
#include "access/xlogutils.h"
#include "nodes/primnodes.h"
#include "storage/block.h"
#include "storage/lmgr.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/tqual.h"

/* in common/heaptuple.c */
extern Datum nocachegetattr(HeapTuple tup, int attnum, TupleDesc att);
extern Datum heap_getsysattr(HeapTuple tup, int attnum, bool *isnull);


/* ----------------
 *		fastgetattr
 *
 *		Fetch a user attribute's value as a Datum (might be either a
 *		value, or a pointer into the data area of the tuple).
 *
 *		This must not be used when a system attribute might be requested.
 *		Furthermore, the passed attnum MUST be valid.  Use heap_getattr()
 *		instead, if in doubt.
 *
 *		This gets called many times, so we macro the cacheable and NULL
 *		lookups, and call nocachegetattr() for the rest.
 *
 *      CDB:  Implemented as inline function instead of macro.
 * ----------------
 */
static inline Datum
fastgetattr(HeapTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull)
{
    Datum               result;
    Form_pg_attribute   att = tupleDesc->attrs[attnum-1];

    Assert(attnum > 0);

    if (isnull)
        *isnull = false;

    if (HeapTupleNoNulls(tup))
    {
        if (att->attcacheoff >= 0)
			result = fetchatt(att,
				              (char *)tup->t_data + tup->t_data->t_hoff +
					            att->attcacheoff);
        else
            result = nocachegetattr(tup, attnum, tupleDesc);
    }
    else if (att_isnull(attnum-1, tup->t_data->t_bits))
    {
        result = Int32GetDatum(0);
        if (isnull)
            *isnull = true;
    }
    else
        result = nocachegetattr(tup, attnum, tupleDesc);

    return result;
}                               /* fastgetattr */


/* ----------------
 *		heap_getattr
 *
 *		Extract an attribute of a heap tuple and return it as a Datum.
 *		This works for either system or user attributes.  The given attnum
 *		is properly range-checked.
 *
 *		If the field in question has a NULL value, we return a zero Datum
 *		and set *isnull == true.  Otherwise, we set *isnull == false.
 *
 *		<tup> is the pointer to the heap tuple.  <attnum> is the attribute
 *		number of the column (field) caller wants.	<tupleDesc> is a
 *		pointer to the structure describing the row and all its fields.
 *
 *      CDB:  Implemented as inline function instead of macro.
 * ----------------
 */
static inline Datum
heap_getattr(HeapTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull)
{
    Datum       result;

    Assert(tup != NULL);

    if (attnum > (int)HeapTupleHeaderGetNatts(tup->t_data))
    {
        result = DatumGetInt32(0);
        if (isnull)
            *isnull = true;
    }
    else if (attnum > 0)
        result = fastgetattr(tup, attnum, tupleDesc, isnull);
    else
        result = heap_getsysattr(tup, attnum, isnull);

    return result;
}                               /* heap_getattr */

// UNDONE: Temporarily.
extern void RelationFetchGpRelationNodeForXLog_Index(Relation relation);

/*
 * Fetch the persistent TID and serial number for a relation from the gp_relation_node
 * if needed to put in the XLOG record header.
 */
inline static void RelationFetchGpRelationNodeForXLog(
	Relation		relation)
{
	if (!InRecovery && !relation->rd_relationnodeinfo.isPresent &&
		!GpPersistent_SkipXLogInfo(relation->rd_id))
	{
	
		if (relation->rd_rel->relkind == RELKIND_INDEX )
		{
			// UNDONE: Temporarily.
			RelationFetchGpRelationNodeForXLog_Index(relation);
			return;
				 
		}

		/*
		if (relation->rd_rel->relstorage != RELSTORAGE_AOROWS
			&& relation->rd_rel->relstorage != RELSTORAGE_PARQUET)
			RelationFetchSegFile0GpRelationNode(relation, MASTER_CONTENT_ID);
		else
		{
			int i;
			for (i = 0; i < relation->rd_segfile0_count; ++i)
				RelationFetchSegFile0GpRelationNode(relation, i - 1);

		}
		*/
		RelationFetchGpRelationNode(relation);
	}
}


/* ----------------
 *		function prototypes for heap access method
 *
 * heap_create, heap_create_with_catalog, and heap_drop_with_catalog
 * are declared in catalog/heap.h
 * ----------------
 */

/* heapam.c */

typedef enum
{
	LockTupleShared,
	LockTupleExclusive
} LockTupleMode;

typedef enum
{
	LockTupleWait,		/* wait for lock until it's acquired */
	LockTupleNoWait,	/* if can't get lock right away, report error */
	LockTupleIfNotLocked/* if can't get lock right away, give up. no error */
} LockTupleWaitType;

inline static void xl_heaptid_set(
	struct xl_heaptid	*heaptid,
	Relation rel,
	ItemPointer tid)
{
	heaptid->node = rel->rd_node;
	heaptid->persistentTid = rel->rd_relationnodeinfo.persistentTid;
	heaptid->persistentSerialNum = rel->rd_relationnodeinfo.persistentSerialNum;
	heaptid->tid = *tid;
}

inline static void xl_heapnode_set(
	struct xl_heapnode	*heapnode,

	Relation rel)
{
	heapnode->node = rel->rd_node;
	heapnode->persistentTid = rel->rd_relationnodeinfo.persistentTid;
	heapnode->persistentSerialNum = rel->rd_relationnodeinfo.persistentSerialNum;
}

extern Relation relation_open(Oid relationId, LOCKMODE lockmode);
extern Relation try_relation_open(Oid relationId, LOCKMODE lockmode, 
								  bool noWait);
extern Relation relation_open_nowait(Oid relationId, LOCKMODE lockmode);
extern Relation relation_openrv(const RangeVar *relation, LOCKMODE lockmode);
extern Relation try_relation_openrv(const RangeVar *relation, LOCKMODE lockmode,
									bool noWait);

extern void relation_close(Relation relation, LOCKMODE lockmode);
extern void relation_close_at_resource_owner(Relation relation, LOCKMODE lockmode, ResourceOwner resowner);

extern Relation heap_open(Oid relationId, LOCKMODE lockmode);
extern Relation heap_openrv(const RangeVar *relation, LOCKMODE lockmode);
extern Relation try_heap_open(Oid relationId, LOCKMODE lockmode, bool noWait);
extern Relation try_heap_openrv(const RangeVar *relation, LOCKMODE lockmode,
								bool noWait);

#define heap_close(r,l)  relation_close(r,l)

/* CDB */
extern Relation CdbOpenRelation(Oid relid, LOCKMODE reqmode, bool noWait, 
								bool *lockUpgraded);
extern Relation CdbTryOpenRelation(Oid relid, LOCKMODE reqmode, bool noWait, 
								   bool *lockUpgraded);
extern Relation CdbOpenRelationRv(const RangeVar *relation, LOCKMODE reqmode, 
								  bool noWait, bool *lockUpgraded);


extern HeapScanDesc heap_beginscan(Relation relation, Snapshot snapshot,
			   int nkeys, ScanKey key);
extern void heap_rescan(HeapScanDesc scan, ScanKey key);
extern void heap_endscan(HeapScanDesc scan);
extern HeapTuple heap_getnext(HeapScanDesc scan, ScanDirection direction);
extern void heap_getnextx(HeapScanDesc scan, ScanDirection direction,
			  HeapTupleData tdata[], int *tdatacnt,
			  int *seen_EOS);

extern bool heap_fetch(Relation relation, Snapshot snapshot,
		   HeapTuple tuple, Buffer *userbuf, bool keep_buf,
		   Relation stats_relation);
extern bool heap_release_fetch(Relation relation, Snapshot snapshot,
				   HeapTuple tuple, Buffer *userbuf, bool keep_buf,
		   		   Relation stats_relation);

extern void heap_get_latest_tid(Relation relation, Snapshot snapshot,
					ItemPointer tid);
extern void setLastTid(const ItemPointer tid);

extern Oid heap_insert(Relation relation, HeapTuple tup, CommandId cid,
			bool use_wal, bool use_fsm, TransactionId xid);
extern HTSU_Result heap_delete(Relation relation, ItemPointer tid,
			ItemPointer ctid, TransactionId *update_xmax,
			CommandId cid, Snapshot crosscheck, bool wait);
extern HTSU_Result heap_update(Relation relation, ItemPointer otid,
			HeapTuple newtup,
			ItemPointer ctid, TransactionId *update_xmax,
			CommandId cid, Snapshot crosscheck, bool wait);
extern HTSU_Result heap_lock_tuple(Relation relation, HeapTuple tuple,
				Buffer *buffer, ItemPointer ctid,
				TransactionId *update_xmax, CommandId cid,
				LockTupleMode mode, LockTupleWaitType waittype);
extern void heap_inplace_update(Relation relation, HeapTuple tuple);
extern void frozen_heap_inplace_update(Relation relation, HeapTuple tuple);
extern void frozen_heap_inplace_delete(Relation relation, HeapTuple tuple);
extern bool heap_freeze_tuple(HeapTupleHeader tuple, TransactionId cutoff_xid,
							  Buffer buf);

extern Oid	simple_heap_insert(Relation relation, HeapTuple tup);
extern Oid frozen_heap_insert(Relation relation, HeapTuple tup);
extern Oid frozen_heap_insert_directed(Relation relation, HeapTuple tup, BlockNumber blockNum);
extern void simple_heap_delete(Relation relation, ItemPointer tid);
extern void simple_heap_update(Relation relation, ItemPointer otid,
				   HeapTuple tup);

extern void heap_markpos(HeapScanDesc scan);
extern void heap_markposx(HeapScanDesc scan, HeapTuple tuple);
extern void heap_restrpos(HeapScanDesc scan);

extern void heap_redo(XLogRecPtr beginLoc, XLogRecPtr lsn, XLogRecord *rptr);
extern void heap_desc(StringInfo buf, XLogRecPtr beginLoc, XLogRecord *record);
extern bool heap_getrelfilenode(
	XLogRecord 		*record,
	RelFileNode		*relFileNode);
extern void heap2_redo(XLogRecPtr beginLoc, XLogRecPtr lsn, XLogRecord *rptr);
extern void heap2_desc(StringInfo buf, XLogRecPtr beginLoc, XLogRecord *record);

extern void log_heap_newpage(Relation rel, 
							 Page page,
							 BlockNumber bno);
extern XLogRecPtr log_heap_move(Relation reln, Buffer oldbuf,
			  ItemPointerData from,
			  Buffer newbuf, HeapTuple newtup);
extern XLogRecPtr log_heap_clean(Relation reln, Buffer buffer,
			   OffsetNumber *unused, int uncnt);
extern XLogRecPtr log_heap_freeze(Relation reln, Buffer buffer,
								  TransactionId cutoff_xid,
								  OffsetNumber *offsets, int offcnt);

/* in common/heaptuple.c */
extern Size heap_compute_data_size(TupleDesc tupleDesc,
					   Datum *values, bool *isnull);
extern Size heap_fill_tuple(TupleDesc tupleDesc,
				Datum *values, bool *isnull,
				char *data, uint16 *infomask, bits8 *bit);
extern bool heap_attisnull(HeapTuple tup, int attnum);
extern bool heap_attisnull_normalattr(HeapTuple tup, int attnum);

extern HeapTuple heaptuple_copy_to(HeapTuple tup, HeapTuple result, uint32 *len);

static inline HeapTuple heap_copytuple(HeapTuple tuple)
{
	return heaptuple_copy_to(tuple, NULL, NULL);
}

extern void heap_copytuple_with_tuple(HeapTuple src, HeapTuple dest);

extern HeapTuple heaptuple_form_to(TupleDesc tupdesc, Datum* values, bool *isnull, HeapTuple tup, uint32 *len);
static inline HeapTuple heap_form_tuple(TupleDesc tupleDescriptor, Datum *values, bool *isnull)
{
	return heaptuple_form_to(tupleDescriptor, values, isnull, NULL, NULL);
}
extern HeapTuple heap_formtuple(TupleDesc tupleDescriptor, Datum *values, char *nulls) __attribute__ ((deprecated));

extern HeapTuple heap_modify_tuple(HeapTuple tuple,
				  TupleDesc tupleDesc,
				  Datum *replValues,
				  bool *replIsnull,
				  bool *doReplace);
extern HeapTuple heap_modifytuple(HeapTuple tuple,
				 TupleDesc tupleDesc,
				 Datum *replValues,
				 char *replNulls,
				 char *replActions) __attribute__ ((deprecated));
extern void heap_deform_tuple(HeapTuple tuple, TupleDesc tupleDesc,
				  Datum *values, bool *isnull);
extern void heap_deformtuple(HeapTuple tuple, TupleDesc tupleDesc,
				 Datum *values, char *nulls) __attribute__ ((deprecated));
extern void heap_freetuple(HeapTuple htup);
extern HeapTuple heap_addheader(int natts, bool withoid,
			   Size structlen, void *structure);

#endif   /* HEAPAM_H */
