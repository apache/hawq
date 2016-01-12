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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * htup.h
 *	  POSTGRES heap tuple definitions.
 *
 *
 * Portions Copyright (c) 2006-2009, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/access/htup.h,v 1.87 2006/11/05 22:42:10 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef HTUP_H
#define HTUP_H

#include "access/tupdesc.h"
#include "access/tupmacs.h"
#include "storage/itemptr.h"
#include "storage/relfilenode.h"
#include "access/sysattr.h"

/*
 * MaxTupleAttributeNumber limits the number of (user) columns in a tuple.
 * The key limit on this value is that the size of the fixed overhead for
 * a tuple, plus the size of the null-values bitmap (at 1 bit per column),
 * plus MAXALIGN alignment, must fit into t_hoff which is uint8.  On most
 * machines the upper limit without making t_hoff wider would be a little
 * over 1700.  We use round numbers here and for MaxHeapAttributeNumber
 * so that alterations in HeapTupleHeaderData layout won't change the
 * supported max number of columns.
 */
#define MaxTupleAttributeNumber 1664	/* 8 * 208 */

/*
 * MaxHeapAttributeNumber limits the number of (user) columns in a table.
 * This should be somewhat less than MaxTupleAttributeNumber.  It must be
 * at least one less, else we will fail to do UPDATEs on a maximal-width
 * table (because UPDATE has to form working tuples that include CTID).
 * In practice we want some additional daylight so that we can gracefully
 * support operations that add hidden "resjunk" columns, for example
 * SELECT * FROM wide_table ORDER BY foo, bar, baz.
 * In any case, depending on column data types you will likely be running
 * into the disk-block-based limit on overall tuple size if you have more
 * than a thousand or so columns.  TOAST won't help.
 */
#define MaxHeapAttributeNumber	1600	/* 8 * 200 */

/*
 * Heap tuple header.  To avoid wasting space, the fields should be
 * laid out in such a way as to avoid structure padding.
 *
 * Datums of composite types (row types) share the same general structure
 * as on-disk tuples, so that the same routines can be used to build and
 * examine them.  However the requirements are slightly different: a Datum
 * does not need any transaction visibility information, and it does need
 * a length word and some embedded type information.  We can achieve this
 * by overlaying the xmin/cmin/xmax/cmax/xvac fields of a heap tuple
 * with the fields needed in the Datum case.  Typically, all tuples built
 * in-memory will be initialized with the Datum fields; but when a tuple is
 * about to be inserted in a table, the transaction fields will be filled,
 * overwriting the datum fields.
 *
 * The overall structure of a heap tuple looks like:
 *			fixed fields (HeapTupleHeaderData struct)
 *			nulls bitmap (if HEAP_HASNULL is set in t_infomask)
 *			alignment padding (as needed to make user data MAXALIGN'd)
 *			object ID (if HEAP_HASOID is set in t_infomask)
 *			user data fields
 *
 * We store five "virtual" fields Xmin, Cmin, Xmax, Cmax, and Xvac in three
 * physical fields.  Xmin and Xmax are always really stored, but Cmin, Cmax
 * and Xvac share a field.	This works because we know that Cmin and Cmax
 * are only interesting for the lifetime of the inserting and deleting
 * transaction respectively.  If a tuple is inserted and deleted in the same
 * transaction, we store a "combo" command id that can be mapped to the real
 * cmin and cmax, but only by use of local state within the originating
 * backend.  See combocid.c for more details.  Meanwhile, Xvac is only set
 * by VACUUM FULL, which does not have any command sub-structure and so does
 * not need either Cmin or Cmax.  (This requires that VACUUM FULL never try
 * to move a tuple whose Cmin or Cmax is still interesting, ie, an insert-
 * in-progress or delete-in-progress tuple.)
 *
 * A word about t_ctid: whenever a new tuple is stored on disk, its t_ctid
 * is initialized with its own TID (location).	If the tuple is ever updated,
 * its t_ctid is changed to point to the replacement version of the tuple.
 * Thus, a tuple is the latest version of its row iff XMAX is invalid or
 * t_ctid points to itself (in which case, if XMAX is valid, the tuple is
 * either locked or deleted).  One can follow the chain of t_ctid links
 * to find the newest version of the row.  Beware however that VACUUM might
 * erase the pointed-to (newer) tuple before erasing the pointing (older)
 * tuple.  Hence, when following a t_ctid link, it is necessary to check
 * to see if the referenced slot is empty or contains an unrelated tuple.
 * Check that the referenced tuple has XMIN equal to the referencing tuple's
 * XMAX to verify that it is actually the descendant version and not an
 * unrelated tuple stored into a slot recently freed by VACUUM.  If either
 * check fails, one may assume that there is no live descendant version.
 *
 * Following the fixed header fields, the nulls bitmap is stored (beginning
 * at t_bits).	The bitmap is *not* stored if t_infomask shows that there
 * are no nulls in the tuple.  If an OID field is present (as indicated by
 * t_infomask), then it is stored just before the user data, which begins at
 * the offset shown by t_hoff.	Note that t_hoff must be a multiple of
 * MAXALIGN.
 */

typedef struct HeapTupleFields
{
	TransactionId t_xmin;		/* inserting xact ID */
	TransactionId t_xmax;		/* deleting or locking xact ID */

	union
	{
		CommandId	t_cid;		/* inserting or deleting command ID, or both */
		TransactionId t_xvac;	/* VACUUM FULL xact ID */
	}			t_field3;
} HeapTupleFields;

typedef struct DatumTupleFields
{
	int32		datum_len_;		/* varlena header (do not touch directly!) */

	int32		datum_typmod;	/* -1, or identifier of a record type */

	Oid			datum_typeid;	/* composite type OID, or RECORDOID */

	/*
	 * Note: field ordering is chosen with thought that Oid might someday
	 * widen to 64 bits.
	 */
} DatumTupleFields;

typedef struct HeapTupleHeaderData
{
	union
	{
		HeapTupleFields t_heap;
		DatumTupleFields t_datum;
	}			t_choice;

	ItemPointerData t_ctid;		/* current TID of this or newer tuple */

	/* Fields below here must match MinimalTupleData! */

	uint16		t_infomask2;	/* number of attributes + various flags */

	uint16		t_infomask;		/* various flag bits, see below */

	uint8		t_hoff;			/* sizeof header incl. bitmap, padding */

	/* ^ - 23 bytes - ^ */

	bits8		t_bits[1];		/* bitmap of NULLs -- VARIABLE LENGTH */

	/* MORE DATA FOLLOWS AT END OF STRUCT */
} HeapTupleHeaderData;

typedef HeapTupleHeaderData *HeapTupleHeader;

/*
 * information stored in t_infomask:
 */
#define HEAP_HASNULL			0x0001	/* has null attribute(s) */
#define HEAP_HASVARWIDTH		0x0002	/* has variable-width attribute(s) */
#define HEAP_HASEXTERNAL		0x0004	/* has external stored attribute(s) */
#define HEAP_HASCOMPRESSED		0x0008	/* has compressed stored attribute(s) */
#define HEAP_HASEXTENDED		0x000C	/* the two above combined */
#define HEAP_HASOID				0x0010	/* has an object-id field */
#define HEAP_COMBOCID			0x0020	/* t_cid is a combo cid */
#define HEAP_XMAX_EXCL_LOCK		0x0040	/* xmax is exclusive locker */
#define HEAP_XMAX_SHARED_LOCK	0x0080	/* xmax is shared locker */
/* if either LOCK bit is set, xmax hasn't deleted the tuple, only locked it */
#define HEAP_IS_LOCKED	(HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_SHARED_LOCK)
#define HEAP_XMIN_COMMITTED		0x0100	/* t_xmin committed */
#define HEAP_XMIN_INVALID		0x0200	/* t_xmin invalid/aborted */
#define HEAP_XMAX_COMMITTED		0x0400	/* t_xmax committed */
#define HEAP_XMAX_INVALID		0x0800	/* t_xmax invalid/aborted */
#define HEAP_XMAX_IS_MULTI		0x1000	/* t_xmax is a MultiXactId */
#define HEAP_UPDATED			0x2000	/* this is UPDATEd version of row */
#define HEAP_MOVED_OFF			0x4000	/* moved to another place by VACUUM
										 * FULL */
#define HEAP_MOVED_IN			0x8000	/* moved from another place by VACUUM
										 * FULL */
#define HEAP_MOVED (HEAP_MOVED_OFF | HEAP_MOVED_IN)

#define HEAP_XACT_MASK			0xFFE0	/* visibility-related bits */

/*
 * information stored in t_infomask2:
 */
#define HEAP_NATTS_MASK			0x7FF	/* 11 bits for number of attributes */

#define HEAP_XMIN_DISTRIBUTED_SNAPSHOT_IGNORE		0x0800	
										/* t_xmin is either an old committed 
										 * distributed transaction or local.
										 * They can be ignored for distributed
										 * snapshots.
										 */
#define HEAP_XMAX_DISTRIBUTED_SNAPSHOT_IGNORE		0x1000
										/* t_xmax same as above. */
/* bits 0xE000 are currently unused */

/*
 * HeapTupleHeader accessor macros
 *
 * Note: beware of multiple evaluations of "tup" argument.	But the Set
 * macros evaluate their other argument only once.
 */

#define HeapTupleHeaderGetXmin(tup) \
( \
	(tup)->t_choice.t_heap.t_xmin \
)

#define HeapTupleHeaderSetXmin(tup, xid) \
( \
	TransactionIdStore((xid), &(tup)->t_choice.t_heap.t_xmin) \
)

#define HeapTupleHeaderGetXmax(tup) \
( \
	(tup)->t_choice.t_heap.t_xmax \
)

#define HeapTupleHeaderSetXmax(tup, xid) \
( \
	TransactionIdStore((xid), &(tup)->t_choice.t_heap.t_xmax) \
)

/*
 * HeapTupleHeaderGetRawCommandId will give you what's in the header whether
 * it is useful or not.  Most code should use HeapTupleHeaderGetCmin or
 * HeapTupleHeaderGetCmax instead, but note that those Assert that you can
 * get a legitimate result, ie you are in the originating transaction!
 */
#define HeapTupleHeaderGetRawCommandId(tup) \
( \
	(tup)->t_choice.t_heap.t_field3.t_cid \
)

/* SetCmin is reasonably simple since we never need a combo CID */
#define HeapTupleHeaderSetCmin(tup, cid) \
do { \
	Assert(!((tup)->t_infomask & HEAP_MOVED)); \
	(tup)->t_choice.t_heap.t_field3.t_cid = (cid); \
	(tup)->t_infomask &= ~HEAP_COMBOCID; \
} while (0)

/* SetCmax must be used after HeapTupleHeaderAdjustCmax; see combocid.c */
#define HeapTupleHeaderSetCmax(tup, cid, iscombo) \
do { \
	Assert(!((tup)->t_infomask & HEAP_MOVED)); \
	(tup)->t_choice.t_heap.t_field3.t_cid = (cid); \
	if (iscombo) \
		(tup)->t_infomask |= HEAP_COMBOCID; \
	else \
		(tup)->t_infomask &= ~HEAP_COMBOCID; \
} while (0)

#define HeapTupleHeaderGetXvac(tup) \
( \
	((tup)->t_infomask & HEAP_MOVED) ? \
		(tup)->t_choice.t_heap.t_field3.t_xvac \
	: \
		InvalidTransactionId \
)

#define HeapTupleHeaderSetXvac(tup, xid) \
do { \
	Assert((tup)->t_infomask & HEAP_MOVED); \
	TransactionIdStore((xid), &(tup)->t_choice.t_heap.t_field3.t_xvac); \
} while (0)

#define HeapTupleHeaderGetDatumLength(tup) \
	VARSIZE(tup)

#define HeapTupleHeaderSetDatumLength(tup, len) \
	SET_VARSIZE(tup, len)

#define HeapTupleHeaderGetTypeId(tup) \
( \
	(tup)->t_choice.t_datum.datum_typeid \
)

#define HeapTupleHeaderSetTypeId(tup, typeid) \
( \
	(tup)->t_choice.t_datum.datum_typeid = (typeid) \
)

#define HeapTupleHeaderGetTypMod(tup) \
( \
	(tup)->t_choice.t_datum.datum_typmod \
)

#define HeapTupleHeaderSetTypMod(tup, typmod) \
( \
	(tup)->t_choice.t_datum.datum_typmod = (typmod) \
)

#define HeapTupleHeaderGetOid(tup) \
( \
	((tup)->t_infomask & HEAP_HASOID) ? \
		*((Oid *) ((char *)(tup) + (tup)->t_hoff - sizeof(Oid))) \
	: \
		InvalidOid \
)

#define HeapTupleHeaderSetOid(tup, oid) \
do { \
	Assert((tup)->t_infomask & HEAP_HASOID); \
	*((Oid *) ((char *)(tup) + (tup)->t_hoff - sizeof(Oid))) = (oid); \
} while (0)

#define HeapTupleHeaderGetNatts(tup) \
	((tup)->t_infomask2 & HEAP_NATTS_MASK)

#define HeapTupleHeaderSetNatts(tup, natts) \
( \
	(tup)->t_infomask2 = ((tup)->t_infomask2 & ~HEAP_NATTS_MASK) | (natts) \
)

/*
 * BITMAPLEN(NATTS) -
 *		Computes size of null bitmap given number of data columns.
 */
#define BITMAPLEN(NATTS)	(((int)(NATTS) + 7) / 8)

/*
 * MaxTupleSize is the maximum allowed size of a tuple, including header and
 * MAXALIGN alignment padding.	Basically it's BLCKSZ minus the other stuff
 * that has to be on a disk page.  The "other stuff" includes access-method-
 * dependent "special space", which we assume will be no more than
 * MaxSpecialSpace bytes (currently, on heap pages it's actually zero).
 *
 * NOTE: we do not need to count an ItemId for the tuple because
 * sizeof(PageHeaderData) includes the first ItemId on the page.
 */
#define MaxSpecialSpace  32

#define MaxTupleSize	\
	(BLCKSZ - MAXALIGN(sizeof(PageHeaderData) + MaxSpecialSpace))

/*
 * MaxHeapTupleSize is the maximum allowed size of a heap tuple, including
 * header and MAXALIGN alignment padding.  Basically it's BLCKSZ minus the
 * other stuff that has to be on a disk page.  Since heap pages use no
 * "special space", there's no deduction for that.
 *
 * NOTE: we allow for the ItemId that must point to the tuple, ensuring that
 * an otherwise-empty page can indeed hold a tuple of this size.  Because
 * ItemIds and tuples have different alignment requirements, don't assume that
 * you can, say, fit 2 tuples of size MaxHeapTupleSize/2 on the same page.
 */
//#define MaxHeapTupleSize  (BLCKSZ - MAXALIGN(SizeOfPageHeaderData + sizeof(ItemIdData)))
// Play it safe until I test better... Make MaxHeapTupleSize restricted to MaxTupleSize
#define MaxHeapTupleSize MaxTupleSize

/*
 * MaxHeapTuplesPerPage is an upper bound on the number of tuples that can
 * fit on one heap page.  (Note that indexes could have more, because they
 * use a smaller tuple header.)  We arrive at the divisor because each tuple
 * must be maxaligned, and it must have an associated item pointer.
 */
#define MaxHeapTuplesPerPage	\
	((int) ((BLCKSZ - offsetof(PageHeaderData, pd_linp)) / \
			(MAXALIGN(offsetof(HeapTupleHeaderData, t_bits)) + sizeof(ItemIdData))))

/*
 * MaxAttrSize is a somewhat arbitrary upper limit on the declared size of
 * data fields of char(n) and similar types.  It need not have anything
 * directly to do with the *actual* upper limit of varlena values, which
 * is currently 1Gb (see TOAST structures in postgres.h).  I've set it
 * at 10Mb which seems like a reasonable number --- tgl 8/6/00.
 */
#define MaxAttrSize		(10 * 1024 * 1024)


/*
 * HeapTupleData is an in-memory data structure that points to a tuple.
 *
 * There are several ways in which this data structure is used:
 *
 * * Pointer to a tuple in a disk buffer: t_data points directly into the
 *	 buffer (which the code had better be holding a pin on, but this is not
 *	 reflected in HeapTupleData itself).
 *
 * * Pointer to nothing: t_data is NULL.  This is used as a failure indication
 *	 in some functions.
 *
 * * Part of a palloc'd tuple: the HeapTupleData itself and the tuple
 *	 form a single palloc'd chunk.  t_data points to the memory location
 *	 immediately following the HeapTupleData struct (at offset HEAPTUPLESIZE).
 *	 This is the output format of heap_form_tuple and related routines.
 *
 * * Separately allocated tuple: t_data points to a palloc'd chunk that
 *	 is not adjacent to the HeapTupleData.	(This case is deprecated since
 *	 it's difficult to tell apart from case #1.  It should be used only in
 *	 limited contexts where the code knows that case #1 will never apply.)
 *
 * * Separately allocated minimal tuple: t_data points MINIMAL_TUPLE_OFFSET
 *	 bytes before the start of a MinimalTuple.	As with the previous case,
 *	 this can't be told apart from case #1 by inspection; code setting up
 *	 or destroying this representation has to know what it's doing.
 *
 * t_len should always be valid, except in the pointer-to-nothing case.
 * t_self and t_tableOid should be valid if the HeapTupleData points to
 * a disk buffer, or if it represents a copy of a tuple on disk.  They
 * should be explicitly set invalid in manufactured tuples.
 *
 * CDB: t_tableOid deleted.  Instead, use tts_tableOid in TupleTableSlot.
 */
typedef struct HeapTupleData
{
	uint32		t_len;			/* length of *t_data */
	ItemPointerData t_self;		/* SelfItemPointer */
	HeapTupleHeader t_data;		/* -> tuple header and data */
} HeapTupleData;

typedef HeapTupleData *HeapTuple;

#define HEAPTUPLESIZE	MAXALIGN(sizeof(HeapTupleData))

/* XXX Hack Hack Hack 
 * heaptuple, or memtuple, cannot be more than 2G, so, if
 * the first bit is ever set, it is really a memtuple
 */
static inline bool is_heaptuple_memtuple(HeapTuple htup)
{
	return ((htup->t_len & 0x80000000) != 0);
}
static inline bool is_heaptuple_splitter(HeapTuple htup)
{
	return ((char *) htup->t_data) != ((char *) htup + HEAPTUPLESIZE);
}
static inline uint32 heaptuple_get_size(HeapTuple htup)
{
	return htup->t_len + HEAPTUPLESIZE;
}

/*
 * GETSTRUCT - given a HeapTuple pointer, return address of the user data
 */
#define GETSTRUCT(TUP) ((char *) ((TUP)->t_data) + (TUP)->t_data->t_hoff)

/*
 * Accessor macros to be used with HeapTuple pointers.
 */
#define HeapTupleIsValid(tuple) PointerIsValid(tuple)

#define HeapTupleHasNulls(tuple) \
		(((tuple)->t_data->t_infomask & HEAP_HASNULL) != 0)

#define HeapTupleNoNulls(tuple) \
		(!((tuple)->t_data->t_infomask & HEAP_HASNULL))

#define HeapTupleHasVarWidth(tuple) \
		(((tuple)->t_data->t_infomask & HEAP_HASVARWIDTH) != 0)

#define HeapTupleAllFixed(tuple) \
		(!((tuple)->t_data->t_infomask & HEAP_HASVARWIDTH))

#define HeapTupleHasExternal(tuple) \
		(((tuple)->t_data->t_infomask & HEAP_HASEXTERNAL) != 0)

#define HeapTupleHasCompressed(tuple) \
		(((tuple)->t_data->t_infomask & HEAP_HASCOMPRESSED) != 0)

#define HeapTupleHasExtended(tuple) \
		(((tuple)->t_data->t_infomask & HEAP_HASEXTENDED) != 0)

#define HeapTupleGetOid(tuple) \
		HeapTupleHeaderGetOid((tuple)->t_data)

#define HeapTupleSetOid(tuple, oid) \
		HeapTupleHeaderSetOid((tuple)->t_data, (oid))


/*
 * WAL record definitions for heapam.c's WAL operations
 *
 * XLOG allows to store some information in high 4 bits of log
 * record xl_info field.  We use 3 for opcode and one for init bit.
 */
#define XLOG_HEAP_INSERT	0x00
#define XLOG_HEAP_DELETE	0x10
#define XLOG_HEAP_UPDATE	0x20
#define XLOG_HEAP_MOVE		0x30
#define XLOG_HEAP_CLEAN		0x40
#define XLOG_HEAP_NEWPAGE	0x50
#define XLOG_HEAP_LOCK		0x60
#define XLOG_HEAP_INPLACE	0x70
#define XLOG_HEAP_OPMASK	0x70
/*
 * When we insert 1st item on new page in INSERT/UPDATE
 * we can (and we do) restore entire page in redo
 */
#define XLOG_HEAP_INIT_PAGE 0x80
/*
 * We ran out of opcodes, so heapam.c now has a second RmgrId.  These opcodes
 * are associated with RM_HEAP2_ID, but are not logically different from
 * the ones above associated with RM_HEAP_ID.  We apply XLOG_HEAP_OPMASK,
 * although currently XLOG_HEAP_INIT_PAGE is not used for any of these.
 */
#define XLOG_HEAP2_FREEZE	0x00

/*
 * All what we need to find changed tuple
 *
 * NB: on most machines, sizeof(xl_heaptid) will include some trailing pad
 * bytes for alignment.  We don't want to store the pad space in the XLOG,
 * so use SizeOfHeapTid for space calculations.  Similar comments apply for
 * the other xl_FOO structs.
 */
typedef struct xl_heaptid
{
	RelFileNode node;
	ItemPointerData persistentTid;
	int64 persistentSerialNum;
	ItemPointerData tid;		/* changed tuple id */
} xl_heaptid;

#define SizeOfHeapTid		(offsetof(xl_heaptid, tid) + SizeOfIptrData)

typedef struct xl_heapnode
{
	RelFileNode node;
	ItemPointerData persistentTid;
	int64 persistentSerialNum;
} xl_heapnode;

/* This is what we need to know about delete */
typedef struct xl_heap_delete
{
	xl_heaptid	target;			/* deleted tuple id */
} xl_heap_delete;

#define SizeOfHeapDelete	(offsetof(xl_heap_delete, target) + SizeOfHeapTid)

/*
 * We don't store the whole fixed part (HeapTupleHeaderData) of an inserted
 * or updated tuple in WAL; we can save a few bytes by reconstructing the
 * fields that are available elsewhere in the WAL record, or perhaps just
 * plain needn't be reconstructed.  These are the fields we must store.
 * NOTE: t_hoff could be recomputed, but we may as well store it because
 * it will come for free due to alignment considerations.
 */
typedef struct xl_heap_header
{
	uint16		t_infomask2;
	uint16		t_infomask;
	uint8		t_hoff;
} xl_heap_header;

#define SizeOfHeapHeader	(offsetof(xl_heap_header, t_hoff) + sizeof(uint8))

/* This is what we need to know about insert */
typedef struct xl_heap_insert
{
	xl_heaptid	target;			/* inserted tuple id */
	/* xl_heap_header & TUPLE DATA FOLLOWS AT END OF STRUCT */
} xl_heap_insert;

#define SizeOfHeapInsert	(offsetof(xl_heap_insert, target) + SizeOfHeapTid)

/* This is what we need to know about update|move */
typedef struct xl_heap_update
{
	xl_heaptid	target;			/* deleted tuple id */
	ItemPointerData newtid;		/* new inserted tuple id */
	/* NEW TUPLE xl_heap_header (PLUS xmax & xmin IF MOVE OP) */
	/* and TUPLE DATA FOLLOWS AT END OF STRUCT */
} xl_heap_update;

#define SizeOfHeapUpdate	(offsetof(xl_heap_update, newtid) + SizeOfIptrData)

/* This is what we need to know about vacuum page cleanup */
typedef struct xl_heap_clean
{
	xl_heapnode heapnode;
	BlockNumber block;
	/* UNUSED OFFSET NUMBERS FOLLOW AT THE END */
} xl_heap_clean;

#define SizeOfHeapClean (offsetof(xl_heap_clean, block) + sizeof(BlockNumber))

/* This is for replacing a page's contents in toto */
/* NB: this is used for indexes as well as heaps */
typedef struct xl_heap_newpage
{
	xl_heapnode heapnode;
	BlockNumber blkno;			/* location of new page */
	/* entire page contents follow at end of record */
} xl_heap_newpage;

#define SizeOfHeapNewpage	(offsetof(xl_heap_newpage, blkno) + sizeof(BlockNumber))

/* This is what we need to know about lock */
typedef struct xl_heap_lock
{
	xl_heaptid	target;			/* locked tuple id */
	TransactionId locking_xid;	/* might be a MultiXactId not xid */
	bool		xid_is_mxact;	/* is it? */
	bool		shared_lock;	/* shared or exclusive row lock? */
} xl_heap_lock;

#define SizeOfHeapLock	(offsetof(xl_heap_lock, shared_lock) + sizeof(bool))

/* This is what we need to know about in-place update */
typedef struct xl_heap_inplace
{
	xl_heaptid	target;			/* updated tuple id */
	/* TUPLE DATA FOLLOWS AT END OF STRUCT */
} xl_heap_inplace;

#define SizeOfHeapInplace	(offsetof(xl_heap_inplace, target) + SizeOfHeapTid)

/* This is what we need to know about tuple freezing during vacuum */
typedef struct xl_heap_freeze
{
	xl_heapnode heapnode;
	BlockNumber block;
	TransactionId cutoff_xid;
	/* TUPLE OFFSET NUMBERS FOLLOW AT THE END */
} xl_heap_freeze;

#define SizeOfHeapFreeze (offsetof(xl_heap_freeze, cutoff_xid) + sizeof(TransactionId))

/* HeapTupleHeader functions implemented in utils/time/combocid.c */
extern CommandId HeapTupleHeaderGetCmin(HeapTupleHeader tup);
extern CommandId HeapTupleHeaderGetCmax(HeapTupleHeader tup);
extern void HeapTupleHeaderAdjustCmax(HeapTupleHeader tup,
						  CommandId *cmax,
						  bool *iscombo);
						  
#endif   /* HTUP_H */
