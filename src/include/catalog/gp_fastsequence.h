/*-------------------------------------------------------------------------
 *
 * gp_fastsequence.h
 *    a table maintaining a light-weight fast sequence number for a unique
 *    object.
 *
 * Copyright (c) 2009-2011, Greenplum Inc.
 *
 * $Id: $
 * $Change: $
 * $DateTime: $
 * $Author: $
 *-------------------------------------------------------------------------
 */
#ifndef GP_FASTSEQUENCE_H
#define GP_FASTSEQUENCE_H

#include "catalog/genbki.h"
#include "storage/itemptr.h"

/*
 * gp_fastsequence definition
 */

/* TIDYCAT_BEGINFAKEDEF

   CREATE TABLE gp_fastsequence
   with (camelcase=FastSequence, oid=false, relid=5043, reltype_oid=6453, content=SEGMENT_LOCAL)
   (
   objid   oid,
   objmod  bigint,
   last_sequence bigint
   contentid	integer
   );

   create unique index on gp_fastsequence(objid, objmod, contentid) with (indexid=6067);

   alter table gp_fastsequence add fk objid on pg_class(oid);

   TIDYCAT_ENDFAKEDEF
*/
#define FastSequenceRelationId 5043

CATALOG(gp_fastsequence,5043) BKI_WITHOUT_OIDS
{
	Oid				objid;				/* object oid */
	int8			objmod;				/* object modifier */
	int8			last_sequence;      /* the last sequence number used by the object */
	int4			contentid;			/* content id */
} FormData_gp_fastsequence;


/* ----------------
*		Form_gp_fastsequence corresponds to a pointer to a tuple with
*		the format of gp_fastsequence relation.
* ----------------
*/
typedef FormData_gp_fastsequence *Form_gp_fastsequence;

#define Natts_gp_fastsequence				4
#define Anum_gp_fastsequence_objid			1
#define Anum_gp_fastsequence_objmod         2
#define Anum_gp_fastsequence_last_sequence	3
#define Anum_gp_fastsequence_contentid		4

#define NUM_FAST_SEQUENCES					 100

/* No initial content */

/*
 * Insert a new light-weight fast sequence entry for a given object.
 *
 * The tid for the new entry is returned.
 */
extern void InsertFastSequenceEntry(Oid objid, int64 objmod, int64 lastSequence,
		ItemPointer tid);

/*
 * GetFastSequences
 *
 * Get a list of consecutive sequence numbers. The starting sequence
 * number is the maximal value between 'lastsequence' + 1 and minSequence.
 * The length of the list is given.
 *
 * If there is not such an entry for objid in the table, create
 * one here.
 *
 * The existing entry for objid in the table is updated with a new
 * lastsequence value.
 *
 * The tuple id value for this entry is copied out to 'tid'.
 */
extern int64 GetFastSequences(Oid objid, int64 objmod,
							  int64 minSequence, int64 numSequences,
							  ItemPointer tid);

/*
 * GetFastSequencesByTid
 *
 * Same as GetFastSequences, except that the tuple tid is given, and the tuple id
 * is not valid.
 */
extern int64 GetFastSequencesByTid(ItemPointer tid,
								   int64 minSequence,
								   int64 numSequences);

/*
 * RemoveFastSequenceEntry
 *
 * Remove all entries associated with the given object id.
 *
 * If the given objid is an invalid OID, this function simply
 * returns.
 *
 * If the given valid objid does not have an entry in
 * gp_fastsequence, this function errors out.
 */
extern void RemoveFastSequenceEntry(Oid objid);

#endif
