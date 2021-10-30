/*-------------------------------------------------------------------------
 *
 * pg_index.h
 *	  definition of the system "index" relation (pg_index)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/catalog/pg_index.h,v 1.41 2006/10/04 00:30:07 momjian Exp $
 *
 * NOTES
 *	  the genbki.sh script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_INDEX_H
#define PG_INDEX_H

#include "catalog/genbki.h"

/* TIDYCAT_BEGINFAKEDEF

   CREATE TABLE pg_index
   with (oid=false, relid=2610)
   (
   indexrelid      oid        ,
   indrelid        oid        ,
   indnatts        smallint   ,
   indisunique     boolean    ,
   indisprimary    boolean    ,
   indisclustered  boolean    ,
   indisvalid      boolean    ,
   indkey          int2vector ,
   indclass        oidvector  ,
   indexprs        text       ,
   indpred         text       
   );

   create index on pg_index(indrelid) with (indexid=2678, CamelCase=IndexIndrelid);
   create unique index on pg_index(indexrelid) with (indexid=2679, CamelCase=IndexRelid, syscacheid=INDEXRELID, syscache_nbuckets=1024);

   alter table pg_index add fk indexrelid on pg_class(oid);
   alter table pg_index add fk indrelid on pg_class(oid);
   alter table pg_index add vector_fk indclass on pg_opclass(oid);

   TIDYCAT_ENDFAKEDEF
*/

/* ----------------
 *		pg_index definition.  cpp turns this into
 *		typedef struct FormData_pg_index.
 * ----------------
 */
#define IndexRelationId  2610

CATALOG(pg_index,2610) BKI_WITHOUT_OIDS
{
	Oid			indexrelid;		/* OID of the index */
	Oid			indrelid;		/* OID of the relation it indexes */
	int2		indnatts;		/* number of columns in index */
	int2    indnkeyatts ;  /* number of key columns in index */
	bool		indisunique;	/* is this a unique index? */
	bool		indisprimary;	/* is this index for primary key? */
	bool		indisclustered; /* is this the index last clustered by? */
	bool		indisvalid;		/* is this index valid for use by queries? */

	/* VARIABLE LENGTH FIELDS: */
	int2vector	indkey;			/* column numbers of indexed cols, or 0 */
	oidvector	indclass;		/* opclass identifiers */
	text		indexprs;		/* expression trees for index attributes that
								 * are not simple column references; one for
								 * each zero entry in indkey[] */
	text		indpred;		/* expression tree for predicate, if a partial
								 * index; else NULL */
} FormData_pg_index;

/* ----------------
 *		Form_pg_index corresponds to a pointer to a tuple with
 *		the format of pg_index relation.
 * ----------------
 */
typedef FormData_pg_index *Form_pg_index;

/* ----------------
 *		compiler constants for pg_index
 * ----------------
 */
#define Natts_pg_index					12
#define Anum_pg_index_indexrelid		1
#define Anum_pg_index_indrelid			2
#define Anum_pg_index_indnatts			3
#define Anum_pg_index_indnkeyatts   4
#define Anum_pg_index_indisunique		5
#define Anum_pg_index_indisprimary  6
#define Anum_pg_index_indisclustered	7
#define Anum_pg_index_indisvalid		8
#define Anum_pg_index_indkey			9
#define Anum_pg_index_indclass			10
#define Anum_pg_index_indexprs			11
#define Anum_pg_index_indpred			12

#endif   /* PG_INDEX_H */
