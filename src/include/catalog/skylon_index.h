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
#ifndef SKYLON_INDEX_H
#define SKYLON_INDEX_H

#include "catalog/genbki.h"
#include "c.h"

#define SkylonIndexRelationId  4857

CATALOG(skylon_index,4857) BKI_WITHOUT_OIDS
{
  NameData schemaname;
  NameData graphname;
  NameData elename;
	NameData		indexname;
	char    indextype;
	/* VARIABLE LENGTH FIELDS: */
	int2vector	indexkeys;
	int2vector	includekeys;
} FormData_skylon_index;

typedef FormData_skylon_index *Form_skylon_index;

/* ----------------
 *		compiler constants for pg_index
 * ----------------
 */
#define Natts_skylon_index					7
#define Anum_skylon_index_schemaname	 1
#define Anum_skylon_index_graphname		2
#define Anum_skylon_index_elename			3
#define Anum_skylon_index_indexname		4
#define Anum_skylon_index_indextype   5
#define Anum_skylon_index_indexkeys  6
#define Anum_skylon_index_includekeys	7

extern void InsertSkylonIndexEntry(const char* schemaname , const char* graphname,
                       const char* elename, char indextype, const char* indexname,
                       const int2* indexkeys, int indexkeysnum, const int2* includekeys, int includekeysnum);

#endif
