/*-------------------------------------------------------------------------
 *
 * toasting.h
 *	  This file provides some definitions to support creation of aoseg tables
 *
 *
 * Portions Copyright (c) 2008, Greenplum Inc.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *-------------------------------------------------------------------------
 */
#ifndef AOSEG_H
#define AOSEG_H

#define Natts_pg_aoseg_XXX 6
#define Anum_pg_aoseg_XXX_segno 1
#define Anum_pg_aoseg_XXX_eof 2
#define Anum_pg_aoseg_XXX_tupcount 3
#define Anum_pg_aoseg_XXX_varblockcount 4
#define Anum_pg_aoseg_XXX_eofuncompressedo 5
#define Anum_pg_aoseg_XXX_content 6

#define Natts_pg_aocsseg_XXX 5
#define Anum_pg_aocsseg_XXX_segno 1
#define Anum_pg_aocsseg_XXX_tupcount 2
#define Anum_pg_aocsseg_XXX_varblockcount 3
#define Anum_pg_aocsseg_XXX_vpinfo 4
#define Anum_pg_aocsseg_XXX_content 5



/*
 * aoseg.c prototypes
 */
extern void AlterTableCreateAoSegTable(Oid relOid);
extern void AlterTableCreateAoSegTableWithOid(Oid relOid, Oid newOid,
											  Oid newIndexOid,
											  Oid *comptypeOid,
											  bool is_part_child);

extern void gpsql_appendonly_segfile_create(PG_FUNCTION_ARGS);

#endif   /* AOSEG_H */
