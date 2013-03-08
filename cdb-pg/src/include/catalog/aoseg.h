/*-------------------------------------------------------------------------
 *
 * aoseg.h
 *	  This file provides some definitions to support creation of aoseg tables
 *
 *
 * Portions Copyright (c) 2008-2013, Greenplum Inc.
 *-------------------------------------------------------------------------
 */
#ifndef AOSEG_H
#define AOSEG_H

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
