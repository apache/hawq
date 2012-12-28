/*-------------------------------------------------------------------------
 *
 * filespace.h
 *		Filespace management commands (create/drop filespace).
 *
 *
 * Copyright (c) 2009-2009, Greenplum Inc
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef FILESPACE_H
#define FILESPACE_H

#include "nodes/parsenodes.h"
#include "utils/relcache.h"

/* CREATE FILESPACE */
extern void CreateFileSpace(CreateFileSpaceStmt *stmt);

/* DROP FILESPACE */
extern void RemoveFileSpace(List *names, DropBehavior behavior, bool missing_ok);
extern void RemoveFileSpaceById(Oid fsoid);

/* ALTER FILESPACE ... OWNER TO ... */
extern void AlterFileSpaceOwner(List *names, Oid newowner);

/* ALTER FILESPACE ... RENAME TO ... */
extern void RenameFileSpace(const char *oldname, const char *newname);

/* utility functions */
extern Oid get_filespace_oid(Relation rel, const char *filespacename);
extern char *get_filespace_name(Oid fsoid);
extern bool	is_filespace_shared(Oid fsoid);
extern char *get_filespace_path(Oid fseoid, int16 dbid);
extern void add_catalog_filespace_entry(Relation rel, Oid fsoid, int16 dbid,
										char *location);
extern void dbid_remove_filespace_entries(Relation rel, int16 dbid);
extern int num_filespaces(void);
extern List *get_filespace_contentids(Oid filespace);
#endif   /* FILESPACE_H */
