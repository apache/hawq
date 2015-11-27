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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*-------------------------------------------------------------------------
 *
 * filespace.h
 *		Filespace management commands (create/drop filespace).
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
extern void add_catalog_filespace_entry(Relation rel, Oid fsoid, int16 dbid,
										char *location);
extern void dbid_remove_filespace_entries(Relation rel, int16 dbid);
#endif   /* FILESPACE_H */
