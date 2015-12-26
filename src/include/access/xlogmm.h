/*
 * prototypes for master mirror synchronisation write ahead logging.
 *
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
 *
 */
#ifndef MMXLOG_H
#define MMXLOG_H

/* 
 * We define this to make it clear where temporary master mirror sync code
 * resides. This is to be replaced by master file rep.
 */
#define MASTER_MIRROR_SYNC

#include "c.h"
#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "lib/stringinfo.h"

#define MMXLOG_CREATE_DIR 0x00
#define MMXLOG_CREATE_FILE 0x10
#define MMXLOG_REMOVE_DIR 0x20
#define MMXLOG_REMOVE_FILE 0x30

typedef enum mm_fs_obj_type
{
	MM_OBJ_FILESPACE,
	MM_OBJ_TABLESPACE,
	MM_OBJ_DATABASE,
	MM_OBJ_RELATION,
	MM_OBJ_RELFILENODE
} mm_fs_obj_type;

typedef struct xl_mm_fs_obj
{
	mm_fs_obj_type objtype; /* One of the MM_OBJ_* types */

	Oid filespace;
	Oid tablespace;
	Oid database;
	Oid relfilenode;
	int32 segnum;

	bool shared;
	char path[MAXPGPATH];
    
	ItemPointerData persistentTid;
	int64 persistentSerialNum;
} xl_mm_fs_obj;

/*
 * XLOG format.
 */

typedef struct fspc_map
{
	Oid filespaceoid;
	int16 dbid1;
	int16 dbid2;
	char path1[MAXPGPATH];
	char path2[MAXPGPATH];
} fspc_map;

typedef struct tspc_map
{
	Oid tablespaceoid;
	Oid filespaceoid;
} tspc_map;

typedef struct dbdir_map
{
	Oid tablespaceoid;
	Oid databaseoid;
} dbdir_map;


/*
 * Container of filespaces/tablespaces used to accumulate mappings
 */
typedef struct fspc_agg_state
{
	union
	{
		int count;
		int64 dummy;
	};
	fspc_map maps[0]; /* variable length */
} fspc_agg_state;

#define FSPC_CHECKPOINT_BYTES(count) \
    (SIZEOF_VARSTRUCT(count, fspc_agg_state, maps))

typedef struct tspc_agg_state
{
	union
	{
		int count;
		int64 dummy;
	};
	tspc_map maps[0]; /* variable length */
} tspc_agg_state;

#define TSPC_CHECKPOINT_BYTES(count) \
    (SIZEOF_VARSTRUCT(count, tspc_agg_state, maps))

typedef struct dbdir_agg_state
{
	union
	{
		int count;
		int64 dummy;
	};
	dbdir_map maps[0]; /* variable length */
}dbdir_agg_state;

#define DBDIR_CHECKPOINT_BYTES(count) \
    (SIZEOF_VARSTRUCT(count, dbdir_agg_state, maps))


extern void mmxlog_redo(XLogRecPtr beginLoc, XLogRecPtr lsn, XLogRecord *record);
extern void mmxlog_desc(StringInfo buf, XLogRecPtr beginLoc, XLogRecord *record);

/* 
 * Functions to generate WAL records to remove file system objects on the
 * master / standby master.
 */
extern void mmxlog_log_remove_filespace(Oid filespace,ItemPointer persistentTid, int64 persistentSerialNum);
extern void mmxlog_log_remove_tablespace(Oid tablespace,ItemPointer persistentTid, int64 persistentSerialNum);
extern void mmxlog_log_remove_database(Oid tablespace, Oid database,ItemPointer persistentTid, int64 persistentSerialNum);
extern void mmxlog_log_remove_relation(Oid tablespace, Oid database, Oid relfilenode,ItemPointer persistentTid, int64 persistentSerialNum);
extern void mmxlog_log_remove_relfilenode(Oid tablespace, Oid database,
										  Oid relfilenode, int32 segnum,ItemPointer persistentTid, int64 persistentSerialNum);

/*
 * Functions to generate WAL records to add file system objects on the
 * master / standby master.
 */
extern void mmxlog_log_create_filespace(Oid filespace,ItemPointer persistentTid, int64 persistentSerialNum);
extern void mmxlog_log_create_tablespace(Oid filespace, Oid tablespace,ItemPointer persistentTid, int64 persistentSerialNum);
extern void mmxlog_log_create_database(Oid tablespace, Oid database,ItemPointer persistentTid, int64 persistentSerialNum);
extern void mmxlog_log_create_relation(Oid tablespace, Oid database, Oid relfilenode,ItemPointer persistentTid, int64 persistentSerialNum);
extern void mmxlog_log_create_relfilenode(Oid tablespace, Oid database,
										  Oid relfilenode, int32 segnum,ItemPointer persistentTid, int64 persistentSerialNum);
extern void mmxlog_append_checkpoint_data(XLogRecData rdata[5]);
extern void mmxlog_read_checkpoint_data(char *cpdata, int masterMirroringLen, int checkpointLen, XLogRecPtr *beginLoc);
extern bool mmxlog_filespace_get_path(
	Oid fspcoid,

	char **filespacePath);
extern bool mmxlog_tablespace_get_filespace(
	Oid tspcoid,

	Oid *filespaceOid);

extern void mmxlog_add_filespace_init(
	fspc_agg_state **fas, int *maxCount);
extern void mmxlog_add_filespace(
	fspc_agg_state **fas, int *maxCount,
	Oid filespace, 
	char *path1,
	char *caller);

extern void mmxlog_add_tablespace_init(
		tspc_agg_state **tas, int *maxCount);
extern void mmxlog_add_tablespace( 
		tspc_agg_state **tas, int *maxCount,
		Oid filespace, Oid tablespace, char *caller);

extern void mmxlog_add_database_init(
		dbdir_agg_state **das, int *maxCount);
extern void mmxlog_add_database( 
		dbdir_agg_state **das, int *maxCount,
		Oid database, Oid tablespace, char *caller);

extern char *mmxlog_get_checkpoint_record_suffix(XLogRecord *checkpointRecord);

extern bool mmxlog_get_checkpoint_info(char *cpdata, int masterMirroringLen, int checkpointLen, XLogRecPtr *beginLoc, int errlevel,
		fspc_agg_state **f,
		tspc_agg_state **t,
	        dbdir_agg_state **d);
extern bool mmxlog_verify_checkpoint_info(char *cpdata, int masterMirroringLen, int checkpointLen, XLogRecPtr *beginLoc, int errlevel);
extern bool mmxlog_get_checkpoint_counts(char *cpdata, int masterMirroringLen, int checkpointLen, XLogRecPtr *beginLoc, int errlevel, int *filespaceCount, int *tablespaceCount, int *databaseCount);

extern void mmxlog_print_filespaces(int elevel, char *caller);
extern void mmxlog_print_tablespaces(int elevel, char *caller);

extern void mmxlog_empty_hashtables(void);

#endif /* MMXLOG_H */
