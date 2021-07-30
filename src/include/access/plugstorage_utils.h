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
 * plugstorage_utils.h
 *
 * Pluggable storage utility definitions. Support external table for now.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PLUGSTORAGE_UTILS_H
#define PLUGSTORAGE_UTILS_H

#include "fmgr.h"

/*
 * Scan functions for pluggable format
 */
typedef struct PlugStorageScanFuncs
{
	FmgrInfo *beginscan;
	FmgrInfo *getnext_init;
	FmgrInfo *getnext;
	FmgrInfo *rescan;
	FmgrInfo *endscan;
	FmgrInfo *stopscan;

} PlugStorageScanFuncs;

/*
 * Insert functions for pluggable format
 */
typedef struct PlugStorageInsertFuncs
{
	FmgrInfo *insert_init;
	FmgrInfo *insert;
	FmgrInfo *insert_finish;

} PlugStorageInsertFuncs;

/*
 * Delete functions for pluggable format
 * */
typedef struct PlugStorageDeleteFuncs
{
	FmgrInfo *begindeletes;
	FmgrInfo *deletes;
	FmgrInfo *enddeletes;
} PlugStorageDeleteFuncs;

/*
 * Update functions for pluggable format
 */
typedef struct PlugStorageUpdateFuncs
{
	FmgrInfo *beginupdates;
	FmgrInfo *updates;
	FmgrInfo *endupdates;
} PlugStorageUpdateFuncs;

#endif	/* PLUGSTORAGE_UTILS_H */
