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
 * cdbdispatchedtablespaceinfo.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef _CDBDISPATCHEDTABLESPACEINFO_H_
#define _CDBDISPATCHEDTABLESPACEINFO_H_

#include "catalog/gp_persistent.h"

/*
 * in gpsql, dispatcher will dispatch the filespace info to all QE.
 * QE keep this info in hash table.
 */
struct DispatchedFilespaceDirEntryData
{
	Oid 	tablespace;
	char	location[FilespaceLocationBlankPaddedWithNullTermLen];
};

typedef struct DispatchedFilespaceDirEntryData DispatchedFilespaceDirEntryData;

typedef DispatchedFilespaceDirEntryData *DispatchedFilespaceDirEntry;

extern void DispatchedFilespace_GetPathForTablespace(Oid tablespace, char **filespacePath, bool * found);
extern void DispatchedFilespace_AddForTablespace(Oid tablespace, const char * path);
extern DispatchedFilespaceDirEntry DispatchedFilespace_SeqSearch_GetNext(void);
extern void DispatchedFilespace_SeqSearch_Term(void);

#endif /* _CDBDISPATCHEDTABLESPACEINFO_H_ */
