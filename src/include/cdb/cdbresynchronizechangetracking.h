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
 * cdbresynchronizechangetracking.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBRESYNCHRONIZECHANGETRACKING_H
#define CDBRESYNCHRONIZECHANGETRACKING_H

#include "access/gist_private.h"
#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "storage/relfilenode.h"

/*
 * The information needed for a change tracking record to be
 * stored.
 */
typedef struct RelationChangeInfo
{
	RelFileNode 	relFileNode;
	BlockNumber 	blockNumber;
	ItemPointerData persistentTid;
	int64	  		persistentSerialNum;
} RelationChangeInfo;

extern void ChangeTracking_GetRelationChangeInfoFromXlog(
									  RmgrId	xl_rmid,
									  uint8 	xl_info,
									  void		*data, 
									  RelationChangeInfo	*relationChangeInfoArray,
									  int					*relationChangeInfoArrayCount,
									  int					relationChangeInfoMaxSize);

extern bool ChangeTracking_PrintRelationChangeInfo(
									  RmgrId	xl_rmid,
									  uint8		xl_info,
									  void		*data, 
									  XLogRecPtr *loc,
									  bool	  weAreGeneratingXLogNow,
									  bool	  printSkipIssuesOnly);

extern int ChangeTracking_GetInfoArrayDesiredMaxLength(RmgrId rmid, uint8 info);

#endif   /* CDBRESYNCHRONIZECHANGETRACKING_H */

