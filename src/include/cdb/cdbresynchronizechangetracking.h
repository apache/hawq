/*-------------------------------------------------------------------------
 *
 * cdbresynchronizechangetracking.h
 *
 * Copyright (c) 2009-2010, Greenplum inc
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

