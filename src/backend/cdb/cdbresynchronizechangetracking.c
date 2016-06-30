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
 * cdbresynchronizechangetracking.c
 *
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <sys/stat.h>
#include "postgres.h"
#include "miscadmin.h"
#include "cdb/cdbutil.h"
#include <unistd.h>

#include "access/bitmap.h"
#include "access/htup.h"
#include "access/nbtree.h"
#include "catalog/pg_authid.h"
#include "commands/sequence.h"
#include "executor/spi.h"
#include "postmaster/primary_mirror_mode.h"
#include "postmaster/fts.h"
#include "storage/fd.h"
#include "storage/lock.h"
#include "storage/relfilenode.h"
#include "storage/shmem.h"
#include "utils/faultinjector.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/faultinjector.h"

#include "cdb/cdbfilerep.h"
#include "cdb/cdbresynchronizechangetracking.h"
#include "cdb/cdbpersistentrelfile.h"
#include "cdb/cdbpersistentstore.h"

bool ChangeTracking_PrintRelationChangeInfo(
									  RmgrId	xl_rmid,
									  uint8		xl_info,
									  void		*data, 
									  XLogRecPtr *loc,
									  bool		weAreGeneratingXLogNow,
									  bool		printSkipIssuesOnly)
{
	bool				atLeastOneSkipIssue = false;
	int					relationChangeInfoArrayCount;
	int					i;
	int					arrlen = ChangeTracking_GetInfoArrayDesiredMaxLength(xl_rmid, xl_info);
	RelationChangeInfo 	relationChangeInfoArray[arrlen];
	
	ChangeTracking_GetRelationChangeInfoFromXlog(
										  xl_rmid,
										  xl_info,
										  data,
										  relationChangeInfoArray,
										  &relationChangeInfoArrayCount,
										  arrlen);

	for (i = 0; i < relationChangeInfoArrayCount; i++)
	{
		RelationChangeInfo	*relationChangeInfo;
		int64				maxPersistentSerialNum;
		bool				skip;
		bool				zeroTid = false;
		bool				invalidTid = false;
		bool				zeroSerialNum = false;
		bool				invalidSerialNum = false;
		bool				skipIssue = false;

		relationChangeInfo = &relationChangeInfoArray[i];

		if (weAreGeneratingXLogNow)
			maxPersistentSerialNum = PersistentRelfile_MyHighestSerialNum();
		else
			maxPersistentSerialNum = PersistentRelfile_CurrentMaxSerialNum();

		skip = GpPersistent_SkipXLogInfo(relationChangeInfo->relFileNode.relNode);
		if (!skip)
		{
			zeroTid = PersistentStore_IsZeroTid(&relationChangeInfo->persistentTid);
			if (!zeroTid)
				invalidTid = !ItemPointerIsValid(&relationChangeInfo->persistentTid);
			zeroSerialNum = (relationChangeInfo->persistentSerialNum == 0);
			if (!zeroSerialNum)
			{
				invalidSerialNum = (relationChangeInfo->persistentSerialNum < 0);

				/*
				 * If we have'nt done the scan yet... do not do upper range check.
				 */
				if (maxPersistentSerialNum != 0 &&
					relationChangeInfo->persistentSerialNum > maxPersistentSerialNum)
					invalidSerialNum = true;
			}
			skipIssue = (zeroTid || invalidTid || zeroSerialNum || invalidSerialNum);
		}

		if (!printSkipIssuesOnly || skipIssue)
			elog(LOG, 
				 "ChangeTracking_PrintRelationChangeInfo: [%d] xl_rmid %d, xl_info 0x%X, %u/%u/%u, block number %u, LSN %s, persistent serial num " INT64_FORMAT ", TID %s, maxPersistentSerialNum " INT64_FORMAT ", skip %s, zeroTid %s, invalidTid %s, zeroSerialNum %s, invalidSerialNum %s, skipIssue %s",
				i,
				xl_rmid,
				xl_info,
				relationChangeInfo->relFileNode.spcNode,
				relationChangeInfo->relFileNode.dbNode,
				relationChangeInfo->relFileNode.relNode,
				relationChangeInfo->blockNumber,
				XLogLocationToString(loc),
				relationChangeInfo->persistentSerialNum,
				ItemPointerToString(&relationChangeInfo->persistentTid),
				maxPersistentSerialNum,
				(skip ? "true" : "false"),
				(zeroTid ? "true" : "false"),
				(invalidTid ? "true" : "false"),
				(zeroSerialNum ? "true" : "false"),
				(invalidSerialNum ? "true" : "false"),
				(skipIssue ? "true" : "false"));

		if (skipIssue)
			atLeastOneSkipIssue = true;
	}

	return atLeastOneSkipIssue;
}


static void ChangeTracking_AddRelationChangeInfo(
									  RelationChangeInfo	*relationChangeInfoArray,
									  int					*relationChangeInfoArrayCount,
									  int					 relationChangeInfoMaxSize,
									  RelFileNode *relFileNode,
									  BlockNumber blockNumber,
									  ItemPointer persistentTid,
									  int64	  	  persistentSerialNum)
{
	RelationChangeInfo	*relationChangeInfo;
	
	Assert (*relationChangeInfoArrayCount < relationChangeInfoMaxSize);

	relationChangeInfo = &relationChangeInfoArray[*relationChangeInfoArrayCount];

	relationChangeInfo->relFileNode = 			*relFileNode;
	relationChangeInfo->blockNumber = 			blockNumber;
	relationChangeInfo->persistentTid = 		*persistentTid;
	relationChangeInfo->persistentSerialNum = 	persistentSerialNum;

	(*relationChangeInfoArrayCount)++;
}

void ChangeTracking_GetRelationChangeInfoFromXlog(
									  RmgrId	xl_rmid,
									  uint8 	xl_info,
									  void		*data, 
									  RelationChangeInfo	*relationChangeInfoArray,
									  int					*relationChangeInfoArrayCount,
									  int					relationChangeInfoMaxSize)
{

	uint8	info = xl_info & ~XLR_INFO_MASK;
	uint8	op = 0;

	MemSet(relationChangeInfoArray, 0, sizeof(RelationChangeInfo) * relationChangeInfoMaxSize);
	*relationChangeInfoArrayCount = 0;

	/*
	 * Find the RM for this xlog record and see whether we are
	 * interested in logging it as a buffer pool change or not.
	 */
	switch (xl_rmid)
	{

		/*
		 * The following changes aren't interesting to the change log
		 */
		case RM_XLOG_ID:
		case RM_CLOG_ID:
		case RM_MULTIXACT_ID:
		case RM_XACT_ID:
		case RM_SMGR_ID:
		case RM_DBASE_ID:
		case RM_TBLSPC_ID:
			break;
		case RM_MMXLOG_ID:
			switch (info)
			{
				// relation dirs and node files create/drop on hdfs need to be tracked down
				// so that we can skip at recovery pass3
				case MMXLOG_CREATE_DIR:
				case MMXLOG_REMOVE_DIR:
				case MMXLOG_REMOVE_FILE:
				case MMXLOG_CREATE_FILE:
				{
					xl_mm_fs_obj *xlrec = (xl_mm_fs_obj *) data;
					if(xlrec->shared) // it is hdfs
					{
						// Only hdfs file/dir need to be skip
						RelFileNode rfn;
						rfn.spcNode = xlrec->tablespace;
						rfn.dbNode = xlrec->database;
						rfn.relNode = xlrec->relfilenode;
						ChangeTracking_AddRelationChangeInfo(
															relationChangeInfoArray,
															relationChangeInfoArrayCount,
															relationChangeInfoMaxSize,
															&(rfn),
															InvalidBlockNumber,
															&xlrec->persistentTid,
															xlrec->persistentSerialNum);
					}
					break;
				}
				default:
					break;
			}
			break;

		/* 
		 * These aren't supported in GPDB
		 */
		case RM_HASH_ID:
			elog(ERROR, "internal error: unsupported RM ID (%d) in ChangeTracking_GetRelationChangeInfoFromXlog", xl_rmid);
			break;
		case RM_GIN_ID:
			/* keep LOG severity till crash recovery or GIN is implemented in order to avoid double failures during cdbfast */
			elog(LOG, "internal error: unsupported RM ID (%d) in ChangeTracking_GetRelationChangeInfoFromXlog", xl_rmid);
			break;
		/*
		 * The following changes must be logged in the change log.
		 */
		case RM_HEAP2_ID:
			switch (info)
			{
				case XLOG_HEAP2_FREEZE:
				{
					xl_heap_freeze *xlrec = (xl_heap_freeze *) data;

					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->heapnode.node),
													   xlrec->block,
													   &xlrec->heapnode.persistentTid,
													   xlrec->heapnode.persistentSerialNum);
					break;
				}
				default:
					elog(ERROR, "internal error: unsupported RM_HEAP2_ID op (%u) in ChangeTracking_GetRelationChangeInfoFromXlog", info);
			}
			break;
		case RM_HEAP_ID:
			op = info & XLOG_HEAP_OPMASK;
			switch (op)
			{
				case XLOG_HEAP_INSERT:
				{
					xl_heap_insert *xlrec = (xl_heap_insert *) data;
										
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->target.node),
													   ItemPointerGetBlockNumber(&(xlrec->target.tid)),
													   &xlrec->target.persistentTid,
													   xlrec->target.persistentSerialNum);
					break;
				}
				case XLOG_HEAP_DELETE:
				{
					xl_heap_delete *xlrec = (xl_heap_delete *) data;
										
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->target.node),
													   ItemPointerGetBlockNumber(&(xlrec->target.tid)),
													   &xlrec->target.persistentTid,
													   xlrec->target.persistentSerialNum);
					break;
				}
				case XLOG_HEAP_UPDATE:
				case XLOG_HEAP_MOVE:
				{
					xl_heap_update *xlrec = (xl_heap_update *) data;
					
					BlockNumber oldblock = ItemPointerGetBlockNumber(&(xlrec->target.tid));
					BlockNumber	newblock = ItemPointerGetBlockNumber(&(xlrec->newtid));						
					bool		samepage = (oldblock == newblock);
					
					
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->target.node),
													   newblock,
													   &xlrec->target.persistentTid,
													   xlrec->target.persistentSerialNum);
					if(!samepage)
						ChangeTracking_AddRelationChangeInfo(
														   relationChangeInfoArray,
														   relationChangeInfoArrayCount,
														   relationChangeInfoMaxSize,
														   &(xlrec->target.node),
														   oldblock,
														   &xlrec->target.persistentTid,
														   xlrec->target.persistentSerialNum);
						
					break;
				}
				case XLOG_HEAP_CLEAN:
				{
					xl_heap_clean *xlrec = (xl_heap_clean *) data;

					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->heapnode.node),
													   xlrec->block,
													   &xlrec->heapnode.persistentTid,
													   xlrec->heapnode.persistentSerialNum);
					break;
				}
				case XLOG_HEAP_NEWPAGE:
				{
					xl_heap_newpage *xlrec = (xl_heap_newpage *) data;

					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->heapnode.node),
													   xlrec->blkno,
													   &xlrec->heapnode.persistentTid,
													   xlrec->heapnode.persistentSerialNum);
					break;
				}
				case XLOG_HEAP_LOCK:
				{
					xl_heap_lock *xlrec = (xl_heap_lock *) data;
					BlockNumber block = ItemPointerGetBlockNumber(&(xlrec->target.tid));

					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->target.node),
													   block,
													   &xlrec->target.persistentTid,
													   xlrec->target.persistentSerialNum);
					break;
				}
				case XLOG_HEAP_INPLACE:
				{
					xl_heap_inplace *xlrec = (xl_heap_inplace *) data;
					BlockNumber block = ItemPointerGetBlockNumber(&(xlrec->target.tid));
					
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->target.node),
													   block,
													   &xlrec->target.persistentTid,
													   xlrec->target.persistentSerialNum);
					break;
				}

				default:
					elog(ERROR, "internal error: unsupported RM_HEAP_ID op (%u) in ChangeTracking_GetRelationChangeInfoFromXlog", op);
			}
			break;

		case RM_BTREE_ID:
			switch (info)
			{
				case XLOG_BTREE_INSERT_LEAF:
				case XLOG_BTREE_INSERT_UPPER:
				case XLOG_BTREE_INSERT_META:
				{
					xl_btree_insert *xlrec = (xl_btree_insert *) data;
					BlockIdData blkid = xlrec->target.tid.ip_blkid;
					
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->target.node),
													   BlockIdGetBlockNumber(&blkid),
													   &xlrec->target.persistentTid,
													   xlrec->target.persistentSerialNum);
					
					if(info == XLOG_BTREE_INSERT_META)
						ChangeTracking_AddRelationChangeInfo(
														   relationChangeInfoArray,
														   relationChangeInfoArrayCount,
														   relationChangeInfoMaxSize,
														   &(xlrec->target.node),
														   BTREE_METAPAGE,
														   &xlrec->target.persistentTid,
														   xlrec->target.persistentSerialNum);

					break;
				}
				case XLOG_BTREE_SPLIT_L:
				case XLOG_BTREE_SPLIT_R:
				case XLOG_BTREE_SPLIT_L_ROOT:
				case XLOG_BTREE_SPLIT_R_ROOT:
				{
					xl_btree_split *xlrec = (xl_btree_split *) data;
					BlockIdData blkid = xlrec->target.tid.ip_blkid;
					
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->target.node),
													   BlockIdGetBlockNumber(&blkid),
													   &xlrec->target.persistentTid,
													   xlrec->target.persistentSerialNum);
					
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->target.node),
													   xlrec->otherblk,
													   &xlrec->target.persistentTid,
													   xlrec->target.persistentSerialNum);
					
					if (xlrec->rightblk != P_NONE)
					{
						ChangeTracking_AddRelationChangeInfo(
														   relationChangeInfoArray,
														   relationChangeInfoArrayCount,
														   relationChangeInfoMaxSize,
														   &(xlrec->target.node),
														   xlrec->rightblk,
														   &xlrec->target.persistentTid,
														   xlrec->target.persistentSerialNum);
					}
					
					break;
				}
				case XLOG_BTREE_DELETE:
				{
					xl_btree_delete *xlrec = (xl_btree_delete *) data;

					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->btreenode.node),
													   xlrec->block,
													   &xlrec->btreenode.persistentTid,
													   xlrec->btreenode.persistentSerialNum);
					break;
				}
				case XLOG_BTREE_DELETE_PAGE:
				case XLOG_BTREE_DELETE_PAGE_HALF:
				case XLOG_BTREE_DELETE_PAGE_META:
				{
					xl_btree_delete_page *xlrec = (xl_btree_delete_page *) data;
					BlockIdData blkid = xlrec->target.tid.ip_blkid;
					BlockNumber block = BlockIdGetBlockNumber(&blkid);
					
					if (block != P_NONE)
						ChangeTracking_AddRelationChangeInfo(
														   relationChangeInfoArray,
														   relationChangeInfoArrayCount,
														   relationChangeInfoMaxSize,
														   &(xlrec->target.node),
														   block,
														   &xlrec->target.persistentTid,
														   xlrec->target.persistentSerialNum);
					
					if (xlrec->rightblk != P_NONE)
						ChangeTracking_AddRelationChangeInfo(
														   relationChangeInfoArray,
														   relationChangeInfoArrayCount,
														   relationChangeInfoMaxSize,
														   &(xlrec->target.node),
														   xlrec->rightblk,
														   &xlrec->target.persistentTid,
														   xlrec->target.persistentSerialNum);

					if (xlrec->leftblk != P_NONE)
						ChangeTracking_AddRelationChangeInfo(
														   relationChangeInfoArray,
														   relationChangeInfoArrayCount,
														   relationChangeInfoMaxSize,
														   &(xlrec->target.node),
														   xlrec->leftblk,
														   &xlrec->target.persistentTid,
														   xlrec->target.persistentSerialNum);
					
					if (xlrec->deadblk != P_NONE)
						ChangeTracking_AddRelationChangeInfo(
														   relationChangeInfoArray,
														   relationChangeInfoArrayCount,
														   relationChangeInfoMaxSize,
														   &(xlrec->target.node),
														   xlrec->deadblk,
														   &xlrec->target.persistentTid,
														   xlrec->target.persistentSerialNum);						
					
					if (info == XLOG_BTREE_DELETE_PAGE_META)
						ChangeTracking_AddRelationChangeInfo(
														   relationChangeInfoArray,
														   relationChangeInfoArrayCount,
														   relationChangeInfoMaxSize,
														   &(xlrec->target.node),
														   BTREE_METAPAGE,
														   &xlrec->target.persistentTid,
														   xlrec->target.persistentSerialNum);
					break;
				}
				case XLOG_BTREE_NEWROOT:
				{
					xl_btree_newroot *xlrec = (xl_btree_newroot *) data;

					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->btreenode.node),
													   xlrec->rootblk,
													   &xlrec->btreenode.persistentTid,
													   xlrec->btreenode.persistentSerialNum);	
					 
					/* newroot always updates the meta page */
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->btreenode.node),
													   BTREE_METAPAGE,
													   &xlrec->btreenode.persistentTid,
													   xlrec->btreenode.persistentSerialNum);	
					
					break;
				}

				default:
					elog(ERROR, "internal error: unsupported RM_BTREE_ID op (%u) in ChangeTracking_GetRelationChangeInfoFromXlog", info);
			}
			break;
		case RM_BITMAP_ID:
			switch (info)
			{
				case XLOG_BITMAP_INSERT_NEWLOV:
				{
					xl_bm_newpage	*xlrec = (xl_bm_newpage *) data;
					
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->bm_node),
													   xlrec->bm_new_blkno,
													   &xlrec->bm_persistentTid,
													   xlrec->bm_persistentSerialNum);
					break;
				}
				case XLOG_BITMAP_INSERT_LOVITEM:
				{
					xl_bm_lovitem	*xlrec = (xl_bm_lovitem *) data;
					
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->bm_node),
													   xlrec->bm_lov_blkno,
													   &xlrec->bm_persistentTid,
													   xlrec->bm_persistentSerialNum);
					
					if (xlrec->bm_is_new_lov_blkno)
						ChangeTracking_AddRelationChangeInfo(
														   relationChangeInfoArray,
														   relationChangeInfoArrayCount,
														   relationChangeInfoMaxSize,
														   &(xlrec->bm_node),
														   BM_METAPAGE,
														   &xlrec->bm_persistentTid,
														   xlrec->bm_persistentSerialNum);
					break;
				}
				case XLOG_BITMAP_INSERT_META:
				{
					xl_bm_metapage	*xlrec = (xl_bm_metapage *) data;
					
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->bm_node),
													   BM_METAPAGE,
													   &xlrec->bm_persistentTid,
													   xlrec->bm_persistentSerialNum);
					break;
				}
				case XLOG_BITMAP_INSERT_BITMAP_LASTWORDS:
				{
					xl_bm_bitmap_lastwords	*xlrec = (xl_bm_bitmap_lastwords *) data;
					
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->bm_node),
													   xlrec->bm_lov_blkno,
													   &xlrec->bm_persistentTid,
													   xlrec->bm_persistentSerialNum);
					break;
				}
				case XLOG_BITMAP_INSERT_WORDS:
				{
					xl_bm_bitmapwords	*xlrec = (xl_bm_bitmapwords *) data;
					
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->bm_node),
													   xlrec->bm_lov_blkno,
													   &xlrec->bm_persistentTid,
													   xlrec->bm_persistentSerialNum);

					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->bm_node),
													   xlrec->bm_blkno,
													   &xlrec->bm_persistentTid,
													   xlrec->bm_persistentSerialNum);
					
					if (!xlrec->bm_is_last)
						ChangeTracking_AddRelationChangeInfo(
														   relationChangeInfoArray,
														   relationChangeInfoArrayCount,
														   relationChangeInfoMaxSize,
														   &(xlrec->bm_node),
														   xlrec->bm_next_blkno,
														   &xlrec->bm_persistentTid,
														   xlrec->bm_persistentSerialNum);
					break;
				}
				case XLOG_BITMAP_UPDATEWORD:
				{
					xl_bm_updateword	*xlrec = (xl_bm_updateword *) data;
					
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->bm_node),
													   xlrec->bm_blkno,
													   &xlrec->bm_persistentTid,
													   xlrec->bm_persistentSerialNum);
					break;
				}
				case XLOG_BITMAP_UPDATEWORDS:
				{
					xl_bm_updatewords	*xlrec = (xl_bm_updatewords *) data;
					
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->bm_node),
													   xlrec->bm_first_blkno,
													   &xlrec->bm_persistentTid,
													   xlrec->bm_persistentSerialNum);
					
					if (xlrec->bm_two_pages)
						ChangeTracking_AddRelationChangeInfo(
														   relationChangeInfoArray,
														   relationChangeInfoArrayCount,
														   relationChangeInfoMaxSize,
														   &(xlrec->bm_node),
														   xlrec->bm_second_blkno,
														   &xlrec->bm_persistentTid,
														   xlrec->bm_persistentSerialNum);
					
					if (xlrec->bm_new_lastpage)
						ChangeTracking_AddRelationChangeInfo(
														   relationChangeInfoArray,
														   relationChangeInfoArrayCount,
														   relationChangeInfoMaxSize,
														   &(xlrec->bm_node),
														   xlrec->bm_lov_blkno,
														   &xlrec->bm_persistentTid,
														   xlrec->bm_persistentSerialNum);
						
					break;
				}
				default:
					elog(ERROR, "internal error: unsupported RM_BITMAP_ID op (%u) in ChangeTracking_GetRelationChangeInfoFromXlog", info);
			}
			break;
		case RM_SEQ_ID:
			switch (info)
			{
				case XLOG_SEQ_LOG:
				{
					xl_seq_rec 	*xlrec = (xl_seq_rec *) data;
					
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xlrec->node),
													   0, /* seq_redo touches block 0 only */
													   &xlrec->persistentTid,
													   xlrec->persistentSerialNum);

					break;
				}
				default:
					elog(ERROR, "internal error: unsupported RM_SEQ_ID op (%u) in ChangeTracking_GetRelationChangeInfoFromXlog", info);
			}
			break;
			
		case RM_GIST_ID:
			switch (info)
			{
				case XLOG_GIST_PAGE_UPDATE:
				case XLOG_GIST_NEW_ROOT:
				{
					gistxlogPageUpdate *xldata = (gistxlogPageUpdate *) data;					
					
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xldata->node),
													   xldata->blkno,
													   &xldata->persistentTid,
													   xldata->persistentSerialNum);
					break;
				}
				case XLOG_GIST_PAGE_DELETE:
				{
					gistxlogPageDelete *xldata = (gistxlogPageDelete *) data;

					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xldata->node),
													   xldata->blkno,
													   &xldata->persistentTid,
													   xldata->persistentSerialNum);
					break;
				}
				case XLOG_GIST_PAGE_SPLIT:
				{
					gistxlogPageSplit*	xldata = (gistxlogPageSplit *) data;
					char*				ptr;
					int 				j, 
										i = 0;

					/* first, log the splitted page */
					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xldata->node),
													   xldata->origblkno,
													   &xldata->persistentTid,
													   xldata->persistentSerialNum);

					/* now log all the pages that we split into */					
					ptr = (char *)data + sizeof(gistxlogPageSplit);	

					for (i = 0; i < xldata->npage; i++)
					{
						gistxlogPage*  gistp;
						
						gistp = (gistxlogPage *) ptr;
						ptr += sizeof(gistxlogPage);

						//elog(LOG, "CHANGETRACKING GIST SPLIT: block [%d/%d]:%d", i+1,xldata->npage, gistp->blkno);
						ChangeTracking_AddRelationChangeInfo(
														   relationChangeInfoArray,
														   relationChangeInfoArrayCount,
														   relationChangeInfoMaxSize,
														   &(xldata->node),
														   gistp->blkno,
														   &xldata->persistentTid,
														   xldata->persistentSerialNum);
						
						/* skip over all index tuples. we only care about block numbers */
						j = 0;
						while (j < gistp->num)
						{
							ptr += IndexTupleSize((IndexTuple) ptr);
							j++;
						}
					}
					
					break;
				}
				case XLOG_GIST_CREATE_INDEX:
				{
					gistxlogCreateIndex*  xldata = (gistxlogCreateIndex *) data;

					ChangeTracking_AddRelationChangeInfo(
													   relationChangeInfoArray,
													   relationChangeInfoArrayCount,
													   relationChangeInfoMaxSize,
													   &(xldata->node),
													   GIST_ROOT_BLKNO,
													   &xldata->persistentTid,
													   xldata->persistentSerialNum);
					break;
				}
				case XLOG_GIST_INSERT_COMPLETE:
				{
					/* nothing to be done here */
					break;
				}
				default:
					elog(ERROR, "internal error: unsupported RM_GIST_ID op (%u) in ChangeTracking_GetRelationChangeInfoFromXlog", info);
			}

			break;
		default:
			elog(ERROR, "internal error: unsupported resource manager type (%d) in ChangeTracking_GetRelationChangeInfoFromXlog", xl_rmid);
	}

}

/*
 * Any RM will not change more than 5 blocks per xlog record. The only exception
 * is a GIST RM SPLIT xlog record, which has an undefined number of blocks to change.
 * This function will return the maximum number of change infos that could occur, so
 * that we could set the array size accordingly.
 */
int ChangeTracking_GetInfoArrayDesiredMaxLength(RmgrId rmid, uint8 info)
{
	int 	MaxRelChangeInfoReturns = 5;
	int 	MaxRelChangeInfoReturns_GistSplit = 1024; //TODO: this is some sort of a very large guess. check if realistic.
	bool	gist_split = ((rmid == RM_GIST_ID && (info & ~XLR_INFO_MASK) == XLOG_GIST_PAGE_SPLIT));
	int		arrLen = (!gist_split ? MaxRelChangeInfoReturns : MaxRelChangeInfoReturns_GistSplit);
	
	return arrLen;
}
