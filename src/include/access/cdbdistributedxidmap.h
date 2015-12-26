/*
 * cdbdistributedxidmap.h
 *		Maps distributed DTM XIDs to local xids 
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
#ifndef DISTRIBUTEDXIDMAP_H
#define DISTRIBUTEDXIDMAP_H

/* Map entry state */
typedef enum
{
	DISTRIBUTEDXIDMAP_STATE_NONE = 0,		
						// None must be 0 since we zero pages initially.
	DISTRIBUTEDXIDMAP_STATE_PREALLOC_FOR_OPEN_TRANS,
	DISTRIBUTEDXIDMAP_STATE_IN_PROGRESS,
	DISTRIBUTEDXIDMAP_STATE_PREPARED,
	DISTRIBUTEDXIDMAP_STATE_COMMITTED,
	DISTRIBUTEDXIDMAP_STATE_ABORTED,
}	DistributedMapState;

/* Number of SLRU buffers to use for subtrans */
#define NUM_DISTRIBUTEDXIDMAP_BUFFERS	32

extern void AllocOrGetLocalXidForStartDistributedTransaction(DistributedTransactionId gxid, TransactionId *xid);
extern void PreallocLocalXidsForOpenDistributedTransactions(DistributedTransactionId *gxidArray, uint32 count);
extern TransactionId GetLocalXidForDistributedTransaction(DistributedTransactionId gxid);
extern void UpdateDistributedXidMapState(DistributedTransactionId gxid, DistributedMapState newState);

extern Size DistributedXidMapShmemSize_SLru(void);
extern Size DistributedXidMapShmemSize(void);
extern void DistributedXidMapShmemInit_SLru(void);
extern void DistributedXidMapShmemInit(void);
extern void BootStrapDistributedXidMap(void);
extern void StartupDistributedXidMap(void);
extern void ShutdownDistributedXidMap(void);
extern void CheckPointDistributedXidMap(void);
extern void TruncateDistributedXidMap(DistributedTransactionId oldestXact);
extern DistributedTransactionId GetMaxDistributedXid(void);
extern void RecordMaxDistributedXid(DistributedTransactionId gxid);
#endif   /* DISTRIBUTEDXIDMAP_H */

