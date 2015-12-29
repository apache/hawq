/*
 *  faultinjector.h
 *  
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

#ifndef FAULTINJECTOR_H
#define FAULTINJECTOR_H

#include "pg_config_manual.h"

#define FAULTINJECTOR_MAX_SLOTS	16

/*
 *
 */
typedef enum FaultInjectorIdentifier_e {
	FaultInjectorIdNotSpecified=0,
	
	FaultInjectorIdAll,
		/* 
		 * affects all faults injected
		 *		*) remove all faults injected from the segment
		 *		*) display status for all faults injected 
		 */
	Postmaster,
	
	PgControl,
	
	PgXlog,
	
	StartPrepareTx,

	FaultBeforePendingDeleteRelationEntry,

	FaultBeforePendingDeleteDatabaseEntry,

	FaultBeforePendingDeleteTablespaceEntry,

	FaultBeforePendingDeleteFilespaceEntry,
	
	FileRepConsumer,
	
	FileRepConsumerVerification,

	FileRepChangeTrackingCompacting,
	
	FileRepSender,
	
	FileRepReceiver,
	
	FileRepFlush,
	
	FileRepResync,
	
	FileRepResyncInProgress,	
	
	FileRepResyncWorker,
	
	FileRepResyncWorkerRead,
	
	FileRepTransitionToInResyncMirrorReCreate,
	
	FileRepTransitionToInResyncMarkReCreated,
	
	FileRepTransitionToInResyncMarkCompleted,
	
	FileRepTransitionToInSyncBegin,
	
	FileRepTransitionToInSync,
	
	FileRepTransitionToInSyncBeforeCheckpoint,
	
	FileRepTransitionToInSyncMarkCompleted,
	
	FileRepTransitionToChangeTracking,
	
	Checkpoint,
	
	ChangeTrackingCompactingReport,
	
	ChangeTrackingDisable,
	
	TransactionAbortAfterDistributedPrepared,
	
	TransactionCommitPass1FromCreatePendingToCreated,
	
	TransactionCommitPass1FromDropInMemoryToDropPending,
	
	TransactionCommitPass1FromAbortingCreateNeededToAbortingCreate,
	
	TransactionAbortPass1FromCreatePendingToAbortingCreate,
	
	TransactionAbortPass1FromAbortingCreateNeededToAbortingCreate,
	
	TransactionCommitPass2FromDropInMemoryToDropPending,
	
	TransactionCommitPass2FromAbortingCreateNeededToAbortingCreate,
	
	TransactionAbortPass2FromCreatePendingToAbortingCreate,
	
	TransactionAbortPass2FromAbortingCreateNeededToAbortingCreate,
	
	
	FinishPreparedTransactionCommitPass1FromCreatePendingToCreated,
	
	FinishPreparedTransactionCommitPass2FromCreatePendingToCreated,
		/* commit: create pending => created */
			
	FinishPreparedTransactionAbortPass1FromCreatePendingToAbortingCreate,
	FinishPreparedTransactionAbortPass2FromCreatePendingToAbortingCreate,
		/* abort: create pending => aborting create */

	FinishPreparedTransactionCommitPass1FromDropInMemoryToDropPending,
	
	FinishPreparedTransactionCommitPass2FromDropInMemoryToDropPending,
		/* commit: drop in memory => drop pending */

	FinishPreparedTransactionCommitPass1AbortingCreateNeeded,
	
	FinishPreparedTransactionCommitPass2AbortingCreateNeeded,
		/* commit: create pending => created */
	
	FinishPreparedTransactionAbortPass1AbortingCreateNeeded,
	
	FinishPreparedTransactionAbortPass2AbortingCreateNeeded,	
		/* abort: create pending => aborting create */

	FileRepVerification,
		/* trigger filerep verification for testing */
	
	TwoPhaseTransactionCommitPrepared,
	
	TwoPhaseTransactionAbortPrepared,
	
	DtmBroadcastPrepare,
	
	DtmBroadcastCommitPrepared,
	
	DtmBroadcastAbortPrepared,
	
	DtmXLogDistributedCommit,

	DtmInit,
	
        EndPreparedTwoPhaseSleep,
	       /* sleep after creating two phase files */

	SegmentTransitionRequest,

	SegmentProbeResponse,

	SubtransactionFlushToFile,
	SubtransactionReadFromFile,
	SubtransactionRelease,
	SubtransactionRollback,
	SyncPersistentTable,
	XLOGInsert,
	LocalTmRecordTransactionCommit,
	
	MallocFailure,
	AbortTransactionFail,

	UpdateCommittedEofInPersistentTable,

	FaultDuringExecDynamicTableScan,
	GangThreadCreation,
	DispatchThreadCreation,

/*	DispatchWait,*/
	ConnectionFailAfterGangCreation,

/*	MakeDispatchThread,*/

	BeforeDispatch,

	DispatchToGangThreadStructureInitialization,
	
	InternalFlushError,
	ExecSimpleQueryEndCommand,
	MultiExecHashLargeVmem,
	ExecSortBeforeSorting,
	WorkfileCleanupSet,
	ExecSortMKSortMergeRuns,
    CdbCopyStartAfterDispatch,
	FaultInBackgroundWriterMain,

	FaultExecHashJoinNewBatch,
	AnalyzeSubxactError,

	OptTaskAllocateStringBuffer,

	RunawayCleanup,

	/* INSERT has to be done before that line */
	FaultInjectorIdMax,
	
} FaultInjectorIdentifier_e;

/*
 *
 */
typedef enum FaultInjectorType_e {
	FaultInjectorTypeNotSpecified=0,
	
	FaultInjectorTypeSleep,
	
	FaultInjectorTypeFault,
	
	FaultInjectorTypeFatal,
	
	FaultInjectorTypePanic,
	
	FaultInjectorTypeError,
	
	FaultInjectorTypeInfiniteLoop,
	
	FaultInjectorTypeDataCorruption,
	
	FaultInjectorTypeSuspend,
	
	FaultInjectorTypeResume,
	
	FaultInjectorTypeSkip,
	
	FaultInjectorTypeMemoryFull,
	
	FaultInjectorTypeReset,
	
	FaultInjectorTypeStatus,
	
	FaultInjectorTypePanicSuppress,

	FaultInjectorTypeSegv,
	
	FaultInjectorTypeCreateThreadFail,

	FaultInjectorTypeTimeOut,

	FaultInjectorTypeDispatchError,

	FaultInjectorTypeConnectionNull,

	FaultInjectorTypeConnectionNullInRestoreMode,

	FaultInjectorTypeUserCancel,

	FaultInjectorTypeProcDie,
	FaultInjectorTypeInterrupt,

	/* INSERT has to be done before that line */
	FaultInjectorTypeMax,
	
} FaultInjectorType_e;

/*
 *
 */
typedef enum DDLStatement_e {
	DDLNotSpecified=0,
	
	CreateDatabase,
	DropDatabase,
	
	CreateTable,
	DropTable,
	
	CreateIndex,
	AlterIndex,
	ReIndex,
	DropIndex,
	
	CreateFilespaces,
	DropFilespaces,
	
	CreateTablespaces,
	DropTablespaces,
	
	Truncate,
	
	Vacuum,
	
	/* INSERT has to be done before that line */
	DDLMax,
} DDLStatement_e;

/*
 *
 */
typedef enum FaultInjectorState_e {
	FaultInjectorStateNotInitialized=0,
		
	FaultInjectorStateWaiting,
		/* Request is waiting to be injected */
	
	FaultInjectorStateTriggered,
		/* Fault is injected */
	
	FaultInjectorStateCompleted,
		/* Fault was injected and completed successfully */
	
	FaultInjectorStateFailed,
		/* Fault was NOT injected */
	
} FaultInjectorState_e;


/*
 *
 */
typedef struct FaultInjectorEntry_s {
	
	FaultInjectorIdentifier_e	faultInjectorIdentifier;
	
	FaultInjectorType_e		faultInjectorType;
	
	int						sleepTime;
		/* in seconds, in use if fault injection type is sleep */
		
	DDLStatement_e			ddlStatement;
	
	char					databaseName[NAMEDATALEN];
	
	char					tableName[NAMEDATALEN];
	
	volatile	int			occurrence;
	
	volatile	FaultInjectorState_e	faultInjectorState;
		/* the state of the fault injection */
	char					bufOutput[2500];
	
} FaultInjectorEntry_s;


extern FaultInjectorType_e	FaultInjectorTypeStringToEnum(
									char*		faultTypeString);

extern	FaultInjectorIdentifier_e FaultInjectorIdentifierStringToEnum(
									char*			faultName);

extern DDLStatement_e FaultInjectorDDLStringToEnum(
									char*	ddlString);

extern Size FaultInjector_ShmemSize(void);

extern void FaultInjector_ShmemInit(void);

extern FaultInjectorType_e FaultInjector_InjectFaultIfSet(
							   FaultInjectorIdentifier_e identifier,
							   DDLStatement_e			 ddlStatement,
							   char*					 databaseName,
							   char*					 tableName);

extern int FaultInjector_SetFaultInjection(
							FaultInjectorEntry_s	*entry);


extern bool FaultInjector_IsFaultInjected(
							FaultInjectorIdentifier_e identifier);

#ifdef FAULT_INJECTOR
#define SIMPLE_FAULT_INJECTOR(FaultName) \
	FaultInjector_InjectFaultIfSet(FaultName, DDLNotSpecified, "", "")

#else
#define SIMPLE_FAULT_INJECTOR(FaultName)
#endif

#endif	/* FAULTINJECTOR_H */

