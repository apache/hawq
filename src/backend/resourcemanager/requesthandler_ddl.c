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

#include "envswitch.h"
#include "utils/linkedlist.h"
#include "utils/memutilities.h"
#include "utils/network_utils.h"
#include "utils/kvproperties.h"
#include "communication/rmcomm_QD2RM.h"
#include "communication/rmcomm_QD_RM_Protocol.h"
#include "communication/rmcomm_MessageHandler.h"
#include "catalog/pg_resqueue.h"
#include "catalog/catquery.h"
#include "access/xact.h"

#include "commands/comment.h"
#include "catalog/heap.h"

#include "gp-libpq-fe.h"
#include "gp-libpq-int.h"

#include "conntrack.h"

int updateResqueueCatalog(int					 action,
					      DynResourceQueueTrack  queuetrack,
						  List					*rsqattr);

int performInsertActionForPGResqueue(List *colvalues, Oid *newoid);
int performUpdateActionForPGResqueue(List *colvalues, char *queuename);
int performDeleteActionForPGResqueue(char *queuename);

int buildInsertActionForPGResqueue(DynResourceQueue   queue,
							   	   List 			 *rsqattr,
							   	   List		   		**insvalues);

int buildUpdateActionForPGResqueue(DynResourceQueue   queue,
							   	   List 		   	 *rsqattr,
							   	   List		   	    **updvalues);

int buildUpdateStatusActionForPGResqueue(DynResourceQueue   queue,
								  	  	 List 		   	   *rsqattr,
								  	  	 List		   	  **updvalues);

void freeUpdateActionList(MCTYPE context, List **actions);

/* Column names of pg_resqueue table
 * mapping with the definition of table pg_resqueue in pg_resqueue.h
 */
const char* PG_Resqueue_Column_Names[Natts_pg_resqueue] = {
	"rsqname",
	"parentoid",
	"activestats",
	"memorylimit",
	"corelimit",
	"resovercommit",
	"allocpolicy",
	"vsegresourcequota",
	"nvsegupperlimit",
	"nvseglowerlimit",
	"nvsegupperlimitperseg",
	"nvseglowerlimitperseg",
	"creationtime",
	"updatetime",
	"status"
};

/**
 * HAWQ RM handles the resource queue definition manipulation including CREATE,
 * ALTER and DROP RESOURCE QUEUE statements.
 */
bool handleRMDDLRequestManipulateResourceQueue(void **arg)
{
	static char 			errorbuf[ERRORMESSAGE_SIZE];
	int      				res		 		= FUNC_RETURN_OK;
	uint32_t				ddlres   		= FUNC_RETURN_OK;
	ConnectionTrack        *conntrack       = (ConnectionTrack *)arg;
	DynResourceQueueTrack 	newtrack 		= NULL;
	DynResourceQueueTrack   todroptrack		= NULL;
	SelfMaintainBufferData  responsebuff;
	List 				   *fineattr		= NULL;
	List 				   *rsqattr			= NULL;
	DynResourceQueue 		newqueue 		= NULL;

	/* Check context and retrieve the connection track based on connection id.*/
	RPCRequestHeadManipulateResQueue request = (RPCRequestHeadManipulateResQueue)
											   ((*conntrack)->MessageBuff.Buffer);

	elog(LOG, "Resource manager gets a request from ConnID %d to submit resource "
			  "queue DDL statement.",
			  request->ConnID);

	elog(DEBUG3, "With attribute list size %d", request->WithAttrLength);

	if ( (*conntrack)->ConnID == INVALID_CONNID )
	{
		res = retrieveConnectionTrack((*conntrack), request->ConnID);
		if ( res != FUNC_RETURN_OK )
		{
			elog(WARNING, "Not valid resource context with id %d.", request->ConnID);
			goto senderr;
		}

		elog(DEBUG5, "Resource manager fetched existing connection track "
					 "ID=%d, Progress=%d.",
					 (*conntrack)->ConnID,
					 (*conntrack)->Progress);
	}

	/*
	 * Only registered connection can manipulate resource queue, the status
	 * should be CONN_REGISTER_DONE.
	 */
	Assert( (*conntrack)->Progress == CONN_PP_REGISTER_DONE );

	/*
	 * Only the super user can manipulate resource queue. This is already
	 * checked before sending RPC to RM this process.
	 */
	Assert((*conntrack)->User != NULL &&
		   ((UserInfo)((*conntrack)->User))->isSuperUser);

	/* Build property list for the resource queue to be created. */
	request = (RPCRequestHeadManipulateResQueue)((*conntrack)->MessageBuff.Buffer);

	/* Get resource queue name. */
	char *string = (*conntrack)->MessageBuff.Buffer +
				   sizeof(RPCRequestHeadManipulateResQueueData);
	KVProperty nameattr = createPropertyString(PCONTEXT,
											   NULL,
											   getRSQTBLAttributeName(RSQ_TBL_ATTR_NAME),
											   NULL,
											   string);
	{
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		rsqattr = lappend(rsqattr, nameattr);
		MEMORY_CONTEXT_SWITCH_BACK
	}

	string += nameattr->Val.Len+1;

	/* Get with list. <key>=<value> */
	for ( int i = 0 ; i < request->WithAttrLength ; ++i )
	{
		KVProperty withattr = createPropertyEmpty(PCONTEXT);
		setSimpleStringNoLen(&(withattr->Key), string);
		string += withattr->Key.Len + 1;
		setSimpleStringNoLen(&(withattr->Val), string);
		string += withattr->Val.Len + 1;

		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		rsqattr = lappend(rsqattr, withattr);
		MEMORY_CONTEXT_SWITCH_BACK
	}

	/* Log the received attributes in DDL request. */
	ListCell *cell = NULL;
	foreach(cell, rsqattr)
	{
		KVProperty attribute = lfirst(cell);
		elog(RMLOG, "Resource manager received DDL Request: %s=%s",
				    attribute->Key.Str,
					attribute->Val.Str);
	}

	/* Shallow parse the 'withlist' attributes. */
	res = shallowparseResourceQueueWithAttributes(rsqattr,
												  &fineattr,
												  errorbuf,
												  sizeof(errorbuf));
	if (res != FUNC_RETURN_OK)
	{
		ddlres = res;
		elog(WARNING, "Cannot recognize DDL attribute, %s", errorbuf);
		goto senderr;
	}

	cell = NULL;
	foreach(cell, fineattr)
	{
		KVProperty attribute = lfirst(cell);
		elog(RMLOG, "DDL parsed request: %s=%s",
				    attribute->Key.Str,
				    attribute->Val.Str);
	}

	/* Add into resource queue hierarchy to validate the request. */
	switch(request->ManipulateAction)
	{
		case MANIPULATE_RESQUEUE_CREATE:
			/* Resource queue number check. */
			if (list_length(PQUEMGR->Queues) >= rm_nresqueue_limit)
			{
				ddlres = RESQUEMGR_EXCEED_MAX_QUEUE_NUMBER;
				snprintf(errorbuf, sizeof(errorbuf),
						"exceed maximum resource queue number %d",
						rm_nresqueue_limit);
				elog(WARNING, "Resource manager can not create resource queue "
							  "because %s",
							  errorbuf);
				goto senderr;
			}

			newqueue = rm_palloc0(PCONTEXT, sizeof(DynResourceQueueData));
			res = parseResourceQueueAttributes(fineattr,
											   newqueue,
											   false,
											   false,
											   errorbuf,
											   sizeof(errorbuf));
			if (res != FUNC_RETURN_OK)
			{
				rm_pfree(PCONTEXT, newqueue);
				ddlres = res;
				elog(WARNING, "Resource manager can not create resource queue "
							  "with its attributes because %s",
							  errorbuf);
				goto senderr;
			}

			res = checkAndCompleteNewResourceQueueAttributes(newqueue,
															 errorbuf,
															 sizeof(errorbuf));
			if (res != FUNC_RETURN_OK)
			{
				rm_pfree(PCONTEXT, newqueue);
				ddlres = res;
				elog(WARNING, "Resource manager can not complete resource queue's "
							  "attributes because %s",
							  errorbuf);
				goto senderr;
			}

			newtrack = NULL;
			res = createQueueAndTrack(newqueue, &newtrack, errorbuf, sizeof(errorbuf));
			if (res != FUNC_RETURN_OK)
			{
				rm_pfree(PCONTEXT, newqueue);
				if (newtrack != NULL)
					rm_pfree(PCONTEXT, newtrack);
				ddlres = res;
				elog(WARNING, "Resource manager can not create resource queue %s "
							  "because %s",
							  newqueue->Name,
							  errorbuf);
				goto senderr;
			}

			res = updateResqueueCatalog(request->ManipulateAction,
										newtrack,
										rsqattr);
			if (res != FUNC_RETURN_OK)
			{
				ddlres = res;
				elog(WARNING, "Cannot update resource queue changes in pg_resqueue.");

				/* If fail in updating catalog table, revert previous operations in RM. */
				res = dropQueueAndTrack(newtrack, errorbuf, sizeof(errorbuf));
				if (res != FUNC_RETURN_OK)
				{
					elog(WARNING, "Resource manager cannot drop queue and track "
								  "because %s",
								  errorbuf);
				}
				goto senderr;
			}

			/* Refresh resource queue capacity. */
			refreshResourceQueueCapacity(true);

			break;

		case MANIPULATE_RESQUEUE_ALTER:
		{
			/*------------------------------------------------------------------
			 * The strategy of altering one resource queue is, firstly, we always
			 * allow user to alter a resource queue no matter whether it is busy,
			 * no matter whether it has busy descendant resource queues. This is
			 * for the practicality of managing resource queues.
			 *
			 * The challenge is that, if we have one resource queue capacity
			 * limits, or active statement limit or resource quota or NVSEG*
			 * limits changed, the queued query resource requests' actual
			 * resource requests should be recalculated, which may cause some
			 * conflicts, for example, some requests require more resource than
			 * the queue capacity limits, some requests encounter deadlock issue.
			 *
			 * The idea for altering a queue is :
			 * STEP 1. Guarantee the ALTER RESOURCE QUQUE statement is valid to
			 * 		   be processed for practical altering;
			 * STEP 2. Making shadow instances for all resource queue track
			 * 		   instances potentially having definition changed or
			 * 		   queued resource requests changed;
			 * STEP 3. Try to do all altering related updates in shadows;
			 * STEP 4. Update catalog if the altering can be performed without
			 * 		   logical errors;
			 * STEP 5. Update resource queue track status based on the shadows;
			 * STEP 6. Remove shadows to clean up the resource queue track tree.
			 *
			 * In case we can not complete the whole altering procedure, we will
			 * only need to clean up the shadows to cleanup the resource queue
			 * track tree.
			 *------------------------------------------------------------------
			 */

			/* STEP 1. */
			DynResourceQueueTrack	toaltertrack  = NULL;
			List 				   *qhavingshadow = NULL;
			newqueue = rm_palloc0(PCONTEXT, sizeof(DynResourceQueueData));
			res = parseResourceQueueAttributes(fineattr,
											   newqueue,
											   true,
											   false,
											   errorbuf,
											   sizeof(errorbuf));
			if (res != FUNC_RETURN_OK)
			{
				rm_pfree(PCONTEXT, newqueue);
				ddlres = res;
				elog(WARNING, "Resource manager cannot alter resource queue %s, %s",
							  nameattr->Val.Str,
							  errorbuf);
				goto senderr;
			}
			rm_pfree(PCONTEXT, newqueue);
			newqueue = NULL;

			toaltertrack = getQueueTrackByQueueName((char *)(nameattr->Val.Str),
					   	   	   	   	   	   	   	   	nameattr->Val.Len);
			if (toaltertrack == NULL)
			{
				ddlres = RESQUEMGR_NO_QUENAME;
				snprintf(errorbuf, sizeof(errorbuf), "the queue doesn't exist");
				elog(WARNING, "Resource manager cannot alter resource queue %s, %s",
							  nameattr->Val.Str,
							  errorbuf);
				goto senderr;
			}

			/*
			 * If the target resource queue is a branch queue, only queue
			 * capacity limits are alterable.
			 */
			if ( list_length(toaltertrack->ChildrenTracks) > 0 )
			{
				ListCell *cell = NULL;
				foreach(cell, fineattr)
				{
					KVProperty attribute = lfirst(cell);
					if ( SimpleStringComp(
							 &(attribute->Key),
							 RSQTBLAttrNames[RSQ_TBL_ATTR_MEMORY_LIMIT_CLUSTER]) != 0 &&
						 SimpleStringComp(
							 &(attribute->Key),
							 RSQTBLAttrNames[RSQ_TBL_ATTR_CORE_LIMIT_CLUSTER]) != 0 &&
						 SimpleStringComp(
							 &(attribute->Key),
							 RSQTBLAttrNames[RSQ_TBL_ATTR_NAME]) != 0	 )
					{
						ddlres = RESQUEMGR_ALTERQUEUE_NOTALLOWED;
						snprintf(errorbuf, sizeof(errorbuf),
								 "user can only alter branch resource queue %s "
								 "attributes %s and %s",
								 nameattr->Val.Str,
								 RSQDDLAttrNames[RSQ_DDL_ATTR_MEMORY_LIMIT_CLUSTER],
								 RSQDDLAttrNames[RSQ_DDL_ATTR_CORE_LIMIT_CLUSTER]);
						elog(WARNING, "Resource manager cannot alter resource "
									  "queue %s, %s",
									  nameattr->Val.Str,
									  errorbuf);
						goto senderr;
					}
				}
			}

			/* STEP 2. Build all necessary shadow resource queue track instance. */
			buildQueueTrackShadows(toaltertrack, &qhavingshadow);

			/* STEP 3. Update resource queue attribute in shadow instance. */
			res = updateResourceQueueAttributesInShadow(fineattr,
														toaltertrack,
														errorbuf,
														sizeof(errorbuf));
			if (res != FUNC_RETURN_OK)
			{
				ddlres = res;
				elog(WARNING, "Resource manager cannot update resource queue with "
							  "its attributes, %s",
							  errorbuf);
				cleanupQueueTrackShadows(&qhavingshadow);
				goto senderr;
			}

			res = checkAndCompleteNewResourceQueueAttributes(
					  toaltertrack->ShadowQueueTrack->QueueInfo,
					  errorbuf,
					  sizeof(errorbuf));
			if (res != FUNC_RETURN_OK)
			{
				ddlres = res;
				elog(WARNING, "Resource manager cannot complete resource queue "
							  "attributes, %s",
							  errorbuf);
				cleanupQueueTrackShadows(&qhavingshadow);
				goto senderr;
			}

			/*
			 * Refresh actual capacity of the resource queue, the change is
			 * expected to be updated in the shadow instances.
			 */
			if ( PRESPOOL->ClusterMemoryCoreRatio > 0 )
			{
				refreshResourceQueuePercentageCapacity(true);
			}

			/*------------------------------------------------------------------
			 * Till now, we expect the input for altering a resource queue is
			 * valid, and we have built the necessary shadows for those queues
			 * whose dynamic status are possible to be updated, including queued
			 * query resource requests and corresponding deadlock detector status.
			 *
			 * If we force resource manager to cancel queued resource request,
			 * the queued resource requests impossible to be satisfied due to
			 * queue capacity shrinking or deadlock detection are canceled at
			 * once.
			 *
			 * If we do not, ALTER RESOURCE QUEUE statement is cancelled. res
			 * has error code returned in this function.
			 *------------------------------------------------------------------
			 */

			res = rebuildAllResourceQueueTrackDynamicStatusInShadow(qhavingshadow,
																	true,
																	errorbuf,
																	sizeof(errorbuf));
			if ( res != FUNC_RETURN_OK )
			{
				ddlres = res;
				elog(WARNING, "Can not apply alter resource queue changes, %s",
							  errorbuf);
				cleanupQueueTrackShadows(&qhavingshadow);
				goto senderr;
			}

			/* STEP 4. Update catalog. */
			res = updateResqueueCatalog(request->ManipulateAction,
										toaltertrack,
										rsqattr);
			if (res != FUNC_RETURN_OK)
			{
				ddlres = res;
				elog(WARNING, "Cannot alter resource queue changes in pg_resqueue.");
				cleanupQueueTrackShadows(&qhavingshadow);
				goto senderr;
			}

			/* STEP 5. Update resource queue tracks referencing corresponding
			 * 		   shadows. */
			applyResourceQueueTrackChangesFromShadows(qhavingshadow);

			/* STEP 6. Clean up. */
			cleanupQueueTrackShadows(&qhavingshadow);
			break;
		}
		case MANIPULATE_RESQUEUE_DROP:
			todroptrack = getQueueTrackByQueueName((char *)(nameattr->Val.Str),
												   nameattr->Val.Len);
			if ( todroptrack == NULL )
			{
				ddlres = RESQUEMGR_NO_QUENAME;
				snprintf(errorbuf, sizeof(errorbuf),
						 "resource queue %s doesn't exist",
						 nameattr->Val.Str);
				elog(WARNING, "Resource manager cannot drop resource queue %s, %s",
							  nameattr->Val.Str,
							  errorbuf);
				goto senderr;
			}

			if ( list_length(todroptrack->ChildrenTracks) > 0 )
			{
				ddlres = RESQUEMGR_IN_USE;
				snprintf(errorbuf, sizeof(errorbuf),
						 "resource queue %s is a branch queue",
						 todroptrack->QueueInfo->Name);
				elog(WARNING, "Resource manager cannot drop resource queue %s, %s",
							  nameattr->Val.Str,
							  errorbuf);
				goto senderr;
			}

			if ( todroptrack->isBusy ||
				 todroptrack->QueryResRequests.NodeCount > 0 )
			{
				ddlres = RESQUEMGR_IN_USE;
				snprintf(errorbuf, sizeof(errorbuf), "resource queue %s is busy",
						 todroptrack->QueueInfo->Name);
				elog(WARNING, "Resource manager cannot drop resource queue %s, %s",
							  nameattr->Val.Str,
							  errorbuf);
				goto senderr;
			}

			if (todroptrack->QueueInfo->OID == DEFAULTRESQUEUE_OID)
			{
				/* already check before send RPC to RM */
				Assert(todroptrack->QueueInfo->OID != DEFAULTRESQUEUE_OID);
				ddlres = RESQUEMGR_IN_USE;
				snprintf(errorbuf, sizeof(errorbuf),
						"pg_default as system queue cannot be dropped");
				elog(WARNING, "Resource manager cannot drop resource queue %s, %s",
							  nameattr->Val.Str,
							  errorbuf);
				goto senderr;
			}

			if (todroptrack->QueueInfo->OID == ROOTRESQUEUE_OID)
			{
				/* already check before send RPC to RM */
				Assert(todroptrack->QueueInfo->OID != ROOTRESQUEUE_OID);
				ddlres = RESQUEMGR_IN_USE;
				snprintf(errorbuf, sizeof(errorbuf),
						"pg_root as system queue cannot be dropped.");
				elog(WARNING, "Resource manager cannot drop resource queue %s, %s",
							  nameattr->Val.Str,
							  errorbuf);
				goto senderr;
			}

			res = updateResqueueCatalog(request->ManipulateAction,
									    todroptrack,
										rsqattr);
			if (res != FUNC_RETURN_OK)
			{
				ddlres = res;
				snprintf(errorbuf, sizeof(errorbuf),
						 "cannot update resource queue changes in pg_resqueue");
				elog(WARNING, "Resource manager cannot drop resource queue %s, %s",
							  nameattr->Val.Str,
							  errorbuf);
				goto senderr;
			}

			res = dropQueueAndTrack(todroptrack,errorbuf,sizeof(errorbuf));
			if (res != FUNC_RETURN_OK)
			{
				ddlres = res;
				elog(WARNING, "Resource manager cannot drop resource queue %s, %s",
							  nameattr->Val.Str,
						      errorbuf);
				goto senderr;
			}

			break;

		default:
			Assert(false);
		}

	/* Recalculate all memory/core ratio instances' limits. */
	refreshMemoryCoreRatioLimits();
	/* Refresh memory/core ratio level water mark. */
	refreshMemoryCoreRatioWaterMark();

	/* Build response. */
	RPCResponseHeadManipulateResQueueData response;
	response.Result 	= FUNC_RETURN_OK;
	response.Reserved 	= 0;

	/* Build message saved in the connection track instance. */
	buildResponseIntoConnTrack((*conntrack),
							   (char *)&response,
							   sizeof(response),
							   (*conntrack)->MessageMark1,
							   (*conntrack)->MessageMark2,
							   RESPONSE_QD_DDL_MANIPULATERESQUEUE);
	(*conntrack)->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, *conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	/* Clean up temporary variable. */
	cleanPropertyList(PCONTEXT, &fineattr);
	cleanPropertyList(PCONTEXT, &rsqattr);
	return true;

senderr:
	{
		initializeSelfMaintainBuffer(&responsebuff, PCONTEXT);

		RPCResponseHeadManipulateResQueueERRORData response;
		response.Result.Result 	 = ddlres;
		response.Result.Reserved = 0;

		appendSMBVar(&responsebuff, response.Result);
		appendSMBStr(&responsebuff, errorbuf);
		appendSelfMaintainBufferTill64bitAligned(&responsebuff);

		/* Build message saved in the connection track instance. */
		buildResponseIntoConnTrack((*conntrack),
								   responsebuff.Buffer,
								   responsebuff.Cursor + 1,
								   (*conntrack)->MessageMark1,
								   (*conntrack)->MessageMark2,
								   RESPONSE_QD_DDL_MANIPULATERESQUEUE);
		(*conntrack)->ResponseSent = false;
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, *conntrack);
		MEMORY_CONTEXT_SWITCH_BACK
		destroySelfMaintainBuffer(&responsebuff);

		/* Clean up temporary variable. */
		cleanPropertyList(PCONTEXT, &fineattr);
		cleanPropertyList(PCONTEXT, &rsqattr);
		return true;
	}
}


bool handleRMDDLRequestManipulateRole(void **arg)
{
	RPCResponseHeadManipulateRoleData response;

    ConnectionTrack conntrack = (ConnectionTrack)(*arg);
    UserInfo 		user	  = NULL;
    int 			res		  = FUNC_RETURN_OK;

    RPCRequestHeadManipulateRole request = (RPCRequestHeadManipulateRole)
    									   (conntrack->MessageBuff.Buffer);

    switch(request->Action)
    {
    	case MANIPULATE_ROLE_RESQUEUE_CREATE:
    	{
    		/*
    		 * In case creating new role, resource manager expects no error, as
    		 * in QD side, the validation was passed.
    		 */
			user = rm_palloc0(PCONTEXT, sizeof(UserInfoData));
			user->OID 		  = request->RoleOID;
			user->QueueOID 	  = request->QueueOID;
			user->isSuperUser = request->isSuperUser;
			strncpy(user->Name, request->Name, sizeof(user->Name)-1);
			createUser(user);
			elog(LOG, "Resource manager handles request CREATE ROLE oid:%d, "
					  "queueID:%d, isSuper:%d, roleName:%s",
					  request->RoleOID,
					  request->QueueOID,
					  request->isSuperUser,
					  request->Name);
			break;
    	}
    	case MANIPULATE_ROLE_RESQUEUE_ALTER:
    	{
    		/*
    		 * In case altering one role, the old one is deleted firstly.
    		 * Resource manager expects the role always exists.
    		 */
    		int64_t roleoid = request->RoleOID;
			res = dropUser(roleoid, request->Name);
			Assert(res == FUNC_RETURN_OK);

			/* Create new user instance. */
			user = (UserInfo)rm_palloc0(PCONTEXT, sizeof(UserInfoData));
			user->OID 		  = request->RoleOID;
			user->QueueOID 	  = request->QueueOID;
			user->isSuperUser = request->isSuperUser;
			strncpy(user->Name, request->Name, sizeof(user->Name)-1);
			createUser(user);
			elog(LOG, "Resource manager handles request ALTER ROLE oid:%d, "
					  "queueID:%d, isSuper:%d, roleName:%s",
					  request->RoleOID,
					  request->QueueOID,
					  request->isSuperUser,
					  request->Name);
			break;
    	}
		case MANIPULATE_ROLE_RESQUEUE_DROP:
		{
			/* Resource manager expects the role always exists. */
			int64_t roleoid = request->RoleOID;
			res = dropUser(roleoid, request->Name);
			Assert(res == FUNC_RETURN_OK);
			elog(LOG, "Resource manager handles request drop role oid:%d, "
					  "roleName:%s",
					  request->RoleOID,
					  request->Name);
			break;
		}
		default:
		{
			Assert(0);
		}
    }

	/* Build response. */
	response.Result 	= res;
	response.Reserved 	= 0;

	/* Build message saved in the connection track instance. */
	buildResponseIntoConnTrack(conntrack,
							   (char *)&response,
							   sizeof(response),
							   conntrack->MessageMark1,
							   conntrack->MessageMark2,
							   RESPONSE_QD_DDL_MANIPULATEROLE);
	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	return true;
}

int updateResqueueCatalog(int					 action,
					      DynResourceQueueTrack  queuetrack,
						  List					*rsqattr)
{
	int res = FUNC_RETURN_OK;

	switch( action )
	{
	case MANIPULATE_RESQUEUE_CREATE:
	{
		List *insertaction = NULL;
		Oid	  newoid	   = InvalidOid;
		res = buildInsertActionForPGResqueue(queuetrack->QueueInfo,
											 rsqattr,
											 &insertaction);
		Assert(res == FUNC_RETURN_OK);

		res = performInsertActionForPGResqueue(insertaction, &newoid);
		if(res != FUNC_RETURN_OK)
		{
			elog(WARNING, "Resource manager performs insert operation on "
						  "pg_resqueue failed : %d",
						  res);

			DRMGlobalInstance->ResManagerMainKeepRun = false;
			break;
		}
		Assert(newoid != InvalidOid);

		/* Update queue as new oid and make it indexed by new oid. */
		queuetrack->QueueInfo->OID = newoid;
		setQueueTrackIndexedByQueueOID(queuetrack);

		/* Update the status of the parent queue when the status is leaf. */
		DynResourceQueueTrack parenttrack = queuetrack->ParentTrack;
		char *parentname = parenttrack->QueueInfo->Name;
		/* don't update pg_root */
		if(strcmp(parentname, RESOURCE_QUEUE_ROOT_QUEUE_NAME) != 0)
		{
			List *updateaction 	= NULL;
			List *updateattr	= NULL;
			/*construct the update attr list */
			KVProperty statusattr = createPropertyString(
										PCONTEXT,
										NULL,
										getRSQTBLAttributeName(RSQ_TBL_ATTR_STATUS),
										NULL,
										"branch");
			MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
			updateattr = lappend(updateattr, statusattr);
			MEMORY_CONTEXT_SWITCH_BACK

			res = buildUpdateStatusActionForPGResqueue(parenttrack->QueueInfo,
												       updateattr,
												       &updateaction);
			Assert(res == FUNC_RETURN_OK);
			res = performUpdateActionForPGResqueue(updateaction, parentname);
			if(res != FUNC_RETURN_OK)
			{
				elog(WARNING, "Resource manager updates the status of the parent "
							  "resource queue %s failed when create resource "
							  "queue %s",
							  parenttrack->QueueInfo->Name,
							  queuetrack->QueueInfo->Name);
				DRMGlobalInstance->ResManagerMainKeepRun = false;
			}

			cleanPropertyList(PCONTEXT, &updateattr);
			freeUpdateActionList(PCONTEXT, &updateaction);
		}
		break;
	}
	case MANIPULATE_RESQUEUE_ALTER:
	{
		char *queuename = queuetrack->QueueInfo->Name;
		List *updateaction = NULL;
		res = buildUpdateActionForPGResqueue(queuetrack->QueueInfo,
											 rsqattr,
											 &updateaction);
		Assert(res == FUNC_RETURN_OK);

		res = performUpdateActionForPGResqueue(updateaction, queuename);
		if(res != FUNC_RETURN_OK)
		{
			elog(WARNING, "Resource manager performs update operation on "
						  "pg_resqueue failed when update resource queue %s",
						  queuename);
			DRMGlobalInstance->ResManagerMainKeepRun = false;
		}
		freeUpdateActionList(PCONTEXT, &updateaction);
		break;
	}
	case MANIPULATE_RESQUEUE_DROP:
	{
		char *queuename = queuetrack->QueueInfo->Name;
		res = performDeleteActionForPGResqueue(queuename);
		if(res != FUNC_RETURN_OK)
		{
			elog(WARNING, "Resource manager performs delete operation on "
						  "pg_resqueue failed when drop resource queue %s.",
						  queuename);
			DRMGlobalInstance->ResManagerMainKeepRun = false;
			break;
		}

		/* update the status of the parent queue after the child is removed */
		DynResourceQueueTrack parenttrack = queuetrack->ParentTrack;
		char *parentname = parenttrack->QueueInfo->Name;
		if (list_length(queuetrack->ParentTrack->ChildrenTracks) == 1 )
		{
			List *updateaction = NULL;
			List *updateattr   = NULL;
			/*construct the update attr list to 'branch' status. */
			KVProperty statusattr = createPropertyString(
										PCONTEXT,
										NULL,
										getRSQTBLAttributeName(RSQ_TBL_ATTR_STATUS),
										NULL,
										"");
			MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
			updateattr = lappend(updateattr, statusattr);
			MEMORY_CONTEXT_SWITCH_BACK

			res = buildUpdateStatusActionForPGResqueue(parenttrack->QueueInfo,
													   updateattr,
													   &updateaction);

			Assert(res == FUNC_RETURN_OK);
			res = performUpdateActionForPGResqueue(updateaction, parentname);
			if(res != FUNC_RETURN_OK)
			{
				elog(WARNING, "Resource manager updates the status of the parent "
							  "resource queue %s failed when drop resource queue %s",
							  parenttrack->QueueInfo->Name,
							  queuetrack->QueueInfo->Name);
				DRMGlobalInstance->ResManagerMainKeepRun = false;
			}
			cleanPropertyList(PCONTEXT, &updateattr);
			freeUpdateActionList(PCONTEXT, &updateaction);
		}
		break;
	}
	default:
	{
		Assert(false);
	}
	}
	return res;
}



/*******************************************************************************
 * Build response message for successfully adding new resource queue.
 *
 * Response format (SUCCEED):
 * 		uint32_t		return code
 * 		uint8_t			action count
 * 		uint8_t			reserved[3]
 *
 * 		uint8_t			action code (1=create,2=alter,3=drop)
 * 		uint8_t 		column count
 * 		uint8_t			reserved[2]
 * 		int64_t			queue oid
 * 		uint8_t			column index x column count
 *		column new value \0 column new value \0 ...
 *		append multiple \0  to make 64-bit aligned.
 ******************************************************************************/

#define ADD_PG_RESQUEUE_COLVALUE_CONSTSTR(list,colval,colidx)			   	   \
	{																		   \
		PAIR _pair = createPAIR(PCONTEXT, NULL, NULL);						   \
		_pair->Key = TYPCONVERT(void *, colidx);							   \
		_pair->Value = createSimpleString(PCONTEXT);						   \
		setSimpleStringNoLen((SimpStringPtr)(_pair->Value), (char *)colval);   \
		*list = lappend(*list, _pair);									   	   \
	}

#define ADD_PG_RESQUEUE_COLVALUE_OID(list,colval,colidx)			   	   	   \
	{																		   \
		PAIR _pair = createPAIR(PCONTEXT, NULL, NULL);						   \
		_pair->Key = TYPCONVERT(void *, colidx);							   \
		_pair->Value = createSimpleString(PCONTEXT);						   \
		Oid _oid = colval;													   \
		SimpleStringSetOid((SimpStringPtr)(_pair->Value), _oid);		   	   \
		*list = lappend(*list, _pair);								   	   	   \
	}

#define ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(list,ddlattr,ddlidx,colidx)		   \
	{																		   \
		SimpStringPtr _colvalue = NULL;										   \
		if ( findPropertyValue(											   	   \
				ddlattr, 													   \
				getRSQDDLAttributeName(ddlidx),					   		   	   \
				&_colvalue) == FUNC_RETURN_OK ) {							   \
			PAIR _pair = createPAIR(PCONTEXT, NULL, NULL);					   \
			_pair->Key = TYPCONVERT(void *, colidx);						   \
			_pair->Value = createSimpleString(PCONTEXT);					   \
			SimpleStringCopy((SimpStringPtr)(_pair->Value), _colvalue);		   \
			*list = lappend(*list, _pair);								   	   \
		}																	   \
	}

#define ADD_PG_RESQUEUE_COLVALUE_INATTR(list,ddlattr,ddlidx,colidx)			   \
	{																		   \
		SimpStringPtr _colvalue = NULL;										   \
		if ( findPropertyValue(											   	   \
				ddlattr, 													   \
				getRSQTBLAttributeName(ddlidx),					   			   \
				&_colvalue) == FUNC_RETURN_OK ) {							   \
			PAIR _pair = createPAIR(PCONTEXT, NULL, NULL);					   \
			_pair->Key = TYPCONVERT(void *, colidx);						   \
			_pair->Value = createSimpleString(PCONTEXT);					   \
			SimpleStringCopy((SimpStringPtr)(_pair->Value), _colvalue);		   \
			*list = lappend(*list, _pair);								   	   \
		}																	   \
	}

int buildInsertActionForPGResqueue(DynResourceQueue   queue,
							   	   List 			 *rsqattr,
							   	   List		   		**insvalues)
{
	static char defaultActiveStats[] 	    	= DEFAULT_RESQUEUE_ACTIVESTATS;
	static char defaultResOvercommit[]     		= DEFAULT_RESQUEUE_OVERCOMMIT;
	static char defaultNVSegUpperLimit[]   		= DEFAULT_RESQUEUE_NVSEG_UPPER_LIMIT;
	static char defaultNVSegLowerLimit[]   		= DEFAULT_RESQUEUE_NVSEG_LOWER_LIMIT;
	static char defaultNVSegUpperLimitPerSeg[] 	= DEFAULT_RESQUEUE_NVSEG_UPPER_PERSEG_LIMIT;
	static char defaultNVSegLowerLimitPerSeg[] 	= DEFAULT_RESQUEUE_NVSEG_LOWER_PERSEG_LIMIT;
	static char defaultAllocPolicy[]	    	= DEFAULT_RESQUEUE_ALLOCPOLICY;
	static char defaultVSegResourceQuota[] 		= DEFAULT_RESQUEUE_VSEGRESOURCEQUOTA;
	int	 res				 	    			= FUNC_RETURN_OK;
	PAIR newpair				    			= NULL;

	Assert( queue     != NULL );
	Assert( rsqattr   != NULL );
	Assert( insvalues != NULL );

	/* Insert resource queue column value. */
	newpair = createPAIR(PCONTEXT,
						 TYPCONVERT(void *, Anum_pg_resqueue_rsqname),
						 createSimpleString(PCONTEXT));

	setSimpleStringWithContent((SimpStringPtr)(newpair->Value),
							   queue->Name,
							   queue->NameLen);

	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)

	*insvalues = lappend(*insvalues, newpair);

	SimpStringPtr colvalue = NULL;
	if (findPropertyValue(rsqattr,
						  getRSQDDLAttributeName(RSQ_DDL_ATTR_ACTIVE_STATMENTS),
						  &colvalue) != FUNC_RETURN_OK)
	{
		ADD_PG_RESQUEUE_COLVALUE_CONSTSTR(insvalues,
										  defaultActiveStats,
										  Anum_pg_resqueue_activestats);
	}

	if (findPropertyValue(rsqattr,
						  getRSQDDLAttributeName(RSQ_DDL_ATTR_RESOURCE_OVERCOMMIT_FACTOR),
						  &colvalue) != FUNC_RETURN_OK)
	{
		ADD_PG_RESQUEUE_COLVALUE_CONSTSTR(insvalues,
										  defaultResOvercommit,
										  Anum_pg_resqueue_resovercommit);
	}

	if (findPropertyValue(rsqattr,
						  getRSQDDLAttributeName(RSQ_DDL_ATTR_NVSEG_UPPER_LIMIT),
						  &colvalue) != FUNC_RETURN_OK)
	{
		ADD_PG_RESQUEUE_COLVALUE_CONSTSTR(insvalues,
										  defaultNVSegUpperLimit,
										  Anum_pg_resqueue_nvsegupperlimit);
	}

	if (findPropertyValue(rsqattr,
						  getRSQDDLAttributeName(RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT),
						  &colvalue) != FUNC_RETURN_OK)
	{
		ADD_PG_RESQUEUE_COLVALUE_CONSTSTR(insvalues,
										  defaultNVSegLowerLimit,
										  Anum_pg_resqueue_nvseglowerlimit);
	}

	if (findPropertyValue(rsqattr,
						  getRSQDDLAttributeName(RSQ_DDL_ATTR_NVSEG_UPPER_LIMIT_PERSEG),
						  &colvalue) != FUNC_RETURN_OK)
	{
		ADD_PG_RESQUEUE_COLVALUE_CONSTSTR(insvalues,
										  defaultNVSegUpperLimitPerSeg,
										  Anum_pg_resqueue_nvsegupperlimitperseg);
	}

	/* Default value for rsq_vseg_upper_limit if not set */
	if (findPropertyValue(rsqattr,
						  getRSQDDLAttributeName(RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT_PERSEG),
						  &colvalue) != FUNC_RETURN_OK)
	{
		ADD_PG_RESQUEUE_COLVALUE_CONSTSTR(insvalues,
										  defaultNVSegLowerLimitPerSeg,
										  Anum_pg_resqueue_nvseglowerlimitperseg);
	}

	/* Default value for rsq_allocation_policy if not set */
	if (findPropertyValue(rsqattr,
					  	  getRSQDDLAttributeName(RSQ_DDL_ATTR_ALLOCATION_POLICY),
						  &colvalue) != FUNC_RETURN_OK)
	{
		ADD_PG_RESQUEUE_COLVALUE_CONSTSTR(insvalues,
										  defaultAllocPolicy,
										  Anum_pg_resqueue_allocpolicy);
	}

	/* Default value for rsq_vseg_resource_quota if not set */
	if (findPropertyValue(rsqattr,
						  getRSQDDLAttributeName(RSQ_DDL_ATTR_VSEG_RESOURCE_QUOTA),
						  &colvalue) != FUNC_RETURN_OK)
	{
		ADD_PG_RESQUEUE_COLVALUE_CONSTSTR(insvalues,
										  defaultVSegResourceQuota,
										  Anum_pg_resqueue_vsegresourcequota);
	}

	ADD_PG_RESQUEUE_COLVALUE_OID(insvalues, queue->ParentOID, Anum_pg_resqueue_parentoid);

	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(insvalues, rsqattr, RSQ_DDL_ATTR_ACTIVE_STATMENTS, 		 	Anum_pg_resqueue_activestats);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(insvalues, rsqattr, RSQ_DDL_ATTR_MEMORY_LIMIT_CLUSTER, 	 	Anum_pg_resqueue_memorylimit);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(insvalues, rsqattr, RSQ_DDL_ATTR_CORE_LIMIT_CLUSTER, 	 	Anum_pg_resqueue_corelimit);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(insvalues, rsqattr, RSQ_DDL_ATTR_RESOURCE_OVERCOMMIT_FACTOR, Anum_pg_resqueue_resovercommit);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(insvalues, rsqattr, RSQ_DDL_ATTR_NVSEG_UPPER_LIMIT, 	 		Anum_pg_resqueue_nvsegupperlimit);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(insvalues, rsqattr, RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT, 	 		Anum_pg_resqueue_nvseglowerlimit);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(insvalues, rsqattr, RSQ_DDL_ATTR_NVSEG_UPPER_LIMIT_PERSEG, 	Anum_pg_resqueue_nvsegupperlimitperseg);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(insvalues, rsqattr, RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT_PERSEG, 	Anum_pg_resqueue_nvseglowerlimitperseg);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(insvalues, rsqattr, RSQ_DDL_ATTR_ALLOCATION_POLICY, 		 	Anum_pg_resqueue_allocpolicy);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(insvalues, rsqattr, RSQ_DDL_ATTR_VSEG_RESOURCE_QUOTA, 		Anum_pg_resqueue_vsegresourcequota);

	/* creation time and update time */
	TimestampTz curtime    = GetCurrentTimestamp();
	const char *curtimestr = timestamptz_to_str(curtime);

	ADD_PG_RESQUEUE_COLVALUE_CONSTSTR(insvalues, curtimestr, Anum_pg_resqueue_creationtime);
	ADD_PG_RESQUEUE_COLVALUE_CONSTSTR(insvalues, curtimestr, Anum_pg_resqueue_updatetime);

	/* status */
	char statusstr[256];
	statusstr[0] = '\0';
	if ( RESQUEUE_IS_BRANCH(queue) )
		strcat(statusstr, "branch");

	ADD_PG_RESQUEUE_COLVALUE_CONSTSTR(insvalues, statusstr, Anum_pg_resqueue_status);

	MEMORY_CONTEXT_SWITCH_BACK
	return res;
}

int buildUpdateActionForPGResqueue(DynResourceQueue   queue,
							   	   List 		   	 *rsqattr,
							   	   List		   	    **updvalues)
{
	int res = FUNC_RETURN_OK;
	/* Insert resource queue column value. */
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(updvalues, rsqattr, RSQ_DDL_ATTR_ACTIVE_STATMENTS, 		 	Anum_pg_resqueue_activestats);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(updvalues, rsqattr, RSQ_DDL_ATTR_MEMORY_LIMIT_CLUSTER, 	 	Anum_pg_resqueue_memorylimit);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(updvalues, rsqattr, RSQ_DDL_ATTR_CORE_LIMIT_CLUSTER, 	 	Anum_pg_resqueue_corelimit);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(updvalues, rsqattr, RSQ_DDL_ATTR_RESOURCE_OVERCOMMIT_FACTOR, Anum_pg_resqueue_resovercommit);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(updvalues, rsqattr, RSQ_DDL_ATTR_ALLOCATION_POLICY, 		 	Anum_pg_resqueue_allocpolicy);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(updvalues, rsqattr, RSQ_DDL_ATTR_VSEG_RESOURCE_QUOTA, 		Anum_pg_resqueue_vsegresourcequota);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(updvalues, rsqattr, RSQ_DDL_ATTR_NVSEG_UPPER_LIMIT, 	 		Anum_pg_resqueue_nvsegupperlimit);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(updvalues, rsqattr, RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT, 	 		Anum_pg_resqueue_nvseglowerlimit);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(updvalues, rsqattr, RSQ_DDL_ATTR_NVSEG_UPPER_LIMIT_PERSEG, 	Anum_pg_resqueue_nvsegupperlimitperseg);
	ADD_PG_RESQUEUE_COLVALUE_INDDLATTR(updvalues, rsqattr, RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT_PERSEG, 	Anum_pg_resqueue_nvseglowerlimitperseg);

	/* creation time and update time */
	TimestampTz curtime = GetCurrentTimestamp();
	const char *curtimestr = timestamptz_to_str(curtime);
	ADD_PG_RESQUEUE_COLVALUE_CONSTSTR(updvalues, curtimestr, Anum_pg_resqueue_updatetime);

	return res;
}


int buildUpdateStatusActionForPGResqueue(DynResourceQueue   queue,
								  	  	 List 		   	   *rsqattr,
								  	  	 List		   	  **updvalues)
{
	int res = FUNC_RETURN_OK;

	ListCell *cell = NULL;
	foreach(cell, rsqattr)
	{
		KVProperty attribute = lfirst(cell);
		elog(DEBUG3, "Received update Request: %s=%s",
					 attribute->Key.Str,
					 attribute->Val.Str);
	}

	/* update time */
	TimestampTz curtime = GetCurrentTimestamp();
	const char *curtimestr = timestamptz_to_str(curtime);

	ADD_PG_RESQUEUE_COLVALUE_CONSTSTR(updvalues,curtimestr, Anum_pg_resqueue_updatetime);

	/* status */
	ADD_PG_RESQUEUE_COLVALUE_INATTR(updvalues, rsqattr, RSQ_TBL_ATTR_STATUS, Anum_pg_resqueue_status);
	return res;
}

int performInsertActionForPGResqueue(List *colvalues, Oid *newoid)
{
	Assert(newoid != NULL);
	Assert(colvalues != NULL);

	int 		res 	 = FUNC_RETURN_OK;
	int			libpqres = CONNECTION_OK;
	PGconn 	   *conn 	 = NULL;
	char 		conninfo[512];
	PQExpBuffer sql 	 = NULL;
	int 		colcnt 	 = 0;
	PGresult   *result 	 = NULL;
	char 		name[65];
	ListCell   *cell	 = NULL;

	sprintf(conninfo, "options='-c gp_session_role=UTILITY "
					           "-c allow_system_table_mods=dml' "
					  "dbname=template1 port=%d connect_timeout=%d",
					  master_addr_port,
					  LIBPQ_CONNECT_TIMEOUT);

	conn = PQconnectdb(conninfo);
	if ((libpqres = PQstatus(conn)) != CONNECTION_OK)
	{
		elog(WARNING, "Resource manager failed to connect database when insert "
					  "row into pg_resqueue, error code: %d, reason: %s",
				      libpqres,
				      PQerrorMessage(conn));
		PQfinish(conn);
		return LIBPQ_FAIL_EXECUTE;
	}

	result = PQexec(conn, "BEGIN");
	if (!result || PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		elog(WARNING, "Resource manager failed to run SQL: %s when insert row "
					  "into pg_resqueue, reason: %s",
				      "BEGIN",
				      PQresultErrorMessage(result));
		res = LIBPQ_FAIL_EXECUTE;
		goto cleanup;
	}

	PQclear(result);

	/**
	 * compose an INSERT sql statement
	 */
	sql = createPQExpBuffer();
	if ( sql == NULL )
	{
		elog(WARNING, "Resource manager failed to allocate buffer for building "
					  "sql statement.");
		goto cleanup;
	}
	appendPQExpBuffer(sql, "%s", "INSERT INTO pg_resqueue(");
	colcnt = list_length(colvalues);
	memset(name, 0, sizeof(name));

	foreach(cell, colvalues)
	{
		PAIR action = lfirst(cell);
		int colindex = TYPCONVERT(int, action->Key);
		Assert(colindex <= Natts_pg_resqueue && colindex > 0);
		appendPQExpBuffer(sql, "%s", PG_Resqueue_Column_Names[colindex-1]);
		if ( colindex == Anum_pg_resqueue_rsqname )
		{
			strncpy(name,
					((SimpStringPtr)action->Value)->Str,
					sizeof(name)-1);
		}
		colcnt--;
		if (colcnt != 0)
		{
			appendPQExpBuffer(sql,"%s",",");
		}
	}
	appendPQExpBuffer(sql,"%s",") VALUES(");

	colcnt = list_length(colvalues);

	foreach(cell, colvalues)
	{
		PAIR action = lfirst(cell);
		SimpStringPtr valueptr = (SimpStringPtr)(action->Value);
		appendPQExpBuffer(sql, "'%s'", valueptr->Str);
		colcnt--;
		if (colcnt !=0 )
		{
			appendPQExpBuffer(sql,"%s",",");
		}
	}
	appendPQExpBuffer(sql,"%s",")");

	elog(LOG, "Resource manager created a new queue: %s", sql->data);
	result = PQexec(conn, sql->data);
	if (!result || PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		elog(WARNING, "Resource manager failed to run SQL: %s failed "
				      "when insert row into pg_resqueue, reason : %s",
				      sql->data,
				      PQresultErrorMessage(result));
		res = LIBPQ_FAIL_EXECUTE;
		goto cleanup;
	}

	resetPQExpBuffer(sql);
	PQclear(result);
	appendPQExpBuffer(sql, "SELECT oid FROM pg_resqueue WHERE rsqname = '%s'", name);
	result = PQexec(conn, sql->data);
	if (!result || PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		elog(WARNING, "Resource manager failed to run SQL: %s failed, reason : %s",
				      sql->data,
				      PQresultErrorMessage(result));
		PQexec(conn, "ABORT");
		res = LIBPQ_FAIL_EXECUTE;
		goto cleanup;
	}

	*newoid = (uint32) atoi(PQgetvalue(result, 0, 0));
	if(*newoid == InvalidOid)
	{
		elog(WARNING, "Resource manager gets an invalid oid after insert row "
					  "into pg_resqueue");
		PQexec(conn, "ABORT");
		res = LIBPQ_FAIL_EXECUTE;;
		goto cleanup;
	}

	PQclear(result);
	result = PQexec(conn, "COMMIT");
	if (!result || PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		elog(WARNING, "Resource manager failed to run SQL: %s "
			      	  "when insert row into pg_resqueue, reason : %s",
					  "COMMIT",
					  PQresultErrorMessage(result));
		PQexec(conn, "ABORT");
		res = LIBPQ_FAIL_EXECUTE;
		goto cleanup;
	}
	elog(LOG, "Resource manager created a new resource queue, oid is: %d", *newoid);

cleanup:
	if(sql != NULL)
	{
		destroyPQExpBuffer(sql);
	}
	if(result != NULL)
	{
		PQclear(result);
	}

	PQfinish(conn);
	return res;
}

int performUpdateActionForPGResqueue(List *colvalues, char *queuename)
{
	int 		res 		= FUNC_RETURN_OK;
	int			libpqres 	= CONNECTION_OK;
	PGconn 	   *conn 		= NULL;
	char 		conninfo[512];
	PQExpBuffer sql 		= NULL;
	int 		colcnt 		= 0;
	PGresult   *result 		= NULL;
	ListCell   *cell		= NULL;

	sprintf(conninfo, "options='-c gp_session_role=UTILITY "
							   "-c allow_system_table_mods=dml' "
					  "dbname=template1 port=%d connect_timeout=%d",
					  master_addr_port,
					  LIBPQ_CONNECT_TIMEOUT);

	conn = PQconnectdb(conninfo);
	if ((libpqres = PQstatus(conn)) != CONNECTION_OK)
	{
		elog(WARNING, "Resource manager failed to connect database when update "
					  "row of pg_resqueue, error code: %d, reason: %s",
			      	  libpqres,
			      	  PQerrorMessage(conn));
		PQfinish(conn);
		return LIBPQ_FAIL_EXECUTE;
	}

	result = PQexec(conn, "BEGIN");
	if (!result || PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		elog(WARNING, "Resource manager failed to run SQL: %s when update row "
					  "of pg_resqueue, reason : %s",
					  "BEGIN",
					  PQresultErrorMessage(result));
		res = LIBPQ_FAIL_EXECUTE;
		goto cleanup;
	}
	PQclear(result);

	/**
	 * compose an UPDATE sql statement
	 */
	sql = createPQExpBuffer();
	if ( sql == NULL )
	{
		elog(WARNING, "Resource manager failed to allocate buffer for building "
					  "sql statement.");
		goto cleanup;
	}
	appendPQExpBuffer(sql, "%s", "UPDATE pg_resqueue SET ");
	colcnt = list_length(colvalues);

	foreach(cell, colvalues)
	{
		PAIR action = lfirst(cell);
		int colindex = TYPCONVERT(int, action->Key);
		Assert(colindex <= Natts_pg_resqueue && colindex > 0);
		SimpStringPtr valueptr = (SimpStringPtr)(action->Value);
		appendPQExpBuffer(sql,
						  "%s='%s'",
						  PG_Resqueue_Column_Names[colindex-1],
						  valueptr->Str);
		colcnt--;
		if (colcnt != 0)
		{
			appendPQExpBuffer(sql,"%s",",");
		}
	}
	appendPQExpBuffer(sql, " WHERE rsqname='%s'", queuename);

	elog(LOG, "Resource manager updates resource queue: %s",sql->data);
	result = PQexec(conn, sql->data);
	if (!result || PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		elog(WARNING, "Resource manager failed to run SQL: %s "
				  	  "when update row of pg_resqueue, reason : %s",
				  	  sql->data,
				  	  PQresultErrorMessage(result));
		res = LIBPQ_FAIL_EXECUTE;
		goto cleanup;
	}

	PQclear(result);
	result = PQexec(conn, "COMMIT");
	if (!result || PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		elog(WARNING, "Resource manager failed to run SQL: %s "
				  	  "when update row of pg_resqueue, reason : %s",
					  "COMMIT",
					  PQresultErrorMessage(result));
		PQexec(conn, "ABORT");
		res = LIBPQ_FAIL_EXECUTE;
		goto cleanup;
	}
	elog(LOG, "Resource queue %s is updated", queuename);

cleanup:
	if(sql != NULL)
	{
		destroyPQExpBuffer(sql);
	}
	if(result != NULL)
	{
		PQclear(result);
	}
	PQfinish(conn);
	return res;
}

int performDeleteActionForPGResqueue(char *queuename)
{
	Assert(queuename != NULL);

	int res = FUNC_RETURN_OK;
	int	libpqres = CONNECTION_OK;
	PGconn *conn = NULL;
	char conninfo[1024];
	PQExpBuffer sql = NULL;
	PGresult* result = NULL;
	Oid queueid = InvalidOid;

	sprintf(conninfo, "options='-c gp_session_role=UTILITY -c allow_system_table_mods=dml' "
			"dbname=template1 port=%d connect_timeout=%d", master_addr_port, LIBPQ_CONNECT_TIMEOUT);
	conn = PQconnectdb(conninfo);
	if ((libpqres = PQstatus(conn)) != CONNECTION_OK) {
		elog(WARNING, "Resource manager failed to connect database when delete a row from pg_resqueue,"
		      	  	  "error code: %d, reason: %s",
		      	  	  libpqres,
		      	  	  PQerrorMessage(conn));
		res = libpqres;
		PQfinish(conn);
		return res;
	}

	result = PQexec(conn, "BEGIN");
	if (!result || PQresultStatus(result) != PGRES_COMMAND_OK) {
		elog(WARNING, "Resource manager failed to run SQL: %s "
				  	  "when delete a row from pg_resqueue, reason : %s",
	      	  	  	  "BEGIN",
	      	  	  	  PQresultErrorMessage(result));
		res = PGRES_FATAL_ERROR;
		goto cleanup;
	}
	PQclear(result);

	/**
	 * firstly, get oid of this resource queue.
	 */
	sql = createPQExpBuffer();
	if ( sql == NULL )
	{
		elog(WARNING, "Resource manager failed to allocate buffer for building "
					  "sql statement.");
		goto cleanup;
	}
	appendPQExpBuffer(sql, "SELECT oid FROM pg_resqueue WHERE rsqname = '%s'", queuename);
	result = PQexec(conn, sql->data);
	if (!result || PQresultStatus(result) != PGRES_TUPLES_OK) {
		elog(WARNING, "Resource manager failed to run SQL: %s "
			  	  	  "when delete a row from pg_resqueue, reason : %s",
					  sql->data,
					  PQresultErrorMessage(result));
		res = PGRES_FATAL_ERROR;
		goto cleanup;
	}
	queueid = (uint32) atoi(PQgetvalue(result, 0, 0));
	if(queueid == InvalidOid) {
		elog(WARNING, "Resource manager gets an invalid oid when delete a row from pg_resqueue");
		res = PGRES_FATAL_ERROR;
		goto cleanup;
	}

	/*
	 * Drop resource queue
	 */
	PQclear(result);
	resetPQExpBuffer(sql);
	appendPQExpBuffer(sql, "DELETE FROM pg_resqueue WHERE rsqname = '%s'", queuename);
	elog(LOG, "Resource manager drops a resource queue: %s",sql->data);
	result = PQexec(conn, sql->data);
	if (!result || PQresultStatus(result) != PGRES_COMMAND_OK) {
		elog(WARNING, "Resource manager failed to run SQL: %s "
		  	  	  	  "when delete a row from pg_resqueue, reason : %s",
					  sql->data,
					  PQresultErrorMessage(result));
		res = PGRES_FATAL_ERROR;
		goto cleanup;
	}

	/*
	 * Remove any comments on this resource queue
	 */
	PQclear(result);
	resetPQExpBuffer(sql);
	appendPQExpBuffer(sql,
					  "DELETE FROM pg_shdescription WHERE objoid  = %d AND classoid = %d",
					  queueid, ResQueueRelationId);
	result = PQexec(conn, sql->data);
	if (!result || PQresultStatus(result) != PGRES_COMMAND_OK)
		elog(WARNING, "Resource manager failed to run SQL: %s "
		  	  	  	  "when delete a row from pg_resqueue, reason : %s",
					  sql->data,
					  PQresultErrorMessage(result));

	/* MPP-6929, MPP-7583: metadata tracking */
	PQclear(result);
	resetPQExpBuffer(sql);
	appendPQExpBuffer(sql,
				      "DELETE FROM pg_stat_last_shoperation WHERE classid = %d AND objid = %d ",
				      ResQueueRelationId, queueid);
	result = PQexec(conn, sql->data);
	if (!result || PQresultStatus(result) != PGRES_COMMAND_OK)
		elog(WARNING, "Resource manager failed to run SQL: %s "
		  	  	  	  "when delete a row from pg_resqueue, reason : %s",
					  sql->data,
					  PQresultErrorMessage(result));

	PQclear(result);
	result = PQexec(conn, "COMMIT");
	if (!result || PQresultStatus(result) != PGRES_COMMAND_OK) {
		elog(WARNING, "Resource manager failed to run SQL: %s "
		  	  	  	  "when delete a row from pg_resqueue, reason : %s",
					  "COMMIT",
					  PQresultErrorMessage(result));
		PQexec(conn, "ABORT");
		res = PGRES_FATAL_ERROR;
		goto cleanup;
	}

	elog(LOG, "Resource queue %s is dropped", queuename);

cleanup:
	if(sql != NULL)
	{
		destroyPQExpBuffer(sql);
	}
	if(result != NULL)
	{
		PQclear(result);
	}
	PQfinish(conn);
	return res;
}

void freeUpdateActionList(MCTYPE context, List **actions)
{
	while( list_length(*actions) > 0 )
	{
		PAIR pair = lfirst(list_head(*actions));
		MEMORY_CONTEXT_SWITCH_TO(context)
		*actions = list_delete_first(*actions);
		MEMORY_CONTEXT_SWITCH_BACK

		SimpStringPtr content = (SimpStringPtr)(pair->Value);
		freeSimpleStringContent(content);
		rm_pfree(context, pair);
	}
}

