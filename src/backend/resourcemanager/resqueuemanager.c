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

#include "dynrm.h"
#include "utils/simplestring.h"
#include "utils/network_utils.h"
#include "utils/kvproperties.h"
#include "utils/memutilities.h"
#include "communication/rmcomm_MessageHandler.h"
#include "communication/rmcomm_QD_RM_Protocol.h"
#include "catalog/pg_resqueue.h"
#include "resourcemanager/resqueuemanager.h"

/*
 * The DDL statement attribute name strings.
 */
char RSQDDLAttrNames[RSQ_DDL_ATTR_COUNT]
					[RESOURCE_QUEUE_DDL_ATTR_LENGTH_MAX+1] = {
	"parent",
	"active_statements",
	"memory_limit_cluster",
	"core_limit_cluster",
	"vseg_resource_quota",
	"allocation_policy",
	"resource_overcommit_factor",
	"nvseg_upper_limit",
	"nvseg_lower_limit",
	"nvseg_upper_limit_perseg",
	"nvseg_lower_limit_perseg"
};

/*
 * The attribute names for expressing one complete resource queue definition.
 */
char RSQTBLAttrNames[RSQ_TBL_ATTR_COUNT]
					 [RESOURCE_QUEUE_TBL_COLNAME_LENGTH_MAX+1] = {
	"parentoid",
	"activestats",
	"memorylimit",
	"corelimit",
	"vsegresourcequota",
	"allocpolicy",
	"resovercommit",
	"nvsegupperlimit",
	"nvseglowerlimit",
	"nvsegupperlimitperseg",
	"nvseglowerlimitperseg",

	"oid",
	"rsqname",
	"creationtime",
	"updatetime",
	"status"
};

/*
 * The possible resource allocation policy names.
 */
static char RSQDDLValueAllocationPolicy[RSQ_ALLOCATION_POLICY_COUNT]
									   [RESOURCE_QUEUE_DDL_POLICY_LENGTH_MAX] = {
	"even"
};

/*
 * The attributes for expressing one complete role/user definition.
 */
static char USRTBLAttrNames[USR_TBL_ATTR_COUNT]
						   [RESOURCE_ROLE_DDL_ATTR_LENGTH_MAX] = {
	"oid",
	"name",
	"target",
	"priority",
	"is_superuser"
};

/* Internal functions. */

/*------------------------------------------
 * The resource quota calculation functions.
 *------------------------------------------*/
typedef int  (* computeQueryQuotaByPolicy )(DynResourceQueueTrack,
											int32_t *,
											int32_t *,
											int32_t,
											char *,
											int);

int computeQueryQuota_EVEN(DynResourceQueueTrack	track,
						   int32_t			   	   *segnum,
						   int32_t			   	   *segnummin,
						   int32_t					segnumlimit,
						   char				   	   *errorbuf,
						   int						errorbufsize);

int32_t min(int32_t a, int32_t b);
int32_t max(int32_t a, int32_t b);
computeQueryQuotaByPolicy AllocationPolicy[RSQ_ALLOCATION_POLICY_COUNT] = {
	computeQueryQuota_EVEN
};

/*------------------------------------------
 * The resource distribution functions.
 *------------------------------------------*/
typedef int (* dispatchResourceToQueriesByPolicy )(DynResourceQueueTrack);

int dispatchResourceToQueries_EVEN(DynResourceQueueTrack track);

dispatchResourceToQueriesByPolicy DispatchPolicy[RSQ_ALLOCATION_POLICY_COUNT] = {
	dispatchResourceToQueries_EVEN
};

void dispatchResourceToQueriesInOneQueue(DynResourceQueueTrack track);

/* Functions for operating resource queue tracker instance. */
DynResourceQueueTrack createDynResourceQueueTrack(DynResourceQueue queue);

void returnAllocatedResourceToLeafQueue(DynResourceQueueTrack 	track,
								    	int32_t 				memorymb,
								    	double					core);

void refreshResourceQueuePercentageCapacityInternal(uint32_t clustermemmb,
													uint32_t clustercore,
													bool	 queuechanged);

/* Internal APIs for maintaining memory/core ratio trackers. */
int32_t addResourceQueueRatio(DynResourceQueueTrack track);
void removeResourceQueueRatio(DynResourceQueueTrack track);

DynMemoryCoreRatioTrack createDynMemoryCoreRatioTrack(uint32_t ratio,
		  	  	  	  	  	  	  	  	  	  	      int32_t  index );
void freeMemoryCoreTrack(DynMemoryCoreRatioTrack mctrack);
int removeQueueTrackFromMemoryCoreTrack(DynMemoryCoreRatioTrack mctrack,
										DynResourceQueueTrack   track);

int getRSQTBLAttributeNameIndex(SimpStringPtr attrname);
int getRSQDDLAttributeNameIndex(SimpStringPtr attrname);
int getUSRTBLAttributeNameIndex(SimpStringPtr attrname);

void detectAndDealWithDeadLock(DynResourceQueueTrack track);

void markMemoryCoreRatioWaterMark(DQueue 		marks,
								  uint64_t 		curmicrosec,
								  int32_t 		memmb,
								  double 		core);

void buildAcquireResourceErrorResponseAndSend(ConnectionTrack  conntrack,
										  	  int 		  	   errorcode,
											  char			  *errorbuf);

void buildAcquireResourceErrorResponse(ConnectionTrack	conntrack,
									   int				errorcode,
									   char			   *errorbuf);

RESOURCEPROBLEM isResourceAcceptable(ConnectionTrack conn, int segnumact);
/*----------------------------------------------------------------------------*/
/*                    RESOURCE QUEUE MANAGER EXTERNAL APIs                    */
/*----------------------------------------------------------------------------*/

int32_t min(int32_t a, int32_t b)
{
	return a>b ? b : a;
}

int32_t max(int32_t a, int32_t b)
{
	return a<b ? b : a;
}

/* Initialize the resource queue manager instance. */
void initializeResourceQueueManager(void)
{
    ASSERT_DRM_GLOBAL_INSTANCE_CREATED

	PQUEMGR->RootTrack    	= NULL;
    PQUEMGR->DefaultTrack 	= NULL;
    PQUEMGR->Queues 		= NULL;

	/* 
	 * The two hash tables hold only the mapping from queue id or queue name to
	 * the queue object saved in Queues.
  	 */
    initializeHASHTABLE(&(PQUEMGR->QueuesIDIndex),
    					PCONTEXT,
						HASHTABLE_SLOT_VOLUME_DEFAULT,
						HASHTABLE_SLOT_VOLUME_DEFAULT_MAX,
						HASHTABLE_KEYTYPE_CHARARRAY,
						NULL);

    initializeHASHTABLE(&(PQUEMGR->QueuesNameIndex),
    					PCONTEXT,
						HASHTABLE_SLOT_VOLUME_DEFAULT,
						HASHTABLE_SLOT_VOLUME_DEFAULT_MAX,
						HASHTABLE_KEYTYPE_SIMPSTR,
						NULL);

    /* Initialize user information part. */
    PQUEMGR->Users = NULL;

    initializeHASHTABLE(&(PQUEMGR->UsersIDIndex),
    					PCONTEXT,
						HASHTABLE_SLOT_VOLUME_DEFAULT,
						HASHTABLE_SLOT_VOLUME_DEFAULT_MAX,
						HASHTABLE_KEYTYPE_CHARARRAY,
						NULL);

    initializeHASHTABLE(&(PQUEMGR->UsersNameIndex),
    					PCONTEXT,
						HASHTABLE_SLOT_VOLUME_DEFAULT,
						HASHTABLE_SLOT_VOLUME_DEFAULT_MAX,
						HASHTABLE_KEYTYPE_SIMPSTR,
						NULL);

    /* Initialize memory/core ratio counter and index. */
    PQUEMGR->RatioCount = 0;
    initializeHASHTABLE(&(PQUEMGR->RatioIndex),
    					PCONTEXT,
    			  	  	HASHTABLE_SLOT_VOLUME_DEFAULT,
    			  	  	HASHTABLE_SLOT_VOLUME_DEFAULT_MAX,
    			  	  	HASHTABLE_KEYTYPE_UINT32,
    			  	  	NULL);
    for ( int i = 0 ; i < RESOURCE_QUEUE_RATIO_SIZE ; ++i)
    {
    	PQUEMGR->RatioReverseIndex[i]     = -1;
    	PQUEMGR->RatioReferenceCounter[i] = 0;
    	PQUEMGR->RatioTrackers[i]         = NULL;
    	initializeDQueue(&(PQUEMGR->RatioWaterMarks[i]), PCONTEXT);
    }

    PQUEMGR->LastCheckingDeadAllocationTime = 0;
    PQUEMGR->LastCheckingQueuedTimeoutTime  = 0;
    PQUEMGR->GRMQueueMaxCapacity 			= 1.0;
    PQUEMGR->GRMQueueCapacity				= 1.0;
    PQUEMGR->GRMQueueCurCapacity			= 0.0;
    PQUEMGR->GRMQueueResourceTight			= false;
    PQUEMGR->toRunQueryDispatch 			= false;

    for ( int i = 0 ; i < RESPROBLEM_COUNT ; ++i )
    {
    	PQUEMGR->hasResourceProblem[i] = false;
    }

    PQUEMGR->ActualMinGRMContainerPerSeg = rm_min_resource_perseg;
}

/*
 * Recognize DDL attributes and shallow parse to fine grained attributes. This
 * function should be expanded when we hope to map one DDL attribute to more
 * than one TABLE attributes or we hope to reformat the value.
 */
int shallowparseResourceQueueWithAttributes(List 	*rawattr,
											List   **fineattr,
											char  	*errorbuf,
											int		 errorbufsize)
{
	ListCell *cell = NULL;
	foreach(cell, rawattr)
	{
		KVProperty property = lfirst(cell);

		if ( SimpleStringComp(&(property->Key),
							  (char *)getRSQTBLAttributeName(RSQ_TBL_ATTR_NAME)) == 0 )
		{
			KVProperty newprop = createPropertyString(
									 PCONTEXT,
									 NULL,
									 getRSQTBLAttributeName(RSQ_TBL_ATTR_NAME),
									 NULL,
									 property->Val.Str);

			MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
			*fineattr = lappend(*fineattr, newprop);
			MEMORY_CONTEXT_SWITCH_BACK
			continue;
		}

		int attrindex = getRSQDDLAttributeNameIndex(&(property->Key));
		if ( attrindex == -1 )
		{
			snprintf(errorbuf, errorbufsize,
					 "not defined DDL attribute name %s",
					 property->Key.Str);
			elog(WARNING, "Resource manager failed parsing attribute, %s",
						  errorbuf);
			return RMDDL_WRONG_ATTRNAME;
		}

		switch(attrindex)
		{
		case RSQ_DDL_ATTR_PARENT:
		{
			/* Find oid of the parent resource queue. */
			DynResourceQueueTrack parentque =
				getQueueTrackByQueueName(property->Val.Str, property->Val.Len);
			if ( parentque == NULL )
			{
				snprintf(errorbuf, errorbufsize,
						 "cannot recognize parent resource queue name %s.",
						 property->Val.Str);
				elog(WARNING, "Resource manager failed parsing attribute, %s",
							  errorbuf);
				return RMDDL_WRONG_ATTRVALUE;
			}
			Assert( parentque != NULL );

			/* Build property. */
			Oid parentoid = (Oid) (parentque->QueueInfo->OID);
			KVProperty newprop = createPropertyOID(
									 PCONTEXT,
									 NULL,
									 getRSQTBLAttributeName(RSQ_TBL_ATTR_PARENT),
									 NULL,
									 parentoid);
			MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
			*fineattr = lappend(*fineattr, newprop);
			MEMORY_CONTEXT_SWITCH_BACK
			break;
		}
		case RSQ_DDL_ATTR_ACTIVE_STATMENTS:
		case RSQ_DDL_ATTR_MEMORY_LIMIT_CLUSTER:
		case RSQ_DDL_ATTR_CORE_LIMIT_CLUSTER:
		case RSQ_DDL_ATTR_VSEG_RESOURCE_QUOTA:
		case RSQ_DDL_ATTR_ALLOCATION_POLICY:
		case RSQ_DDL_ATTR_RESOURCE_OVERCOMMIT_FACTOR:
		case RSQ_DDL_ATTR_NVSEG_UPPER_LIMIT:
		case RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT:
		case RSQ_DDL_ATTR_NVSEG_UPPER_LIMIT_PERSEG:
		case RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT_PERSEG:
		{

			/* The empty value is not allowed. */
			if ( SimpleStringEmpty(&(property->Val)) )
			{
				snprintf(errorbuf, errorbufsize,
						 "the value of %s cannot be empty",
						 RSQDDLAttrNames[attrindex]);
				elog(WARNING, "Resource manager failed parsing attribute, %s",
							  errorbuf);
				return RMDDL_WRONG_ATTRVALUE;
			}
			/*
			 * Build property.
			 *
			 * NOTE, this logic works, because there is a premise.
			 * RSQ_TBL_ATTR_XXX == RSQ_DDL_ATTR_XXX, for all enum values of
			 * RESOURCE_QUEUE_DDL_ATTR_INDEX.
			 *
			 */
			KVProperty newprop = createPropertyString(
									 PCONTEXT,
									 NULL,
									 getRSQTBLAttributeName(attrindex),
									 NULL,
									 property->Val.Str);

			MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
			*fineattr = lappend(*fineattr, newprop);
			MEMORY_CONTEXT_SWITCH_BACK
			break;
		}
		default:
			Assert(false); /* Should never occur. */
		}

	DQUEUE_LOOP_END

	return FUNC_RETURN_OK;
}

/*
 * This function parses the attributes and translate into DynResourceQueue
 * struct's attributes. This functions does not generate logs higher than
 * WARNING, the concrete error is also saved in error buffer to make the caller
 * able to pass back the message to client.
 */
int parseResourceQueueAttributes( List 			 	*attributes,
								  DynResourceQueue 	 queue,
								  bool				 checkformatonly,
								  bool				 loadcatalog,
								  char 				*errorbuf,
								  int   			 errorbufsize)
{
	int 			res 		 = FUNC_RETURN_OK;
	int 			attrindex 	 = -1;
	Oid 			parentid;
	Oid				oid;

	bool			memlimit_percentage  = false;
	bool			memlimit_value		 = false;
	bool			corelimit_percentage = false;
	bool			corelimit_value		 = false;

	SimpStringPtr 	attrname  			 = NULL;
	SimpStringPtr 	attrvalue 			 = NULL;

	Assert( queue != NULL );

	/* Initialize attributes. */
	queue->OID						= InvalidOid;
	queue->ParentOID 				= InvalidOid;
	queue->ParallelCount			= -1;
	queue->ClusterMemoryMB			= -1;
	queue->Status					= RESOURCE_QUEUE_STATUS_VALID_LEAF;

	queue->ClusterVCore				= -1.0;
	queue->SegResourceQuotaVCore 	= -1.0;
	queue->SegResourceQuotaMemoryMB = -1;

	queue->ResourceOvercommit 		= DEFAULT_RESQUEUE_OVERCOMMIT_N;
	queue->NVSegUpperLimit			= DEFAULT_RESQUEUE_NVSEG_UPPER_LIMIT_N;
	queue->NVSegLowerLimit			= DEFAULT_RESQUEUE_NVSEG_LOWER_LIMIT_N;
	queue->NVSegUpperLimitPerSeg	= DEFAULT_RESQUEUE_NVSEG_UPPER_PERSEG_LIMIT_N;
	queue->NVSegLowerLimitPerSeg	= DEFAULT_RESQUEUE_NVSEG_LOWER_PERSEG_LIMIT_N;

	queue->AllocatePolicy 			= -1;
	queue->QueuingPolicy 			= -1;
	queue->InterQueuePolicy 		= -1;

	queue->ClusterMemoryPer			= -1;
	queue->ClusterVCorePer			= -1;

	memset(queue->Name, '\0', sizeof(queue->Name));

	/* Go through each attribute content. */
	errorbuf[0] = '\0';
	ListCell *cell = NULL;
	foreach(cell, attributes)
	{
		KVProperty value = lfirst(cell);
		attrname  = &(value->Key);
		attrvalue = &(value->Val);

		attrindex = getRSQTBLAttributeNameIndex(attrname);

		if ( SimpleStringEmpty(attrvalue) )
		{
			elog(DEBUG3, "No value for attribute %s.", attrname->Str);
			continue;
		}

		Assert( attrindex != -1 );
		/*
		 * Actually parse each attribute.
		 */
		switch(attrindex)
		{
		case RSQ_TBL_ATTR_OID:
			res = SimpleStringToOid(attrvalue, &oid);
			queue->OID = oid;
			break;

		case RSQ_TBL_ATTR_PARENT:
			res = SimpleStringToOid(attrvalue, &parentid);
			queue->ParentOID = parentid;
			break;

		case RSQ_TBL_ATTR_ACTIVE_STATMENTS:
			res = SimpleStringToInt32(attrvalue, &(queue->ParallelCount));
			break;

		case RSQ_TBL_ATTR_MEMORY_LIMIT_CLUSTER:
			if ( SimpleStringIsPercentage(attrvalue) )
			{
				memlimit_percentage = true;
				int8_t inputval = 0;
				res = SimpleStringToPercentage(attrvalue, &inputval);
				queue->ClusterMemoryPer = inputval;
				queue->Status |= RESOURCE_QUEUE_STATUS_EXPRESS_PERCENT;
			}
			else
			{
				memlimit_value = true;
				res = SimpleStringToStorageSizeMB(attrvalue,
												  &(queue->ClusterMemoryMB));
			}
			break;

		case RSQ_TBL_ATTR_CORE_LIMIT_CLUSTER:
			if ( SimpleStringIsPercentage(attrvalue) )
			{
				corelimit_percentage = true;
				int8_t inputval = 0;
				res = SimpleStringToPercentage(attrvalue, &inputval);
				queue->ClusterVCorePer = inputval;
				queue->Status |= RESOURCE_QUEUE_STATUS_EXPRESS_PERCENT;
			}
			else
			{
				corelimit_value = true;
				res = SimpleStringToDouble(attrvalue, &(queue->ClusterVCore));
			}
			break;

		case RSQ_TBL_ATTR_VSEG_RESOURCE_QUOTA:
			/* Decide it is a memory quota or core quota. */
			if ( SimpleStringStartWith(attrvalue,
									   RESOURCE_QUEUE_SEG_RES_QUOTA_MEM) == FUNC_RETURN_OK )
			{
				SimpString valuestr;
				setSimpleStringRef(&valuestr,
								   attrvalue->Str+sizeof(RESOURCE_QUEUE_SEG_RES_QUOTA_MEM)-1,
								   attrvalue->Len-sizeof(RESOURCE_QUEUE_SEG_RES_QUOTA_MEM)+1);

				res = SimpleStringToStorageSizeMB(&valuestr,
												  &(queue->SegResourceQuotaMemoryMB));

				/*
				 *--------------------------------------------------------------
				 * Check the value. We accept only :
				 * 128mb, 256mb, 512mb, 1gb, 2gb, 4gb, 8gb, 16gb
				 *--------------------------------------------------------------
				 */
				if ( res == FUNC_RETURN_OK )
				{
					elog(DEBUG3, "Resource manager parseResourceQueueAttributes() "
								 "parsed segment resource quota %d MB",
								 queue->SegResourceQuotaMemoryMB);

					if ( !(queue->SegResourceQuotaMemoryMB == (1<<7))  &&
						 !(queue->SegResourceQuotaMemoryMB == (1<<8))  &&
						 !(queue->SegResourceQuotaMemoryMB == (1<<9))  &&
						 !(queue->SegResourceQuotaMemoryMB == (1<<10)) &&
						 !(queue->SegResourceQuotaMemoryMB == (1<<11)) &&
						 !(queue->SegResourceQuotaMemoryMB == (1<<12)) &&
						 !(queue->SegResourceQuotaMemoryMB == (1<<13)) &&
						 !(queue->SegResourceQuotaMemoryMB == (1<<14)) )
					{
						res = RESQUEMGR_WRONG_RES_QUOTA_EXP;
						snprintf(errorbuf, errorbufsize,
								 "%s value %s is not valid, only 128mb, 256mb, "
								 "512mb, 1gb, 2gb, 4gb, 8gb, 16gb are "
								 "valid.",
								 loadcatalog ?
									 RSQTBLAttrNames[RSQ_TBL_ATTR_VSEG_RESOURCE_QUOTA] :
									 RSQDDLAttrNames[RSQ_DDL_ATTR_VSEG_RESOURCE_QUOTA],
								 attrvalue->Str);
					}
				}
			}
			else
			{
				res = RESQUEMGR_WRONG_RES_QUOTA_EXP;
				snprintf(errorbuf, errorbufsize,
						 "%s format %s is not valid.",
						 loadcatalog ?
							 RSQTBLAttrNames[RSQ_TBL_ATTR_VSEG_RESOURCE_QUOTA] :
							 RSQDDLAttrNames[RSQ_DDL_ATTR_VSEG_RESOURCE_QUOTA],
						 attrvalue->Str);
			}
			break;

		case RSQ_TBL_ATTR_RESOURCE_OVERCOMMIT_FACTOR:
			res = SimpleStringToDouble(attrvalue, &(queue->ResourceOvercommit));
			break;

		case RSQ_TBL_ATTR_NVSEG_UPPER_LIMIT:
			res = SimpleStringToInt32(attrvalue, &(queue->NVSegUpperLimit));
			break;

		case RSQ_TBL_ATTR_NVSEG_LOWER_LIMIT:
			res = SimpleStringToInt32(attrvalue, &(queue->NVSegLowerLimit));
			break;

		case RSQ_TBL_ATTR_NVSEG_UPPER_LIMIT_PERSEG:
			res = SimpleStringToDouble(attrvalue, &(queue->NVSegUpperLimitPerSeg));
			break;

		case RSQ_TBL_ATTR_NVSEG_LOWER_LIMIT_PERSEG:
			res = SimpleStringToDouble(attrvalue, &(queue->NVSegLowerLimitPerSeg));
			break;

		case RSQ_TBL_ATTR_ALLOCATION_POLICY:
			res = SimpleStringToMapIndexInt8(attrvalue,
											 (char *)RSQDDLValueAllocationPolicy,
											 RSQ_ALLOCATION_POLICY_COUNT,
											 sizeof(RSQDDLValueAllocationPolicy[0]),
											 &(queue->AllocatePolicy));
			break;

		case RSQ_TBL_ATTR_NAME:
			queue->NameLen = attrvalue->Len;
			strncpy(queue->Name, attrvalue->Str, sizeof(queue->Name)-1);

			if ( SimpleStringComp(attrvalue,
								  RESOURCE_QUEUE_DEFAULT_QUEUE_NAME) == 0 )
			{
				queue->Status |= RESOURCE_QUEUE_STATUS_IS_DEFAULT;
			}
			else if ( SimpleStringComp(attrvalue,
									   RESOURCE_QUEUE_ROOT_QUEUE_NAME) == 0 )
			{
				queue->Status |= RESOURCE_QUEUE_STATUS_IS_ROOT;
			}
			break;

		case RSQ_TBL_ATTR_STATUS:
			/*
			 * 'branch' indicates one branch queue in-use.
			 * 'invalidate' indicates the queue is invalid, and not in-use.
			 */
			if ( SimpleStringFind(attrvalue, "branch") == FUNC_RETURN_OK )
			{
				queue->Status |= RESOURCE_QUEUE_STATUS_VALID_BRANCH;
			}
			if ( SimpleStringFind(attrvalue, "invalid") == FUNC_RETURN_OK )
			{
				queue->Status |= RESOURCE_QUEUE_STATUS_VALID_INVALID;
			}

			if ( !RESQUEUE_IS_BRANCH(queue) )
			{
				queue->Status |= RESOURCE_QUEUE_STATUS_VALID_LEAF;
			}

			if ( (queue->Status & RESOURCE_QUEUE_STATUS_VALID_INVALID) == 0 )
			{
				queue->Status |= RESOURCE_QUEUE_STATUS_VALID_INUSE;
			}
			break;

		case RSQ_TBL_ATTR_CREATION_TIME:
		case RSQ_TBL_ATTR_UPDATE_TIME:
			break;
		default:
			/* Should not occur. Invalid attribute name has been checked. */
			Assert(false);
		}

		if ( res != FUNC_RETURN_OK )
		{
			res = RESQUEMGR_WRONG_ATTR;
			if ( errorbuf[0] == '\0' )
			{
				snprintf(errorbuf, errorbufsize,
						 "wrong resource queue attribute setting %s=%s",
						 loadcatalog ?
							 RSQTBLAttrNames[attrindex] :
							 RSQDDLAttrNames[attrindex],
						 attrvalue->Str);
			}
			elog(WARNING, "Resource manager failed to parse resource queue "
						  "attribute, %s",
						  errorbuf);
			return res;
		}
	}

	if ( checkformatonly )
	{
		return res;
	}

	/*
	 * Memory and Core resource must be specified and they must use the same way
	 * to express the resource.
	 */
	if ( !memlimit_value && !memlimit_percentage )
	{
		res = RESQUEMGR_LACK_ATTR;
		snprintf(errorbuf, errorbufsize,
				 "%s must be specified",
				 loadcatalog?
					 RSQTBLAttrNames[RSQ_TBL_ATTR_MEMORY_LIMIT_CLUSTER] :
					 RSQDDLAttrNames[RSQ_DDL_ATTR_MEMORY_LIMIT_CLUSTER]);
		elog(WARNING, "%s", errorbuf);
		return res;
	}

	if ( !corelimit_value && !corelimit_percentage )
	{
		res = RESQUEMGR_LACK_ATTR;
		snprintf(errorbuf, errorbufsize,
				 "%s must be specified",
				 loadcatalog ?
					 RSQTBLAttrNames[RSQ_TBL_ATTR_CORE_LIMIT_CLUSTER] :
					 RSQDDLAttrNames[RSQ_DDL_ATTR_CORE_LIMIT_CLUSTER]);
		elog(WARNING, "%s", errorbuf);
		return res;
	}

	if ( (memlimit_value 		&& corelimit_percentage	) ||
		 (memlimit_percentage 	&& corelimit_value		)	)
	{
		res = RESQUEMGR_INCONSISTENT_RESOURCE_EXP;
		snprintf(errorbuf, errorbufsize,
				 "%s and %s must use the same way to express resource limit",
				 loadcatalog ?
					 RSQTBLAttrNames[RSQ_TBL_ATTR_MEMORY_LIMIT_CLUSTER] :
					 RSQDDLAttrNames[RSQ_DDL_ATTR_MEMORY_LIMIT_CLUSTER],
				 loadcatalog ?
					 RSQTBLAttrNames[RSQ_TBL_ATTR_CORE_LIMIT_CLUSTER] :
					 RSQDDLAttrNames[RSQ_DDL_ATTR_CORE_LIMIT_CLUSTER]);
		elog(WARNING, "%s", errorbuf);
		return res;
	}

	if ( memlimit_percentage && corelimit_percentage )
	{
		queue->Status |= RESOURCE_QUEUE_STATUS_EXPRESS_PERCENT;
	}
	else
	{
		Assert(memlimit_value && corelimit_value);
	}
	return res;
}

/*
 * This function parses the attributes and translate into an exist DynResourceQueue
 * struct's attributes. This functions does not generate logs higher than
 * WARNING, the concrete error is also saved in error buffer to make the caller
 * able to pass back the message to remote process.
 *
 */
int updateResourceQueueAttributesInShadow(List 			 		*attributes,
								  	  	  DynResourceQueueTrack	 queue,
										  char					*errorbuf,
										  int					 errorbufsize)
{
	int			  			res				  = FUNC_RETURN_OK;
	ListCell 			   *cell 			  = NULL;
	int 		  			attrindex		  = -1;
	int			  			percentage_change = 0;
	DynResourceQueue		shadowqueinfo	  = NULL;

	SimpStringPtr			attrname		  = NULL;
	SimpStringPtr			attrvalue		  = NULL;

	Assert(queue != NULL);
	shadowqueinfo = queue->ShadowQueueTrack->QueueInfo;

	/* Go through each property content. */
	foreach(cell, attributes)
	{
		KVProperty value = lfirst(cell);

		attrname  = &(value->Key);
		attrvalue = &(value->Val);

		attrindex = getRSQTBLAttributeNameIndex(attrname);

		/*
		 * Actually parse each attribute.
		 */
		switch(attrindex)
		{
		case RSQ_TBL_ATTR_PARENT:
			res = RESQUEMGR_WRONG_ATTRNAME;
			snprintf(errorbuf, errorbufsize,
					 "cannot alter resource queue parent name");
			elog(WARNING, "Cannot update resource queue %s attribute, %s",
						  queue->QueueInfo->Name,
						  errorbuf);
			return res;
		case RSQ_TBL_ATTR_NAME:
			break;

		case RSQ_TBL_ATTR_ACTIVE_STATMENTS:
			res = SimpleStringToInt32(attrvalue, &(shadowqueinfo->ParallelCount));
			if ( res == FUNC_RETURN_OK )
			{
				elog(RMLOG, "Resource manager updated %s %d in shadow "
							"of resource queue %s.",
							RSQTBLAttrNames[RSQ_TBL_ATTR_ACTIVE_STATMENTS],
							shadowqueinfo->ParallelCount,
							queue->QueueInfo->Name);
			}
			break;

		case RSQ_TBL_ATTR_MEMORY_LIMIT_CLUSTER:
			if ( SimpleStringIsPercentage(attrvalue) )
			{
				percentage_change += 1;
				int8_t inputval = 0;
				res = SimpleStringToPercentage(attrvalue, &inputval);
				shadowqueinfo->ClusterMemoryPer = inputval;

				if ( res == FUNC_RETURN_OK )
				{
					elog(RMLOG, "Resource manager updated %s %lf.0%% in shadow "
								"of resource queue %s.",
								RSQTBLAttrNames[RSQ_TBL_ATTR_MEMORY_LIMIT_CLUSTER],
								shadowqueinfo->ClusterMemoryPer,
								queue->QueueInfo->Name);
				}

				shadowqueinfo->Status |= RESOURCE_QUEUE_STATUS_EXPRESS_PERCENT;
			}
			else
			{
				Assert(false);
			}
			break;

		case RSQ_TBL_ATTR_CORE_LIMIT_CLUSTER:
			if ( SimpleStringIsPercentage(attrvalue) )
			{
				percentage_change += 1;
				int8_t inputval = 0;
				res = SimpleStringToPercentage(attrvalue, &inputval);
				shadowqueinfo->ClusterVCorePer = inputval;

				if ( res == FUNC_RETURN_OK )
				{
					elog(RMLOG, "Resource manager updated %s %lf.0%% in shadow "
								"of resource queue %s.",
								RSQTBLAttrNames[RSQ_TBL_ATTR_CORE_LIMIT_CLUSTER],
								shadowqueinfo->ClusterVCorePer,
								queue->QueueInfo->Name);
				}

				shadowqueinfo->Status |= RESOURCE_QUEUE_STATUS_EXPRESS_PERCENT;
			}
			else
			{
				Assert(false);
			}
			break;

		case RSQ_TBL_ATTR_VSEG_RESOURCE_QUOTA:
			/* Decide it is a memory quota or core quota. */
			if ( SimpleStringStartWith(
					 attrvalue,
					 RESOURCE_QUEUE_SEG_RES_QUOTA_MEM) == FUNC_RETURN_OK )
			{
				SimpString valuestr;
				setSimpleStringRef(
					&valuestr,
					attrvalue->Str+sizeof(RESOURCE_QUEUE_SEG_RES_QUOTA_MEM)-1,
					attrvalue->Len-sizeof(RESOURCE_QUEUE_SEG_RES_QUOTA_MEM)+1);

				res = SimpleStringToStorageSizeMB(
						&valuestr,
						&(shadowqueinfo->SegResourceQuotaMemoryMB));
				if ( res == FUNC_RETURN_OK )
				{
					shadowqueinfo->SegResourceQuotaVCore = -1;
					elog(RMLOG, "Resource manager updated %s memory quota %d MB "
								"in shadow of resource queue %s.",
								RSQTBLAttrNames[RSQ_TBL_ATTR_VSEG_RESOURCE_QUOTA],
								shadowqueinfo->SegResourceQuotaMemoryMB,
								queue->QueueInfo->Name);
				}
			}
			break;

		case RSQ_TBL_ATTR_RESOURCE_OVERCOMMIT_FACTOR:
			res = SimpleStringToDouble(attrvalue,
									   &(shadowqueinfo->ResourceOvercommit));
			if ( res == FUNC_RETURN_OK )
			{
				elog(RMLOG, "Resource manager updated %s %lf in shadow of "
							"resource queue %s.",
							RSQTBLAttrNames[RSQ_TBL_ATTR_RESOURCE_OVERCOMMIT_FACTOR],
							shadowqueinfo->ResourceOvercommit,
							queue->QueueInfo->Name);
			}
			break;

		case RSQ_TBL_ATTR_ALLOCATION_POLICY:
			res = SimpleStringToMapIndexInt8(attrvalue,
											 (char *)RSQDDLValueAllocationPolicy,
											 RSQ_ALLOCATION_POLICY_COUNT,
											 sizeof(RSQDDLValueAllocationPolicy[0]),
											 &(shadowqueinfo->AllocatePolicy));
			if ( res == FUNC_RETURN_OK )
			{
				elog(RMLOG, "Resource manager updated %s %s index %d in shadow of "
							"resource queue %s.",
							RSQTBLAttrNames[RSQ_TBL_ATTR_ALLOCATION_POLICY],
							RSQDDLValueAllocationPolicy[shadowqueinfo->AllocatePolicy],
							shadowqueinfo->AllocatePolicy,
							queue->QueueInfo->Name);
			}
			break;

		case RSQ_TBL_ATTR_NVSEG_UPPER_LIMIT:
			res = SimpleStringToInt32(attrvalue,
									  &(shadowqueinfo->NVSegUpperLimit));
			if ( res == FUNC_RETURN_OK )
			{
				elog(RMLOG, "Resource manager updated %s %d in shadow of "
							"resource queue %s.",
							RSQTBLAttrNames[RSQ_TBL_ATTR_NVSEG_UPPER_LIMIT],
							shadowqueinfo->NVSegUpperLimit,
							queue->QueueInfo->Name);
			}
			break;
		case RSQ_TBL_ATTR_NVSEG_LOWER_LIMIT:
			res = SimpleStringToInt32(attrvalue,
									  &(shadowqueinfo->NVSegLowerLimit));
			if ( res == FUNC_RETURN_OK )
			{
				elog(RMLOG, "Resource manager updated %s %d in shadow of "
							"resource queue %s.",
							RSQTBLAttrNames[RSQ_TBL_ATTR_NVSEG_LOWER_LIMIT],
							shadowqueinfo->NVSegLowerLimit,
							queue->QueueInfo->Name);
			}
			break;
		case RSQ_TBL_ATTR_NVSEG_UPPER_LIMIT_PERSEG:
			res = SimpleStringToDouble(attrvalue,
									   &(shadowqueinfo->NVSegUpperLimitPerSeg));
			if ( res == FUNC_RETURN_OK )
			{
				elog(RMLOG, "Resource manager updated %s %lf in shadow of "
							"resource queue %s.",
							RSQTBLAttrNames[RSQ_TBL_ATTR_NVSEG_UPPER_LIMIT_PERSEG],
							shadowqueinfo->NVSegUpperLimitPerSeg,
							queue->QueueInfo->Name);
			}
			break;
		case RSQ_TBL_ATTR_NVSEG_LOWER_LIMIT_PERSEG:
			res = SimpleStringToDouble(attrvalue,
									   &(shadowqueinfo->NVSegLowerLimitPerSeg));
			if ( res == FUNC_RETURN_OK )
			{
				elog(RMLOG, "Resource manager updated %s %lf in shadow of "
							"resource queue %s.",
							RSQTBLAttrNames[RSQ_TBL_ATTR_NVSEG_LOWER_LIMIT_PERSEG],
							shadowqueinfo->NVSegLowerLimitPerSeg,
							queue->QueueInfo->Name);
			}
			break;
		default:
			/* Should not occur. Invalid attribute name has been checked. */
			Assert(false);
		}

		if ( res != FUNC_RETURN_OK )
		{
			res = RESQUEMGR_WRONG_ATTR;
			snprintf(errorbuf, errorbufsize,
					 "failed to recognize attribute setting %s=%s",
					 attrname->Str,
					 attrvalue->Str);
			elog(WARNING, "Resource manager failed to update resource queue "
						  "attribute in shadow of resource queue %s, %s",
						  queue->QueueInfo->Name,
						  errorbuf);
			return res;
		}
	}

	return res;
}

#define ELOG_WARNING_ERRORMESSAGE_COMPLETEQUEUE(queue,errorbuf)				   \
		elog(WARNING, "Resource manager cannot complete resource queue %s, %s",\
					  (queue)->Name,										   \
					  (errorbuf));
/*
 * This is one API for checking if new resource queue definition is valid to be
 * created.This functions does not generate logs higher than WARNING, the error
 * is also saved in error buffer to make the caller able to pass the message to
 * remote process.
 */
int checkAndCompleteNewResourceQueueAttributes(DynResourceQueue  queue,
											   char				*errorbuf,
											   int				 errorbufsize)
{
	DynResourceQueueTrack	parenttrack	 = NULL;
	int 					res 		 = FUNC_RETURN_OK;

	Assert( queue != NULL );

	if ( queue->Status & RESOURCE_QUEUE_STATUS_IS_VER1X )
	{
		/* TODO: Validate Version 1.x resource queue definition here. */
		return res;
	}

	/*** Validate HAWQ 2.0 resource queue definition ***/

	/*
	 * STEP 1. Validate parent queue attribute.
	 */
	if ( !RESQUEUE_IS_ROOT(queue) && queue->ParentOID == InvalidOid )
	{
		res = RESQUEMGR_LACK_ATTR;
		snprintf(errorbuf, errorbufsize,
				 "attribute %s must be specified",
				 RSQDDLAttrNames[RSQ_DDL_ATTR_PARENT]);
		ELOG_WARNING_ERRORMESSAGE_COMPLETEQUEUE(queue, errorbuf)
		return res;
	}

	if ( !RESQUEUE_IS_ROOT(queue) && queue->ParentOID != InvalidOid )
	{
		parenttrack = getQueueTrackByQueueOID(queue->ParentOID);
		Assert(parenttrack != NULL);

		/* pg_default cannot be a parent queue. */
		if ( RESQUEUE_IS_DEFAULT(parenttrack->QueueInfo) )
		{
			res = RESQUEMGR_WRONG_ATTR;
			snprintf(errorbuf, errorbufsize,
					 "%s cannot have children resource queues",
					 RESOURCE_QUEUE_DEFAULT_QUEUE_NAME);
			ELOG_WARNING_ERRORMESSAGE_COMPLETEQUEUE(queue, errorbuf)
			return res;
		}

		/* The parent queue cannot have roles assigned. */
		if ( hasUserAssignedToQueue(parenttrack->QueueInfo) )
		{
			res = RESQUEMGR_WRONG_ATTR;
			snprintf(errorbuf, errorbufsize,
					 "%s has at least one role assigned",
					 parenttrack->QueueInfo->Name);
			ELOG_WARNING_ERRORMESSAGE_COMPLETEQUEUE(queue, errorbuf)
			return res;
		}
	}

	/*
	 * Parent queue must exist. Basically, default queue is already created as
	 * the root of whole resource queue tree. This checking is for self-test.
	 */
	if ( RESQUEUE_IS_ROOT(queue) )
	{
		Assert(queue->ParentOID == InvalidOid);
		parenttrack = NULL;
	}

	/*
	 * STEP 2. Validate active_statements attributes. For leaf queue only.
	 */

	if ( queue->ParallelCount <= 0 )
	{
		queue->ParallelCount = DEFAULT_RESQUEUE_ACTIVESTATS_N;
	}

	/*
     * STEP 3. Validate resource limit attributes.
	 */

	/*======================================*/
	/* STEP 3 CASE1: percentage expression. */
	/*======================================*/
	if ( RESQUEUE_IS_PERCENT(queue) )
	{
		/*
		 * The values of MEMORY_LIMIT_CLUSER, CORE_LIMIT_CLUSTER must be
		 * identical.
		 */
		if ( queue->ClusterVCorePer != queue->ClusterMemoryPer )
		{
			res = RESQUEMGR_WRONG_ATTR;
			snprintf(errorbuf, errorbufsize,
					 "the value of %s must be identical with the value of %s, "
					 "wrong value of %s = %.0lf%%, "
					 "wrong value of %s = %.0lf%% ",
					 RSQDDLAttrNames[RSQ_DDL_ATTR_MEMORY_LIMIT_CLUSTER],
					 RSQDDLAttrNames[RSQ_DDL_ATTR_CORE_LIMIT_CLUSTER],
					 RSQDDLAttrNames[RSQ_DDL_ATTR_MEMORY_LIMIT_CLUSTER],
					 queue->ClusterMemoryPer,
					 RSQDDLAttrNames[RSQ_DDL_ATTR_CORE_LIMIT_CLUSTER],
					 queue->ClusterVCorePer);
			ELOG_WARNING_ERRORMESSAGE_COMPLETEQUEUE(queue, errorbuf)
			return res;
		}

		/*
		 * check siblings' resource limit
		 */
		if (queue->ParentOID != InvalidOid)
		{
			double current = 0.0;
			parenttrack = getQueueTrackByQueueOID(queue->ParentOID);
			if (parenttrack != NULL)
			{
				ListCell *cell = NULL;
				foreach(cell, parenttrack->ChildrenTracks)
				{
					DynResourceQueueTrack track = (DynResourceQueueTrack)lfirst(cell);
					if (strncmp(track->QueueInfo->Name, queue->Name, queue->NameLen) != 0)
					{
						current += track->QueueInfo->ClusterMemoryPer;
					}
				}

				if ((current + queue->ClusterMemoryPer) > 100)
				{
					res = RESQUEMGR_WRONG_ATTR;
					snprintf(errorbuf, errorbufsize,
							 "the value of %s and %s exceeds parent queue's limit, "
							 "wrong value = %.0lf%%",
							 RSQDDLAttrNames[RSQ_DDL_ATTR_CORE_LIMIT_CLUSTER],
							 RSQDDLAttrNames[RSQ_DDL_ATTR_MEMORY_LIMIT_CLUSTER],
							 queue->ClusterMemoryPer);
					ELOG_WARNING_ERRORMESSAGE_COMPLETEQUEUE(queue, errorbuf)
					return res;
				}
			}
		}
	}
	/*============================================================*/
	/* STEP 3 CASE2: value expression. Temporarily not supported. */
	/*============================================================*/
	else {
		Assert(false);
	}

	/*
	 * STEP 4: Check resource quota.
	 */
	if ( queue->SegResourceQuotaMemoryMB == -1 &&
		 queue->SegResourceQuotaVCore    == -1.0 )
	{
		queue->SegResourceQuotaMemoryMB = DEFAULT_RESQUEUE_VSEGRESOURCEQUOTA_N;
	}

	Assert( queue->SegResourceQuotaMemoryMB != -1 );

	/*
	 * STEP 5: Check policy and set default value.
	 */
	if ( queue->AllocatePolicy == -1 )
	{
		queue->AllocatePolicy = RSQ_ALLOCATION_POLICY_EVEN;
	}

	/*
	 * STEP 6. Check number of vseg limit.
	 */

	if ( queue->NVSegUpperLimit > 0 &&
		 queue->NVSegLowerLimit > 0 &&
		 queue->NVSegUpperLimit < queue->NVSegLowerLimit )
	{
		res = RESQUEMGR_WRONG_ATTR;
		snprintf(errorbuf, errorbufsize,
				 "%s is less than %s",
				 RSQDDLAttrNames[RSQ_DDL_ATTR_NVSEG_UPPER_LIMIT],
				 RSQDDLAttrNames[RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT]);
		ELOG_WARNING_ERRORMESSAGE_COMPLETEQUEUE(queue, errorbuf)
		return res;
	}

	/*
	 * STEP 7. Check number of vseg limit per segment.
	 */
	if ( queue->NVSegUpperLimitPerSeg < MINIMUM_RESQUEUE_NVSEG_UPPER_PERSEG_LIMIT_N )
	{
		res = RESQUEMGR_WRONG_ATTR;
		snprintf(errorbuf, errorbufsize,
				 "%s is less than %lf, wrong value %lf",
				 RSQDDLAttrNames[RSQ_DDL_ATTR_NVSEG_UPPER_LIMIT_PERSEG],
				 MINIMUM_RESQUEUE_NVSEG_UPPER_PERSEG_LIMIT_N,
				 queue->NVSegUpperLimitPerSeg);
		ELOG_WARNING_ERRORMESSAGE_COMPLETEQUEUE(queue, errorbuf)
		return res;
	}

	if ( queue->NVSegLowerLimitPerSeg < MINIMUM_RESQUEUE_NVSEG_LOWER_PERSEG_LIMIT_N )
	{
		res = RESQUEMGR_WRONG_ATTR;
		snprintf(errorbuf, errorbufsize,
				 "%s is less than %lf, wrong value %lf",
				 RSQDDLAttrNames[RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT_PERSEG],
				 MINIMUM_RESQUEUE_NVSEG_LOWER_PERSEG_LIMIT_N,
				 queue->NVSegLowerLimitPerSeg);
		ELOG_WARNING_ERRORMESSAGE_COMPLETEQUEUE(queue, errorbuf)
		return res;
	}

	if ( queue->NVSegUpperLimitPerSeg > 0 &&
		 queue->NVSegLowerLimitPerSeg > 0 &&
		 queue->NVSegUpperLimitPerSeg < queue->NVSegLowerLimitPerSeg )
	{
		res = RESQUEMGR_WRONG_ATTR;
		snprintf(errorbuf, errorbufsize,
				 "%s is less than %s",
				 RSQDDLAttrNames[RSQ_DDL_ATTR_NVSEG_UPPER_LIMIT_PERSEG],
				 RSQDDLAttrNames[RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT_PERSEG]);
		ELOG_WARNING_ERRORMESSAGE_COMPLETEQUEUE(queue, errorbuf)
		return res;
	}
	return res;
}

#define ELOG_WARNING_ERRORMESSAGE_CREATEQUEUETRACK(queue,errorbuf)			   \
		elog(WARNING, "Resource manager cannot create resource queue track "   \
					  "instance for queue %s, %s",  						   \
					  (queue)->Name,										   \
					  (errorbuf));
/**
 * Create queue definition and tracker in the resource queue manager.
 *
 * queue[in]			The resource queue definition instance.
 * track[out]			The corresponding resource queue tracker instance. This
 * 						is a new created instance.
 * errorbuf[out]		The error message if something is wrong.
 * errorbufsize[out]	The limit of error message buffer.
 */
int createQueueAndTrack( DynResourceQueue		queue,
						 DynResourceQueueTrack *track,
						 char				   *errorbuf,
						 int					errorbufsize)
{
	int 				  	res 			 = FUNC_RETURN_OK;
	DynResourceQueueTrack	parenttrack		 = NULL;
    DynResourceQueueTrack 	newqueuetrack 	 = NULL;
    bool					isDefaultQueue   = false;
    bool					isRootQueue		 = false;

    /* Input validation. */
    Assert(track != NULL);
    Assert(queue != NULL);

	/*
	 * Create new queue tracking instance. If there is something wrong, this
	 * instance will be freed.
	 */
	newqueuetrack = createDynResourceQueueTrack(queue);

    /*
     * Check queue oid ( for loading existing queues only ). In case loading
     * existing queues from file or catalog, the oid should be explicitly
     * specified. In case the queue is to be created, no need to check this.
     */
    if ( queue->OID > InvalidOid )
    {
    	DynResourceQueueTrack tmpquetrack = getQueueTrackByQueueOID(queue->OID);
		Assert(tmpquetrack == NULL);
    }

    /* New queue name must be set and unique. */
    Assert(queue->NameLen > 0);

    DynResourceQueueTrack tmpquetrack = getQueueTrackByQueueName((char *)(queue->Name),
    															 queue->NameLen);
	if (tmpquetrack != NULL) {
		res = RESQUEMGR_DUPLICATE_QUENAME;
		snprintf(errorbuf, errorbufsize,
				 "duplicate queue name %s for creating resource queue.",
			     queue->Name);
		ELOG_WARNING_ERRORMESSAGE_CREATEQUEUETRACK(queue, errorbuf)
		goto exit;
	}

	/* Decide if the queue is special one: pg_root or pg_default */
	isDefaultQueue = RESQUEUE_IS_DEFAULT(queue);
	isRootQueue	   = RESQUEUE_IS_ROOT(queue);

	elog(DEBUG3, "HAWQ RM :: To create resource queue instance %s", queue->Name);

	/*
	 * Check the queue parent-child relationship. No matter the queue is to be
	 * created or to be loaded, the parent queue must have been loaded. The only
	 * queue has no parent is 'pg_root' say isRootQueue. The queue 'pg_default'
	 * must has 'pg_root' as the parent queue.
	 */
	if ( !isRootQueue )
	{
		/* Check if the parent queue id exists. */
		parenttrack = getQueueTrackByQueueOID(queue->ParentOID);
		Assert(parenttrack != NULL);

		/* The parent queue can not have connections. */
		if ( parenttrack->CurConnCounter > 0 )
		{
			res = RESQUEMGR_IN_USE;
			snprintf( errorbuf, errorbufsize,
					  "the parent queue %s has active connections",
					  parenttrack->QueueInfo->Name);
			ELOG_WARNING_ERRORMESSAGE_CREATEQUEUETRACK(queue, errorbuf)
			goto exit;
		}

		/*
		 * If the parent track changes the role from LEAF to BRANCH, its memory
		 * core ratio related information should also be updated.
		 */
		if ( RESQUEUE_IS_LEAF(parenttrack->QueueInfo) &&
			 parenttrack->trackedMemCoreRatio )
		{
			/* Remove parent track from memory core ratio track */
			removeResourceQueueRatio(parenttrack);

			/* Change parent track to branch queue. */
			parenttrack->QueueInfo->Status &= NOT_RESOURCE_QUEUE_STATUS_VALID_LEAF;
			parenttrack->QueueInfo->Status |= RESOURCE_QUEUE_STATUS_VALID_BRANCH;
		}
	}

	/* Set parent resource queue track reference. */
	newqueuetrack->ParentTrack = parenttrack;

	/* Build resource queue tree structure. Save 'pg_root' and 'pg_default' */
	if ( isRootQueue )
	{
		PQUEMGR->RootTrack = newqueuetrack;
	}
	else
	{
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		parenttrack->ChildrenTracks = lappend(parenttrack->ChildrenTracks,
											  newqueuetrack);
		MEMORY_CONTEXT_SWITCH_BACK
	}

	if ( isDefaultQueue )
	{
		PQUEMGR->DefaultTrack = newqueuetrack;
	}

	/* Save the queue track into the list and build index.*/
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PQUEMGR->Queues = lappend(PQUEMGR->Queues, newqueuetrack);
	MEMORY_CONTEXT_SWITCH_BACK
	if( newqueuetrack->QueueInfo->OID != InvalidOid )
	{
		setQueueTrackIndexedByQueueOID(newqueuetrack);
	}
	setQueueTrackIndexedByQueueName(newqueuetrack);

	/* Update overall ratio index. */
	Assert(RESQUEUE_IS_PERCENT(newqueuetrack->QueueInfo));

	/* Set return value. */
	*track = newqueuetrack;

exit:
	if ( res != FUNC_RETURN_OK )
	{
		/* Free resource queue track instance. */
		shallowFreeResourceQueueTrack(newqueuetrack);
		*track = NULL;
	}
	return res;
}

int dropQueueAndTrack( DynResourceQueueTrack track,
					   char				     *errorbuf,
					   int					  errorbufsize)
{
	int 				  	res 			 = FUNC_RETURN_OK;
	DynResourceQueueTrack	parenttrack		 = NULL;
	ListCell			   *cell			 = NULL;
	ListCell			   *prevcell		 = NULL;

	/* remove track from parent queue's children list */
	parenttrack = track->ParentTrack;
	foreach(cell, parenttrack->ChildrenTracks)
	{
		DynResourceQueueTrack todeltrack = (DynResourceQueueTrack)lfirst(cell);
		if ( todeltrack == track )
		{
			MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
			parenttrack->ChildrenTracks = list_delete_cell(parenttrack->ChildrenTracks,
														   cell,
														   prevcell);
			MEMORY_CONTEXT_SWITCH_BACK
			break;
		}
		prevcell = cell;
	}

	/*
	 * If the resource queue has been assigned by a memory/core ratio, the
	 * corresponding reference in memory/core ratio tracker should also be
	 * updated.
	 */
	if ( track->trackedMemCoreRatio ) {
		removeResourceQueueRatio(track);
	}

	/* Directly remove the instances. */
	removeQueueTrackIndexedByQueueName(track);
	removeQueueTrackIndexedByQueueOID(track);

	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	cell 	 = NULL;
	prevcell = NULL;
	foreach(cell, PQUEMGR->Queues)
	{
		DynResourceQueueTrack todeltrack = lfirst(cell);
		if ( todeltrack == track )
		{
			PQUEMGR->Queues = list_delete_cell(PQUEMGR->Queues, cell, prevcell);
			break;
		}
		prevcell = cell;
	}
	MEMORY_CONTEXT_SWITCH_BACK

	rm_pfree(PCONTEXT, track->QueueInfo);
	shallowFreeResourceQueueTrack(track);
	return res;
}


DynResourceQueueTrack getQueueTrackByQueueOID (int64_t queoid)
{
	PAIR pair = NULL;
	SimpArray key;
	setSimpleArrayRef(&key, (char *)&queoid, sizeof(int64_t));
	pair = getHASHTABLENode(&(PQUEMGR->QueuesIDIndex), (void *)&key);
	if ( pair == NULL )
	{
		return NULL;
	}
	Assert(pair->Value != NULL);
	return (DynResourceQueueTrack)(pair->Value);
}

DynResourceQueueTrack getQueueTrackByQueueName(char 	*quename,
											   int 		 quenamelen)
{
	SimpString quenamestr;
	setSimpleStringRef(&quenamestr, quename, quenamelen);
	PAIR pair = getHASHTABLENode(&(PQUEMGR->QueuesNameIndex),
								 (void *)(&quenamestr));
	if ( pair == NULL ) {
		return NULL;
	}
	Assert(pair->Value != NULL);
	return (DynResourceQueueTrack)(pair->Value);
}

void setQueueTrackIndexedByQueueOID(DynResourceQueueTrack queuetrack)
{
	SimpArray key;
	setSimpleArrayRef(&key,
					  (void *)&(queuetrack->QueueInfo->OID),
					  sizeof(int64_t));
	setHASHTABLENode(&(PQUEMGR->QueuesIDIndex), &key, queuetrack, false);
}

void removeQueueTrackIndexedByQueueOID(DynResourceQueueTrack queuetrack)
{
	SimpArray key;
	setSimpleArrayRef(&key,
					(void *)&(queuetrack->QueueInfo->OID),
					sizeof(int64_t));
	removeHASHTABLENode(&(PQUEMGR->QueuesIDIndex),&key);
}

void setQueueTrackIndexedByQueueName(DynResourceQueueTrack queuetrack)
{
	SimpString quenamestr;
	setSimpleStringRef(&quenamestr,
					   queuetrack->QueueInfo->Name,
					   queuetrack->QueueInfo->NameLen);
	setHASHTABLENode(&(PQUEMGR->QueuesNameIndex),
					 (void *)(&quenamestr),
					 queuetrack,
					 false);
}

void removeQueueTrackIndexedByQueueName(DynResourceQueueTrack queuetrack)
{
	SimpString quenamestr;
	setSimpleStringRef(&quenamestr,
					   queuetrack->QueueInfo->Name,
					   queuetrack->QueueInfo->NameLen);
	removeHASHTABLENode(&(PQUEMGR->QueuesNameIndex), (void *) (&quenamestr));
}

const char *getRSQTBLAttributeName(int attrindex)
{
	Assert( attrindex >= 0 && attrindex < RSQ_TBL_ATTR_COUNT );
	return RSQTBLAttrNames[attrindex];
}

const char *getRSQDDLAttributeName(int colindex)
{
	Assert( colindex >= 0 && colindex < RSQ_DDL_ATTR_COUNT );
	return RSQDDLAttrNames[colindex];
}

/**
 * Get memory/core ratio index. Return -1 if no such ratio tracked.
 */
int32_t getResourceQueueRatioIndex(uint32_t ratio)
{
	if ( ratio <= 0 )
		return -1;
	PAIR ratiopair = getHASHTABLENode(&(PQUEMGR->RatioIndex),
									  TYPCONVERT(void *, ratio));
	return ratiopair == NULL ? -1 : TYPCONVERT(int32_t, ratiopair->Value);
}

DynMemoryCoreRatioTrack createDynMemoryCoreRatioTrack(uint32_t ratio,
													  int32_t index )
{
	DynMemoryCoreRatioTrack res = rm_palloc(PCONTEXT,
				  	  	  	  	  	  	    sizeof(DynMemoryCoreRatioTrackData));
	res->MemCoreRatio 				= ratio;
	res->RatioIndex   				= -1;
	res->ClusterMemory				= 0;
	res->ClusterVCore				= 0.0;
	res->ClusterMemoryMaxMB 		= 0;
	res->ClusterVCoreMax   			= 0.0;
	res->TotalPendingStartTime 		= 0;
	res->QueueTrackers				= NULL;
	res->ClusterWeightMarker 		= 0;
	res->QueueIndexForLeftResource 	= 0;

	resetResourceBundleData(&(res->TotalPending)  , 0, 0.0, ratio);
	resetResourceBundleData(&(res->TotalAllocated), 0, 0.0, ratio);
	resetResourceBundleData(&(res->TotalRequest)  , 0, 0.0, ratio);
	resetResourceBundleData(&(res->TotalUsed)     , 0, 0.0, ratio);

	return res;
}

void freeMemoryCoreTrack(DynMemoryCoreRatioTrack mctrack)
{
	Assert(list_length(mctrack->QueueTrackers) == 0);
	rm_pfree(PCONTEXT, mctrack);
}

int removeQueueTrackFromMemoryCoreTrack(DynMemoryCoreRatioTrack mctrack,
											DynResourceQueueTrack   track)
{
	ListCell *cell 	   = NULL;
	ListCell *prevcell = NULL;
	foreach(cell, mctrack->QueueTrackers)
	{
		DynResourceQueueTrack trackiter = (DynResourceQueueTrack)lfirst(cell);
		if ( trackiter == track )
		{
			MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
			mctrack->QueueTrackers = list_delete_cell(mctrack->QueueTrackers,
													  cell,
													  prevcell);
			MEMORY_CONTEXT_SWITCH_BACK
			return FUNC_RETURN_OK;
		}
		prevcell = cell;
	}
	return RESQUEMGR_NO_QUE_IN_RATIO;
}
/**
 * Add one queue track of memory/core ratio into resource queue manager.
 */
int32_t addResourceQueueRatio(DynResourceQueueTrack track)
{
	if ( track->MemCoreRatio <= 0 )
		return -1;

	uint32_t ratio = track->MemCoreRatio;
	int32_t res = getResourceQueueRatioIndex(ratio);
	if ( res >= 0 ) {
		PQUEMGR->RatioReferenceCounter[res]++;
	} else {
		res = PQUEMGR->RatioCount;
		PQUEMGR->RatioReverseIndex[PQUEMGR->RatioCount] = ratio;
		setHASHTABLENode(&(PQUEMGR->RatioIndex),
						 TYPCONVERT(void *, ratio),
						 TYPCONVERT(void *, PQUEMGR->RatioCount),
						 false);
		PQUEMGR->RatioCount++;
		PQUEMGR->RatioReferenceCounter[res] = 1;
		PQUEMGR->RatioTrackers[res] = createDynMemoryCoreRatioTrack(ratio, res);
		elog(RMLOG, "Added new memory/core ratio %u, assigned index %d.",
					ratio, res);

		/* Add all resource info into the alloc/avail resource ordered indices */
		BBST newindex = NULL; /* variable for calling the following function. */
		addOrderedResourceAllocTreeIndexByRatio(ratio, &newindex);
		addOrderedResourceAvailTreeIndexByRatio(ratio, &newindex);
	}

	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PQUEMGR->RatioTrackers[res]->QueueTrackers =
		lappend(PQUEMGR->RatioTrackers[res]->QueueTrackers, track);
	MEMORY_CONTEXT_SWITCH_BACK

	track->trackedMemCoreRatio = true;
	return res;
}

void removeResourceQueueRatio(DynResourceQueueTrack track)
{
	int res = FUNC_RETURN_OK;

	/* Ignore invalid ratio number. */
	if ( track->MemCoreRatio <= 0 )
		return;

	uint32_t ratio      = track->MemCoreRatio;
	int32_t  ratioindex = getResourceQueueRatioIndex(ratio);

	/* Ignore unkonwn ratio number. */
	if ( ratioindex < 0 ) {
		elog( WARNING, "HAWQ RM :: Cannot track resource queue %s with memory "
					   "core ratio %d MB Per CORE.",
					   track->QueueInfo->Name,
					   track->MemCoreRatio);
		return;
	}

	/* Minus ratio counter, if the counter is set to 0, delete this ratio. */
	Assert(PQUEMGR->RatioReferenceCounter[ratioindex] > 0);
	PQUEMGR->RatioReferenceCounter[ratioindex]--;

	/* Remove the reference from memory/core ratio tracker */
	res = removeQueueTrackFromMemoryCoreTrack(PQUEMGR->RatioTrackers[ratioindex],
											  track);
	if ( res != FUNC_RETURN_OK ) {
		elog( WARNING, "HAWQ RM :: Cannot find resource queue %s with memory "
					   "core ratio %d MB Per CORE in memory core ratio tracker.",
					   track->QueueInfo->Name,
					   track->MemCoreRatio);
		return;
	}

	if (PQUEMGR->RatioReferenceCounter[ratioindex] == 0) {

		/* Free the memory/core tracker instance. */
		freeMemoryCoreTrack(PQUEMGR->RatioTrackers[ratioindex]);
		PQUEMGR->RatioTrackers[ratioindex] = NULL;

		/*
		 * The top one is to be deleted, this is easy, we just adjust the counter
		 * and leave the value dirty. Because, when new ratio is added to reuse
		 * the same slots, the value is reset to zero. Check addResourceQueueRatio()
		 */
		if ( ratioindex == PQUEMGR->RatioCount - 1 ) {
			removeHASHTABLENode(&(PQUEMGR->RatioIndex), TYPCONVERT(void *, ratio));
			PQUEMGR->RatioCount--;
		}
		/*
		 * If another one is to be deleted, the idea is to copy the counters of
		 * the top one to the slots to be deleted.
		 */
		else{
			int top = PQUEMGR->RatioCount - 1;

			/* Change the tracker array */
			PQUEMGR->RatioTrackers[ratioindex] = PQUEMGR->RatioTrackers[top];

			/* Change the hash table. */
			setHASHTABLENode(&(PQUEMGR->RatioIndex),
							 TYPCONVERT(void *, PQUEMGR->RatioReverseIndex[top]),
							 TYPCONVERT(void *, ratioindex),
							 false);

			removeHASHTABLENode(&(PQUEMGR->RatioIndex),
							    TYPCONVERT(void *, ratio));

			/* Change the reverse index. */
			PQUEMGR->RatioReverseIndex[ratioindex] = PQUEMGR->RatioReverseIndex[top];
			PQUEMGR->RatioCount--;
		}

		elog(RMLOG, "HAWQ RM :: Removed ratio %d MBPCORE", ratio);
	}

	track->trackedMemCoreRatio = false;
}

void generateQueueReport( int queid, char *buff, int buffsize )
{
	DynResourceQueue 	  que 		= NULL;
	DynResourceQueueTrack quetrack 	= getQueueTrackByQueueOID(queid);

	Assert( quetrack != NULL );
	que = quetrack->QueueInfo;

	if ( RESQUEUE_IS_PERCENT(que) )
	{
		sprintf(buff,
				"\n"
				"RESQUEUE:ID="INT64_FORMAT",Name=%s,"
				"PARENT="INT64_FORMAT","
				"LIMIT(MEM=%lf%%,CORE=%lf%%),"
				"RATIO=%d MBPCORE,"
				"INUSE(%d MB, %lf CORE),"
				"CONN=%d,"
				"INQUEUE=%d.\n",
				que->OID, que->Name,
				que->ParentOID,
				que->ClusterMemoryPer,
				que->ClusterVCorePer,
				quetrack->MemCoreRatio,
				quetrack->TotalUsed.MemoryMB,
				quetrack->TotalUsed.Core,
				quetrack->CurConnCounter,
				quetrack->QueryResRequests.NodeCount);
	}
	else {
		sprintf(buff,
				"\n"
				"RESQUEUE:ID="INT64_FORMAT",Name=%s,"
				"PARENT="INT64_FORMAT","
				"LIMIT(MEM=%d MB,CORE=%f CORE),"
				"RATIO=%d MBPCORE,"
				"INUSE(%d MB, %lf CORE),"
				"CONN=%d,"
				"INQUEUE=%d.\n",
				que->OID, que->Name,
				que->ParentOID,
				que->ClusterMemoryMB,
				que->ClusterVCore,
				quetrack->MemCoreRatio,
				quetrack->TotalUsed.MemoryMB,
				quetrack->TotalUsed.Core,
				quetrack->CurConnCounter,
				quetrack->QueryResRequests.NodeCount);
	}
}

void generateUserReport( const char   *userid,
						 int 		   useridlen,
						 char 		  *buff,
						 int 		   buffsize)
{
	UserInfo 	userinfo	= NULL;
	bool		exist		= false;

	userinfo = getUserByUserName(userid, useridlen, &exist);

	if ( !exist ) {
		sprintf(buff, "NULL USER.\n");
	}
	else {
		Assert(userinfo != NULL);
		sprintf(buff, "USER:ID=%s,QUEUEID=" INT64_FORMAT ",ISSUPERUSER=%s\n",
					  userinfo->Name,
					  userinfo->QueueOID,
					  userinfo->isSuperUser?"YES":"NO");
	}
}

/**
 * Register and check the parallel limitation.
 *
 * conntrack[in] 		: The result will be saved in this connection track.
 *
 * Return:
 * 	FUNC_RETURN_OK				: Everything is ok.
 * 	RESQUEMGR_NO_USERID			: Can not find the user.
 * 	RESQUEMGR_NO_ASSIGNEDQUEUE	: No assigned queue for current user.
 * 	RESQUEMGR_PARALLEL_FULL		: Can not accept more connection.
 * 	RESQUEMGR_INTERNAL_ERROR	: HAWQ RM internal error.
 *
 * The progress attribute is updated.
 *
 * NOTE: In order to facilitate test automation, currently all undefined users
 * 	 	 are assigned to 'default' queue.
 */
int registerConnectionByUserID(ConnectionTrack  conntrack,
							   char			   *errorbuf,
							   int				errorbufsize)
{
	int 					res 		  = FUNC_RETURN_OK;
	UserInfo 				userinfo 	  = NULL;
	DynResourceQueueTrack 	queuetrack 	  = NULL;
	bool					exist		  = false;

	Assert( conntrack != NULL );
	Assert( conntrack->Progress == CONN_PP_ESTABLISHED );

	/* Check if the user exists and save reference to corresponding user. */
	userinfo = getUserByUserName(conntrack->UserID,
							     strlen(conntrack->UserID),
							     &exist);
	if ( exist )
	{
		/* Mark the user is in use in some connections.*/
		userinfo->isInUse++;
		/* Get the queue, and check if the parallel limit is achieved. */
		queuetrack = getQueueTrackByQueueOID(userinfo->QueueOID);
	}
	else
	{
		snprintf(errorbuf, errorbufsize,
				 "role %s is not defined for registering connection",
				 conntrack->UserID);
		elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
		res = RESQUEMGR_NO_USERID;
		goto exit;
	}

	Assert(queuetrack != NULL);

	queuetrack->CurConnCounter++;

	conntrack->User 		= (void *)userinfo;
	conntrack->QueueTrack 	= (void *)queuetrack;
	conntrack->RegisterTime = gettime_microsec();
	conntrack->LastActTime  = conntrack->RegisterTime;

	transformConnectionTrackProgress(conntrack, CONN_PP_REGISTER_DONE);

	elog(DEBUG3, "Resource queue %s has %d connections after registering "
				 "connection.",
				 queuetrack->QueueInfo->Name,
				 queuetrack->CurConnCounter);
exit:
	if ( res != FUNC_RETURN_OK )
	{
		conntrack->User 		= NULL;
		conntrack->QueueTrack	= NULL;
		transformConnectionTrackProgress(conntrack, CONN_PP_REGISTER_FAIL);
	}
	return res;
}


/**
 * Return one connection to resource queue.
 */
void returnConnectionToQueue(ConnectionTrack conntrack, bool istimeout)
{
	DynResourceQueueTrack track = (DynResourceQueueTrack)(conntrack->QueueTrack);
	if ( !istimeout )
	{
		transformConnectionTrackProgress(conntrack, CONN_PP_ESTABLISHED);
	}
	else
	{
		transformConnectionTrackProgress(conntrack, CONN_PP_TIMEOUT_FAIL);
	}

	elog(DEBUG3, "Resource queue %s has %d connections before returning "
				 "connection ConnID %d.",
				 track->QueueInfo->Name,
				 track->CurConnCounter,
				 conntrack->ConnID);
	track->CurConnCounter--;
	if ( track->CurConnCounter == 0 )
	{
		elog(RMLOG, "Resource queue %s becomes idle.", track->QueueInfo->Name);
		track->isBusy = false;
		refreshMemoryCoreRatioLimits();
		refreshMemoryCoreRatioWaterMark();
	}
}

/*
 * Cancel one queued resource allocation request.
 */
void cancelResourceAllocRequest(ConnectionTrack  conntrack,
								char 			*errorbuf,
								bool			 generror)
{
	Assert(conntrack->Progress == CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT);

	DynResourceQueueTrack queuetrack = (DynResourceQueueTrack)(conntrack->QueueTrack);

	/* Remove from queueing list.  */
	DQUEUE_LOOP_BEGIN(&(queuetrack->QueryResRequests), iter, ConnectionTrack, track)
		if ( track == conntrack )
		{
			removeDQueueNode(&(queuetrack->QueryResRequests), iter);
			break;
		}
	DQUEUE_LOOP_END

	/* Unlock session in deadlock */
	unlockSessionResource(&(queuetrack->DLDetector), conntrack->SessionID);

	if (generror)
	{
		buildAcquireResourceErrorResponseAndSend(conntrack,
												 RESQUEMGR_NORESOURCE_TIMEOUT,
												 errorbuf);
	}
}

/* Acquire resource from queue. */
int acquireResourceFromResQueMgr(ConnectionTrack  conntrack,
								 char 			 *errorbuf,
								 int 			  errorbufsize)
{
	int						res			= FUNC_RETURN_OK;
	DynResourceQueueTrack	queuetrack	= conntrack->QueueTrack;

	elog(RMLOG, "ConnID %d. Expect query resource for session "INT64_FORMAT,
			    conntrack->ConnID,
			    conntrack->SessionID);

	/* Call quota logic to make decision of resource for current query. */
	res = computeQueryQuota(conntrack, errorbuf, errorbufsize);

	if ( res == FUNC_RETURN_OK )
	{
		if ( conntrack->StatNVSeg == 0 )
		{
			/*------------------------------------------------------------------
			 * Adjust the number of virtual segments again based on
			 * NVSEG_*_LIMITs and NVSEG_*_LIMIT_PERSEGs. This adjustment must
			 * succeed.
			 *------------------------------------------------------------------
			 */
			res = adjustResourceExpectsByQueueNVSegLimits(conntrack,
														  errorbuf,
														  errorbufsize);
			if ( res == FUNC_RETURN_OK )
			{
				elog(LOG, "ConnID %d. Expect query resource (%d MB, %lf CORE) "
						  "x %d ( MIN %d ) resource after adjusting based on "
						  "queue NVSEG limits.",
						  conntrack->ConnID,
						  conntrack->SegMemoryMB,
						  conntrack->SegCore,
						  conntrack->SegNum,
						  conntrack->SegNumMin);
			}
			else
			{
				elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
				transformConnectionTrackProgress(conntrack,
												 CONN_PP_RESOURCE_ACQUIRE_FAIL);
				return res;
			}
		}

		/* Add request to the resource queue and return. */
		addQueryResourceRequestToQueue(queuetrack, conntrack);
		transformConnectionTrackProgress(conntrack,
										 CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT);
		/* Exit on succeeding in adding request to the queue. */
	}
	else
	{
		elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
		transformConnectionTrackProgress(conntrack, CONN_PP_RESOURCE_ACQUIRE_FAIL);
	}
	return res;
}

int acquireResourceQuotaFromResQueMgr(ConnectionTrack	conntrack,
									  char			   *errorbuf,
									  int				errorbufsize)
{
	int 					res 		= FUNC_RETURN_OK;
	DynResourceQueueTrack   queuetrack	= conntrack->QueueTrack;
	UserInfo 				userinfo 	= NULL;
	bool					exist		= false;
	/* Check if the user exists and save reference to corresponding user. */
	userinfo = getUserByUserName(conntrack->UserID,
							     strlen(conntrack->UserID),
							     &exist);
	Assert(exist && userinfo != NULL);

	/* Get the queue, and check if the parallel limit is achieved. */
	queuetrack = getQueueTrackByQueueOID(userinfo->QueueOID);
	Assert( queuetrack != NULL );

	conntrack->QueueTrack = queuetrack;
	conntrack->QueueID	  = queuetrack->QueueInfo->OID;

	/* Call quota logic to make decision of resource for current query. */
	res = computeQueryQuota(conntrack, errorbuf, errorbufsize);

	if ( res == FUNC_RETURN_OK )
	{
		if ( conntrack->StatNVSeg == 0 )
		{
			/*------------------------------------------------------------------
			 * Adjust the number of virtual segments again based on
			 * NVSEG_*_LIMITs and NVSEG_*_LIMIT_PERSEGs. This adjustment must
			 * succeed.
			 *------------------------------------------------------------------
			 */
			res = adjustResourceExpectsByQueueNVSegLimits(conntrack,
														  errorbuf,
														  errorbufsize);

			if ( res == FUNC_RETURN_OK )
			{
				elog(LOG, "ConnID %d. Query resource quota expects (%d MB, %lf CORE) "
						  "x %d ( MIN %d ) resource after adjusting based on queue "
						  "NVSEG limits.",
						  conntrack->ConnID,
						  conntrack->SegMemoryMB,
						  conntrack->SegCore,
						  conntrack->SegNum,
						  conntrack->SegNumMin);
			}
			else
			{
				elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
				return res;
			}
		}
	}
	else
	{
		elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
	}
	return res;
}

int adjustResourceExpectsByQueueNVSegLimits(ConnectionTrack	 conntrack,
											char			*errorbuf,
											int			 	 errorbufsize)
{
	DynResourceQueueTrack queuetrack = conntrack->QueueTrack;
	DynResourceQueue	  queue		 = queuetrack->QueueInfo;
	bool				  adjusted	 = false;

	if ( queue->NVSegLowerLimit > MINIMUM_RESQUEUE_NVSEG_LOWER_LIMIT_N )
	{
		/*----------------------------------------------------------------------
		 * Check if there are some conflicts between NVSEG_LOWER_LIMIT and
		 * resource upper limits.
		 *----------------------------------------------------------------------
		 */
		if ( queue->NVSegLowerLimit > conntrack->VSegLimit )
		{
			snprintf(errorbuf, errorbufsize,
					 "queue %s's limit %s=%d is greater than maximum number of "
					 "virtual segments in cluster %d, check queue definition and "
					 "guc variable hawq_rm_nvseg_perquery_limit",
					 queue->Name,
					 RSQDDLAttrNames[RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT],
					 queue->NVSegLowerLimit,
					 conntrack->VSegLimit);
			return RESQUEMGR_WRONG_NVSEG_LIMIT_LOWER;
		}

		if ( conntrack->MinSegCountFixed != conntrack->MaxSegCountFixed )
		{
			int limit = conntrack->VSegLimitPerSeg * PRESPOOL->AvailNodeCount;
			if ( queue->NVSegLowerLimit > limit )
			{
				snprintf(errorbuf, errorbufsize,
						 "queue %s's limit %s=%d is greater than maximum number "
						 "of virtual segments in cluster %d, there are %d available "
						 "segments, each segment has %d maximum virtual segments, "
						 "check queue definition and guc variable "
						 "rm_nvseg_perquery_perseg_limit",
						 queue->Name,
						 RSQDDLAttrNames[RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT],
						 queue->NVSegLowerLimit,
						 limit,
						 PRESPOOL->AvailNodeCount,
						 conntrack->VSegLimitPerSeg);
				return RESQUEMGR_WRONG_NVSEG_LIMIT_LOWER;
			}
		}

		if ( queue->NVSegLowerLimit > queuetrack->ClusterSegNumberMax )
		{
			snprintf(errorbuf, errorbufsize,
					 "queue %s's limit %s=%d is greater than queue maximum "
					 "number of virtual segments %d, check queue definition",
					 queue->Name,
					 RSQDDLAttrNames[RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT],
					 queue->NVSegLowerLimit,
					 queuetrack->ClusterSegNumberMax);
			return RESQUEMGR_WRONG_NVSEG_LIMIT_LOWER;
		}


		if ( conntrack->SegNum >= queue->NVSegLowerLimit &&
			 conntrack->SegNumMin < queue->NVSegLowerLimit )
		{
			conntrack->SegNumMin = queue->NVSegLowerLimit;
			adjusted =true;
			elog(RMLOG, "ConnID %d. Minimum vseg number adjusted to %d",
						conntrack->ConnID,
						conntrack->SegNumMin);
		}
	}
	else if ( queue->NVSegLowerLimitPerSeg >
			  MINIMUM_RESQUEUE_NVSEG_LOWER_PERSEG_LIMIT_N )
	{
		/*----------------------------------------------------------------------
		 * Check if there are some conflicts between NVSEG_LOWER_PERSEG_LIMIT
		 * and resource upper limits.
		 *----------------------------------------------------------------------
		 */
		int minnvseg = ceil(queue->NVSegLowerLimitPerSeg * PRESPOOL->AvailNodeCount);
		if ( minnvseg > conntrack->VSegLimit )
		{
			snprintf(errorbuf, errorbufsize,
					 "queue %s's limit %s=%lf requires minimum %d virtual "
					 "segments in cluster having %d available segments, it is "
					 "greater than maximum number of virtual segments in cluster "
					 "%d, check queue definition and guc variable "
					 "hawq_rm_nvseg_perquery_perseg_limit",
					 queue->Name,
					 RSQDDLAttrNames[RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT_PERSEG],
					 queue->NVSegLowerLimitPerSeg,
					 minnvseg,
					 PRESPOOL->AvailNodeCount,
					 conntrack->VSegLimit);
			return RESQUEMGR_WRONG_NVSEG_PERSEG_LIMIT_LOWER;
		}

		if ( conntrack->MinSegCountFixed != conntrack->MaxSegCountFixed )
		{
			int limit = conntrack->VSegLimitPerSeg * PRESPOOL->AvailNodeCount;
			if ( minnvseg > limit )
			{
				snprintf(errorbuf, errorbufsize,
						 "queue %s's limit %s=%lf requires minimum %d virtual "
						 "segments in cluster having %d available segments, it "
						 "is greater than maximum number of virtual segments in "
						 "cluster %d, each segment has %d maximum virtual "
						 "segments, check queue definition and guc variable "
						 "hawq_rm_nvseg_perquery_perseg_limit",
						 queue->Name,
						 RSQDDLAttrNames[RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT_PERSEG],
						 queue->NVSegLowerLimitPerSeg,
						 minnvseg,
						 PRESPOOL->AvailNodeCount,
						 limit,
						 conntrack->VSegLimitPerSeg);
				return RESQUEMGR_WRONG_NVSEG_PERSEG_LIMIT_LOWER;
			}
		}

		if ( minnvseg > queuetrack->ClusterSegNumberMax )
		{
			snprintf(errorbuf, errorbufsize,
					 "queue %s's limit %s=%lf requires minimum %d virtual "
					 "segments in cluster having %d available segments, it is "
					 "greater than queue maximum number of virtual segments %d, "
					 "check queue definition",
					 queue->Name,
					 RSQDDLAttrNames[RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT_PERSEG],
					 queue->NVSegLowerLimitPerSeg,
					 minnvseg,
					 PRESPOOL->AvailNodeCount,
					 queuetrack->ClusterSegNumberMax);
			return RESQUEMGR_WRONG_NVSEG_PERSEG_LIMIT_LOWER;
		}

		if ( conntrack->SegNumMin < minnvseg )
		{
			conntrack->SegNumMin = minnvseg;
			adjusted =true;
			elog(RMLOG, "ConnID %d. Minimum vseg number adjusted to %d",
						conntrack->ConnID,
						conntrack->SegNumMin);
		}
	}

	if ( queue->NVSegUpperLimit > MINIMUM_RESQUEUE_NVSEG_UPPER_LIMIT_N )
	{
		if ( conntrack->SegNum > queue->NVSegUpperLimit )
		{
			conntrack->SegNum = queue->NVSegUpperLimit;
			adjusted =true;
			elog(RMLOG, "ConnID %d. Maximum vseg number adjusted to %d",
						conntrack->ConnID,
						conntrack->SegNum);
		}
	}
	else if ( queue->NVSegUpperLimitPerSeg >
			  MINIMUM_RESQUEUE_NVSEG_UPPER_PERSEG_LIMIT_N )
	{
		int maxnvseg = ceil(queuetrack->QueueInfo->NVSegUpperLimitPerSeg *
							PRESPOOL->AvailNodeCount);
		if ( conntrack->SegNum > maxnvseg )
		{
			conntrack->SegNum = maxnvseg;
			adjusted =true;
			elog(RMLOG, "ConnID %d. Maximum vseg number adjusted to %d",
						conntrack->ConnID,
						conntrack->SegNum);
		}
	}

	/*--------------------------------------------------------------------------
	 * Finally, we must ensure that upper limits limit the minimum resource
	 * quota. This means, the resource limits from NVSEG upper limits are always
	 * respected.
	 *--------------------------------------------------------------------------
	 */
	if ( conntrack->SegNumMin > conntrack->SegNum )
	{
		conntrack->SegNumMin = conntrack->SegNum;
		elog(RMLOG, "ConnID %d. Minimum vseg number is forced to be equal to "
					"maximum vseg number %d",
					conntrack->ConnID,
					conntrack->SegNumMin);
	}

	return FUNC_RETURN_OK;
}

/* Resource is returned from query to resource queue. */
int returnResourceToResQueMgr(ConnectionTrack conntrack)
{
	int res = FUNC_RETURN_OK;

	if ( !conntrack->isOld ) {

		/* return resource quota to resource queue. */
		returnAllocatedResourceToLeafQueue(
				conntrack->QueueTrack,
				conntrack->SegMemoryMB * conntrack->SegNumActual,
				conntrack->SegCore     * conntrack->SegNumActual);

		/* Minus resource usage by session. */
		if ( conntrack->SessionID >= 0 ) {
			DynResourceQueueTrack quetrack = (DynResourceQueueTrack)
											 (conntrack->QueueTrack);
			minusSessionInUseResource(&(quetrack->DLDetector),
									   conntrack->SessionID,
									   conntrack->SegMemoryMB * conntrack->SegNumActual,
									   conntrack->SegCore     * conntrack->SegNumActual);
		}
	}

	((DynResourceQueueTrack)(conntrack->QueueTrack))->NumOfRunningQueries--;

	/* return allocated resource to resource pool. */
	returnResourceToResourcePool(conntrack->SegMemoryMB,
								 conntrack->SegCore,
								 conntrack->SegIOBytes,
								 conntrack->SliceSize,
								 &(conntrack->Resource),
								 conntrack->isOld);

	transformConnectionTrackProgress(conntrack, CONN_PP_REGISTER_DONE);

	/* Some resource is returned. Try to dispatch resource to queries. */
	PQUEMGR->toRunQueryDispatch = true;

	validateResourcePoolStatus(true);

	return res;
}

void refreshActualMinGRMContainerPerSeg(void)
{
	/*--------------------------------------------------------------------------
	 * There are 3 limits should be considered, the actual water level is the
	 * least value of the 3 limits : resource queue normal capacity caused mean
	 * GRM container number, minimum value of all segments' maximum GRM container
	 * numbers, user setting saved in guc.
	 *
	 *--------------------------------------------------------------------------
	 */

	/* STEP 1. go through each segment to get segment maximum capacity. */
	int minctncount = INT32_MAX;
	int normalctncount = INT32_MAX;
	if ( DRMGlobalInstance->ImpType != NONE_HAWQ2 )
	{
		List 	 *allsegres = NULL;
		ListCell *cell		= NULL;
		getAllPAIRRefIntoList(&(PRESPOOL->Segments), &allsegres);

		foreach(cell, allsegres)
		{
			SegResource segres = (SegResource)(((PAIR)lfirst(cell))->Value);
			if (IS_SEGSTAT_FTSAVAILABLE(segres->Stat))
			{
				continue;
			}

			if ( segres->Stat->GRMTotalCore < minctncount )
			{
				minctncount = segres->Stat->GRMTotalCore;
			}
		}
		freePAIRRefList(&(PRESPOOL->Segments), &allsegres);

		elog(RMLOG, "Resource manager finds minimum global resource manager "
					"container count can contained by all segments is %d",
					minctncount);

		/* STEP 2. check the queue normal capacity introduced water level. */
		if ( PRESPOOL->AvailNodeCount > 0 &&
			 PQUEMGR->GRMQueueCapacity > 0 &&
			 PRESPOOL->GRMTotalHavingNoHAWQNode.Core > 0 )
		{
			normalctncount = trunc(PRESPOOL->GRMTotalHavingNoHAWQNode.Core *
								   PQUEMGR->GRMQueueCapacity /
								   PRESPOOL->AvailNodeCount);

			elog(RMLOG, "Resource manager calculates normal global resource "
						"manager container count based on target queue capacity "
						"is %d",
						normalctncount);
		}
	}

	/* STEP 3. Get final water level result. */
	int oldval = PQUEMGR->ActualMinGRMContainerPerSeg;
	int newval = minctncount < normalctncount ? minctncount : normalctncount;
	newval = newval < rm_min_resource_perseg ? newval : rm_min_resource_perseg;

	if ( newval != oldval )
	{
		elog(WARNING, "Resource manager adjusts minimum global resource manager "
					  "container count in each segment from %d to %d.",
					  oldval,
					  newval);
	}
	PQUEMGR->ActualMinGRMContainerPerSeg = newval;
}

void refreshResourceQueueCapacity(bool queuechanged)
{
	static char errorbuf[ERRORMESSAGE_SIZE];
	List *qhavingshadow = NULL;

	/*
	 * If the cluster level memory to core ratio is not fixed based on FTS heart-
	 * beat or GRM cluster report, try to fix it firstly, if unfortunately, this
	 * ratio cannot be fixed, skip refreshing queue capacity temporarily.
	 *
	 * NOTE, once this ratio is fixed, it is not changed again until restart
	 * master.
	 *
	 */
	if ( PRESPOOL->ClusterMemoryCoreRatio <= 0 )
	{
		fixClusterMemoryCoreRatio();
		if ( PRESPOOL->ClusterMemoryCoreRatio <= 0 )
		{
			return;
		}
	}

	/* STEP 1. Build all necessary shadow resource queue track instances. */
	buildQueueTrackShadows(PQUEMGR->RootTrack, &qhavingshadow);

	/* STEP 2. Refresh resource queue capacities. */
	refreshResourceQueuePercentageCapacity(queuechanged);

	/* STEP 3. Rebuild queued resource requests. */
	rebuildAllResourceQueueTrackDynamicStatusInShadow(qhavingshadow,
													  queuechanged,
													  errorbuf,
													  sizeof(errorbuf));

	/* STEP 4. Apply changes from resource queue shadows. */
	applyResourceQueueTrackChangesFromShadows(qhavingshadow);

	/* STEP 5. Clean up. */
	cleanupQueueTrackShadows(&qhavingshadow);
}

/* Refresh actual resource queue capacity. */
void refreshResourceQueuePercentageCapacity(bool queuechanged)
{
	/*
	 * Decide The actual capacity. This is necessary because there maybe some
	 * physical machines having different ratio capacity.
	 */
	uint32_t mem  = 0;
	uint32_t core = 0;

	if ( PQUEMGR->RootTrack != NULL )
	{
		if ( DRMGlobalInstance->ImpType == YARN_LIBYARN )
		{
			mem  = PRESPOOL->GRMTotalHavingNoHAWQNode.MemoryMB *
				   PQUEMGR->GRMQueueMaxCapacity;
			core = PRESPOOL->GRMTotalHavingNoHAWQNode.Core     *
				   PQUEMGR->GRMQueueMaxCapacity;

		}
		else if ( DRMGlobalInstance->ImpType == NONE_HAWQ2 )
		{
			mem  = PRESPOOL->FTSTotal.MemoryMB;
			core = PRESPOOL->FTSTotal.Core;
		}
		else
		{
			Assert(false);
		}
	}
	else
	{
		return;
	}

	/*
	 * If we use global resource manager to manage resource, the total capacity
	 * might not follow the cluster memory to core ratio.
	 */
	adjustMemoryCoreValue(&mem, &core);

	elog(DEBUG3, "HAWQ RM :: Use cluster (%d MB, %d CORE) resources as whole.",
				mem, core);

	refreshResourceQueuePercentageCapacityInternal(mem, core, queuechanged);

	/*
	 * After freshing resource queue capacity, it is necessary to try to dispatch
	 * resource to queries.
	 */
	PQUEMGR->toRunQueryDispatch = true;
}

void refreshMemoryCoreRatioLevelUsage(uint64_t curmicrosec)
{
	ListCell *cell = NULL;

	for( int i = 0 ; i < PQUEMGR->RatioCount ; ++i ) {

		DynMemoryCoreRatioTrack mctrack = PQUEMGR->RatioTrackers[i];

		resetResourceBundleData(&(mctrack->TotalUsed),    0, 0.0, mctrack->TotalUsed.Ratio);
		resetResourceBundleData(&(mctrack->TotalRequest), 0, 0.0, mctrack->TotalUsed.Ratio);

		foreach(cell, mctrack->QueueTrackers)
		{
			DynResourceQueueTrack track = lfirst(cell);
			addResourceBundleData(&(mctrack->TotalUsed),
								  track->TotalUsed.MemoryMB,
								  track->TotalUsed.Core);
			if (track->TotalRequest.MemoryMB > track->ClusterMemoryMaxMB)
			{
				addResourceBundleData(&(mctrack->TotalRequest),
								  	  track->ClusterMemoryMaxMB,
									  track->ClusterVCoreMax);
			}
			else
			{
				addResourceBundleData(&(mctrack->TotalRequest),
									  track->TotalRequest.MemoryMB,
									  track->TotalRequest.Core);
			}
		}

		if ( mctrack->TotalRequest.MemoryMB > mctrack->ClusterMemoryMaxMB )
		{
			mctrack->TotalRequest.MemoryMB = mctrack->ClusterMemoryMaxMB;
		}

		markMemoryCoreRatioWaterMark(&(PQUEMGR->RatioWaterMarks[i]),
									 curmicrosec,
									 mctrack->TotalUsed.MemoryMB,
									 mctrack->TotalUsed.Core);
	}
}

void markMemoryCoreRatioWaterMark(DQueue 		marks,
								  uint64_t 		curmicrosec,
								  int32_t 		memmb,
								  double 		core)
{
	/* Each mark instance records the maximum usage in one sencond. */
	uint64_t 					cursec   = curmicrosec / 1000000;
	DynMemoryCoreRatioWaterMark lastmark = NULL;
	int32_t						oldmarkmem = 0;
	double 						oldmarkcore = 0;

	elog(DEBUG5, "Resource water mark candidate (%d MB, %lf CORE) "UINT64_FORMAT,
			     memmb,
			     core,
			     cursec);

	if ( marks->NodeCount > 0 ) {
		DynMemoryCoreRatioWaterMark firstmark =
				(DynMemoryCoreRatioWaterMark)
				getDQueueContainerData(getDQueueContainerHead(marks));
		oldmarkmem  = firstmark->ClusterMemoryMB;
		oldmarkcore = firstmark->ClusterVCore;
		elog(DEBUG5, "Resource water mark old (%d MB, %lf CORE)",
				     oldmarkmem,
				     oldmarkcore);
	}

	if ( marks->NodeCount > 0 ) {
		lastmark = (DynMemoryCoreRatioWaterMark)
				   getDQueueContainerData(getDQueueContainerTail(marks));

		if ( lastmark->LastRecordTime == cursec ) {
			lastmark->ClusterMemoryMB = lastmark->ClusterMemoryMB > memmb ?
									    lastmark->ClusterMemoryMB :
									    memmb;
			lastmark->ClusterVCore    = lastmark->ClusterVCore > core ?
					                    lastmark->ClusterVCore :
									    core;
			/* Get the last mark cut. We will process the left marks now. */
			removeDQueueTailNode(marks);
		}
		else {
			lastmark = NULL;
		}
	}

	if ( lastmark == NULL) {
		lastmark = rm_palloc0(PCONTEXT, sizeof(DynMemoryCoreRatioWaterMarkData));
		lastmark->LastRecordTime  = cursec;
		lastmark->ClusterMemoryMB = memmb;
		lastmark->ClusterVCore    = core;
	}

	elog(DEBUG5, "Resource water mark list size %d before timeout old marks.",
			     marks->NodeCount);

	/* Check if we should remove some marks from the head. */
	while( marks->NodeCount > 0 ) {
		DynMemoryCoreRatioWaterMark firstmark =
				(DynMemoryCoreRatioWaterMark)
				getDQueueContainerData(getDQueueContainerHead(marks));
		if ( lastmark->LastRecordTime - firstmark->LastRecordTime > rm_resource_timeout ) {
			removeDQueueHeadNode(marks);
			rm_pfree(PCONTEXT, firstmark);
		}
		else {
			break;
		}
	}

	elog(DEBUG5, "Resource water mark list size %d after timeout old marks.",
			     marks->NodeCount);

	/* Check if we can skip some marks before the last one (the one we have cut). */
	while( marks->NodeCount > 0 ) {
		DynMemoryCoreRatioWaterMark last2mark =
				(DynMemoryCoreRatioWaterMark)
				getDQueueContainerData(getDQueueContainerTail(marks));
		if ( last2mark->ClusterMemoryMB <= lastmark->ClusterMemoryMB ) {
			removeDQueueTailNode(marks);
			rm_pfree(PCONTEXT, last2mark);
		}
		else {
			break;
		}
	}

	elog(DEBUG5, "Resource water mark list size %d after remove low marks.",
			     marks->NodeCount);

	/* Add the last one back to the tail. */
	insertDQueueTailNode(marks, lastmark);

	Assert(marks->NodeCount > 0);
	{
		DynMemoryCoreRatioWaterMark firstmark =
				(DynMemoryCoreRatioWaterMark)
				getDQueueContainerData(getDQueueContainerHead(marks));
		if ( firstmark->ClusterMemoryMB != oldmarkmem ) {
			elog(LOG, "Resource water mark changes from (%d MB, %lf CORE) to "
					  "(%d MB, %lf CORE)",
					  oldmarkmem,
					  oldmarkcore,
					  firstmark->ClusterMemoryMB,
					  firstmark->ClusterVCore);
		}
	}
}

#define ELOG_WARNING_ERRORMESSAGE_PARSEUSERATTR(errorbuf)					   \
		elog(WARNING, "Resource queue cannot parse role attribute, %s",  	   \
					  (errorbuf));

/**
 * This function parses the attributes and translate into UserInfo structure's
 * attributes. This functions does not generate logs higher than WARNING, the
 * concrete error is also saved in error buffer to make the caller able to pass
 * back the message to remote process.
 */
int parseUserAttributes( List 	 	*attributes,
						 UserInfo 	 user,
						 char		*errorbuf,
						 int		 errorbufsize)
{
	int 					res 		 = FUNC_RETURN_OK;
	int						res2		 = FUNC_RETURN_OK;
	int 					attrindex 	 = -1;
	DynResourceQueueTrack 	track		 = NULL;
	Oid 					useroid 	 = InvalidOid;
	Oid						queueoid	 = InvalidOid;
	bool					exist		 = false;
	int64_t					queueoid64	 = -1;

	/* Initialize attributes. */
	user->isSuperUser 	= false;
	user->QueueOID 		= -1;

	ListCell *cell = NULL;
	foreach(cell, attributes)
	{
		KVProperty value = lfirst(cell);

		SimpStringPtr attrname  = &(value->Key);
		SimpStringPtr attrvalue = &(value->Val);

		attrindex = getUSRTBLAttributeNameIndex(attrname);

		if ( SimpleStringEmpty(attrvalue) )
		{
			if ( SimpleStringEmpty(attrvalue) )
			{
				elog(LOG, "No value for attribute [%s].", attrname->Str);
				continue;
			}
		}

		if ( attrindex == -1 )
		{
			res = RESQUEMGR_WRONG_ATTRNAME;
			snprintf(errorbuf, errorbufsize,
					 "cannot recognize resource queue attribute %s",
					 attrname->Str);
			ELOG_WARNING_ERRORMESSAGE_PARSEUSERATTR(errorbuf)
			return res;
		}

		switch(attrindex)
		{
		case USR_TBL_ATTR_OID:
			/* The oid is expected to be unique. */
			res2 = SimpleStringToOid(attrvalue, &useroid);
			getUserByUserOID(useroid, &exist);
			if ( exist )
			{
				res = RESQUEMGR_DUPLICATE_USERID;
				snprintf(errorbuf, errorbufsize, "duplicate user oid %s", attrvalue->Str);
				ELOG_WARNING_ERRORMESSAGE_PARSEUSERATTR(errorbuf)
				return res;
			}
			user->OID = useroid;
			break;

		case USR_TBL_ATTR_NAME:

			/* The user name must be unique. */
			getUserByUserName(attrvalue->Str, attrvalue->Len, &exist);
			if ( exist )
			{
				res = RESQUEMGR_DUPLICATE_USERID;
				snprintf(errorbuf, errorbufsize, "duplicate user name %s", attrvalue->Str);
				ELOG_WARNING_ERRORMESSAGE_PARSEUSERATTR(errorbuf)
				return res;
			}

			/* Set user name value. */
			strncpy(user->Name, attrvalue->Str, attrvalue->Len);
			break;

		case USR_TBL_ATTR_TARGET_QUEUE:

			res2 = SimpleStringToOid(attrvalue, &queueoid);
			if ( res2 != FUNC_RETURN_OK )
			{
				res = RESQUEMGR_WRONG_ATTR;
				snprintf(errorbuf, errorbufsize,
						 "wrong target resource queue oid %s",
						 attrvalue->Str);
				ELOG_WARNING_ERRORMESSAGE_PARSEUSERATTR(errorbuf)
				return res;
			}
			/* The target queue must exist. */
			queueoid64 = queueoid;
			track = getQueueTrackByQueueOID(queueoid64);
			if ( track == NULL )
			{
				res = RESQUEMGR_WRONG_TARGET_QUEUE;
				snprintf(errorbuf, errorbufsize,
						 "cannot find target resource queue %s",
						 attrvalue->Str);
				ELOG_WARNING_ERRORMESSAGE_PARSEUSERATTR(errorbuf)
				return res;
			}

			/* Set value. */
			user->QueueOID = track->QueueInfo->OID;

			break;
		case USR_TBL_ATTR_PRIORITY:
			break;
		case USR_TBL_ATTR_IS_SUPERUSER:
		{
			bool issuper = false;
			res2 = SimpleStringToBool(attrvalue,&issuper);
			if ( res2 != FUNC_RETURN_OK )
			{
				res = RESQUEMGR_WRONG_ATTR;
				snprintf(errorbuf, errorbufsize,
						 "Wrong user issuper setting %s",
						 attrvalue->Str);
				ELOG_WARNING_ERRORMESSAGE_PARSEUSERATTR(errorbuf)
				return res;
			}
			user->isSuperUser = issuper ? 1 : 0;
			break;
		}
		default:
			Assert(0);
		}
	}

	return FUNC_RETURN_OK;
}

int checkUserAttributes( UserInfo user, char *errorbuf, int errorbufsize)
{
	if ( user->QueueOID == -1 ) {
		user->QueueOID = DEFAULTRESQUEUE_OID;
	}
	return FUNC_RETURN_OK;
}

/* Create one user. Expect always no error. */
void createUser(UserInfo userinfo)
{
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PQUEMGR->Users = lappend(PQUEMGR->Users, userinfo);
	MEMORY_CONTEXT_SWITCH_BACK

	if ( userinfo->OID > InvalidOid )
	{
		setUserIndexedByUserOID(userinfo);
	}
	setUserIndexedByUserName(userinfo);
}

void setUserIndexedByUserOID(UserInfo userinfo)
{
	SimpArray key;
	setSimpleArrayRef(&key, (void *)&(userinfo->OID), sizeof(int64_t));
	setHASHTABLENode(&(PQUEMGR->UsersIDIndex), &key, userinfo, false);
}

void setUserIndexedByUserName(UserInfo userinfo)
{
	SimpString key;
	setSimpleStringRefNoLen(&key, userinfo->Name);
	setHASHTABLENode(&(PQUEMGR->UsersNameIndex),
					 (void *)(&(key)),
					 userinfo,
					 false);
}

UserInfo getUserByUserName( const char *userid, int useridlen, bool *exist)
{
	PAIR						pair				= NULL;
	SimpString					key;

	/* Check if the user exists. */
	setSimpleStringRef(&key, (char *)userid, useridlen);
	pair = getHASHTABLENode(&(PQUEMGR->UsersNameIndex), &key);
	if( pair == NULL ) {
		*exist = false;
		return NULL;
	}

	*exist = true;
	return (UserInfo)(pair->Value);
}

UserInfo getUserByUserOID ( int64_t useroid, bool *exist)
{
	PAIR		pair	= NULL;
	SimpArray 	key;
	setSimpleArrayRef(&key, (void *)&useroid, sizeof(int64_t));
	pair = getHASHTABLENode(&(PQUEMGR->UsersIDIndex), &key);
	if ( pair == NULL )
	{
		*exist = false;
		return NULL;
	}
	*exist = true;
	return (UserInfo)(pair->Value);
}

bool hasUserAssignedToQueue(DynResourceQueue queue)
{
	ListCell *cell = NULL;
	foreach(cell, PQUEMGR->Users)
	{
		UserInfo user = lfirst(cell);
		if ( user->QueueOID == queue->OID )
		{
			return true;
		}
	}
	return false;
}

int dropUser(int64_t useroid, char* name)
{
	Assert(useroid != InvalidOid && name != NULL);

	ListCell *cell 	   = NULL;
	ListCell *prevcell = NULL;
	foreach(cell, PQUEMGR->Users)
	{
		UserInfo user = lfirst(cell);
		if(user->OID == useroid)
		{
			MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
			PQUEMGR->Users = list_delete_cell(PQUEMGR->Users, cell, prevcell);
			MEMORY_CONTEXT_SWITCH_BACK
			elog(LOG, "Resource manager finds user oid "INT64_FORMAT" and delete.",
					  useroid);

			SimpArray key1;
			setSimpleArrayRef(&key1, (void *)&useroid, sizeof(int64_t));
			int res = removeHASHTABLENode(&(PQUEMGR->UsersIDIndex), &key1);
			elog(DEBUG3, "Resource manager removed node from UsersIDIndex returns %d",
						 res);
			Assert(res == FUNC_RETURN_OK);

			SimpString key2;
			setSimpleStringRef(&key2, name, strlen(name));
			res = removeHASHTABLENode(&(PQUEMGR->UsersNameIndex), &key2);
			elog(DEBUG3, "Resource manager removed node from UsersNameIndex returns %d",
						 res);
			Assert(res == FUNC_RETURN_OK);
			return FUNC_RETURN_OK;
		}
		prevcell = cell;
	}

	return RESQUEMGR_NO_USERID;
}

void dispatchResourceToQueries(void)
{
	bool 		hasresourceallocated = false;
	bool 		hasrequest 		  	 = false;

	elog(DEBUG3, "Resource manager tries to dispatch resource to queries.");

	/*
	 *--------------------------------------------------------------------------
	 * STEP 1. Re-balance resource among different mem/core ratio trackers. After
	 * 		   this step, each mem/core ratio trackers process their own queues
	 * 		   only.
	 *
	 * 		   IN CURRENT VERSION. This is not implemented.
	 *--------------------------------------------------------------------------
	 */

	/*
	 * STEP 2. Decide how much resource are dispatched to each segment.
	 */
	for ( int i = 0 ; i < PQUEMGR->RatioCount ; ++i )
	{
		DQueueData  toallocqueues;
		initializeDQueue(&toallocqueues, PCONTEXT);
		DynMemoryCoreRatioTrack mctrack = PQUEMGR->RatioTrackers[i];

		/* Ignore the memory/core ratio 1) not in use. 2) no resource allocated. */
		if ( (mctrack->ClusterMemoryMaxMB == 0 || mctrack->ClusterVCoreMax == 0) ||
			 (mctrack->TotalAllocated.MemoryMB == 0 && mctrack->TotalAllocated.Core == 0) )
		{
			elog(DEBUG3, "Resource manager skipped memory core ratio index %d, "
						 "memory max limit %d MB, %lf CORE, "
						 "total allocated %d MB, %lf CORE",
						 i,
						 mctrack->ClusterMemoryMaxMB,
						 mctrack->ClusterVCoreMax,
						 mctrack->TotalAllocated.MemoryMB,
						 mctrack->TotalAllocated.Core);
			continue;
		}

		uint32_t	allmemory 		  = mctrack->TotalAllocated.MemoryMB;
		uint32_t	availmemory 	  = mctrack->TotalAllocated.MemoryMB;
		double		availcore		  = mctrack->TotalAllocated.Core;
		uint32_t	totalmemoryweight = 0;

		/*
		 * Count out over used resource queues. They will pause allocating resource
		 * until the water mark is lower than expected weight.
		 */
		ListCell *cell = NULL;
		foreach(cell, mctrack->QueueTrackers)
		{
			DynResourceQueueTrack track = (DynResourceQueueTrack)lfirst(cell);

			/* Reset queue resource expect status. */
			track->expectMoreResource = false;

			/* Ignore the queues not in use. */
			if ( !track->isBusy )
			{
				elog(DEBUG3, "Resource manager skips idle resource queue %s",
							 track->QueueInfo->Name);
				continue;
			}

			double expweight  = 1.0 * track->QueueInfo->ClusterMemoryMB /
							    	mctrack->ClusterMemory;
			double actweight  = allmemory == 0 ?
									0 :
									(1.0 * track->TotalUsed.MemoryMB / allmemory);

			/* If the queue is overusing resource, keep it. */
			if ( actweight > expweight ||
				 track->TotalUsed.MemoryMB > track->ClusterMemoryMaxMB )
			{
				resetResourceBundleData(&(track->TotalAllocated),
										track->TotalUsed.MemoryMB,
										track->TotalUsed.Core,
										track->TotalAllocated.Ratio);
				track->pauseAllocation = true;
				availmemory -= track->TotalUsed.MemoryMB;
				availcore   -= track->TotalUsed.Core;

				elog(DEBUG3, "Resource queue %s over uses resource with weight "
							 "%lf, expect weight %lf. Currently total used "
							 "(%d MB, %lf CORE). Allocation to queries is paused.",
							 track->QueueInfo->Name,
							 actweight,
							 expweight,
							 track->TotalUsed.MemoryMB,
							 track->TotalUsed.Core);

				/* We still need to handle the resource queue dead lock here. */
				detectAndDealWithDeadLock(track);

			}
			else
			{
				insertDQueueTailNode(&toallocqueues, track);
				track->pauseAllocation = false;

				totalmemoryweight += track->QueueInfo->ClusterMemoryMB;

				elog(DEBUG3, "Resource queue %s uses resource with weight "
							 "%lf, expect weight %lf. Currently total used "
							 "(%d MB, %lf CORE). To assign more resource.",
							 track->QueueInfo->Name,
							 actweight,
							 expweight,
							 track->TotalUsed.MemoryMB,
							 track->TotalUsed.Core);
			}
		}

		/* Assign resource to not over used resource queues. */
		elog(DEBUG3, "Reassignable resource is (%d MB, %lf CORE)",
					 availmemory,
					 availcore);

		/*
		 * Handle all the other queues to assign resource to queues. Remaining
		 * resource is dispatched to the chosen queues based on their resource
		 * weight.
		 */
		uint32_t leftmemory2 = availmemory;
		DQUEUE_LOOP_BEGIN(&toallocqueues, iter, DynResourceQueueTrack, track)
			double expweight  = 1.0 * track->QueueInfo->ClusterMemoryMB /
							    totalmemoryweight;

			uint32_t potentialmemuse =
				track->TotalUsed.MemoryMB + track->TotalRequest.MemoryMB >
					track->ClusterMemoryMaxMB ?
				track->ClusterMemoryMaxMB :
				track->TotalUsed.MemoryMB + track->TotalRequest.MemoryMB;

			double actweight2 = 1.0 * potentialmemuse  / availmemory;

			/*
			 * CASE 1. The queue acquire only a little resource that does not
			 * 		   exceed the target weight in this memory core ratio. We
			 * 		   exactly allocate the resource it wants.
			 */
			if ( actweight2 < expweight )
			{
				resetResourceBundleData(&(track->TotalAllocated),
										potentialmemuse,
										1.0 * potentialmemuse / track->MemCoreRatio,
										track->TotalAllocated.Ratio);
				leftmemory2 -= potentialmemuse;
				Assert(leftmemory2 >= 0);
				elog(DEBUG3, "Resource manager fully satisfies to resource queue "
							 "%s with (%d MB, %lf CORE) allocated.",
							 track->QueueInfo->Name,
							 track->TotalAllocated.MemoryMB,
							 track->TotalAllocated.Core);
			}

			/*
			 * CASE 2. The queue can only get partial requested resource.
			 */
			else
			{
				uint32_t allocmemory = trunc(expweight * availmemory);
				double   alloccore   = 1.0 * availmemory / track->MemCoreRatio;

				resetResourceBundleData(&(track->TotalAllocated),
										allocmemory,
										alloccore,
										track->TotalAllocated.Ratio);
				/* Mark that the queue needs more resource if possible. */
				track->expectMoreResource = true;

				leftmemory2 -= allocmemory;
				Assert(leftmemory2 >= 0);
				elog(DEBUG3, "Resource manager partially satisfies to resource "
							 "queue %s with (%d MB, %lf CORE) allocated.",
							 track->QueueInfo->Name,
							 track->TotalAllocated.MemoryMB,
							 track->TotalAllocated.Core);
			}

			elog(DEBUG3, "Resource manager allocates resource (%d MB, %lf CORE) "
						 "in queue %s.",
						 track->TotalAllocated.MemoryMB,
						 track->TotalAllocated.Core,
						 track->QueueInfo->Name);

			double evalcore = track->TotalAllocated.Core == 0 ?
							  VALIDATE_RESOURCE_BIAS :
							  track->TotalAllocated.Core * (1+VALIDATE_RESOURCE_BIAS);
			Assert(evalcore >= track->TotalUsed.Core);
		DQUEUE_LOOP_END

		/*
		 * Decide left resource. The resource is assigned to one in-use queue
		 * expecting more resource in a round-robin way. Add resource to as few
		 * queues as possible.
		 */
		if ( list_length(mctrack->QueueTrackers) > 0 && leftmemory2 > 0 )
		{
			/* In case the count of queues is less than before. */
			if ( mctrack->QueueIndexForLeftResource >= list_length(mctrack->QueueTrackers) )
			{
				mctrack->QueueIndexForLeftResource = 0;
			}

			ListCell *p = list_nth_cell(mctrack->QueueTrackers,
										mctrack->QueueIndexForLeftResource);
			DynResourceQueueTrack q = NULL;
			for ( int cq = 0 ; cq < list_length(mctrack->QueueTrackers) ; ++cq )
			{
				DynResourceQueueTrack tmpq = lfirst(p);
				if ( tmpq->expectMoreResource )
				{
					q = tmpq;
					if ( leftmemory2 + q->TotalAllocated.MemoryMB <= q->ClusterMemoryMaxMB)
					{
						elog(DEBUG3, "Resource manager allocates resource (%d MB, %lf CORE) "
									 "in queue %s.",
									 leftmemory2,
									 1.0 * leftmemory2 / q->MemCoreRatio,
									 q->QueueInfo->Name);

						addResourceBundleData(&(q->TotalAllocated),
										  	  leftmemory2,
											  1.0 * leftmemory2 / q->MemCoreRatio);
						leftmemory2 = 0;
						break;
					}
					else {
						uint32_t memorydelta = q->ClusterMemoryMaxMB - q->TotalAllocated.MemoryMB;

						elog(DEBUG3, "Resource manager allocates resource (%d MB, %lf CORE) "
									 "in queue %s.",
									 memorydelta,
									 1.0 * memorydelta / q->MemCoreRatio,
									 q->QueueInfo->Name);

						addResourceBundleData(&(q->TotalAllocated),
											  memorydelta,
											  1.0 * memorydelta / q->MemCoreRatio);
						leftmemory2 -= memorydelta;
					}
					break;
				}

				/* Try next queue in next iteration. */
				p = lnext(p);
				if ( p == NULL )
				{
					mctrack->QueueIndexForLeftResource = 0;
					p = list_head(mctrack->QueueTrackers);
				}
				else
				{
					mctrack->QueueIndexForLeftResource++;
				}
			}

			mctrack->QueueIndexForLeftResource++;
		}

		/*
		 * Dispatch resource to queries. We firstly handle the queues having
		 * resource fragment problem. Then the left queues.
		 */
		for ( int i = 0 ; i < toallocqueues.NodeCount ; ++i )
		{
			DynResourceQueueTrack track = (DynResourceQueueTrack)
										  (removeDQueueHeadNode(&toallocqueues));
			if ( !track->troubledByFragment )
			{
				insertDQueueTailNode(&toallocqueues, track);
				continue;
			}
			int oldreqnum = track->QueryResRequests.NodeCount;
			hasrequest = oldreqnum > 0;
			dispatchResourceToQueriesInOneQueue(track);
			int newreqnum = track->QueryResRequests.NodeCount;
			if ( newreqnum != oldreqnum )
			{
				hasresourceallocated = true;
			}
		}

		while( toallocqueues.NodeCount > 0 )
		{
			DynResourceQueueTrack track = (DynResourceQueueTrack)
										  (removeDQueueHeadNode(&toallocqueues));
			int oldreqnum = track->QueryResRequests.NodeCount;
			hasrequest = oldreqnum > 0;
			dispatchResourceToQueriesInOneQueue(track);
			int newreqnum = track->QueryResRequests.NodeCount;
			if ( newreqnum != oldreqnum )
			{
				hasresourceallocated = true;
			}
		}
		Assert(toallocqueues.NodeCount == 0);
		cleanDQueue(&toallocqueues);

	}

	PQUEMGR->toRunQueryDispatch = !hasrequest || hasresourceallocated;
	if ( !PQUEMGR->toRunQueryDispatch )
	{
		elog(DEBUG3, "Resource manager pauses allocating resource to query because of "
				     "lack of resource.");
	}
}

/*----------------------------------------------------------------------------*/
/*                    RESOURCE QUEUE MANAGER INTERNAL APIs                    */
/*----------------------------------------------------------------------------*/

/**
 * Create new resource queue tracker instance for one resource queue.
 */
DynResourceQueueTrack createDynResourceQueueTrack(DynResourceQueue queue)
{
	DynResourceQueueTrack newtrack =
		(DynResourceQueueTrack)rm_palloc0(PCONTEXT,
										  sizeof(DynResourceQueueTrackData));

	initializeDQueue(&(newtrack->QueryResRequests), PCONTEXT);

	newtrack->QueueInfo   		  = queue;
	newtrack->ParentTrack 		  = NULL;
	newtrack->ChildrenTracks 	  = NULL;
	newtrack->CurConnCounter 	  = 0;
	newtrack->RatioIndex          = -1;
	newtrack->ClusterSegNumber    = 0;
	newtrack->ClusterSegNumberMax = 0;
	newtrack->ClusterMemoryMaxMB  = 0;
	newtrack->ClusterVCoreMax     = 0;
	newtrack->ClusterMemoryActPer = 0;
	newtrack->ClusterMemoryMaxPer = 0;
	newtrack->ClusterVCoreActPer  = 0;
	newtrack->ClusterVCoreMaxPer  = 0;
	newtrack->trackedMemCoreRatio = false;
	newtrack->isBusy			  = false;
	newtrack->pauseAllocation	  = false;
	newtrack->troubledByFragment  = false;
	newtrack->NumOfRunningQueries = 0;

	resetResourceBundleData(&(newtrack->TotalAllocated), 0, 0.0, 0);
	resetResourceBundleData(&(newtrack->TotalRequest)  , 0, 0.0, 0);
	resetResourceBundleData(&(newtrack->TotalUsed)     , 0, 0.0, 0);

	initializeResqueueDeadLockDetector(&(newtrack->DLDetector), newtrack);
	return newtrack;
}

/**
 * Free one resource queue tracker instance, expect that this tracker has no
 * active connection information saved, the connection with the other tracker
 * instance has be cut.
 */
void shallowFreeResourceQueueTrack(DynResourceQueueTrack track)
{
	Assert( list_length(track->ChildrenTracks) == 0 );
	Assert( track->QueryResRequests.NodeCount == 0 );
	cleanDQueue(&(track->QueryResRequests));
	rm_pfree(PCONTEXT, track);
}

void deepFreeResourceQueueTrack(DynResourceQueueTrack track)
{
	Assert( list_length(track->ChildrenTracks) == 0 );
	Assert( track->ParentTrack == NULL );

	resetResourceDeadLockDetector(&(track->DLDetector));

	while(track->QueryResRequests.NodeCount > 0)
	{
		ConnectionTrack conn = (ConnectionTrack)
							   (removeDQueueHeadNode(&(track->QueryResRequests)));
		freeUsedConnectionTrack(conn);
	}

	rm_pfree(PCONTEXT, track->QueueInfo);
	shallowFreeResourceQueueTrack(track);
}

int getRSQTBLAttributeNameIndex(SimpStringPtr attrname)
{
	for ( int i = 0 ; i < RSQ_TBL_ATTR_COUNT ; ++i ) {
		if ( SimpleStringComp(attrname, RSQTBLAttrNames[i]) == 0 ) {
			return i;
		}
	}
	return -1;
}

int getRSQDDLAttributeNameIndex(SimpStringPtr attrname)
{
	for ( int i = 0 ; i < RSQ_DDL_ATTR_COUNT ; ++i )
	{
		if ( SimpleStringComp(attrname, RSQDDLAttrNames[i]) == 0 )
		{
			return i;
		}
	}
	return -1;
}

int getUSRTBLAttributeNameIndex(SimpStringPtr attrname)
{
	for ( int i = 0 ; i < USR_TBL_ATTR_COUNT ; ++i ) {
		if ( SimpleStringComp(attrname, USRTBLAttrNames[i]) == 0 ) {
			return i;
		}
	}
	return -1;
}

const char *getUSRTBLAttributeName(int attrindex)
{
	Assert( attrindex >= 0 && attrindex < USR_TBL_ATTR_COUNT );
	return USRTBLAttrNames[attrindex];
}

void resetResourceBundleData(ResourceBundle detail,
							 uint32_t mem,
							 double core,
							 uint32_t ratio)
{
	detail->MemoryMB = mem;
	detail->Core     = core;
	detail->Ratio	 = ratio;
}

void addResourceBundleData(ResourceBundle detail, int32_t mem, double core)
{
	detail->MemoryMB += mem;
	detail->Core += core;
}

void minusResourceBundleData(ResourceBundle detail, int32_t mem, double core)
{
	detail->MemoryMB -= mem;
	detail->Core -= core;
}

void resetResourceBundleDataByBundle(ResourceBundle detail, ResourceBundle source)
{
	resetResourceBundleData(detail, source->MemoryMB, source->Core, source->Ratio);
}

void addResourceBundleDataByBundle(ResourceBundle detail, ResourceBundle source)
{
	addResourceBundleData(detail, source->MemoryMB, source->Core);
}

void minusResourceBundleDataByBundle(ResourceBundle detail, ResourceBundle source)
{
	minusResourceBundleData(detail, source->MemoryMB, source->Core);
}

/**
 * Compute the query quota.
 */
int computeQueryQuota(ConnectionTrack conn, char *errorbuf, int errorbufsize)
{
	Assert( conn != NULL );
	Assert( conn->QueueTrack != NULL );

	int					  res		= FUNC_RETURN_OK;
	int					  policy	= 0;
	DynResourceQueueTrack track		= (DynResourceQueueTrack)(conn->QueueTrack);

	policy = track->QueueInfo->AllocatePolicy;
	Assert( policy >= 0 && policy < RSQ_ALLOCATION_POLICY_COUNT );

	/*--------------------------------------------------------------------------
	 * Get one segment resource quota. If statement level resource quota is not
	 * specified, the queue vseg resource quota is derived, otherwise, statement
	 * level resource quota. The resource memory/core ratio is not changed, thus
	 * code has to calculate the adjusted vcore quota for each vseg in case
	 * statement level resource quota is active.
	 *--------------------------------------------------------------------------
	 */
	if ( conn->StatNVSeg > 0 )
	{
		conn->SegMemoryMB = conn->StatVSegMemoryMB;
		conn->SegCore	  = track->QueueInfo->SegResourceQuotaVCore *
							conn->StatVSegMemoryMB /
							track->QueueInfo->SegResourceQuotaMemoryMB;
		conn->SegNum	  = conn->StatNVSeg;
		conn->SegNumMin	  = conn->StatNVSeg;

		/* Check if the resource capacity is more than the capacity of queue. */
		conn->SegNumEqual = ceil(1.0 * conn->SegMemoryMB * conn->SegNumMin /
						  	     track->QueueInfo->SegResourceQuotaMemoryMB);
		Assert( conn->SegNumEqual > 0 );
		if ( conn->SegNumEqual > track->ClusterSegNumberMax )
		{
			res = RESQUEMGR_TOO_MANY_FIXED_SEGNUM;
			snprintf(errorbuf, errorbufsize,
					 "statement resource quota %d MB x %d vseg exceeds resource "
					 "queue maximum capacity %d MB",
					 conn->StatVSegMemoryMB,
					 conn->StatNVSeg,
					 track->ClusterSegNumberMax *
					 track->QueueInfo->SegResourceQuotaMemoryMB);

			elog(WARNING, "ConnID %d. %s", conn->ConnID, errorbuf);
			return res;
		}
	}
	else
	{
		conn->SegMemoryMB = track->QueueInfo->SegResourceQuotaMemoryMB;
		conn->SegCore 	  = track->QueueInfo->SegResourceQuotaVCore;

		int vseglimit = 0;
		/*----------------------------------------------------------------------
		 * The limit of vseg number per segment is valid only when query does
		 * not have fixed vseg number to request.
		 *----------------------------------------------------------------------
		 */
		if ( conn->MinSegCountFixed != conn->MaxSegCountFixed )
		{
			vseglimit = conn->VSegLimitPerSeg * PRESPOOL->AvailNodeCount;
			vseglimit = conn->VSegLimit < vseglimit? conn->VSegLimit : vseglimit;
		}
		else
		{
			vseglimit = conn->VSegLimit;
		}

		/*----------------------------------------------------------------------
		 * Compute total resource quota. This calculation already considers the
		 * query vseg limit and vseg perseg limit.
		 *----------------------------------------------------------------------
		 */
		res = AllocationPolicy[policy] (track,
										&(conn->SegNum),
										&(conn->SegNumMin),
										vseglimit ,
										errorbuf,
										errorbufsize);
		if ( res != FUNC_RETURN_OK )
		{
			/* No setting error buffer here. We expect this is already set. */
			return res;
		}

		if ( conn->SegNum < conn->MinSegCountFixed )
		{
			res = RESQUEMGR_TOO_MANY_FIXED_SEGNUM;
			snprintf(errorbuf, errorbufsize,
					 "minimum expected number of virtual segment %d is more than "
					 "maximum possible number %d in queue %s",
					 conn->MinSegCountFixed,
					 conn->SegNum,
					 track->QueueInfo->Name);

			elog(WARNING, "ConnID %d. %s", conn->ConnID, errorbuf);
			return res;
		}

		elog(RMLOG, "ConnID %d. Expect query resource (%d MB, %lf CORE) x %d "
				    "(MIN %d) after checking queue capacity.",
					conn->ConnID,
					conn->SegMemoryMB,
					conn->SegCore,
					conn->SegNum,
					conn->SegNumMin);

		/*------------------------------------------------------------------
		 * The following logic consider the actual resource requirement from
		 * dispatcher based on table size, workload, etc. The requirement is
		 * described by (MinSegCountFixed, MaxSegCountFixed). The requirement
		 * can be satisfied only when there is a non-empty intersect between
		 * (MinSegCountFixed, MaxSegCountFixed) and (SegNumMin, SegNum).
		 *------------------------------------------------------------------
		 */
		if ( conn->MinSegCountFixed < conn->MaxSegCountFixed )
		{
			conn->SegNumMin = conn->MaxSegCountFixed < conn->SegNumMin ?
							  conn->MinSegCountFixed :
							  max(conn->SegNumMin, conn->MinSegCountFixed);
			conn->SegNum = min(conn->SegNum, conn->MaxSegCountFixed);
		}
		else
		{
			Assert(conn->SegNum >= conn->MaxSegCountFixed);
			conn->SegNumMin = conn->MinSegCountFixed;
			conn->SegNum	= conn->MaxSegCountFixed;
		}

		elog(RMLOG, "ConnID %d. Expect query resource (%d MB, %lf CORE) x %d "
				    "(MIN %d) after checking query expectation %d (MIN %d).",
					conn->ConnID,
					conn->SegMemoryMB,
					conn->SegCore,
					conn->SegNum,
					conn->SegNumMin,
					conn->MaxSegCountFixed,
					conn->MinSegCountFixed);

	}

	/*--------------------------------------------------------------------------
	 * Decide vseg number and minimum runnable vseg number. User may set guc
	 * rm_nvseg_perquery_limit at session level, this must be followed. Even
	 * in case the vseg number is set by statement level resource quota.
	 *
	 * Another guc rm_nvseg_perquery_perseg_limit can also limit the number of
	 * vseg for one statement execution.
	 *--------------------------------------------------------------------------
	 */
	if ( conn->SegNumMin > conn->VSegLimit )
	{
		res = RESQUEMGR_TOO_MANY_FIXED_SEGNUM;

		snprintf(errorbuf, errorbufsize,
				 "expected minimum number of virtual segments %d exceeds the "
				 "limit of number of virtual segments per query %d",
				 conn->SegNumMin,
				 conn->VSegLimit);

		elog(WARNING, "ConnID %d. %s", conn->ConnID, errorbuf);
		return res;
	}

	/*--------------------------------------------------------------------------
	 * The vseg number per segment limit is valid only when required vseg num
	 * is a range containing more than one validate values. Generally, in case
	 * querying one hash distributed table, hash bucket number of vseg is
	 * required.
	 *--------------------------------------------------------------------------
	 */
	if (conn->StatNVSeg == 0 &&
		conn->MinSegCountFixed != conn->MaxSegCountFixed &&
		conn->SegNumMin > conn->VSegLimitPerSeg * PRESPOOL->AvailNodeCount )
	{
		res = RESQUEMGR_TOO_MANY_FIXED_SEGNUM;

		snprintf(errorbuf, errorbufsize,
				 "expected minimum number of virtual segments %d exceeds the "
				 "limit of number of virtual segments per query per segment %d "
				 "in cluster having %d available segments",
				 conn->SegNumMin,
				 conn->VSegLimitPerSeg,
				 PRESPOOL->AvailNodeCount);

		elog(WARNING, "ConnID %d. %s", conn->ConnID, errorbuf);
		return res;
	}
	return FUNC_RETURN_OK;
}

/* Implementation of even resource allocation. */
int computeQueryQuota_EVEN(DynResourceQueueTrack	track,
						   int32_t			   	   *segnum,
						   int32_t			   	   *segnummin,
						   int32_t					segnumlimit,
						   char				   	   *errorbuf,
						   int						errorbufsize)
{
	DynResourceQueue queue = track->QueueInfo;

	/* Decide one connection should have how many virtual segments reserved. */
	int reservsegnum = trunc(track->ClusterSegNumber / queue->ParallelCount);
	reservsegnum = reservsegnum <= 0 ? 1 : reservsegnum;

	*segnum = track->ClusterSegNumberMax;
	*segnum = segnumlimit < *segnum ? segnumlimit : *segnum;

	*segnummin = reservsegnum;
	*segnummin = *segnummin > *segnum ? *segnum : *segnummin;

	Assert( *segnummin >= 0 && *segnummin <= *segnum );
	return FUNC_RETURN_OK;
}

int addQueryResourceRequestToQueue(DynResourceQueueTrack queuetrack,
								   ConnectionTrack		 conntrack)
{
	DynResourceQueueTrack queue = queuetrack->ShadowQueueTrack == NULL ?
								  queuetrack :
								  queuetrack->ShadowQueueTrack;

	insertDQueueTailNode(&(queue->QueryResRequests), conntrack);

	/* Add resource request counter. */
	addResourceBundleData(&(queue->TotalRequest),
						  conntrack->SegMemoryMB * conntrack->SegNum,
						  conntrack->SegCore * conntrack->SegNum);

	/*
	 * Set session tracker and make its corresponding session in-use resource
	 * locked.
	 */
	createAndLockSessionResource(&(queue->DLDetector), conntrack->SessionID);

	/* The following logic is triggered only when it is not in a shadow. */
	if ( queue == queuetrack )
	{
		if ( queue->DLDetector.LockedTotal.MemoryMB > 0 )
		{
			PQUEMGR->ForcedReturnGRMContainerCount = 0;
			elog(LOG, "Locking resource and stop forced GRM container breathe out.");
		}

		/*
		 * If this causes the queue to be busy, refresh the limits and weights
		 * of each memory/core ratio tracker.
		 */
		if ( !queue->isBusy )
		{
			queue->isBusy = true;
			refreshMemoryCoreRatioLimits();
			refreshMemoryCoreRatioWaterMark();
		}
		PQUEMGR->toRunQueryDispatch = true;
	}
	return FUNC_RETURN_OK;
}

/*
 * Update the overall resource queue percentage capacity.
 */
void refreshResourceQueuePercentageCapacityInternal(uint32_t clustermemmb,
													uint32_t clustercore,
													bool	 queuechanged)
{
	static uint32_t prevclustermemmb = 0;
	static uint32_t prevclustercore  = 0;

	if ( (!queuechanged) &&
		 (prevclustermemmb == clustermemmb && prevclustercore  == clustercore) )
	{
		elog(DEBUG3, "Resource manager skips updating resource queue capacities "
					 "because the total resource quota does not change.");
		return;
	}

	/*
	 * STEP 1. Decide the limit ranges of memory and core, decide the
	 * 		   memory/core ratio.
	 */
	ListCell *cell = NULL;
	foreach(cell, PQUEMGR->Queues)
	{
		DynResourceQueueTrack track     = lfirst(cell);
		DynResourceQueueTrack origtrack = track;

		/* If this resource queue track has a shadow, the shadow is updated. */
		track = track->ShadowQueueTrack == NULL ? track : track->ShadowQueueTrack;

		if ( RESQUEUE_IS_PERCENT(track->QueueInfo) &&
			 RESQUEUE_IS_LEAF(track->QueueInfo) )
		{
			track->ClusterMemoryActPer = track->QueueInfo->ClusterMemoryPer;
			track->ClusterVCoreActPer  = track->QueueInfo->ClusterVCorePer;

			/* If track references a shadow, we should find back the real parent. */
			DynResourceQueueTrack ptrack =
				(track->ParentTrack == NULL && list_length(track->ChildrenTracks) == 0 )?
				track->ShadowQueueTrack->ParentTrack :
				track->ParentTrack;

			while( ptrack != NULL )
			{
				/*
				 * If the queue track has a shadow, we should calculate based on
				 * the data in the shadow instance.
				 */
				DynResourceQueueTrack ptrackcalc = ptrack->ShadowQueueTrack == NULL ?
												   ptrack :
												   ptrack->ShadowQueueTrack;

				if ( !RESQUEUE_IS_PERCENT(ptrackcalc->QueueInfo) )
				{
					break;
				}

				track->ClusterMemoryActPer = track->ClusterMemoryActPer *
											 ptrackcalc->QueueInfo->ClusterMemoryPer /
											 100;
				track->ClusterVCoreActPer  = track->ClusterVCoreActPer *
											 ptrackcalc->QueueInfo->ClusterVCorePer /
											 100;

				ptrack = ptrack->ParentTrack;
			}

			track->ClusterMemoryMaxPer = track->ClusterMemoryActPer *
										 track->QueueInfo->ResourceOvercommit;
			track->ClusterMemoryMaxPer = track->ClusterMemoryMaxPer > 100 ?
										 100.0 :
										 track->ClusterMemoryMaxPer;
			track->ClusterVCoreMaxPer = track->ClusterVCoreActPer *
										track->QueueInfo->ResourceOvercommit;
			track->ClusterVCoreMaxPer = track->ClusterVCoreMaxPer > 100 ?
										100.0 :
										track->ClusterVCoreMaxPer;

			uint32_t tmpratio = 0;

			/*
			 * All the queues from the root to this queue are expressed by
			 * percentage, and the memory limit has the same limit with core
			 * limit.
			 */
			Assert( ptrack == NULL );
			Assert( track->ClusterMemoryActPer == track->ClusterVCoreActPer );

			tmpratio = clustermemmb / clustercore;
			track->QueueInfo->ClusterMemoryMB =
				1.0 * clustermemmb * track->ClusterMemoryActPer / 100;
			track->QueueInfo->ClusterVCore    =
				1.0 * clustercore * track->ClusterVCoreActPer / 100;

			track->ClusterMemoryMaxMB =
				1.0 * clustermemmb * track->ClusterMemoryMaxPer / 100;
			track->ClusterVCoreMax =
				1.0 * clustercore * track->ClusterVCoreMaxPer / 100;

			if ( tmpratio != track->MemCoreRatio && track->trackedMemCoreRatio )
			{
				removeResourceQueueRatio(track);
			}

			if ( !track->trackedMemCoreRatio )
			{
				track->MemCoreRatio = tmpratio;
				addResourceQueueRatio(origtrack);
			}
		}
	}

	/*
	 * STEP 2. Decide the maximum limit of memory and core of each leaf queue.
	 * 		   That means for each leaf queue, its maximum memory limit adding
	 * 		   all other leaf queues' minimum limits can not exceed cluster
	 * 		   capacity. Maximum core limit follows the same logic.
	 */
	cell = NULL;
	foreach(cell, PQUEMGR->Queues)
	{
		DynResourceQueueTrack track = lfirst(cell);

		track = track->ShadowQueueTrack == NULL ? track : track->ShadowQueueTrack;

		if ( !(RESQUEUE_IS_LEAF(track->QueueInfo)) )
		{
			continue;
		}

		/* Follows the memory/core ratio to adjust the maximum limits. */
		if ( track->ClusterMemoryMaxMB / track->ClusterVCoreMax > track->MemCoreRatio )
		{
			track->ClusterMemoryMaxMB = track->ClusterVCoreMax * track->MemCoreRatio;
		}
		else
		{
			track->ClusterVCoreMax = 1.0 * track->ClusterMemoryMaxMB / track->MemCoreRatio;
		}

		/* Decide cluster segment resource quota. */
		track->QueueInfo->SegResourceQuotaMemoryMB =
				track->QueueInfo->SegResourceQuotaMemoryMB == -1 ?
				track->QueueInfo->SegResourceQuotaVCore * track->MemCoreRatio :
				track->QueueInfo->SegResourceQuotaMemoryMB;
		track->QueueInfo->SegResourceQuotaVCore =
				track->QueueInfo->SegResourceQuotaVCore == -1 ?
				1.0 * track->QueueInfo->SegResourceQuotaMemoryMB / track->MemCoreRatio :
				track->QueueInfo->SegResourceQuotaVCore;

		/* Decide the cluster segment number quota. */
		track->ClusterSegNumber = trunc(track->QueueInfo->ClusterMemoryMB /
										track->QueueInfo->SegResourceQuotaMemoryMB);

		track->ClusterSegNumberMax = trunc(track->ClusterMemoryMaxMB /
										   track->QueueInfo->SegResourceQuotaMemoryMB);

		Assert( track->ClusterSegNumber <= track->ClusterSegNumberMax );

		elog(DEBUG3, "Resource manager refreshed resource queue capacity : %s "
				  	 "(%d MB, %lf CORE) x %d. MAX %d. FACTOR:%lf",
					 track->QueueInfo->Name,
					 track->QueueInfo->SegResourceQuotaMemoryMB,
					 track->QueueInfo->SegResourceQuotaVCore,
					 track->ClusterSegNumber,
					 track->ClusterSegNumberMax,
					 track->QueueInfo->ResourceOvercommit);
	}
}

void refreshMemoryCoreRatioLimits(void)
{
	for ( int i = 0 ; i < PQUEMGR->RatioCount ; ++i ) {

		PQUEMGR->RatioTrackers[i]->ClusterMemoryMaxMB = 0;
		PQUEMGR->RatioTrackers[i]->ClusterVCoreMax    = 0;
		PQUEMGR->RatioTrackers[i]->ClusterMemory	  = 0;
		PQUEMGR->RatioTrackers[i]->ClusterVCore		  = 0;

		ListCell *cell = NULL;
		foreach(cell, PQUEMGR->RatioTrackers[i]->QueueTrackers)
		{
			DynResourceQueueTrack track = lfirst(cell);

			/* We calculate only the queues having connections, which means
			 * potential resource request. */
			if ( !track->isBusy )
			{
				continue;
			}

			PQUEMGR->RatioTrackers[i]->ClusterMemory += track->QueueInfo->ClusterMemoryMB;
			PQUEMGR->RatioTrackers[i]->ClusterVCore  += track->QueueInfo->ClusterVCore;
			PQUEMGR->RatioTrackers[i]->ClusterMemoryMaxMB += track->ClusterMemoryMaxMB;
			PQUEMGR->RatioTrackers[i]->ClusterVCoreMax    += track->ClusterVCoreMax;
		}

		elog(DEBUG3, "Limit of memory/core ratio[%d] %d MBPCORE "
					 "is (%d MB, %lf CORE) maximum (%d MB, %lf CORE).",
					 i,
					 PQUEMGR->RatioTrackers[i]->MemCoreRatio,
					 PQUEMGR->RatioTrackers[i]->ClusterMemory,
					 PQUEMGR->RatioTrackers[i]->ClusterVCore,
					 PQUEMGR->RatioTrackers[i]->ClusterMemoryMaxMB,
					 PQUEMGR->RatioTrackers[i]->ClusterVCoreMax);
	}
}

/* TODO: Not useful yet. */
void refreshMemoryCoreRatioWaterMark(void)
{
	double totalweightmem = 0;
	double totalweightcore = 0;
	for ( int i = 0 ; i < PQUEMGR->RatioCount ; ++i ) {
		totalweightmem  += PQUEMGR->RatioTrackers[i]->ClusterMemory;
		totalweightcore += PQUEMGR->RatioTrackers[i]->ClusterVCore;
	}

	double overcommitmem  = 1;
	double overcommitcore = 1;
	double overcommit     = 1;
	if ( DRMGlobalInstance->ImpType == YARN_LIBYARN ) {
		overcommitmem  = totalweightmem  / PRESPOOL->GRMTotal.MemoryMB;
		overcommitcore = totalweightcore / PRESPOOL->GRMTotal.Core;
	}
	else if ( DRMGlobalInstance->ImpType == NONE_HAWQ2 ) {
		overcommitmem  = totalweightmem  / PRESPOOL->FTSTotal.MemoryMB;
		overcommitcore = totalweightcore / PRESPOOL->FTSTotal.Core;
	}
	else {
		Assert(false);
	}

	overcommit = overcommitmem > overcommitcore ? overcommitmem : overcommitcore;
	overcommit = overcommit > 1 ? overcommit : 1;
	for ( int i = 0 ; i < PQUEMGR->RatioCount ; ++i ) {
		PQUEMGR->RatioTrackers[i]->ClusterWeightMarker =
			PQUEMGR->RatioTrackers[i]->ClusterMemoryMaxMB / overcommit;
		elog(DEBUG5, "HAWQ RM :: Weight balance marker of memory/core ratio "
					 "[%d] %d MBPCORE is %lf MB with overcommit %lf",
					 i,
					 PQUEMGR->RatioTrackers[i]->MemCoreRatio,
					 PQUEMGR->RatioTrackers[i]->ClusterWeightMarker,
					 overcommit);
	}
}

void dispatchResourceToQueriesInOneQueue(DynResourceQueueTrack track)
{
	int			policy			= 0;
	Assert( track != NULL );

	elog(DEBUG3, "Resource manager dispatch resource in queue %s",
				 track->QueueInfo->Name);

	if ( track->QueryResRequests.NodeCount > 0 )
	{
		ConnectionTrack topwaiter = getDQueueHeadNodeData(&(track->QueryResRequests));
		if ( topwaiter->HeadQueueTime == 0 )
		{
			topwaiter->HeadQueueTime = gettime_microsec();
			elog(DEBUG3, "Set timestamp of waiting at head of queue.");
		}
	}

	policy = track->QueueInfo->AllocatePolicy;
	Assert( policy >= 0 && policy < RSQ_ALLOCATION_POLICY_COUNT );
	DispatchPolicy[policy] (track);

	/* Check if the queue has resource fragment problem. */
	track->troubledByFragment = false;
	if ( track->QueryResRequests.NodeCount > 0 )
	{
		ConnectionTrack topwaiter = getDQueueHeadNodeData(&(track->QueryResRequests));
		track->troubledByFragment = topwaiter->troubledByFragment;
	}
}

int addNewResourceToResourceManagerByBundle(ResourceBundle bundle)
{
	return addNewResourceToResourceManager(bundle->MemoryMB, bundle->Core);
}

int addNewResourceToResourceManager(int32_t memorymb, double core)
{
	if ( memorymb == 0 && core == 0 ) {
		return FUNC_RETURN_OK;
	}
	Assert( memorymb != 0 && core != 0 );

	/* Expect integer cores to add. */
	Assert( trunc(core) == core );
	uint32_t ratio = trunc(1.0 * memorymb / core);
	int32_t  ratioindex = getResourceQueueRatioIndex(ratio);
	Assert( ratioindex >= 0 );

	if ( ratioindex >= 0 ) {
		addResourceBundleData(&(PQUEMGR->RatioTrackers[ratioindex]->TotalAllocated),
						  	  memorymb,
							  core);
	}
	else {
		elog(LOG, "To add resource (%d MB, %lf CORE), resource manager gets "
				  "ratio %u not tracked.",
				  memorymb,
				  core,
				  ratio);
		return RESQUEMGR_NO_RATIO;
	}

	/* New resource is added. Try to dispatch resource to queries. */
	PQUEMGR->toRunQueryDispatch = true;
	return FUNC_RETURN_OK;
}

int minusResourceFromResourceManagerByBundle(ResourceBundle bundle)
{
	return minusResourceFromReourceManager(bundle->MemoryMB, bundle->Core);
}

int minusResourceFromReourceManager(int32_t memorymb, double core)
{
	if ( memorymb == 0 && core ==0 )
		return FUNC_RETURN_OK;

	/* Expect integer cores to add. */
	Assert( trunc(core) == core );
	uint32_t ratio = trunc(1.0 * memorymb / core);
	int32_t  ratioindex = getResourceQueueRatioIndex(ratio);
	Assert( ratioindex >= 0 );

	if ( ratioindex >= 0 ) {
		minusResourceBundleData(&(PQUEMGR->RatioTrackers[ratioindex]->TotalAllocated),
						  	  	memorymb,
						  	  	core);
	}
	else {
		elog(WARNING, "HAWQ RM :: minusResourceFromReourceManager: "
					  "Wrong ratio %u not tracked.", ratio);
		return RESQUEMGR_NO_RATIO;
	}
	return FUNC_RETURN_OK;
}

void returnAllocatedResourceToLeafQueue(DynResourceQueueTrack track,
										int32_t			  	  memorymb,
										double				  core)
{
	minusResourceBundleData(&(track->TotalUsed), memorymb, core);

	elog(DEBUG3, "Return resource to queue %s (%d MB, %lf CORE).",
			  track->QueueInfo->Name,
			  memorymb, core);
}

void removePendingResourceRequestInRootQueue(int32_t 	memorymb,
											 uint32_t 	core,
											 bool 		updatependingtime)
{
	if ( memorymb ==0 && core == 0 )
		return;
	Assert(memorymb > 0 && core > 0);

	uint32_t ratio 	    = memorymb / core;
	int32_t  ratioindex = 0;

	/* Get ratio index. */
	PAIR ratiopair = getHASHTABLENode(&(PQUEMGR->RatioIndex),
									  TYPCONVERT(void *, ratio));
	Assert( ratiopair != NULL );
	ratioindex = TYPCONVERT(int, ratiopair->Value);

	/* Add resource quota to free resource statistics. */
	minusResourceBundleData(&(PQUEMGR->RatioTrackers[ratioindex]->TotalPending),
							memorymb,
							core);
	Assert(PQUEMGR->RatioTrackers[ratioindex]->TotalPending.MemoryMB >= 0 &&
		   PQUEMGR->RatioTrackers[ratioindex]->TotalPending.Core >= 0);

	if ( updatependingtime )
	{
		if ( PQUEMGR->RatioTrackers[ratioindex]->TotalPending.MemoryMB == 0 &&
			 PQUEMGR->RatioTrackers[ratioindex]->TotalPending.Core == 0 )
		{
			PQUEMGR->RatioTrackers[ratioindex]->TotalPendingStartTime = 0;
			elog(DEBUG3, "Global resource total pending start time is updated to "UINT64_FORMAT,
						 PQUEMGR->RatioTrackers[ratioindex]->TotalPendingStartTime);
		}
		else if ( memorymb > 0 && core > 0 )
		{
			PQUEMGR->RatioTrackers[ratioindex]->TotalPendingStartTime = gettime_microsec();
			elog(DEBUG3, "Global resource total pending start time is updated to "UINT64_FORMAT,
						 PQUEMGR->RatioTrackers[ratioindex]->TotalPendingStartTime);
		}
	}

	elog(LOG, "Removed pending GRM request from root resource queue by "
			  "(%d MB, %lf CORE) to (%d MB, %lf CORE)",
			  memorymb,
			  core * 1.0,
			  PQUEMGR->RatioTrackers[ratioindex]->TotalPending.MemoryMB,
			  PQUEMGR->RatioTrackers[ratioindex]->TotalPending.Core);
}


void clearPendingResourceRequestInRootQueue(void)
{
	for ( int i = 0 ; i < PQUEMGR->RatioCount ; ++i )
	{
		if ( PQUEMGR->RatioTrackers[i]->TotalPending.MemoryMB > 0 )
		{
			removePendingResourceRequestInRootQueue(
					PQUEMGR->RatioTrackers[i]->TotalPending.MemoryMB,
					PQUEMGR->RatioTrackers[i]->TotalPending.Core,
					true);
		}
	}
}

/*
 * Dispatching allocated resource to queuing queries.
 */
int dispatchResourceToQueries_EVEN(DynResourceQueueTrack track)
{
	/* Check how many segments are available to dispatch. */
	int availsegnum = trunc((track->TotalAllocated.MemoryMB -
							 track->TotalUsed.MemoryMB) /
					  	  	track->QueueInfo->SegResourceQuotaMemoryMB);
	int counter = 0;
	int segcounter = 0;
	int segmincounter = 0;

	elog(DEBUG3, "Resource queue %s expects full parallel count %d, "
				 "current running count %d.",
				 track->QueueInfo->Name,
				 track->QueueInfo->ParallelCount,
				 track->NumOfRunningQueries);

	DQUEUE_LOOP_BEGIN(&(track->QueryResRequests), iter, ConnectionTrack, conntrack)
		/* Consider concurrency no more than defined parallel count. */
		/* TODO: Consider more here... */
		if ( counter + track->NumOfRunningQueries >= track->QueueInfo->ParallelCount )
		{
			elog(RMLOG, "Parallel count limit is encountered, to run %d more",
						counter);
			break;
		}

		int equalsegnummin = conntrack->StatNVSeg <= 0 ?
							 conntrack->SegNumMin :
							 conntrack->SegNumEqual;

		/* Check if the minimum segment requirement is met. */
		if ( segmincounter + equalsegnummin > availsegnum )
		{
			elog(RMLOG, "Resource allocated is up, available vseg num %d, "
						"to run %d more",
						availsegnum,
						counter);
			break;
		}

		segcounter += conntrack->StatNVSeg <= 0 ?
					  conntrack->SegNum :
					  conntrack->SegNumEqual;

		segmincounter += conntrack->StatNVSeg <= 0 ?
						 conntrack->SegNumMin :
						 conntrack->SegNumEqual;
		counter++;
	DQUEUE_LOOP_END

	if ( counter == 0 )
	{
		detectAndDealWithDeadLock(track);
		return FUNC_RETURN_OK; /* Expect requests are processed in next loop. */
	}

	/* Dispatch segments */
	DQueueData todisp;
	initializeDQueue(&todisp, PCONTEXT);
	for ( int i = 0 ; i < counter ; ++i )
	{
		ConnectionTrack conn = removeDQueueHeadNode(&(track->QueryResRequests));
		conn->SegNumActual = conn->SegNumMin;
		insertDQueueTailNode(&todisp, conn);
		availsegnum -= conn->StatNVSeg <= 0 ? conn->SegNumMin : conn->SegNumEqual;
	}

	DQueueNode pnode = getDQueueContainerHead(&todisp);
	int fullcount = 0;
	while(availsegnum > 0)
	{
		ConnectionTrack conn = (ConnectionTrack)(pnode->Data);
		if ( conn->StatNVSeg == 0 && conn->SegNum > conn->SegNumActual )
		{
			conn->SegNumActual++;
			availsegnum--;
			fullcount=0;
		}
		else
		{
			fullcount++;
		}
		if ( fullcount == counter )
		{
			break;
		}
		pnode = pnode->Next == NULL ? getDQueueContainerHead(&todisp) : pnode->Next;
	}

	/* Actually allocate segments from hosts in resource pool and send response.*/
	for ( int processidx = 0 ; processidx < counter ; ++processidx )
	{
		ConnectionTrack conn = removeDQueueHeadNode(&todisp);
		elog(DEBUG3, "Resource manager tries to dispatch resource to connection %d. "
		   		  	 "Expect (%d MB, %lf CORE) x %d(max %d min %d) segment(s). "
		   		  	 "Original vseg %d(min %d). "
		   		  	 "VSeg limit per segment %d VSeg limit per query %d.",
					 conn->ConnID,
				     conn->SegMemoryMB,
					 conn->SegCore,
					 conn->SegNumActual,
					 conn->SegNum,
					 conn->SegNumMin,
					 conn->MaxSegCountFixed,
					 conn->MinSegCountFixed,
					 conn->VSegLimitPerSeg,
					 conn->VSegLimit);

		if ( conn->StatNVSeg > 0 )
		{
			elog(LOG, "Resource manager tries to dispatch resource to connection %d. "
					  "Statement level resource quota is active. "
					  "Total %d vsegs, each vseg has %d MB memory quota.",
					  conn->ConnID,
					  conn->StatNVSeg,
					  conn->StatVSegMemoryMB);
		}

		/* Build resource. */
		int32_t segnumact = 0;
		allocateResourceFromResourcePool(conn->SegNumActual,
										 conn->SegNumMin,
										 conn->SegMemoryMB,
										 conn->SegCore,
										 conn->IOBytes,
										 conn->SliceSize,
										 conn->VSegLimitPerSeg,
										 conn->SegPreferredHostCount,
										 conn->SegPreferredHostNames,
										 conn->SegPreferredScanSizeMB,
										 /* If the segment count is fixed. */
										 conn->MinSegCountFixed == conn->MaxSegCountFixed,
										 &(conn->Resource),
										 &segnumact,
										 &(conn->SegIOBytes));

		RESOURCEPROBLEM accepted = isResourceAcceptable(conn, segnumact);
		if ( accepted == RESPROBLEM_NO )
		{
			elog(DEBUG3, "Resource manager dispatched %d segment(s) to connection %d",
						 segnumact,
						 conn->ConnID);
			conn->SegNumActual = segnumact;

			/* Mark resource used in resource queue. */
			addResourceBundleData(&(track->TotalUsed),
								  conn->SegMemoryMB * conn->SegNumActual,
								  conn->SegCore     * conn->SegNumActual);
			minusResourceBundleData(&(track->TotalRequest),
									conn->SegMemoryMB * conn->SegNum,
									conn->SegCore     * conn->SegNum);
			track->NumOfRunningQueries++;

			/* Unlock and update session resource usage. */
			unlockSessionResource(&(track->DLDetector), conn->SessionID);
			addSessionInUseResource(&(track->DLDetector),
									conn->SessionID,
									conn->SegMemoryMB * conn->SegNumActual,
									conn->SegCore     * conn->SegNumActual);

			/* Transform the connection track status */
			transformConnectionTrackProgress(conn, CONN_PP_RESOURCE_QUEUE_ALLOC_DONE);

			/* Build response message and send it out. */
			buildAcquireResourceResponseMessage(conn);
		}
		else
		{
			PQUEMGR->hasResourceProblem[accepted] = true;
			/*
			 * In case we have 0 segments allocated. This may occur because we
			 * have too many resource small pieces. In this case, we treat the
			 * resource allocation failed, and HAWQ RM tries this connection
			 * again later. We expect some other connections return the resource
			 * back later or some more resource allocated from GRM.
		     */
			elog(WARNING, "HAWQ RM :: Can not find enough number of hosts "
						  "containing sufficient resource for the connection %d.",
						  conn->ConnID);
			elog(WARNING, "HAWQ RM :: Found %d vsegments allocated", segnumact);
			if ( segnumact > 0 )
			{
				Assert(!conn->isOld);
				returnResourceToResourcePool(conn->SegMemoryMB,
											 conn->SegCore,
											 conn->SegIOBytes,
											 conn->SliceSize,
											 &(conn->Resource),
											 conn->isOld);
			}

			/* Mark the request has resource fragment problem. */
			if ( !conn->troubledByFragment && accepted == RESPROBLEM_FRAGMENT )
			{
				conn->troubledByFragmentTimestamp = gettime_microsec();
				conn->troubledByFragment 		  = true;

				elog(LOG, "Resource fragment problem is probably encountered. "
						  "Session "INT64_FORMAT" expects minimum %d virtual segments.",
						  conn->SessionID,
						  conn->SegNumMin);
			}

			/* Decide whether continue to process next query request. */
			if ( rm_force_fifo_queue )
			{
				insertDQueueHeadNode(&todisp, conn);
				break;
			}
			else
			{
				insertDQueueTailNode(&todisp, conn);
			}
		}
	}

	/* Return the request not completed yet. */
	while( todisp.NodeCount > 0 ) {
		ConnectionTrack conn = (ConnectionTrack)(removeDQueueTailNode(&todisp));
		insertDQueueHeadNode(&(track->QueryResRequests), (void *)conn);
	}
	cleanDQueue(&todisp);

	return FUNC_RETURN_OK;
}

RESOURCEPROBLEM isResourceAcceptable(ConnectionTrack conn, int segnumact)
{
	/*--------------------------------------------------------------------------
	 * Enough number of vsegments. If resource queue has enough quota, but
	 * resource pool does not provide enough virtual segments allocated, we
	 * consider this a resource fragment problem.
	 *--------------------------------------------------------------------------
	 */
	if ( segnumact < conn->SegNumMin )
	{
		return RESPROBLEM_FRAGMENT;
	}

	/*
	 *--------------------------------------------------------------------------
	 * Spread wide enough. If there is at least one segment containing 2 or more
	 * vsegments, the number of segments should be not be too small, i.e. the
	 * vsegments should not be assigned in a few segments.
	 *--------------------------------------------------------------------------
	 */
	if ( segnumact > list_length(conn->Resource) )
	{
		int limit = ceil(PRESPOOL->SlavesHostCount * rm_tolerate_nseg_limit);
		if ( PRESPOOL->SlavesHostCount - limit > list_length(conn->Resource) )
		{
			elog(WARNING, "Find virtual segments are dispatched to %d segments in "
						  "the cluster containing %d segments defined in slaves file. "
						  "The number of excluded segments are more than %d.",
						  list_length(conn->Resource),
						  PRESPOOL->SlavesHostCount,
						  limit);
			return RESPROBLEM_TOOFEWSEG;
		}
	}

	/*
	 *--------------------------------------------------------------------------
	 * Spread even enough. If the size of vsegments in each segment varies too
	 * much, the allocation result is not accepted.
	 *--------------------------------------------------------------------------
	 */
	if ( segnumact > list_length(conn->Resource) )
	{
		int minval = segnumact;
		int maxval = 0;
		ListCell *cell = NULL;
		foreach(cell, conn->Resource)
		{
			VSegmentCounterInternal vsegcnt = lfirst(cell);
			minval = minval < vsegcnt->VSegmentCount ? minval : vsegcnt->VSegmentCount;
			maxval = maxval > vsegcnt->VSegmentCount ? maxval : vsegcnt->VSegmentCount;
		}
		if ( rm_nvseg_variance_among_seg_limit < maxval - minval )
		{
			elog(WARNING, "Find virtual segments are not evenly dispatched to segments, "
						  "maximum virtual segment size is %d, "
						  "minimum virtual segment size is %d.",
						  maxval,
						  minval);
			return RESPROBLEM_UNEVEN;
		}
	}
	return RESPROBLEM_NO;
}

void buildAcquireResourceResponseMessage(ConnectionTrack conn)
{
	ListCell *cell = NULL;

	Assert( conn != NULL );
	resetSelfMaintainBuffer(&(conn->MessageBuff));

	/* Set message head. */
	RPCResponseHeadAcquireResourceFromRMData response;
	response.Result 	  			= FUNC_RETURN_OK;
	response.Reserved1	  			= 0;
	response.SegCount	  			= conn->SegNumActual;
	response.SegMemoryMB  			= conn->SegMemoryMB;
	response.SegCore	  			= conn->SegCore;
	response.HostCount				= list_length(conn->Resource);
	response.Reserved2				= 0;
	appendSMBVar(&(conn->MessageBuff), response);

	/* Append HDFS host name index values. */
	uint32_t hdfsidxsize = __SIZE_ALIGN64(sizeof(uint32_t) * conn->SegNumActual);
	prepareSelfMaintainBuffer(&(conn->MessageBuff), hdfsidxsize, true);
	uint32_t *indexarray = (uint32_t *)getSMBCursor(&(conn->MessageBuff));

	int segi = 0;
	foreach(cell, conn->Resource)
	{
		VSegmentCounterInternal vsegcnt = (VSegmentCounterInternal)lfirst(cell);
		for ( int i = 0 ; i < vsegcnt->VSegmentCount ; ++i )
		{
			indexarray[segi] = vsegcnt->HDFSNameIndex;
			segi++;
		}
	}
	jumpforwardSelfMaintainBuffer(&(conn->MessageBuff), hdfsidxsize);

	/* Prepare machine id information. */
	uint32_t messagecursize = getSMBContentSize(&(conn->MessageBuff));
	uint32_t hoffsetsize    = __SIZE_ALIGN64(sizeof(uint32_t) * conn->SegNumActual);
	uint32_t *hoffsetarray  = (uint32_t *)rm_palloc0(PCONTEXT, hoffsetsize);

	/* Temporary buffer containing all distinct machineid instances. */
	SelfMaintainBufferData machineids;
	initializeSelfMaintainBuffer(&machineids, PCONTEXT);

	segi = 0;
	foreach(cell, conn->Resource)
	{
		VSegmentCounterInternal vsegcnt = (VSegmentCounterInternal)lfirst(cell);
		/* Set host offset. */
		for ( int i = 0 ; i < vsegcnt->VSegmentCount ; ++i ) {
			hoffsetarray[segi] = messagecursize +
							     hoffsetsize +
								 getSMBContentSize(&(machineids));
			segi++;
		}

		/* Append machine id. */
		appendSelfMaintainBuffer(&machineids,
								 (char *)(&(vsegcnt->Resource->Stat->Info)),
								 vsegcnt->Resource->Stat->Info.Size);

		elog(DEBUG3, "Resource manager added machine %s:%d containing %d segment(s) "
					 "in response of acquiring resource.",
					 GET_SEGRESOURCE_HOSTNAME(vsegcnt->Resource),
					 vsegcnt->Resource->Stat->Info.port,
					 vsegcnt->VSegmentCount);
	}

	/* Build complete message. */
	appendSelfMaintainBuffer(&(conn->MessageBuff),
							 (char *)hoffsetarray,
							 hoffsetsize);
	appendSelfMaintainBuffer(&(conn->MessageBuff),
							 machineids.Buffer,
							 machineids.Cursor + 1);

	conn->MessageSize  = conn->MessageBuff.Cursor + 1;
	conn->MessageID    = RESPONSE_QD_ACQUIRE_RESOURCE;
	conn->ResAllocTime = gettime_microsec();

	elog(LOG, "Latency of getting resource allocated is "UINT64_FORMAT "us",
			  conn->ResAllocTime - conn->ResRequestTime);

	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conn);
	MEMORY_CONTEXT_SWITCH_BACK

	/* Clean up temporary variables. */
	destroySelfMaintainBuffer(&machineids);
	rm_pfree(PCONTEXT, hoffsetarray);
}

void detectAndDealWithDeadLock(DynResourceQueueTrack track)
{
	static char errorbuf[ERRORMESSAGE_SIZE];
	uint32_t availmemorymb = track->ClusterMemoryMaxMB -
						     track->DLDetector.LockedTotal.MemoryMB;
	double   availcore     = track->ClusterVCoreMax -
						     track->DLDetector.LockedTotal.Core;

	ConnectionTrack firstreq = (ConnectionTrack)
							   getDQueueHeadNodeData(&(track->QueryResRequests));
	if ( firstreq == NULL )
	{
		return;
	}

	uint32_t expmemorymb = firstreq->SegMemoryMB * firstreq->SegNumMin;
	double   expcore     = firstreq->SegCore     * firstreq->SegNumMin;

	if ( expmemorymb > track->ClusterMemoryMaxMB &&
		 expcore     > track->ClusterVCoreMax )
	{
		/* It is impossible to satisfy the request by checking deadlock. */
		return;
	}

	while((availmemorymb < expmemorymb || availcore < expcore) &&
		  track->QueryResRequests.NodeCount > 0 )
	{
		DQueueNode tail = getDQueueContainerTail(&(track->QueryResRequests));
		SessionTrack strack = NULL;
		while(tail != NULL)
		{
			strack = findSession(&(track->DLDetector),
								 ((ConnectionTrack)(tail->Data))->SessionID);
			if ( strack != NULL && strack->InUseTotal.MemoryMB > 0 )
			{
				break;
			}
			tail = tail->Prev;
		}
		if ( tail != NULL )
		{
			ConnectionTrack canceltrack = (ConnectionTrack)
										  removeDQueueNode(&(track->QueryResRequests),
												  	  	   tail);

			snprintf(errorbuf, sizeof(errorbuf),
					 "session "INT64_FORMAT" deadlock is detected",
					 canceltrack->SessionID);

			Assert(canceltrack != NULL);
			availmemorymb += strack->InUseTotal.MemoryMB;
			availcore     += strack->InUseTotal.Core;

			/* Unlock the resource. */
			unlockSessionResource(&(track->DLDetector), canceltrack->SessionID);

			/* Cancel this request. */
			buildAcquireResourceErrorResponse(canceltrack,
											  RESQUEMGR_DEADLOCK_DETECTED,
											  errorbuf);

			transformConnectionTrackProgress(canceltrack,
											 CONN_PP_RESOURCE_QUEUE_ALLOC_FAIL);

			canceltrack->ResponseSent = false;
			MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
			PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, canceltrack);
			MEMORY_CONTEXT_SWITCH_BACK

			/* Recycle connection track instance. */
			Assert(track->CurConnCounter > 0);

			elog(DEBUG3, "Resource queue %s has %d connections before removing "
						 "deadlocked connection ConnID %d.",
						 track->QueueInfo->Name,
						 track->CurConnCounter,
						 canceltrack->ConnID);

			track->CurConnCounter--;
			if ( track->CurConnCounter == 0 )
			{
				elog(RMLOG, "Resource queue %s becomes idle after deadlock checking.",
							track->QueueInfo->Name);
				track->isBusy = false;
				refreshMemoryCoreRatioLimits();
				refreshMemoryCoreRatioWaterMark();
			}
		}
	}
}

void timeoutDeadResourceAllocation(void)
{
	static char errorbuf[ERRORMESSAGE_SIZE];
	uint64_t curmsec = gettime_microsec();

	if ( curmsec - PQUEMGR->LastCheckingDeadAllocationTime <
		 1000000LL * rm_request_timeoutcheck_interval )
	{
		return;
	}

	/* Go through all current allocated connection tracks. */
	List     *allcons = NULL;
	ListCell *cell	  = NULL;

	getAllPAIRRefIntoList(&(PCONTRACK->Connections), &allcons);

	foreach(cell, allcons)
	{
		ConnectionTrack curcon = (ConnectionTrack)(((PAIR)lfirst(cell))->Value);

		switch(curcon->Progress)
		{

		case CONN_PP_RESOURCE_QUEUE_ALLOC_DONE:
		{
			elog(DEBUG5, "Find allocated resource that should check timeout. "
						 "ConnID %d",
						 curcon->ConnID);

			if ( curmsec - curcon->LastActTime >
				 1000000L * rm_session_lease_timeout )
			{
				elog(LOG, "ConnID %d. The allocated resource timeout is detected.",
						  curcon->ConnID);
				returnResourceToResQueMgr(curcon);
				returnConnectionToQueue(curcon, true);
				if ( curcon->CommBuffer != NULL )
				{
					forceCloseFileDesc(curcon->CommBuffer);
				}
				else
				{
					returnConnectionTrack(curcon);
				}
			}
			break;
		}

		case CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT:
		{
			if ( curmsec - curcon->LastActTime >
				 1000000L * rm_session_lease_timeout )
			{
				elog(LOG, "ConnID %d. The queued resource request timeout is "
						  "detected.",
						  curcon->ConnID);

				snprintf(errorbuf, sizeof(errorbuf),
						 "queued resource request is timed out due to no session "
						 "lease heart-beat received");

				cancelResourceAllocRequest(curcon, errorbuf, true);
				returnConnectionToQueue(curcon, true);
				if ( curcon->CommBuffer != NULL )
				{
					forceCloseFileDesc(curcon->CommBuffer);
				}
				else
				{
					returnConnectionTrack(curcon);
				}
			}
			break;
		}

		case CONN_PP_REGISTER_DONE:
		{
			if ( curmsec - curcon->LastActTime >
				 1000000L * rm_session_lease_timeout )
			{
				elog(WARNING, "The registered connection timeout is detected. "
						  	  "ConnID %d",
							  curcon->ConnID);
				returnConnectionToQueue(curcon, true);
				if ( curcon->CommBuffer != NULL )
				{
					forceCloseFileDesc(curcon->CommBuffer);
				}
				else
				{
					returnConnectionTrack(curcon);
				}
			}
			break;
		}
		}

	}
	freePAIRRefList(&(PCONTRACK->Connections), &allcons);

	PQUEMGR->LastCheckingDeadAllocationTime = curmsec;
}

void timeoutQueuedRequest(void)
{
	static char errorbuf[ERRORMESSAGE_SIZE];
	uint64_t curmsec = gettime_microsec();

	if ( curmsec - PQUEMGR->LastCheckingQueuedTimeoutTime <
		 1000000LL * rm_request_timeoutcheck_interval )
	{
		return;
	}

	/* Go through all to be processed requests. */
	ConnectionTrack  ct    		= NULL;
	List			*tryagain	= NULL;

	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	while( list_length(PCONTRACK->ConnHavingRequests) > 0)
	{
		ct = (ConnectionTrack)lfirst(list_head(PCONTRACK->ConnHavingRequests));
		PCONTRACK->ConnHavingRequests = list_delete_first(PCONTRACK->ConnHavingRequests);

		/*
		 * Case 1. RM has no available cluster built yet, the request is not
		 * 		   added into resource queue manager queues.
		 */
		elog(DEBUG3, "Deferred connection track is found. "
					 " Request Time " UINT64_FORMAT
					 " Curr Time " UINT64_FORMAT
					 " Delta " UINT64_FORMAT,
					 ct->RequestTime,
					 curmsec,
					 curmsec - ct->RequestTime);

		if ( curmsec - ct->RequestTime > 1000000L * rm_resource_allocation_timeout )
		{
			snprintf(errorbuf, sizeof(errorbuf),
					 "resource request is timed out due to no available cluster");

			elog(WARNING, "ConnID %d. %s", ct->ConnID, errorbuf);

			/* Build timeout response. */
			transformConnectionTrackProgress(ct, CONN_PP_TIMEOUT_FAIL);
			buildAcquireResourceErrorResponseAndSend(ct,
												 	 RESQUEMGR_NOCLUSTER_TIMEOUT,
													 errorbuf);

		}
		else
		{
			tryagain = lappend(tryagain, ct);
		}
	}

	while( list_length(tryagain) > 0 )
	{
		void *move = lfirst(list_head(tryagain));
		tryagain = list_delete_first(tryagain);
		PCONTRACK->ConnHavingRequests = lappend(PCONTRACK->ConnHavingRequests, move);
	}

	/* Go through all current allocated connection tracks. */
	curmsec = gettime_microsec();

	List     *allcons = NULL;
	ListCell *cell	  = NULL;

	getAllPAIRRefIntoList(&(PCONTRACK->Connections), &allcons);
	foreach(cell, allcons)
	{
		ConnectionTrack curcon = (ConnectionTrack)(((PAIR)lfirst(cell))->Value);

		if ( curcon->Progress == CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT )
		{
			/*
			 * Check if corresponding mem core ratio tracker has long enough
			 * time to waiting for GRM containers.
			 */
			DynResourceQueueTrack queuetrack = (DynResourceQueueTrack)
											   (curcon->QueueTrack);
			int index = getResourceQueueRatioIndex(queuetrack->MemCoreRatio);
			Assert(PQUEMGR->RootTrack != NULL);

			/*
			 * Set the head waiting timestamp if this request is a head request
			 * in the target queue.
			 */
			if ( queuetrack->QueryResRequests.NodeCount > 0 )
			{
				ConnectionTrack topwaiter =
						getDQueueHeadNodeData(&(queuetrack->QueryResRequests));
				if ( topwaiter == curcon && topwaiter->HeadQueueTime == 0 )
				{
					topwaiter->HeadQueueTime = curmsec;
					elog(DEBUG3, "Set timestamp of waiting at head of queue.");
				}
			}

			elog(DEBUG3, "Check waiting connection track: ConnID %d "
						 "Head time "UINT64_FORMAT " "
						 "Global resource pending time "UINT64_FORMAT " ",
						 curcon->ConnID,
						 curmsec - curcon->HeadQueueTime,
						 curmsec - PQUEMGR->RatioTrackers[index]->TotalPendingStartTime);

			bool tocancel = false;

			/*
			 * Case 1. No available cluster for executing.
			 *
			 * Case 2. No enough resource to run, resource manager is still
			 * 		   acquiring resource from global resource manager, and the
			 * 		   request is at the head of the queue.
			 */
			if ( ( (PQUEMGR->RootTrack->ClusterSegNumberMax == 0) &&
				   (curmsec - curcon->ResRequestTime >
						1000000L * rm_resource_allocation_timeout ) ) ||
				 ( (PQUEMGR->RatioTrackers[index]->TotalPendingStartTime > 0) &&
				   (curmsec - PQUEMGR->RatioTrackers[index]->TotalPendingStartTime >
						1000000L * rm_resource_allocation_timeout) &&
				   (curcon->HeadQueueTime > 0) &&
				   (curmsec - curcon->HeadQueueTime >
				 	 	1000000L * rm_resource_allocation_timeout) ) )
			{
				elog(LOG, "ConnID %d. The queued resource request no resource "
						  "timeout is detected, the waiting time in head of the"
						  "queue is "UINT64_FORMAT " global resource pending "
						  "start time is "UINT64_FORMAT
						  ", current time is "UINT64_FORMAT".",
						  curcon->ConnID,
						  curmsec - curcon->HeadQueueTime,
						  PQUEMGR->RatioTrackers[index]->TotalPendingStartTime,
						  curmsec);

				snprintf(errorbuf, sizeof(errorbuf),
						 "queued resource request is timed out due to no resource");
				tocancel = true;
			}

			/* Case 3. Check if resource fragment problem lasts too long time. */
			if ( curcon->troubledByFragment &&
			     curmsec - curcon->troubledByFragmentTimestamp >
					 1000000L * rm_resource_allocation_timeout &&
				 ((DynResourceQueueTrack)(curcon->QueueTrack))->NumOfRunningQueries == 0 )
			{
				elog(LOG, "ConnID %d. The queued resource request timeout is "
						  "detected due to resource fragment problem.",
						  curcon->ConnID);

				snprintf(errorbuf, sizeof(errorbuf),
						 "queued resource request is timed out due to resource "
						 "fragment problem");
				tocancel = true;
			}

			if ( tocancel )
			{
				cancelResourceAllocRequest(curcon, errorbuf, true);
				returnConnectionToQueue(curcon, true);
			}
		}
	}
	freePAIRRefList(&(PCONTRACK->Connections), &allcons);
	PQUEMGR->LastCheckingQueuedTimeoutTime = curmsec;
	MEMORY_CONTEXT_SWITCH_BACK
}

void buildAcquireResourceErrorResponseAndSend(ConnectionTrack  conntrack,
										  	  int 		  	   errorcode,
											  char			  *errorbuf)
{
	buildAcquireResourceErrorResponse(conntrack, errorcode, errorbuf);

	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK
}

void buildAcquireResourceErrorResponse(ConnectionTrack	conntrack,
									   int				errorcode,
									   char			   *errorbuf)
{
	SelfMaintainBufferData responsedata;
	initializeSelfMaintainBuffer(&responsedata, PCONTEXT);

	RPCResponseAcquireResourceFromRMERRORData response;
	response.Result   = errorcode;
	response.Reserved = 0;

	appendSMBVar(&responsedata, response);
	appendSMBStr(&responsedata, errorbuf);
	appendSelfMaintainBufferTill64bitAligned(&responsedata);

	buildResponseIntoConnTrack(conntrack,
							   SMBUFF_CONTENT(&responsedata),
							   getSMBContentSize(&responsedata),
							   conntrack->MessageMark1,
							   conntrack->MessageMark2,
							   RESPONSE_QD_ACQUIRE_RESOURCE);
	destroySelfMaintainBuffer(&responsedata);
}

bool isAllResourceQueueIdle(void)
{
	ListCell *cell = NULL;
	foreach(cell, PQUEMGR->Queues)
	{
		DynResourceQueueTrack quetrack = lfirst(cell);
		if ( quetrack->TotalUsed.MemoryMB > 0 || quetrack->TotalUsed.Core > 0 )
		{
			return false;
		}
	}
	return true;
}

void resetAllDeadLockDetector(void)
{
	ListCell *cell = NULL;
	foreach(cell, PQUEMGR->Queues)
	{
		DynResourceQueueTrack quetrack = lfirst(cell);
		resetResourceDeadLockDetector(&(quetrack->DLDetector));
	}
}

void getIdleResourceRequest(int32_t *mem, double *core)
{
	Assert(PRESPOOL->ClusterMemoryCoreRatio > 0);
	*mem  = PRESPOOL->ClusterMemoryCoreRatio *
			PRESPOOL->AvailNodeCount *
			PQUEMGR->ActualMinGRMContainerPerSeg;
	*core = 1.0 *
			PRESPOOL->AvailNodeCount *
			PQUEMGR->ActualMinGRMContainerPerSeg;
}

void setForcedReturnGRMContainerCount(void)
{
	/* If some queue has locked resource, dont do GRM container breathe. */
	ListCell *cell = NULL;
	foreach(cell, PQUEMGR->Queues)
	{
		DynResourceQueueTrack quetrack = lfirst(cell);

		if ( quetrack->DLDetector.LockedTotal.MemoryMB > 0 )
		{
			elog(LOG, "Queue %s has potential resource deadlock, cancel breathe.",
					  quetrack->QueueInfo->Name);
			PQUEMGR->GRMQueueCurCapacity   = 1.0;
			PQUEMGR->GRMQueueResourceTight = false;
			return;
		}
	}

	/* Get current GRM container size. */
	int clusterctnsize = getClusterGRMContainerSize();
	int toretctnsize = 0;
	double curabscapacity = PQUEMGR->GRMQueueCurCapacity *
							PQUEMGR->GRMQueueCapacity;

	if ( curabscapacity > PQUEMGR->GRMQueueCapacity )
	{
		/*
		 * We would like to return as many containers as possible to make queue
		 * usage lower than expected capacity.
		 */
		double r = (curabscapacity - PQUEMGR->GRMQueueCapacity) / curabscapacity;
		elog(LOG, "GRM queue is over-using, cur capacity %lf*%lf, "
				  "ratio %lf, curent GRM container size %d",
				  PQUEMGR->GRMQueueCurCapacity,
				  PQUEMGR->GRMQueueCapacity,
				  r,
				  clusterctnsize);
		toretctnsize = ceil(r * clusterctnsize);

		if ( rm_return_percentage_on_overcommit > 0 )
		{
			double r = 1.0 * clusterctnsize * rm_return_percentage_on_overcommit / 100;
			int toretctnsize2 = ceil(r);
			elog(DEBUG3, "Calculated r %lf based on return percentage.", r);
			toretctnsize = toretctnsize < toretctnsize2 ? toretctnsize : toretctnsize2;
		}
	}

	elog(LOG, "Resource manager expects to breathe out %d GRM containers. "
			  "Total %d GRM containers, ",
			  toretctnsize,
			  clusterctnsize);

	/* Restore queue report to avoid force return again. */
	PQUEMGR->ForcedReturnGRMContainerCount = toretctnsize;
	PQUEMGR->GRMQueueCurCapacity		   = 1.0;
	PQUEMGR->GRMQueueResourceTight 		   = false;
}

void buildQueueTrackShadows(DynResourceQueueTrack	toaltertrack,
							List 				  **qhavingshadow)
{
	/*--------------------------------------------------------------------------
	 * Iteratively go through each descendant including the queue to be altered.
	 * the queue to be altered should have a shadow, the all the descendant
	 * queue having query running or queued should have a shadow. We care only
	 * the busy queues, if the queue is not in busy status, no need to build
	 * shadow to care of the status, we just need to alter the original instance
	 * directly.
	 *--------------------------------------------------------------------------
	 */

	Assert(qhavingshadow != NULL);

	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)

	List *q = NULL;
	q = lappend(q, toaltertrack);
	while( list_length(q) > 0 )
	{
		/* Pop the first one in the queue. */
		DynResourceQueueTrack curtrack = (DynResourceQueueTrack)linitial(q);
		q = list_delete_first(q);

		elog(DEBUG5, "Resource manager checks queue %s for building shadow "
					 "instance.",
					 curtrack->QueueInfo->Name);

		/*
		 * Add the queue to alter and descendant in use leaf queues to the
		 * result list.
		 */
		if( (curtrack == toaltertrack) ||
			(curtrack->isBusy && list_length(curtrack->ChildrenTracks) == 0) )
		{
			*qhavingshadow = lappend(*qhavingshadow, curtrack);
			buildQueueTrackShadow(curtrack);

			elog(RMLOG, "Resource queue %s has shadow instance built.",
					  	curtrack->QueueInfo->Name);
		}

		/* Add children queue tracks into the queue. */
		ListCell *cell = NULL;
		foreach(cell, curtrack->ChildrenTracks)
		{
			q = lappend(q, lfirst(cell));
		}
	}

	MEMORY_CONTEXT_SWITCH_BACK
}

void buildQueueTrackShadow(DynResourceQueueTrack toaltertrack)
{
	Assert(toaltertrack!= NULL);
	/* It is not acceptable to have another shadow already created. */
	Assert(toaltertrack->ShadowQueueTrack == NULL);
	/* Create shadow instance and build up reference. */
	toaltertrack->ShadowQueueTrack = rm_palloc0(PCONTEXT,
												sizeof(DynResourceQueueTrackData));
	DynResourceQueueTrack shadowtrack = toaltertrack->ShadowQueueTrack;
	shadowtrack->QueueInfo = rm_palloc0(PCONTEXT, sizeof(DynResourceQueueData));
	memcpy(shadowtrack->QueueInfo,
		   toaltertrack->QueueInfo,
		   sizeof(DynResourceQueueData));
	shadowtrack->ShadowQueueTrack = toaltertrack;

	/* Shadow resource queue track does not hold tree structure. */
	shadowtrack->ParentTrack = NULL;
	shadowtrack->ChildrenTracks = NULL;

	shadowtrack->ClusterMemoryActPer	= toaltertrack->ClusterMemoryActPer;
	shadowtrack->ClusterMemoryMaxMB		= toaltertrack->ClusterMemoryMaxMB;
	shadowtrack->ClusterMemoryMaxPer	= toaltertrack->ClusterMemoryMaxPer;

	shadowtrack->ClusterVCoreActPer		= toaltertrack->ClusterVCoreActPer;
	shadowtrack->ClusterVCoreMax		= toaltertrack->ClusterVCoreMax;
	shadowtrack->ClusterVCoreMaxPer		= toaltertrack->ClusterVCoreMaxPer;

	shadowtrack->ClusterSegNumber		= toaltertrack->ClusterSegNumber;
	shadowtrack->ClusterSegNumberMax	= toaltertrack->ClusterSegNumberMax;

	shadowtrack->CurConnCounter			= toaltertrack->CurConnCounter;
	shadowtrack->NumOfRunningQueries	= toaltertrack->NumOfRunningQueries;

	shadowtrack->MemCoreRatio			= toaltertrack->MemCoreRatio;
	shadowtrack->RatioIndex				= toaltertrack->RatioIndex;

	shadowtrack->trackedMemCoreRatio	= toaltertrack->trackedMemCoreRatio;
	shadowtrack->isBusy					= toaltertrack->isBusy;

	initializeResqueueDeadLockDetector(&(shadowtrack->DLDetector), shadowtrack);

	initializeDQueue(&(shadowtrack->QueryResRequests), PCONTEXT);

	resetResourceBundleDataByBundle(&(shadowtrack->TotalUsed),
									&(toaltertrack->TotalUsed));
	resetResourceBundleDataByBundle(&(shadowtrack->TotalAllocated),
									&(toaltertrack->TotalAllocated));

	resetResourceBundleData(&(shadowtrack->TotalRequest), 0, 0.0, 0);
	shadowtrack->expectMoreResource		= false;
	shadowtrack->pauseAllocation		= false;
	shadowtrack->troubledByFragment		= false;
}

void cleanupQueueTrackShadows(List **qhavingshadow)
{
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	while(list_length(*qhavingshadow) > 0)
	{
		DynResourceQueueTrack track = (DynResourceQueueTrack)
									  linitial(*qhavingshadow);
		*qhavingshadow = list_delete_first(*qhavingshadow);
		DynResourceQueueTrack shadowtrack = track->ShadowQueueTrack;
		deepFreeResourceQueueTrack(shadowtrack);
		track->ShadowQueueTrack = NULL;
	}
	MEMORY_CONTEXT_SWITCH_BACK
}

int rebuildAllResourceQueueTrackDynamicStatusInShadow(List *quehavingshadow,
													  bool  queuechanged,
													  char *errorbuf,
													  int	errorbufsize)
{
	int res = FUNC_RETURN_OK;

	ListCell *cell = NULL;
	foreach(cell, quehavingshadow)
	{
		DynResourceQueueTrack quetrack = (DynResourceQueueTrack)lfirst(cell);
		res = rebuildResourceQueueTrackDynamicStatusInShadow(quetrack,
															 queuechanged,
															 errorbuf,
															 errorbufsize);
		if ( res != FUNC_RETURN_OK )
		{
			elog(WARNING, "Resource manager failed to rebuild resource queue %s "
						  "dynamic status in its shadow to reflect altered "
						  "resource queues.",
						  quetrack->QueueInfo->Name);
			return res;
		}
		else
		{
			elog(RMLOG, "Resource manager passed rebuilding resource queue %s "
					    "dynamic status in its shadow.",
					    quetrack->QueueInfo->Name);
		}

		res = detectAndDealWithDeadLockInShadow(quetrack, queuechanged);
		if ( res != FUNC_RETURN_OK )
		{
			elog(WARNING, "Resource manager failed to rebuild resource queue %s "
						  "dynamic status to reflect altered resource queues due "
						  "to the deadlock issue introduced by altering resource "
						  "queue.",
						  quetrack->QueueInfo->Name);
			return res;
		}
		else
		{
			elog(RMLOG, "Resource manager passed detecting deadlock issues in the "
					    "shadow of resource queue %s",
					    quetrack->QueueInfo->Name);
		}
	}

	elog(RMLOG, "Resource manager finished rebuilding resource queues' dynamic "
				"status");
	return FUNC_RETURN_OK;
}

int rebuildResourceQueueTrackDynamicStatusInShadow(DynResourceQueueTrack  quetrack,
												   bool					  queuechanged,
												   char 				 *errorbuf,
												   int					  errorbufsize)
{
	int res = FUNC_RETURN_OK;

	elog(RMLOG, "Rebuild resource queue %s dynamic status in its shadow.",
			    quetrack->QueueInfo->Name);

	DynResourceQueueTrack shadowtrack = quetrack->ShadowQueueTrack;
	/* Get deadlock detector ready in the shadow instance. */
	copyResourceDeadLockDetectorWithoutLocking(&(quetrack->DLDetector),
											   &(shadowtrack->DLDetector));

	elog(DEBUG3, "Deadlock detector in shadow has %d MB in use %d MB locked.",
				 shadowtrack->DLDetector.InUseTotal.MemoryMB,
				 quetrack->DLDetector.LockedTotal.MemoryMB);

	/* Go through all queued query resource requests, recalculate the request. */
	DQUEUE_LOOP_BEGIN(&(quetrack->QueryResRequests), iter, ConnectionTrack, conn)

		ConnectionTrack newconn = NULL;
		createEmptyConnectionTrack(&newconn);
		/* Process only requests waiting here. */
		Assert(conn->Progress == CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT);
		copyAllocWaitingConnectionTrack(conn, newconn);
		/* Make new connection track referencing the shadow instance */
		newconn->QueueTrack = shadowtrack;
		/*
		 * Calculate the resource request again based on new resource queue
		 * definition, i.e. the attributes updated in the shadow instance.
		 */
		res = computeQueryQuota(newconn, errorbuf, errorbufsize);

		if ( res == FUNC_RETURN_OK )
		{
			if ( newconn->StatNVSeg == 0 )
			{
				/*--------------------------------------------------------------
				 * Adjust the number of virtual segments again based on
				 * NVSEG_*_LIMITs and NVSEG_*_LIMIT_PERSEGs. This adjustment
				 * must succeed.
				 *--------------------------------------------------------------
				 */
				res = adjustResourceExpectsByQueueNVSegLimits(newconn,
															  errorbuf,
															  errorbufsize);

				if (res == FUNC_RETURN_OK )
				{
					elog(LOG, "ConnID %d. Expect query resource (%d MB, %lf CORE) "
							  "x %d ( MIN %d ) resource after adjusting based on "
							  "queue NVSEG limits.",
							  newconn->ConnID,
							  newconn->SegMemoryMB,
							  newconn->SegCore,
							  newconn->SegNum,
							  newconn->SegNumMin);
				}
			}
		}

		if ( res == FUNC_RETURN_OK )
		{
			/* Add request to the resource queue and return. */
			addQueryResourceRequestToQueue(quetrack, newconn);
		}
		else
		{
			/*------------------------------------------------------------------
			 * Here we find the request unable to be adjusted based on new
			 * resource queue resource limits. If we force resource manager to
			 * cancel the request, we will cancel this request and generate
			 * error message as response. If we dont, the rebuilding phase should
			 * be stopped, all shadows are removed, then ALTER RESOURCE QUEUE
			 * statement is canceled.
			 *------------------------------------------------------------------
			 */
			elog(WARNING, "ConnID %d. %s", newconn->ConnID, errorbuf);

			if ( !queuechanged || rm_force_alterqueue_cancel_queued_request )
			{
				buildAcquireResourceErrorResponse(newconn, res, errorbuf);
				transformConnectionTrackProgress(newconn,
												 CONN_PP_RESOURCE_QUEUE_ALLOC_FAIL);
				/*
				 * We still add this failed connection track into the shadow
				 * instance, we will remove them later.
				 */
				insertDQueueTailNode(&(shadowtrack->QueryResRequests), newconn);
			}
			else
			{
				elog(WARNING, "Resource manager finds conflict between at least "
							  "one queued query resource and new definition of "
							  "resource queue %s",
							  quetrack->QueueInfo->Name);
				freeUsedConnectionTrack(newconn);
				return RESQUEMGR_ALTERQUEUE_CONFILICT;
			}
		}
	DQUEUE_LOOP_END

	elog(DEBUG3, "Deadlock detector in shadow has %d MB in use %d MB locked "
				 "after rebuilding.",
				 shadowtrack->DLDetector.InUseTotal.MemoryMB,
				 shadowtrack->DLDetector.LockedTotal.MemoryMB);

	elog(RMLOG, "Finished rebuilding resource queue %s dynamic status in its "
				"shadow.",
			    quetrack->QueueInfo->Name);

	return FUNC_RETURN_OK;
}

int detectAndDealWithDeadLockInShadow(DynResourceQueueTrack quetrack,
									  bool					queuechanged)
{
	Assert(quetrack != NULL);
	Assert(quetrack->ShadowQueueTrack != NULL);
	DynResourceQueueTrack shadowtrack = quetrack->ShadowQueueTrack;

	elog(DEBUG3, "Deadlock detector in shadow has %d MB in use, %d MB locked",
				 shadowtrack->DLDetector.InUseTotal.MemoryMB,
				 shadowtrack->DLDetector.LockedTotal.MemoryMB);

	/* Assume more available resource unlocked queued requests. */
	int32_t pavailmemorymb = 0;

	/* Go through all queued query resource requests, recalculate the request. */
	DQUEUE_LOOP_BEGIN(&(shadowtrack->QueryResRequests), iter, ConnectionTrack, conn)

		/* Maybe there are some connection track has FAIL status. */
		if ( conn->Progress != CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT )
		{
			continue;
		}

		/* Check if this connection has deadlock issue. */
		int32_t expmemorymb   = conn->SegMemoryMB * conn->SegNumMin;
		int32_t availmemorymb = shadowtrack->ClusterMemoryMaxMB -
								shadowtrack->DLDetector.LockedTotal.MemoryMB +
								pavailmemorymb;

		/*----------------------------------------------------------------------
		 * If the queue already uses more resource than its maximum capability,
		 * we cannot count the resource available more than maximum possible
		 * limit.
		 *----------------------------------------------------------------------
		 */
		availmemorymb = availmemorymb > shadowtrack->ClusterMemoryMaxMB ?
						shadowtrack->ClusterMemoryMaxMB :
						availmemorymb;

		/* NOTE: availmemorymb maybe less than 0. */
		if ( expmemorymb > availmemorymb )
		{
			/* We encounter a deadlock issue. */
			if ( !queuechanged || rm_force_alterqueue_cancel_queued_request )
			{
				cancelQueryRequestToBreakDeadLockInShadow(shadowtrack,
														  iter,
														  expmemorymb,
														  availmemorymb);
			}
			else
			{
				elog(WARNING, "Resource manager finds at least one deadlock issue"
							  "due to new definition of resource queue %s",
							  quetrack->QueueInfo->Name);
				return RESQUEMGR_ALTERQUEUE_CONFILICT;
			}
		}

		/*----------------------------------------------------------------------
		 * When we try next request in the queue, we should assume the previous
		 * sessions can release resource. This works because we assume that the
		 * sessions waiting for resource allocation have unique session id values.
		 *----------------------------------------------------------------------
		 */
		SessionTrack strack = findSession(&(shadowtrack->DLDetector),
										  conn->SessionID);
		if ( strack != NULL )
		{
			pavailmemorymb += strack->InUseTotal.MemoryMB;
		}

	DQUEUE_LOOP_END

	return FUNC_RETURN_OK;
}

void cancelQueryRequestToBreakDeadLockInShadow(DynResourceQueueTrack shadowtrack,
											   DQueueNode			 iter,
											   int32_t				 expmemorymb,
											   int32_t				 availmemorymb)
{
	static char errorbuf[ERRORMESSAGE_SIZE];
	DQueueNode tailiter = getDQueueContainerTail(&(shadowtrack->QueryResRequests));

	elog(DEBUG3, "ConnID %d. Try to deal with deadlock issue.",
				 ((ConnectionTrack)(iter->Data))->ConnID);

	/*--------------------------------------------------------------------------
	 * Loop to try all subsequent requests.
	 *
	 * NOTE: If canceling all subsequent requests, we still can not satisfy this
	 * request we cancel current requests.
	 *--------------------------------------------------------------------------
	 */
	while((availmemorymb < expmemorymb) && tailiter != iter->Prev)
	{
		ConnectionTrack	curconn	= (ConnectionTrack)(tailiter->Data);

		if ( curconn->Progress != CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT )
		{
			tailiter = tailiter->Prev;
			continue;
		}

		SessionTrack 	strack	= NULL;
		strack = findSession(&(shadowtrack->DLDetector), curconn->SessionID);
		if ( strack != NULL )
		{
			/* This session has locked resource, cancel it to release resource. */
			snprintf(errorbuf, sizeof(errorbuf),
					 "session "INT64_FORMAT" deadlock is detected",
					 curconn->SessionID);

			elog(WARNING, "ConnID %d, %s", curconn->ConnID, errorbuf);

			availmemorymb += strack->InUseTotal.MemoryMB;

			availmemorymb = availmemorymb > shadowtrack->ClusterMemoryMaxMB ?
							shadowtrack->ClusterMemoryMaxMB :
							availmemorymb;

			/* Unlock the resource. */
			unlockSessionResource(&(shadowtrack->DLDetector), curconn->SessionID);

			/* Cancel this request. */
			buildAcquireResourceErrorResponse(curconn,
											  RESQUEMGR_DEADLOCK_DETECTED,
											  errorbuf);

			transformConnectionTrackProgress(curconn,
											 CONN_PP_RESOURCE_QUEUE_ALLOC_FAIL);
		}
		tailiter = tailiter->Prev;
	}
}

void applyResourceQueueTrackChangesFromShadows(List *quehavingshadow)
{
	ListCell *cell = NULL;
	foreach(cell, quehavingshadow)
	{
		DynResourceQueueTrack quetrack = (DynResourceQueueTrack)lfirst(cell);
		DynResourceQueueTrack shadowtrack = quetrack->ShadowQueueTrack;

		/* Update resource queue info. */
		memcpy(quetrack->QueueInfo,
			   shadowtrack->QueueInfo,
			   sizeof(DynResourceQueueData));

		/* Update resource queue track info. */
		quetrack->ClusterMemoryActPer	= shadowtrack->ClusterMemoryActPer;
		quetrack->ClusterMemoryMaxMB	= shadowtrack->ClusterMemoryMaxMB;
		quetrack->ClusterMemoryMaxPer	= shadowtrack->ClusterMemoryMaxPer;

		quetrack->ClusterVCoreActPer	= shadowtrack->ClusterVCoreActPer;
		quetrack->ClusterVCoreMax		= shadowtrack->ClusterVCoreMax;
		quetrack->ClusterVCoreMaxPer	= shadowtrack->ClusterVCoreMaxPer;

		quetrack->ClusterSegNumber		= shadowtrack->ClusterSegNumber;
		quetrack->ClusterSegNumberMax	= shadowtrack->ClusterSegNumberMax;

		quetrack->CurConnCounter		= shadowtrack->CurConnCounter;
		quetrack->NumOfRunningQueries	= shadowtrack->NumOfRunningQueries;

		quetrack->MemCoreRatio			= shadowtrack->MemCoreRatio;
		quetrack->RatioIndex			= shadowtrack->RatioIndex;

		quetrack->trackedMemCoreRatio	= shadowtrack->trackedMemCoreRatio;
		quetrack->isBusy				= shadowtrack->isBusy;

		/* The deadlock detector should use the new one completely. */
		resetResourceDeadLockDetector(&(quetrack->DLDetector));
		copyResourceDeadLockDetectorWithoutLocking(&(shadowtrack->DLDetector),
												   &(quetrack->DLDetector));

		resetResourceBundleDataByBundle(&(quetrack->TotalUsed),
										&(shadowtrack->TotalUsed));
		resetResourceBundleDataByBundle(&(quetrack->TotalAllocated),
										&(shadowtrack->TotalAllocated));

		resetResourceBundleDataByBundle(&(quetrack->TotalRequest),
										&(shadowtrack->TotalRequest));

		quetrack->expectMoreResource	= false;
		quetrack->pauseAllocation 		= false;
		quetrack->troubledByFragment	= false;

		if ( quetrack->TotalUsed.MemoryMB > quetrack->ClusterMemoryMaxMB )
		{
			quetrack->pauseAllocation = true;
		}

		/*
		 * Update queued resource requests. We should lock resource again and
		 * handle the failed requests if there are some found in the queue.
		 * The canceled requests should have the error message sent out.
		 */

		DQueueNode shadowiter = shadowtrack->QueryResRequests.Head;
		DQueueNode iter = quetrack->QueryResRequests.Head;
		while(iter != NULL)
		{
			ConnectionTrack conn = (ConnectionTrack)(iter->Data);
			ConnectionTrack shadowconn = (ConnectionTrack)(shadowiter->Data);
			if ( shadowconn->Progress != CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT )
			{
				/* Remove failed connection track and adjust iterator */
				DQueueNode nextiter = iter->Next;
				removeDQueueNode(&(quetrack->QueryResRequests), iter);
				iter = nextiter;

				/* Set fail status of this connection track. */
				conn->Progress = shadowconn->Progress;

				/* Get built error message. */
				conn->MessageID    = shadowconn->MessageID;
				conn->MessageMark1 = shadowconn->MessageMark1;
				conn->MessageMark2 = shadowconn->MessageMark2;
				conn->MessageSize  = shadowconn->MessageSize;

				setConnectionTrackMessageBuffer(
					conn,
					SMBUFF_CONTENT(&(shadowconn->MessageBuff)),
					getSMBContentSize(&(shadowconn->MessageBuff)));

				/* Send out the message. */
				conn->ResponseSent = false;
				MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
				PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conn);
				MEMORY_CONTEXT_SWITCH_BACK

				/* Recycle connection track instance. */
				Assert(quetrack->CurConnCounter > 0);

				elog(DEBUG3, "Resource queue %s has %d connections before handling "
							 "deadlocked connection ConnID %d detected from a "
							 "shadow.",
							 quetrack->QueueInfo->Name,
							 quetrack->CurConnCounter,
							 conn->ConnID);

				quetrack->CurConnCounter--;
				if ( quetrack->CurConnCounter == 0 )
				{
					elog(RMLOG, "Resource queue %s becomes idle after applying "
								"change from its shadow.",
								quetrack->QueueInfo->Name);
					quetrack->isBusy = false;
					refreshMemoryCoreRatioLimits();
					refreshMemoryCoreRatioWaterMark();
				}
			}
			else
			{
				/* Get updated resource quota. */
				copyResourceQuotaConnectionTrack(shadowconn, conn);

				/* Lock corresponding sessions. */
				createAndLockSessionResource(&(quetrack->DLDetector),
											 conn->SessionID);

				/* Adjust iterator. */
				iter = iter->Next;
			}
			shadowiter = shadowiter->Next;
		}
	}
}

void dumpResourceQueueStatus(const char *filename)
{
	if ( filename == NULL ) { return; }
	FILE *fp = fopen(filename, "w");

	fprintf(fp, "Maximum capacity of queue in global resource manager cluster %lf",
				PQUEMGR->GRMQueueMaxCapacity);
	fprintf(fp, "Number of resource queues : %d\n", list_length(PQUEMGR->Queues));

	/* Output each resource queue. */
	ListCell *cell = NULL;
	foreach(cell, PQUEMGR->Queues)
	{
		DynResourceQueueTrack quetrack = lfirst(cell);

		fprintf(fp, "QUEUE(name=%s:parent=%s:children=%d:busy=%d:paused=%d),",
				    quetrack->QueueInfo->Name,
					quetrack->ParentTrack != NULL ?
							quetrack->ParentTrack->QueueInfo->Name :
							"NULL",
					list_length(quetrack->ChildrenTracks),
					quetrack->isBusy ? 1 : 0,
					quetrack->pauseAllocation ? 1 : 0);

		fprintf(fp, "REQ(conn=%d:request=%d:running=%d),",
					quetrack->CurConnCounter,
					quetrack->QueryResRequests.NodeCount,
					quetrack->NumOfRunningQueries);

		fprintf(fp, "SEGCAP(ratio=%u:ratioidx=%d:segmem=%dMB:segcore=%lf:"
					"segnum=%d:segnummax=%d),",
					quetrack->MemCoreRatio,
					quetrack->RatioIndex,
					quetrack->QueueInfo->SegResourceQuotaMemoryMB,
					quetrack->QueueInfo->SegResourceQuotaVCore,
					quetrack->ClusterSegNumber,
					quetrack->ClusterSegNumberMax);

		fprintf(fp, "QUECAP(memmax=%u:coremax=%lf:"
					"memper=%lf:mempermax=%lf:coreper=%lf:corepermax=%lf),",
					quetrack->ClusterMemoryMaxMB,
					quetrack->ClusterVCoreMax,
					quetrack->ClusterMemoryActPer,
					quetrack->ClusterMemoryMaxPer,
					quetrack->ClusterVCoreActPer,
					quetrack->ClusterVCoreMaxPer);

		fprintf(fp, "QUEUSE(alloc=(%u MB,%lf CORE):"
					       "request=(%u MB,%lf CORE):"
					       "inuse=(%u MB,%lf CORE))\n",
					quetrack->TotalAllocated.MemoryMB,
					quetrack->TotalAllocated.Core,
					quetrack->TotalRequest.MemoryMB,
					quetrack->TotalRequest.Core,
					quetrack->TotalUsed.MemoryMB,
					quetrack->TotalUsed.Core);
	}

	fprintf(fp, "Number of mem/core ratios : %d\n", PQUEMGR->RatioCount);

	/* Output each mem/core ratio. */
	for( int i = 0 ; i < PQUEMGR->RatioCount ; ++i )
	{
		fprintf(fp, "RATIO(ratio=%u:",
					PQUEMGR->RatioReverseIndex[i]);

		if ( PQUEMGR->RatioWaterMarks[i].NodeCount == 0 )
		{
			fprintf(fp, "mem=0MB:core=0.0:time=NULL)\n");
		}
		else
		{
			DynMemoryCoreRatioWaterMark mark =
				(DynMemoryCoreRatioWaterMark)
				getDQueueHeadNodeData(&(PQUEMGR->RatioWaterMarks[i]));
			fprintf(fp, "mem=%uMB:core=%lf:time=%s)\n",
						mark->ClusterMemoryMB,
						mark->ClusterVCore,
						format_time_microsec(mark->LastRecordTime*1000000));
		}
	}

	fclose(fp);
}
