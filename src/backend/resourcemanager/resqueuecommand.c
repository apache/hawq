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
#include "dynrm.h"
#include "resqueuecommand.h"
#include "miscadmin.h"
#include "communication/rmcomm_QD2RM.h"
#include "utils/linkedlist.h"

#include "catalog/pg_resqueue.h"
#include "utils/resscheduler.h"
#include "commands/defrem.h"

/*******************************************************************************
 * This file contains all functions for creating, altering and dropping resource
 * queue through SQL DDL statement. All statement information is saved in the
 * argument stmt.
 ******************************************************************************/

void validateDDLAttributeOptions(List *options);

/*
 * CREATE RESOURCE QUEUE statement handler.
 */
void createResourceQueue(CreateQueueStmt *stmt)
{
	int 		 res 			 = FUNC_RETURN_OK;
	static char  errorbuf[1024];
	Relation	 pg_resqueue_rel;
	cqContext	 cqc;

	/* Permission check - only superuser can create queues. */
	if (!superuser())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create resource queues")));
	}
	/*
     * MPP-7960: We cannot run CREATE RESOURCE QUEUE inside a user transaction
     * block because the shared memory structures are not cleaned up on abort,
     * resulting in "leaked", unreachable queues.
     */

    if (Gp_role == GP_ROLE_DISPATCH)
    {
        PreventTransactionChain((void *) stmt, "CREATE RESOURCE QUEUE");
    }

    /* Validate options. */
    validateDDLAttributeOptions(stmt->options);

	/*
	 * Check for an illegal name ('none' is used to signify no queue in ALTER
	 * ROLE).
	 */
	if (strcmp(stmt->queue, "none") == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("resource queue name %s is reserved",
						stmt->queue),
				 errOmitLocation(true)));
	}
	/*
	 * Check the pg_resqueue relation to be certain the queue doesn't already
	 * exist.
	 */
	pg_resqueue_rel = heap_open(ResQueueRelationId, RowExclusiveLock);

	if (caql_getcount(
			caql_addrel(cqclr(&cqc), pg_resqueue_rel),
			cql("SELECT COUNT(*) FROM pg_resqueue WHERE rsqname = :1",
				CStringGetDatum(stmt->queue))))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("resource queue \'%s\' already exists",
						stmt->queue)));
	}

	heap_close(pg_resqueue_rel, NoLock);
    /*
     * Build the create resource queue request and send it to HAWQ RM process.
     * Basically, HAWQ RM runs all necessary logic to verify the statement and
     * apply the change. Therefore, QD only sends out the original information
     * and waits for the response.
     */
	int resourceid = 0;
	res = createNewResourceContext(&resourceid);
	if ( res != FUNC_RETURN_OK )
	{
		Assert( res == COMM2RM_CLIENT_FULL_RESOURCECONTEXT );
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("too many resource contexts were created")));
	}

	/* Here, using user oid is more convenient. */
	res = registerConnectionInRMByOID(resourceid,
									  GetUserId(),
									  errorbuf,
									  sizeof(errorbuf));
	if ( res != FUNC_RETURN_OK )
	{
		releaseResourceContextWithErrorReport(resourceid);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("%s", errorbuf)));
	}

    res = manipulateResourceQueue(resourceid,
    							  stmt->queue,
    							  MANIPULATE_RESQUEUE_CREATE,
    							  stmt->options,
    							  errorbuf,
    							  sizeof(errorbuf));

	/* We always unregister connection. */
	unregisterConnectionInRMWithErrorReport(resourceid);

	/* We always release resource context. */
	releaseResourceContextWithErrorReport(resourceid);

	if ( res != FUNC_RETURN_OK )
	{
		ereport(ERROR,
				(errcode(IS_TO_RM_RPC_ERROR(res) ?
						 ERRCODE_INTERNAL_ERROR :
						 ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("%s", errorbuf)));
	}
	elog(LOG, "Complete applying CREATE RESOURCE QUEUE statement.");
}

/*******************************************************************************
 * DROP RESOURCE QUEUE statement handler.
 * stmt[in]		The parsed statement tree.
 ******************************************************************************/
void dropResourceQueue(DropQueueStmt *stmt)
{
	int res 		= FUNC_RETURN_OK;
	char errorbuf[1024];
	Relation	 pg_resqueue_rel;
	HeapTuple	 tuple;
	cqContext	 cqc;
	cqContext	*pcqCtx;
	Oid			 queueid;

	/* Permission check - only superuser can create queues. */
	if (!superuser())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to drop resource queues")));
	}

	/* Cannot DROP default and root queue  */
	if ( strcmp(stmt->queue, RESOURCE_QUEUE_DEFAULT_QUEUE_NAME) == 0 ||
		 strcmp(stmt->queue, RESOURCE_QUEUE_ROOT_QUEUE_NAME) == 0 )
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot drop system resource queue \'%s\'",
						stmt->queue)));
	}

	/*
	 * Check the pg_resqueue relation to be certain the queue already
	 * exists.
	 */
	pg_resqueue_rel = heap_open(ResQueueRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), pg_resqueue_rel);

	tuple = caql_getfirst(pcqCtx,
						  cql("SELECT * FROM pg_resqueue WHERE rsqname = :1 FOR UPDATE",
						  CStringGetDatum(stmt->queue)));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("resource queue \'%s\' does not exist",
						stmt->queue)));

	/* Remember the Oid */
	queueid = HeapTupleGetOid(tuple);

	/*
	 * Check to see if any roles are in this queue.
	 */
	if (caql_getcount(
			NULL,
			cql("SELECT COUNT(*) FROM pg_authid WHERE rolresqueue = :1",
				ObjectIdGetDatum(queueid))))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
				 errmsg("resource queue \'%s\' is used by at least one role",
						stmt->queue)));
	}

	heap_close(pg_resqueue_rel, NoLock);

	/*
	 * MPP-7960: We cannot run DROP RESOURCE QUEUE inside a user transaction
	 * block because the shared memory structures are not cleaned up on abort,
	 * resulting in "leaked", unreachable queues.
	 */

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		PreventTransactionChain((void *) stmt, "DROP RESOURCE QUEUE");
	}

	/*
	 * Build the drop resource queue request and send it to HAWQ RM process.
	 * Basically, HAWQ RM runs all necessary logic to verify the statement and
	 * apply the change. Therefore, QD only sends out the original information
	 * and waits for the response.
	 */
	int resourceid = 0;
	res = createNewResourceContext(&resourceid);
	if ( res != FUNC_RETURN_OK ) {
		Assert( res == COMM2RM_CLIENT_FULL_RESOURCECONTEXT );
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("too many resource contexts were created")));
	}

	/* Here, using user oid is more convenient. */
	res = registerConnectionInRMByOID(resourceid,
									  GetUserId(),
									  errorbuf,
									  sizeof(errorbuf));
	if ( res != FUNC_RETURN_OK ) {
		releaseResourceContextWithErrorReport(resourceid);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("%s", errorbuf)));
	}

	res = manipulateResourceQueue(resourceid,
								  stmt->queue,
								  MANIPULATE_RESQUEUE_DROP,
								  NULL,
								  errorbuf,
								  sizeof(errorbuf));

	/* We always unregister connection. */
	unregisterConnectionInRMWithErrorReport(resourceid);

	/* We always release resource context. */
	releaseResourceContextWithErrorReport(resourceid);

	if ( res != FUNC_RETURN_OK )
	{
		ereport(ERROR,
				(errcode(IS_TO_RM_RPC_ERROR(res) ?
						 ERRCODE_INTERNAL_ERROR :
						 ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("%s", errorbuf)));
	}

	elog(LOG, "Completed applying DROP RESOURCE QUEUE statement.");
}

/*******************************************************************************
 * ALTER RESOURCE QUEUE statement handler.
 * stmt[in]		The parsed statement tree.
 ******************************************************************************/
void alterResourceQueue(AlterQueueStmt *stmt)
{
	int 		res 		= FUNC_RETURN_OK;
	static char errorbuf[1024];

	Relation    pg_resqueue_rel;
	cqContext	cqc;

	/* Permission check - only superuser can create queues. */
	if (!superuser())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to alter resource queues")));
	}

	/* Cannot DROP default and root queue  */
	if ( strcmp(stmt->queue, RESOURCE_QUEUE_ROOT_QUEUE_NAME) == 0 )
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system resource queue \'%s\'",
						stmt->queue)));
	}

	/*
	 * MPP-7960: We cannot run ALTER RESOURCE QUEUE inside a user transaction
	 * block because the shared memory structures are not cleaned up on abort,
	 * resulting in "leaked", unreachable queues.
	 */

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		PreventTransactionChain((void *) stmt, "ALTER RESOURCE QUEUE");
	}

    /* Validate options. */
    validateDDLAttributeOptions(stmt->options);

	/*
	 * Check if resource queue exists
	 */
	pg_resqueue_rel = heap_open(ResQueueRelationId, RowExclusiveLock);

	if (caql_getcount(
			caql_addrel(cqclr(&cqc), pg_resqueue_rel),
			cql("SELECT COUNT(*) FROM pg_resqueue WHERE rsqname = :1",
				CStringGetDatum(stmt->queue))) == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("resource queue \'%s\' does not exist",
						stmt->queue)));
	}

	heap_close(pg_resqueue_rel, NoLock);

	/*
	 * Build the alter resource queue request and send it to HAWQ RM process.
	 * Basically, HAWQ RM runs all necessary logic to verify the statement and
	 * apply the change. Therefore, QD only sends out the original information
	 * and waits for the response.
	 */
	int resourceid = 0;
	res = createNewResourceContext(&resourceid);
	if (res != FUNC_RETURN_OK) {
		Assert(res == COMM2RM_CLIENT_FULL_RESOURCECONTEXT);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("too many existing resource context")));
	}

	/* Here, using user oid is more convenient. */
	res = registerConnectionInRMByOID(resourceid,
									  GetUserId(),
									  errorbuf,
									  sizeof(errorbuf));
	if ( res != FUNC_RETURN_OK ) {
		releaseResourceContextWithErrorReport(resourceid);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("%s", errorbuf)));
	}

	res = manipulateResourceQueue(resourceid,
								  stmt->queue,
								  MANIPULATE_RESQUEUE_ALTER,
								  stmt->options,
								  errorbuf,
								  sizeof(errorbuf));
	/* We always unregister connection. */
	unregisterConnectionInRMWithErrorReport(resourceid);

	/* We always release resource context. */
	releaseResourceContextWithErrorReport(resourceid);

	if ( res != FUNC_RETURN_OK )
	{
		ereport(ERROR,
				(errcode(IS_TO_RM_RPC_ERROR(res) ?
						 ERRCODE_INTERNAL_ERROR :
						 ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("%s", errorbuf)));
	}

	elog(LOG, "Completed applying ALTER RESOURCE QUEUE statement.");
}

#define VALID_DDL_DUP(index, defel, targref)				   \
		if (strcmp((defel)->defname, RSQDDLAttrNames[(index)]) == 0)		   \
		{																	   \
			if ((targref) != NULL)											   \
			{																   \
				ereport(ERROR,												   \
						(errcode(ERRCODE_SYNTAX_ERROR),						   \
						 errmsg("redundant attribute %s",					   \
								RSQDDLAttrNames[(index)])));				   \
			}																   \
			(targref) = (defel);											   \
			continue;														   \
		}

void validateDDLAttributeOptions(List *options)
{
	DefElem		*dparent		  	  = NULL;
	DefElem     *dactivelimit 	  	  = NULL;
	DefElem     *dmemorylimit 	  	  = NULL;
	DefElem     *dcorelimit 	  	  = NULL;
	DefElem		*dvsegresquota	  	  = NULL;
	DefElem		*dallocpolicy	  	  = NULL;
	DefElem		*dresovercommit  	  = NULL;
	DefElem		*dnvsegupperlimit 	  = NULL;
	DefElem		*dnvseglowerlimit 	  = NULL;
	DefElem		*dnvsegupperlimitpseg = NULL;
	DefElem		*dnvseglowerlimitpseg = NULL;

	Cost		 activelimit 	 = INVALID_RES_LIMIT_THRESHOLD;
	ListCell    *option			 = NULL;

	/* Extract options from the statement node tree, check duplicate options. */
	foreach(option, options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);
		VALID_DDL_DUP(RSQ_DDL_ATTR_PARENT, 						defel, dparent)
		VALID_DDL_DUP(RSQ_DDL_ATTR_ACTIVE_STATMENTS, 			defel, dactivelimit)
		VALID_DDL_DUP(RSQ_DDL_ATTR_MEMORY_LIMIT_CLUSTER, 		defel, dmemorylimit)
		VALID_DDL_DUP(RSQ_DDL_ATTR_CORE_LIMIT_CLUSTER, 			defel, dcorelimit)
		VALID_DDL_DUP(RSQ_DDL_ATTR_VSEG_RESOURCE_QUOTA, 		defel, dvsegresquota)
		VALID_DDL_DUP(RSQ_DDL_ATTR_ALLOCATION_POLICY, 			defel, dallocpolicy)
		VALID_DDL_DUP(RSQ_DDL_ATTR_RESOURCE_OVERCOMMIT_FACTOR,	defel, dresovercommit)
		VALID_DDL_DUP(RSQ_DDL_ATTR_NVSEG_UPPER_LIMIT, 			defel, dnvsegupperlimit)
		VALID_DDL_DUP(RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT, 			defel, dnvseglowerlimit)
		VALID_DDL_DUP(RSQ_DDL_ATTR_NVSEG_UPPER_LIMIT_PERSEG, 	defel, dnvsegupperlimitpseg)
		VALID_DDL_DUP(RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT_PERSEG, 	defel, dnvseglowerlimitpseg)
	}

	/* Perform range checks on the various thresholds.*/
	if (dactivelimit)
	{
		activelimit = (Cost) defGetInt64(dactivelimit);
		if ( activelimit < MINIMUM_RESQUEUE_ACTIVESTATS_N )
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s is less than %d",
						   RSQDDLAttrNames[RSQ_DDL_ATTR_ACTIVE_STATMENTS],
						   MINIMUM_RESQUEUE_ACTIVESTATS_N)));
		}
	}

	/* Memory and core expression must be the same. */
	if (dmemorylimit && dcorelimit)
	{
		bool  need_free_mem  = false;
		bool  need_free_core = false;
		char *memory_limit   = defGetString(dmemorylimit, &need_free_mem);
		char *core_limit     = defGetString(dcorelimit,   &need_free_core);

		if (memory_limit != NULL && core_limit != NULL)
		{
			if(strcmp(memory_limit, core_limit) != 0)
			{
				if(need_free_mem) { free(memory_limit); }
				if(need_free_core) { free(core_limit); }
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("the values of %s and %s are not identical",
								RSQDDLAttrNames[RSQ_DDL_ATTR_MEMORY_LIMIT_CLUSTER],
								RSQDDLAttrNames[RSQ_DDL_ATTR_CORE_LIMIT_CLUSTER])));
			}
		}
		else
		{
			if(need_free_mem) { free(memory_limit); }
			if(need_free_core) { free(core_limit); }
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("invalid value of %s or %s",
						   RSQDDLAttrNames[RSQ_DDL_ATTR_MEMORY_LIMIT_CLUSTER],
						   RSQDDLAttrNames[RSQ_DDL_ATTR_CORE_LIMIT_CLUSTER])));
		}
	}

	/*
	 * NVSEG_UPPER_LIMIT/NVSEG_LOWER_LIMIT has 0 as default value that means the
	 * setting is not effective, otherwise, it must be greater than 0.
	 */
	int64_t nvsegupperlimit = -1;
	int64_t nvseglowerlimit = -1;
	if (dnvsegupperlimit != NULL)
	{
		nvsegupperlimit = defGetInt64(dnvsegupperlimit);
		if (nvsegupperlimit < MINIMUM_RESQUEUE_NVSEG_UPPER_LIMIT_N)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s is less than %s",
							RSQDDLAttrNames[RSQ_DDL_ATTR_NVSEG_UPPER_LIMIT],
							MINIMUM_RESQUEUE_NVSEG_UPPER_LIMIT)));
		}
	}

	if (dnvseglowerlimit != NULL)
	{
		nvseglowerlimit = defGetInt64(dnvseglowerlimit);
		if (nvseglowerlimit < MINIMUM_RESQUEUE_NVSEG_LOWER_LIMIT_N)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s is less than %s",
							RSQDDLAttrNames[RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT],
							MINIMUM_RESQUEUE_NVSEG_LOWER_LIMIT)));
		}
	}

	/*
	 * NVSEG_UPPER_LIMIT_PERSEG/NVSEG_LOWER_LIMIT_PERSEG has 0 as default value
	 * that means the setting is not effective, otherwise, it must be greater
	 * than 0.
	 */
	double nvsegupperlimitpseg = -1.0;
	double nvseglowerlimitpseg = -1.0;
	if (dnvsegupperlimitpseg != NULL)
	{
		nvsegupperlimitpseg = defGetNumeric(dnvsegupperlimitpseg);
		if (nvsegupperlimitpseg < MINIMUM_RESQUEUE_NVSEG_UPPER_PERSEG_LIMIT_N)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s is less than %s",
							RSQDDLAttrNames[RSQ_DDL_ATTR_NVSEG_UPPER_LIMIT_PERSEG],
							MINIMUM_RESQUEUE_NVSEG_UPPER_PERSEG_LIMIT)));
		}
	}

	if (dnvseglowerlimitpseg != NULL)
	{
		nvseglowerlimitpseg = defGetNumeric(dnvseglowerlimitpseg);
		if (nvseglowerlimitpseg < MINIMUM_RESQUEUE_NVSEG_LOWER_PERSEG_LIMIT_N)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s is less than %s",
							RSQDDLAttrNames[RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT_PERSEG],
							MINIMUM_RESQUEUE_NVSEG_LOWER_PERSEG_LIMIT)));
		}
	}


	/* The resource upper factor must be no less than 1. */
	if( dresovercommit != NULL)
	{
		double resovercommit = defGetNumeric(dresovercommit);
		if (resovercommit < MINIMUM_RESQUEUE_OVERCOMMIT_N)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s is less than %s",
							RSQDDLAttrNames[RSQ_DDL_ATTR_RESOURCE_OVERCOMMIT_FACTOR],
							MINIMUM_RESQUEUE_OVERCOMMIT)));
		}
	}
}
