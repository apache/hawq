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
#include "resourcebroker/resourcebroker_API.h"
#include "resourcebroker/resourcebroker_NONE.h"
#include "resourcebroker/resourcebroker_LIBYARN.h"

bool					ResourceManagerIsForked;
RB_FunctionEntriesData	CurrentRBImp;
List				   *PreferedHostsForGRMContainers;

void RB_prepareImplementation(enum RB_IMP_TYPE imptype)
{
	/* Initialize resource broker implement handlers. */
	CurrentRBImp.acquireResource    = NULL;
	CurrentRBImp.freeClusterReport  = NULL;
	CurrentRBImp.getClusterReport   = NULL;
	CurrentRBImp.handleNotification = NULL;
	CurrentRBImp.handleSigSIGCHLD   = NULL;
	CurrentRBImp.returnResource     = NULL;
	CurrentRBImp.start				= NULL;
	CurrentRBImp.stop				= NULL;

	PreferedHostsForGRMContainers	= NULL;

	switch(imptype) {
	case NONE_HAWQ2:
		RB_NONE_createEntries(&CurrentRBImp);
		break;
	case YARN_LIBYARN:
		RB_LIBYARN_createEntries(&CurrentRBImp);
		break;
	default:
		Assert(false);
	}
}

/* Start resource broker service. */
int RB_start(bool isforked)
{
	return CurrentRBImp.start != NULL ? CurrentRBImp.start(isforked) : FUNC_RETURN_OK;
}
/* Stop resource broker service. */
int RB_stop(void)
{
	return CurrentRBImp.stop != NULL ? CurrentRBImp.stop() : FUNC_RETURN_OK;
}

int RB_getClusterReport(const char *queuename, List **machines, double *maxcapacity)
{
	if ( CurrentRBImp.getClusterReport != NULL )
	{
		return CurrentRBImp.getClusterReport(queuename, machines, maxcapacity);
	}

	*maxcapacity = 1;
	PRESPOOL->RBClusterReportCounter++;

	return FUNC_RETURN_OK;
}

/* Acquire and return resource. */
int RB_acquireResource(uint32_t memorymb, uint32_t core, List *preferred)
{
	Assert(CurrentRBImp.acquireResource != NULL);
	int res = CurrentRBImp.acquireResource(memorymb, core, preferred);

	/*--------------------------------------------------------------------------
	 * We hold preferred host list here, when resource broker gets the resource
	 * allocation result, this is the reference for updating the counter in each
	 * segment for failing of getting GRM containers from GRM.
	 *--------------------------------------------------------------------------
	 */
	if ( PreferedHostsForGRMContainers != NULL )
	{
		RB_freePreferedHostsForGRMContainers();
	}
	PreferedHostsForGRMContainers = preferred;
	return res;
}

int RB_returnResource(List **containers)
{
	Assert(CurrentRBImp.returnResource != NULL);
	return CurrentRBImp.returnResource(containers);
}

int RB_getContainerReport(List **ctnstat)
{
	if( CurrentRBImp.getContainerReport != NULL )
	{
		return CurrentRBImp.getContainerReport(ctnstat);
	}
	return FUNC_RETURN_OK;
}

int RB_handleNotifications(void)
{
	return CurrentRBImp.handleNotification != NULL ?
		   CurrentRBImp.handleNotification() :
		   FUNC_RETURN_OK;
}

void RB_handleSignalSIGCHLD(void)
{
	if (CurrentRBImp.handleSigSIGCHLD != NULL)
	{
		CurrentRBImp.handleSigSIGCHLD();
	}
}

void RB_handleError(int errorcode)
{
	Assert(CurrentRBImp.handleError != NULL);
	CurrentRBImp.handleError(errorcode);
}
bool isCleanGRMResourceStatus(void)
{
	Assert(DRMGlobalInstance != NULL);
	return DRMGlobalInstance->ResBrokerTriggerCleanup == true;
}

void setCleanGRMResourceStatus(void)
{
	Assert(DRMGlobalInstance != NULL);
	DRMGlobalInstance->ResBrokerTriggerCleanup = true;
	elog(WARNING, "Resource manager went into cleanup phase due to error from "
				  "resource broker.");
}

void unsetCleanGRMResourceStatus(void)
{
	Assert(DRMGlobalInstance != NULL);
	DRMGlobalInstance->ResBrokerTriggerCleanup = false;
	elog(WARNING, "Resource manager left cleanup phase.");
}

void RB_clearResource(List **ctnl)
{
	while( (*ctnl) != NULL )
	{
		GRMContainer ctn = (GRMContainer)lfirst(list_head(*ctnl));
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		(*ctnl) = list_delete_first(*ctnl);
		MEMORY_CONTEXT_SWITCH_BACK

		elog(LOG, "Resource broker dropped GRM container "INT64_FORMAT
				  "(%d MB, %d CORE) on host %s",
				  ctn->ID,
				  ctn->MemoryMB,
				  ctn->Core,
				  ctn->HostName == NULL ? "NULL" : ctn->HostName);

		if ( ctn->CalcDecPending )
		{
			minusResourceBundleData(&(ctn->Resource->DecPending),
									ctn->MemoryMB,
									ctn->Core);
			Assert( ctn->Resource->DecPending.Core >= 0 );
			Assert( ctn->Resource->DecPending.MemoryMB >= 0 );
		}

		/* Destroy resource container. */
		freeGRMContainer(ctn);
		PRESPOOL->RetPendingContainerCount--;
	}
}

void RB_freePreferedHostsForGRMContainers(void)
{
	ListCell *cell = NULL;
	foreach(cell, PreferedHostsForGRMContainers)
	{
		PAIR pair = (PAIR)lfirst(cell);
		rm_pfree(PCONTEXT, pair->Value);
		rm_pfree(PCONTEXT, pair);
	}
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	list_free(PreferedHostsForGRMContainers);
	MEMORY_CONTEXT_SWITCH_BACK

	PreferedHostsForGRMContainers = NULL;
}

void RB_updateSegmentsHavingNoExpectedGRMContainers(HASHTABLE segments)
{
	ListCell *cell = NULL;
	foreach(cell, PreferedHostsForGRMContainers)
	{
		PAIR pair = (PAIR)lfirst(cell);
		SegResource 	segres = (SegResource)(pair->Key);
		ResourceBundle	expres = (ResourceBundle)(pair->Value);

		bool failed = false;
		/* Check if the segment exists in the hash table. */
		PAIR pair2 = getHASHTABLENode(segments, segres);
		if ( pair2 == NULL )
		{
			elog(RMLOG, "Resource manager finds segment %s has no resource "
						"container allocated from global resource manager, "
						"expected resource quota (%d MB, %lf CORE)",
						GET_SEGRESOURCE_HOSTNAME(segres),
						expres->MemoryMB,
						expres->Core);
			failed = true;
		}
		else
		{
			ResourceBundle allocres = (ResourceBundle)(pair->Value);
			if ( allocres->MemoryMB < expres->MemoryMB )
			{
				elog(RMLOG, "Resource manager finds segment %s hasn't sufficient "
							"resource containers allocated from global resource "
							"manager, expected resource quota (%d MB, %lf CORE), "
							"actual allocated resource (%d MB, %lf CORE)",
							GET_SEGRESOURCE_HOSTNAME(segres),
							expres->MemoryMB,
							expres->Core,
							allocres->MemoryMB,
							allocres->Core);
				failed = true;
			}
		}

		if ( failed )
		{
			segres->GRMContainerFailAllocCount++;

			elog(WARNING, "Resource manager detects segment %s hasn't gotten "
						  "expected quantity of global resource containers for "
						  "%d times.",
						  GET_SEGRESOURCE_HOSTNAME(segres),
						  segres->GRMContainerFailAllocCount);
		}
		else
		{
			segres->GRMContainerFailAllocCount = 0;
		}
	}
}
