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

#include "resourcebroker/resourcebroker_NONE.h"
#include "resourcemanager.h"
#include "utils/kvproperties.h"
/*******************************************************************************
 * NONE mode does not need resource broker process to help negotiating with
 * global resource manager which does not exist at all.
 ******************************************************************************/

#define RB_NONE_INBUILDHOST 											   \
		SMBUFF_HEAD(SegmentTrackInformation, &machineidbuff)

/*
 *------------------------------------------------------------------------------
 * Global variables.
 *------------------------------------------------------------------------------
 */
int32_t			RoundRobinIndex		= 0;
uint32_t		ContainerIDCounter	= 0;

/*
 *------------------------------------------------------------------------------
 * RB NONE implementation.
 *------------------------------------------------------------------------------
 */
void RB_NONE_createEntries(RB_FunctionEntries entries)
{
	entries->acquireResource 	= RB_NONE_acquireResource;
	entries->returnResource		= RB_NONE_returnResource;
	entries->handleError		= RB_NONE_handleError;
}

/**
 * Acquire resource from hosts in NONE mode.
 *
 * This function use round-robin sequence to select available hosts in HAWQ RM
 * resource pool and choose suitable host to allocate containers.
 */
int RB_NONE_acquireResource(uint32_t memorymb, uint32_t core, List *preferred)
{

	int					contmemorymb  	= memorymb/core;
	int					contcount 		= core;
	int					contactcount	= 0;

	bool				hasallocated 	= false;
	GRMContainer 		newcontainer	= NULL;
	int					hostcount		= PRESPOOL->SegmentIDCounter;
	ListCell		   *cell			= NULL;

	elog(DEBUG3, "NONE mode resource broker received resource allocation request "
				 "(%d MB, %d CORE)",
				 memorymb,
				 core);

	/*
	 * If a list of hosts are preferred for new allocated resource, they are
	 * considered in first priority.
	 */
	foreach(cell, preferred)
	{
		PAIR pair = (PAIR)lfirst(cell);
		SegResource segres = (SegResource)(pair->Key);
		ResourceBundle resource = (ResourceBundle)(pair->Value);

		elog(DEBUG3, "Resource manager expects (%d MB, %lf CORE) resource on "
					 "segment %s",
					 resource->MemoryMB,
					 resource->Core,
					 GET_SEGRESOURCE_HOSTNAME(segres));

		if (!IS_SEGRESOURCE_USABLE(segres))
		{
			elog(DEBUG3, "Resource manager considers segment %s down. No GRM "
						 "containers to be allocated in this host.",
						 GET_SEGRESOURCE_HOSTNAME(segres));
			continue;
		}

		/* Check how many containers can be allocated in this segment. */
		int availctn = segres->Stat->FTSTotalCore -
					   segres->Allocated.Core -
					   segres->IncPending.Core;
		int availctn2 = (segres->Stat->FTSTotalMemoryMB -
						 segres->Allocated.MemoryMB -
						 segres->IncPending.MemoryMB) / contmemorymb;
		availctn = availctn < availctn2 ? availctn : availctn2;
		availctn = availctn < resource->Core ? availctn : resource->Core;

		elog(DEBUG3, "NONE mode resource broker allocates resource "
					 "(%d MB, %d CORE) x %d on segment %s",
					 contmemorymb,
					 1,
					 availctn,
					 GET_SEGRESOURCE_HOSTNAME(segres));

		for ( int i = 0 ; i < availctn ; ++i )
		{
			newcontainer = createGRMContainer(ContainerIDCounter,
											  contmemorymb,
											  1,
											  GET_SEGRESOURCE_HOSTNAME(segres),
										      segres);
			contactcount++;
			ContainerIDCounter++;
			addGRMContainerToToBeAccepted(newcontainer);

			if ( contactcount >= contcount )
			{
				break;
			}
		}

		if ( contactcount >= contcount )
		{
			break;
		}
	}

	elog(LOG, "NONE mode resource broker allocated containers "
			  "(%d MB, %d CORE) x %d. Expected %d containers.",
				contmemorymb,
				1,
				contactcount,
				contcount);

	/* Clean up pending resource quantity. */
	removePendingResourceRequestInRootQueue(contmemorymb * (contcount - contactcount),
											1            * (contcount - contactcount),
											contactcount > 0);
	return FUNC_RETURN_OK;
}

int RB_NONE_returnResource(List **ctnl)
{
	while( (*ctnl) != NULL )
	{
		GRMContainer ctn = (GRMContainer)lfirst(list_head(*ctnl));
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		(*ctnl) = list_delete_first(*ctnl);
		MEMORY_CONTEXT_SWITCH_BACK

		if ( ctn->CalcDecPending ) {
			minusResourceBundleData(&(ctn->Resource->DecPending), ctn->MemoryMB, ctn->Core);
			Assert( ctn->Resource->DecPending.MemoryMB >= 0 );
			Assert( ctn->Resource->DecPending.Core >= 0 );
		}

		elog(LOG, "NONE mode resource broker returned resource container "
				  "(%d MB, %d CORE) to host %s",
				  ctn->MemoryMB,
				  ctn->Core,
				  ctn->HostName == NULL ? "NULL" : ctn->HostName);

		/* Destroy resource container. */
		freeGRMContainer(ctn);
		PRESPOOL->RetPendingContainerCount--;
	}

	return FUNC_RETURN_OK;
}

void RB_NONE_handleError(int errorcode)
{
	/* Do nothing temporarily. */
}
