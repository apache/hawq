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

#include "postmaster/fork_process.h"

#include "resourcebroker/resourcebroker_LIBYARN.h"
#include "resourcebroker/resourcebroker_RM_RB_Protocol.h"
#include "resourcemanager.h"
#include "resourcepool.h"
#include "nodes/pg_list.h"
#include "utils/kvproperties.h"

int handleRB2RM_ClusterReport(void);
int handleRB2RM_AllocatedResource(void);
int handleRB2RM_ReturnedResource(void);
int handleRB2RM_ContainerReport(void);
void buildToReturnNotTrackedGRMContainers(RB_GRMContainerStat ctnstats, int size);

void freeResourceBundle(void *resbundle);
/*
 *------------------------------------------------------------------------------
 * Global variables.
 *------------------------------------------------------------------------------
 */
int				ResBrokerRequestPipe[2];/* Pipe to send request from RM to RB.*/
int				ResBrokerNotifyPipe[2];	/* Pipe to receive response from RB.  */
volatile bool	ResBrokerKeepRun;		/* Tell RB process to run or quit. 	  */
volatile bool	ResBrokerExits;			/* RB process exit signal 			  */
volatile pid_t	ResBrokerPID;			/* Running RB process PID. 			  */
pid_t			ResBrokerParentPID;		/* Parent PID. 						  */
volatile bool	PipeReceivePending;		/* If pipe is waiting for response.	  */

/*
 *------------------------------------------------------------------------------
 * RB YARN implementation.
 *------------------------------------------------------------------------------
 */
void RB_LIBYARN_createEntries(RB_FunctionEntries entries)
{
	entries->start 				= RB_LIBYARN_start;
	entries->stop  				= RB_LIBYARN_stop;
	entries->acquireResource 	= RB_LIBYARN_acquireResource;
	entries->returnResource		= RB_LIBYARN_returnResource;
	entries->getContainerReport = RB_LIBYARN_getContainerReport;
	entries->handleNotification = RB_LIBYARN_handleNotification;
	entries->getClusterReport	= RB_LIBYARN_getClusterReport;
	entries->handleSigSIGCHLD   = RB_LIBYARN_handleSignalSIGCHLD;
	entries->handleError		= RB_LIBYARN_handleError;

	ResBrokerPID 				= -1;
	ResBrokerExits				= false;

	ResBrokerRequestPipe[0]		= -1;
	ResBrokerRequestPipe[1]		= -1;
	ResBrokerNotifyPipe[0]		= -1;
	ResBrokerNotifyPipe[1]		= -1;
}

int RB_LIBYARN_start(bool isforked)
{
	int res = FUNC_RETURN_OK;

	/* If SIGCHLD is received, ResBrokerExits is set true, which means resource
	 * broker process might quits, so before starting new one, we have to do some
	 * clean up. */
	if ( ResBrokerExits )
	{
		/* Check if current resource broker process exits. */
		if ( ResBrokerPID > 0 )
		{
			int status = 0;
			if( ResBrokerPID != waitpid(ResBrokerPID, &status, WNOHANG) )
			{
				return FUNC_RETURN_OK;
			}
			if (WIFEXITED(status) && WEXITSTATUS(status))
			{
				elog(FATAL, "YARN mode resource broker failed to start resource "
							"broker process. error=%d",
							WEXITSTATUS(status));
			}
		}

		/* Resource broker quits, clean up the pipes and status. */
		ResBrokerPID   = -1;
		ResBrokerExits = false;

		/* Check and clear resource broker pending status. */
		clearPendingResourceRequestInRootQueue();
		PipeReceivePending = false;
	}

	/* Start the resource broker process. */
	if ( ResBrokerPID == -1 )
	{
		/* Close possible open pipes. */
		if ( ResBrokerRequestPipe[0] != -1 )
		{
			close(ResBrokerRequestPipe[0]);
			close(ResBrokerRequestPipe[1]);
		}
		if ( ResBrokerNotifyPipe[0] != -1 )
		{
			close(ResBrokerNotifyPipe[0]);
			close(ResBrokerNotifyPipe[1]);
		}

		/* Create pipe for communication between RM and RB. */
		ResBrokerRequestPipe[0] = -1;
		ResBrokerRequestPipe[1] = -1;
		ResBrokerNotifyPipe[0]  = -1;
		ResBrokerNotifyPipe[1]  = -1;

		res = pgpipe(ResBrokerRequestPipe);
		if ( res < 0 )
		{
			elog(FATAL, "YARN mode resource broker failed to create pipe between "
						"resource manager and resource broker. errno %d",
						errno);
		}

		res = pgpipe(ResBrokerNotifyPipe);
		if ( res < 0 )
		{
			elog(FATAL, "YARN mode resource broker failed to create pipe between "
						"resource manager and resource broker. errno %d",
						 errno);
		}

		ResBrokerExits     = false;
		ResBrokerParentPID = getpid();
		ResBrokerPID       = fork_process();

		switch(ResBrokerPID)
		{
		case 0:
			exit(ResBrokerMain());
		default:
			PipeReceivePending = false;
			elog(LOG, "YARN mode resource broker created resource broker process "
					  "PID=%d.",
					  ResBrokerPID);
		}

		return FUNC_RETURN_OK;
	}

	return FUNC_RETURN_OK;
}

/*
 * Resource broker process notifies resource manager that it quited.
 */
void RB_LIBYARN_handleSignalSIGCHLD(void)
{
	/* Mark the resource broker exited. */
	ResBrokerExits = true;
}

int RB_LIBYARN_stop(void)
{
	int waitres = 0;
	if ( ResBrokerPID > 0 )
	{
	    /* Send signal to resource broker process. */
		kill(ResBrokerPID, SIGINT);
		elog(LOG, "YARN mode resource broker sent SIGINT signal to stop resource "
				  "broker process.");
retry:
		/* Wait the exit of resource broker process. */
		waitres = waitpid(ResBrokerPID, NULL, 0);
		if ( waitres == -1 )
		{
			if ( errno == EINTR )
			{
				/* Wait is interrupted by signal. */
				goto retry;
			}
			else
			{
				elog(WARNING, "Fail to wait for the exit of resource broker "
						  	  "process %d. errno %d.",
							  ResBrokerPID,
							  errno);
				return FUNC_RETURN_FAIL;
			}
		}
	}

	ResBrokerPID = -1;
	elog(LOG, "YARN mode resource broker process exited.");
	return FUNC_RETURN_OK;
}

/*
 * Return cluster report including all node status. The response should be
 * handled asynchronously.
 */
int RB_LIBYARN_getClusterReport(const char  *quename,
								List 	   **machines,
								double 	    *maxcapacity)
{
	int		 res		= FUNC_RETURN_OK;
	uint32_t MessageID	= RM2RB_GET_CLUSTERREPORT;
	int		 piperes	= 0;

	if ( PipeReceivePending )
	{
		elog(DEBUG5, "YARN resource broker skip sending cluster report due to "
					 "not getting response of last request.");
		return FUNC_RETURN_OK;
	}

	/* Write request to pipe */
	SelfMaintainBufferData sendBuffer;
	initializeSelfMaintainBuffer(&sendBuffer, PCONTEXT);
	appendSMBVar(&sendBuffer, MessageID);
	appendSelfMaintainBufferTill64bitAligned(&sendBuffer);

	RPCRequestRBGetClusterReportHeadData request;
	request.QueueNameLen = __SIZE_ALIGN64(strlen(quename) + 1);
	request.Reserved     = 0;
	appendSMBVar(&sendBuffer, request);
	appendSMBStr(&sendBuffer, quename);
	appendSelfMaintainBufferTill64bitAligned(&sendBuffer);

	piperes = pipewrite(ResBrokerRequestPipe[1],
						sendBuffer.Buffer,
						sendBuffer.Cursor + 1);
	if ( piperes != sendBuffer.Cursor + 1 )
	{
		elog(WARNING, "YARN mode resource broker failed to generate cluster "
					  "report request to resource broker process through pipe. "
				      "Wrote length %d, expected length %d, errno %d",
				      piperes,
				      sendBuffer.Cursor + 1,
					  errno);
		res = RESBROK_PIPE_ERROR;
	}
	destroySelfMaintainBuffer(&sendBuffer);
	elog(LOG, "YARN mode resource broker generated cluster report request to "
			  "resource broker process.");
	PipeReceivePending = res == FUNC_RETURN_OK;
	return res;
}

/*
 * Acquire resource from hosts in LIBYARN mode.
 *
 * This function use round-robin sequence to select available hosts in HAWQ RM
 * resource pool and choose suitable host to allocate containers.
 */
int RB_LIBYARN_acquireResource(uint32_t memorymb, uint32_t core, List *preferred)
{
	int		 res	   = FUNC_RETURN_OK;
	uint32_t MessageID = RM2RB_ALLOC_RESOURCE;
	ListCell *cell = NULL;

	if ( PipeReceivePending )
	{
		return RESBROK_PIPE_BUSY;
	}

	elog(LOG, "Allocating container request is triggered.");

	/* Write request to pipe */
	SelfMaintainBufferData sendBuffer;
	initializeSelfMaintainBuffer(&sendBuffer, PCONTEXT);
	appendSMBVar(&sendBuffer, MessageID);
	appendSelfMaintainBufferTill64bitAligned(&sendBuffer);

	RPCRequestRBAllocateResourceContainersData request;
	request.ContainerCount = core;
	request.MemoryMB	   = memorymb / core;
	request.Core		   = 1;
	request.PreferredSize  = list_length(preferred);
	request.MsgLength      = 0;
	foreach(cell, preferred)
	{
		PAIR pair = (PAIR)lfirst(cell);
		SegResource segres = (SegResource)(pair->Key);
		char *hostname = GET_SEGINFO_GRMHOSTNAME(&(segres->Stat->Info));
		char *rackname = GET_SEGINFO_GRMRACKNAME(&(segres->Stat->Info));
		uint32_t preferredLen = sizeof(uint32_t)*2 +
								__SIZE_ALIGN64((strlen(hostname) + 1 + strlen(rackname) + 1));
		request.MsgLength += preferredLen;
	}
	appendSMBVar(&sendBuffer, request);

	/*
	 * parse preferred list and
	 * append (hostname, rackname, requested container number) into the message
	 */
	foreach(cell, preferred)
	{
		PAIR pair = (PAIR)lfirst(cell);
		SegResource segres = (SegResource)(pair->Key);
		ResourceBundle resource = (ResourceBundle)(pair->Value);
		int16_t num_containers = resource->Core;
		int16_t flag = 0; /* not used yet */
		char *hostname = GET_SEGINFO_GRMHOSTNAME(&(segres->Stat->Info));
		char *rackname = GET_SEGINFO_GRMRACKNAME(&(segres->Stat->Info));
		uint32_t preferredLen = sizeof(preferredLen) + sizeof(num_containers) + sizeof(flag) +
								__SIZE_ALIGN64((strlen(hostname) + 1 + strlen(rackname) + 1));
		appendSMBVar(&sendBuffer, preferredLen);
		appendSMBVar(&sendBuffer, num_containers);
		appendSMBVar(&sendBuffer, flag);
		appendSMBStr(&sendBuffer, hostname);
		appendSMBStr(&sendBuffer, rackname);
		appendSelfMaintainBufferTill64bitAligned(&sendBuffer);
		elog(LOG, "YARN mode resource broker build a preferred request."
				  "host:%s, rack:%s, container number:%d",
				  hostname, rackname, num_containers);
	}

	int piperes = pipewrite(ResBrokerRequestPipe[1],
				   	   	    sendBuffer.Buffer,
							sendBuffer.Cursor + 1);
	if ( piperes != sendBuffer.Cursor + 1 )
	{
		elog(WARNING, "YARN mode resource broker failed to write resource "
					  "allocation request to resource broker process. errno %d.",
					  errno);
		res = RESBROK_PIPE_ERROR;
	}

	elog(DEBUG3, "LIBYARN mode resource broker wrote %d bytes out.", sendBuffer.Cursor+1);

	destroySelfMaintainBuffer(&sendBuffer);
	elog(LOG, "YARN mode resource broker wrote resource allocation request to "
			  "resource broker process.");
	PipeReceivePending = res == FUNC_RETURN_OK;
	return res;
}

/*
 * Return resource to YARN.
 */
int RB_LIBYARN_returnResource(List **ctnl)
{
	int			res			= FUNC_RETURN_OK;
	uint32_t	MessageID	= RM2RB_RETURN_RESOURCE;

	if ( *ctnl == NULL )
	{
		return FUNC_RETURN_OK;
	}

	if ( PipeReceivePending )
	{
		return FUNC_RETURN_OK;
	}

	/* Write request to pipe */
	SelfMaintainBufferData sendBuffer;
	initializeSelfMaintainBuffer(&sendBuffer, PCONTEXT);
	appendSMBVar(&sendBuffer, MessageID);
	appendSelfMaintainBufferTill64bitAligned(&sendBuffer);

	RPCRequestRBReturnResourceContainersHeadData request;
	request.ContainerCount = list_length(*ctnl);
	request.Reserved	   = 0;
	appendSMBVar(&sendBuffer, request);

	while( (*ctnl) != NULL )
	{
		GRMContainer ctn = (GRMContainer)lfirst(list_head(*ctnl));
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		(*ctnl) = list_delete_first(*ctnl);
		MEMORY_CONTEXT_SWITCH_BACK

		appendSMBVar(&sendBuffer, ctn->ID);

		elog(LOG, "YARN mode resource broker returned resource container "INT64_FORMAT
				  "(%d MB, %d CORE) to host %s",
				  ctn->ID,
				  ctn->MemoryMB,
				  ctn->Core,
				  ctn->HostName == NULL ? "NULL" : ctn->HostName);

		if ( ctn->CalcDecPending )
		{
			minusResourceBundleData(&(ctn->Resource->DecPending), ctn->MemoryMB, ctn->Core);
			Assert( ctn->Resource->DecPending.Core >= 0 );
			Assert( ctn->Resource->DecPending.MemoryMB >= 0 );
		}

		/* Destroy resource container. */
		freeGRMContainer(ctn);
		PRESPOOL->RetPendingContainerCount--;
	}

	int piperes = pipewrite(ResBrokerRequestPipe[1],
				   	   	    sendBuffer.Buffer,
							sendBuffer.Cursor + 1);
	if ( piperes != sendBuffer.Cursor + 1 )
	{
		elog(WARNING, "YARN mode resource broker failed to write resource return "
					  "request to resource broker process. errno %d.",
					  errno);
		res = RESBROK_PIPE_ERROR;
	}
	destroySelfMaintainBuffer(&sendBuffer);
	elog(LOG, "YARN mode resource broker wrote resource return request to "
			  "resource broker process.");
	PipeReceivePending = res == FUNC_RETURN_OK;
	return res;
}

int RB_LIBYARN_getContainerReport(List **ctnstatl)
{
	int res = FUNC_RETURN_OK;
	uint32_t MessageID = RM2RB_GET_CONTAINERREPORT;

	if ( PipeReceivePending )
	{
		return FUNC_RETURN_OK;
	}

	elog(LOG, "Getting container report request is triggered.");

	/* Write request to pipe. */
	SelfMaintainBufferData sendBuffer;
	initializeSelfMaintainBuffer(&sendBuffer, PCONTEXT);
	appendSMBVar(&sendBuffer, MessageID);
	appendSelfMaintainBufferTill64bitAligned(&sendBuffer);

	int piperes = pipewrite(ResBrokerRequestPipe[1],
				   	   	    sendBuffer.Buffer,
							sendBuffer.Cursor + 1);
	if ( piperes != sendBuffer.Cursor + 1 )
	{
		elog(WARNING, "YARN mode resource broker failed to write get container "
					  "status request to resource broker process. errno %d.",
					  errno);
		res = RESBROK_PIPE_ERROR;
	}
	destroySelfMaintainBuffer(&sendBuffer);
	elog(LOG, "YARN mode resource broker wrote get container status request to "
			  "resource broker process.");
	PipeReceivePending = res == FUNC_RETURN_OK;
	return res;
}

int RB_LIBYARN_handleNotification(void)
{
	/* Check if the pipe has notifications. */
	fd_set      	rfds;
	struct timeval 	timeout;
	int				res;
	int				fd = ResBrokerNotifyPipe[0];
	uint32_t		messagehead[2];
	uint32_t		messageid;



	FD_ZERO(&rfds);
	FD_SET(fd, &rfds);
	timeout.tv_sec  = 0;
	timeout.tv_usec = 0;

	res = select(fd + 1, &rfds, NULL, NULL, &timeout);
	/* Something passes to RM process. */
	if ( res > 0 && FD_ISSET(fd, &rfds) )
	{
		/* Read message id. */
		res = readPipe(fd, messagehead, sizeof(messagehead));
		if ( res != sizeof(messagehead) )
		{
			return RESBROK_PIPE_ERROR;
		}

		messageid = messagehead[0];

		switch( messageid )
		{
			case RB2RM_CLUSTERREPORT:
				res = handleRB2RM_ClusterReport();
				break;
			case RB2RM_ALLOCATED_RESOURCE:
				res = handleRB2RM_AllocatedResource();
				break;
			case RB2RM_RETURNED_RESOURCE:
				res = handleRB2RM_ReturnedResource();
				break;
			case RB2RM_CONTAINERREPORT:
				res = handleRB2RM_ContainerReport();
				break;
			default:
				res = RESBROK_PIPE_ERROR;
		}

		PipeReceivePending = false;
		elog(LOG, "Finish processing message %d", messageid);

		if ( res != FUNC_RETURN_OK )
		{
			elog(WARNING, "Resource broker failed to receive correct message "
						  "from YARN mode resource broker process. message id %d, "
						  "result of handling the message %d.",
						  messageid,
						  res);
			return res;
		}
	}
	else if ( res < 0 && (errno != EAGAIN && errno != EINTR) )
	{
		elog(WARNING, "select system call has error raised. errno %d", errno);
		return RESBROK_PIPE_ERROR;
	}
	return FUNC_RETURN_OK;
}

int handleRB2RM_ClusterReport(void)
{
	int			res		    = FUNC_RETURN_OK;
	SegStat 	segstat	    = NULL;
	uint32_t	segsize;
	int			fd 		    = ResBrokerNotifyPipe[0];
	int			piperes     = 0;
	List		*segstats	= NULL;
	List		*allsegres  = NULL;
	ListCell	*cell		= NULL;

	PRESPOOL->RBClusterReportCounter++;

	/* Read whole result head. */
	RPCResponseRBGetClusterReportHeadData response;
	piperes = readPipe(fd, (char *)&response, sizeof(response));
	if ( piperes != sizeof(response) )
	{
		elog(WARNING, "YARN mode resource broker failed to read cluster report "
					  "response (head) from pipe. "
					  "Read length %d, expected length %lu.",
					  piperes,
					  sizeof(response));
		return RESBROK_PIPE_ERROR;
	}

	if ( response.Result != FUNC_RETURN_OK )
	{
		elog(WARNING, "YARN mode resource broker received error information of "
					  "cluster report response from pipe. error %d",
					  response.Result);
		/*
		 * Resource broker has something wrong. Can not receive concrete node
		 * information.
		 */
		return response.Result;
	}

	elog(LOG, "YARN mode resource broker got cluster report having %d host(s), "
			  "maximum queue capacity is %lf.",
			  response.MachineCount,
			  response.QueueMaxCapacity);

	/* Read machines. */
	for( int i = 0 ; i < response.MachineCount ; ++i )
	{
		/* Machine instance size. */
		piperes = readPipe(fd, &segsize, sizeof(segsize));
		if ( piperes != sizeof(segsize) )
		{
			elog(WARNING, "YARN mode resource broker failed to read cluster "
						  "report response message (machine info size) from pipe. "
						  "Read length %d, expected length %lu.",
						  piperes,
						  sizeof(segsize));
			res = RESBROK_PIPE_ERROR;
			break;
		}

		/* Skip reserved 32bit tag. */
		uint32_t reserved = 0;
		piperes = readPipe(fd, &reserved, sizeof(reserved));
		if ( piperes != sizeof(reserved) )
		{
			elog(WARNING, "YARN mode resource broker failed to read cluster "
						  "report response message (machine info reserved) from "
						  "pipe. Read length %d, expected length %lu.",
						  piperes,
						  sizeof(reserved));
			res = RESBROK_PIPE_ERROR;
			break;
		}

		/* Read segment stat. */
		segstat = rm_palloc0(PCONTEXT, segsize);

		piperes = readPipe(fd, segstat, segsize);
		if ( piperes != segsize )
		{
			elog(WARNING, "YARN mode resource broker failed to read cluster "
						  "report response message (machine info) from pipe. "
						  "Read length %d, expected length %d.",
						  piperes,
						  segsize);
			if (segstat != NULL)
			{
				rm_pfree(PCONTEXT, segstat);
			}
			res = RESBROK_PIPE_ERROR;
			break;
		}

		/* Hold the received segment stat instances. */
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		segstats = lappend(segstats, (void *)segstat);
		MEMORY_CONTEXT_SWITCH_BACK
	}

	/*
	 * TILL NOW, the whole message content is received.
	 */

	if ( res == FUNC_RETURN_OK )
	{
		/* Check if the YARN resource queue report is valid, i.e. maximum
		 * capacity and capacity are all greater than 0.
		 */
		if ( response.QueueCapacity <= 0 || response.QueueMaxCapacity <= 0 )
		{
			elog(WARNING, "YARN mode resource broker got invalid cluster report");
			res = RESBROK_WRONG_GLOB_MGR_QUEUEREPORT;

		}
	}

	/* If something wrong, no need to keep the received content, free them. */
	if ( res != FUNC_RETURN_OK )
	{
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		while( list_length(segstats) > 0 )
		{
			rm_pfree(PCONTEXT, lfirst(list_head(segstats)));
			segstats = list_delete_first(segstats);
		}
		MEMORY_CONTEXT_SWITCH_BACK
		return res;
	}

	setAllSegResourceGRMUnhandled();

	/*
	 * Start to update resource pool content. The YARN cluster total size is
	 * also counted the same time.
	 */

	resetResourceBundleData(&(PRESPOOL->GRMTotalHavingNoHAWQNode), 0, 0.0, 0);

	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	while( list_length(segstats) > 0 )
	{
		SegStat segstat = (SegStat)lfirst(list_head(segstats));

		addResourceBundleData(&(PRESPOOL->GRMTotalHavingNoHAWQNode),
							  segstat->GRMTotalMemoryMB,
							  segstat->GRMTotalCore);

		res = updateHAWQSegWithGRMSegStat(segstat);
		if ( res == FUNC_RETURN_OK )
		{
			SelfMaintainBufferData buffer;
			initializeSelfMaintainBuffer(&buffer, PCONTEXT);
			generateSegStatReport(segstat, &buffer);
			elog(LOG, "YARN mode resource broker updated segment configuration "
					  "in resource pool. %s", buffer.Buffer);
			destroySelfMaintainBuffer(&buffer);
		}
		else
		{
			elog(LOG, "YARN mode resource broker skipped segment configuration "
					  "from host %s",
					  GET_SEGINFO_GRMHOSTNAME(&(segstat->Info)));
		}
		rm_pfree(PCONTEXT, segstat);
		segstats = list_delete_first(segstats);
	}

	/*
	 * iterate all segments without GRM report,
	 * and update its status.
	 */
	getAllPAIRRefIntoList(&(PRESPOOL->Segments), &allsegres);
	foreach(cell, allsegres)
	{
		SegResource segres = (SegResource)(((PAIR)lfirst(cell))->Value);
		bool statusDescChange = false;

		/*
		 * skip segments handled in GRM report list
		 */
		if (segres->Stat->GRMHandled)
			continue;

		/*
		 * Set no GRM node report flag for this segment.
		 */
		if ((segres->Stat->StatusDesc & SEG_STATUS_NO_GRM_NODE_REPORT) == 0)
		{
			segres->Stat->StatusDesc |= SEG_STATUS_NO_GRM_NODE_REPORT;
			statusDescChange = true;
		}

		if (IS_SEGSTAT_FTSAVAILABLE(segres->Stat))
		{
			/*
			 * This segment is FTS available, but master hasn't
			 * gotten its GRM node report, so set this segment to DOWN.
			 */
			setSegResHAWQAvailability(segres, RESOURCE_SEG_STATUS_UNAVAILABLE);
		}

		Assert(!IS_SEGSTAT_FTSAVAILABLE(segres->Stat));
		if (statusDescChange && Gp_role != GP_ROLE_UTILITY)
		{
			SimpStringPtr description = build_segment_status_description(segres->Stat);
			update_segment_status(segres->Stat->ID + REGISTRATION_ORDER_OFFSET,
									SEGMENT_STATUS_DOWN,
									 (description->Len > 0)?description->Str:"");
			add_segment_history_row(segres->Stat->ID + REGISTRATION_ORDER_OFFSET,
									GET_SEGRESOURCE_HOSTNAME(segres),
									description->Str);

			elog(LOG, "Resource manager hasn't gotten GRM node report for segment(%s),"
						"updates its status:'%c', description:%s",
						GET_SEGRESOURCE_HOSTNAME(segres),
						SEGMENT_STATUS_DOWN,
						(description->Len > 0)?description->Str:"");

			freeSimpleStringContent(description);
			rm_pfree(PCONTEXT, description);
		}
	}
	freePAIRRefList(&(PRESPOOL->Segments), &allsegres);

	MEMORY_CONTEXT_SWITCH_BACK

	elog(LOG, "Resource manager YARN resource broker counted HAWQ cluster now "
			  "having (%d MB, %lf CORE) in a YARN cluster of total resource "
			  "(%d MB, %lf CORE).",
			  PRESPOOL->GRMTotal.MemoryMB,
			  PRESPOOL->GRMTotal.Core,
			  PRESPOOL->GRMTotalHavingNoHAWQNode.MemoryMB,
			  PRESPOOL->GRMTotalHavingNoHAWQNode.Core);

	/*
	 * If the segment is GRM unavailable or FTS unavailable,
	 * RM should return all containers located upon them.
	 */
	returnAllGRMResourceFromUnavailableSegments();

	/* Refresh available node count. */
	refreshAvailableNodeCount();

	/* Update GRM resource queue capacity. */
	PQUEMGR->GRMQueueCapacity	 	= response.QueueCapacity;
	PQUEMGR->GRMQueueCurCapacity 	= response.QueueCurCapacity;
	PQUEMGR->GRMQueueMaxCapacity 	= (response.QueueMaxCapacity > 0 &&
									  response.QueueMaxCapacity <= 1) ?
									  response.QueueMaxCapacity :
									  PQUEMGR->GRMQueueMaxCapacity;
	PQUEMGR->GRMQueueResourceTight 	= response.ResourceTight > 0 ? true : false;

	refreshResourceQueueCapacity(false);
	refreshActualMinGRMContainerPerSeg();

	PRESPOOL->LastUpdateTime = gettime_microsec();

	return FUNC_RETURN_OK;
}

/*
 * Handle the notification from resource broker LIBYARN process, when new
 * resource containers are allocated.
 *
 * NOTE: The request usually can not be met completed, therefore, this function
 * 		 has to clean up pending resource quantity to make HAWQ RM able to
 * 		 resubmit request if possible.
 */
int handleRB2RM_AllocatedResource(void)
{
	int				fd 		  		= ResBrokerNotifyPipe[0];
	int64_t 	   *containerids 	= NULL;
	char 		   *buffer 	  		= NULL;
	int   			acceptedcount 	= 0;
	int				piperes			= 0;
	List		   *newcontainers	= NULL;

	/* Read result. */
	RPCResponseRBAllocateResourceContainersHeadData response;
	piperes = readPipe(fd, (char *)&response, sizeof(response));
	if ( piperes != sizeof(response) )
	{
		elog(WARNING, "YARN mode resource broker failed to read resource "
					  "allocation response message from pipe. "
					  "Read length %d, expected length %lu.",
					  piperes,
					  sizeof(response));
		return RESBROK_PIPE_ERROR;
	}

	if ( response.Result == FUNC_RETURN_OK )
	{
		elog(LOG, "YARN mode resource broker got allocated resource containers "
				  "from resource broker. (%d MB, %d CORE) x %d, expected %d "
				  "containers",
				  response.MemoryMB,
				  response.Core,
				  response.ContainerCount,
				  response.ExpectedContainerCount);
	}
	else
	{
		elog(WARNING, "YARN mode resource broker failed to get resource containers.");
	}

	if ( response.ContainerCount > 0 )
	{
		/* Read container ids */
		int contidsize = __SIZE_ALIGN64(sizeof(int64_t) * response.ContainerCount);
		containerids = (int64_t *)rm_palloc0(PCONTEXT, contidsize);

		piperes = readPipe(fd, containerids, contidsize);
		if ( piperes != contidsize )
		{
			elog(WARNING, "YARN mode resource broker failed to read resource "
						  "allocation response message (container ids) from pipe. "
						  "Read length %d, expected length %d, errno %d.",
						  piperes,
						  contidsize,
						  errno);
			if ( containerids != NULL )
			{
				rm_pfree(PCONTEXT, containerids);
			}
			return RESBROK_PIPE_ERROR;
		}

		/* Read host names */
		buffer = rm_palloc(PCONTEXT, response.HostNameStringLen);
		piperes = readPipe(fd, buffer, response.HostNameStringLen);
		if ( piperes != response.HostNameStringLen )
		{
			elog(WARNING, "YARN mode resource broker failed to read resource "
						  "allocation response message (host names) from pipe. "
						  "Read length %d, expected length %d.",
						  piperes,
						  response.HostNameStringLen);
			if ( containerids != NULL )
			{
				rm_pfree(PCONTEXT, containerids);
			}
		    if ( buffer != NULL )
		    {
		    	rm_pfree(PCONTEXT, buffer);
		    }
			return RESBROK_PIPE_ERROR;
		}

		/* Build result and add into the resource pool as a pending container. */
		char *phostname     = buffer;
		for ( int i = 0 ; i < response.ContainerCount ; ++i )
		{
			GRMContainer newcontainer = createGRMContainer(containerids[i],
														   response.MemoryMB,
														   response.Core,
														   phostname,
														   NULL);
			/* Try next host name. */
			phostname += strlen(phostname) + 1;
			MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
			newcontainers = lappend(newcontainers, newcontainer);
			MEMORY_CONTEXT_SWITCH_BACK
		}
	}

	/* Should go into GRM container clean phase. */
	if (!isCleanGRMResourceStatus() &&
		DRMGlobalInstance->ResBrokerAppTimeStamp > 0 &&
		response.SystemStartTimestamp > DRMGlobalInstance->ResBrokerAppTimeStamp)
	{
		setCleanGRMResourceStatus();
	}

	/* We always update resource broker work time stamp here. */
	DRMGlobalInstance->ResBrokerAppTimeStamp = response.SystemStartTimestamp;

	ListCell *cell = NULL;
	foreach(cell, newcontainers)
	{
		GRMContainer newcontainer = (GRMContainer)lfirst(cell);

		if ( isCleanGRMResourceStatus() )
		{
			/*
			 * If resource manager is now in clean phase, return new containers
			 * directly. We want to allocate again after clean phase.
			 */
			addGRMContainerToKicked(newcontainer);
		}
		else
		{
			/*
			 * The container is added to to be accepted container list, if this
			 * container is not recognized, it is directly kicked.
			 */
			if ( addGRMContainerToToBeAccepted(newcontainer) == FUNC_RETURN_OK )
			{
				acceptedcount++;
			}
		}
	}

	/*
	 * Clean up pending resource quantity, if failed to allocate resource, the
	 * acceptedcount is 0.
	 */
	removePendingResourceRequestInRootQueue(
		response.MemoryMB * (response.ExpectedContainerCount - acceptedcount),
		response.Core     * (response.ExpectedContainerCount - acceptedcount),
		(response.Result == FUNC_RETURN_OK) && (acceptedcount > 0));

	elog(LOG, "Resource manager accepted YARN containers (%d MB, %d CORE) x %d "
			  "from resource broker, expected %d containers, skipped %d containers.",
			  response.MemoryMB,
			  response.Core,
			  acceptedcount,
			  response.ExpectedContainerCount,
			  response.ContainerCount - acceptedcount);

	/*
	 * Buildup hash table for updating each segment if it can not have expected
	 * resources allocated.
	 */
	HASHTABLEData	seghavingres;

	initializeHASHTABLE(&seghavingres,
						PCONTEXT,
						HASHTABLE_SLOT_VOLUME_DEFAULT,
						HASHTABLE_SLOT_VOLUME_DEFAULT_MAX,
						HASHTABLE_KEYTYPE_VOIDPT,
						freeResourceBundle);

	while( list_length(newcontainers) > 0 )
	{
		GRMContainer newcontainer = (GRMContainer)
									lfirst(list_head(newcontainers));
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		newcontainers = list_delete_first(newcontainers);
		MEMORY_CONTEXT_SWITCH_BACK

		if ( isCleanGRMResourceStatus() )
		{
			continue;
		}

		int32_t segid = -1;
		int res = getSegIDByGRMHostName(newcontainer->HostName,
										strlen(newcontainer->HostName),
										&segid);
		if ( res != FUNC_RETURN_OK )
		{
			elog(WARNING, "Resource manager finds not recognized YARN "
						  "container on host %s, container id is "INT64_FORMAT,
						  newcontainer->HostName,
						  newcontainer->ID);
			continue;
		}

		SegResource 	segres	   = getSegResource(segid);
		PAIR			oldsegpair = getHASHTABLENode(&seghavingres, segres);
		ResourceBundle	resbundle  = NULL;
		if ( oldsegpair != NULL )
		{
			resbundle = (ResourceBundle)(oldsegpair->Value);
		}
		else
		{
			resbundle = rm_palloc0(PCONTEXT, sizeof(ResourceBundleData));
			resbundle->MemoryMB = 0;
			resbundle->Core     = 0;
			setHASHTABLENode(&seghavingres, segres, resbundle, false);
		}
		addResourceBundleData(resbundle,
							  newcontainer->MemoryMB,
							  newcontainer->Core);
	}

	if ( !isCleanGRMResourceStatus() )
	{
		RB_updateSegmentsHavingNoExpectedGRMContainers(&seghavingres);
	}
	cleanHASHTABLE(&seghavingres);

	if ( containerids != NULL )
	{
		rm_pfree(PCONTEXT, containerids);
	}
    if ( buffer != NULL )
    {
    	rm_pfree(PCONTEXT, buffer);
    }
	return response.Result;
}

void freeResourceBundle(void *resbundle)
{
	rm_pfree(PCONTEXT, resbundle);
}

int handleRB2RM_ReturnedResource(void)
{
	int fd		= ResBrokerNotifyPipe[0];
	int	piperes	= 0;

	/* Read result code. */
	RPCResponseRBReturnResourceContainersData response;
	piperes = readPipe(fd, &response, sizeof(response));
	if ( piperes != sizeof(response) )
	{
		elog(WARNING, "YARN mode resource broker failed to read resource return "
					  "response message from pipe. Read length %d, expected "
					  "length %lu.",
					  piperes,
					  sizeof(response));
		return RESBROK_PIPE_ERROR;
	}

	if ( !isCleanGRMResourceStatus() &&
		 DRMGlobalInstance->ResBrokerAppTimeStamp > 0 &&
		 response.SystemStartTimestamp > DRMGlobalInstance->ResBrokerAppTimeStamp )
	{
		DRMGlobalInstance->ResBrokerAppTimeStamp = response.SystemStartTimestamp;
		setCleanGRMResourceStatus();
		return response.Result;
	}

	elog(LOG, "YARN mode resource broker returned resource container(s) to YARN.");
	return FUNC_RETURN_OK;
}

int handleRB2RM_ContainerReport(void)
{
	int 	fd			= ResBrokerNotifyPipe[0];
	int		piperes		= 0;

	/* Read result code. */
	RPCResponseRBGetContainerReportHeadData response;
	piperes = readPipe(fd, &response, sizeof(response));
	if ( piperes != sizeof(response) )
	{
		elog(WARNING, "YARN mode resource broker failed to read container report "
					  "message from pipe. Read length %d, expected length %lu.",
					  piperes,
					  sizeof(response));
		return RESBROK_PIPE_ERROR;
	}

	if ( response.Result != FUNC_RETURN_OK )
	{
		elog(WARNING, "YARN mode resource broker received error information of "
					  "container report response from pipe. error %d",
					  response.Result);
		/*
		 * Resource broker has something wrong. Can not resume.
		 */
		return response.Result;
	}

	RB_GRMContainerStat ctnstats = NULL;
	if ( response.ContainerCount > 0 )
	{
		/* Read all container status. */
		int totalsize = sizeof(RB_GRMContainerStatData) * response.ContainerCount;
		ctnstats = rm_palloc0(PCONTEXT, totalsize);
		piperes = readPipe(fd, ctnstats, totalsize);
		if ( piperes != totalsize)
		{
			elog(WARNING, "YARN mode resource broker failed to read container "
						  "report container status array. "
						  "Read length %d, expected length %d",
						  piperes,
						  totalsize);
			rm_pfree(PCONTEXT, ctnstats);
			return RESBROK_PIPE_ERROR;
		}
	}

	if ( !isCleanGRMResourceStatus() &&
		 DRMGlobalInstance->ResBrokerAppTimeStamp > 0 &&
		 response.SystemStartTimestamp > DRMGlobalInstance->ResBrokerAppTimeStamp )
	{
		DRMGlobalInstance->ResBrokerAppTimeStamp = response.SystemStartTimestamp;
		setCleanGRMResourceStatus();
		return response.Result;
	}

	/* Pass the container report to resource pool. */
	checkGRMContainerStatus(ctnstats, response.ContainerCount);

	/*
	 * Check all containers that not tracked by HAWQ resource manager. We have
	 * to explicitly return all not tracked GRM containers.
	 */
	buildToReturnNotTrackedGRMContainers(ctnstats, response.ContainerCount);

	/* Free instance. */
	if ( ctnstats != NULL )
	{
		rm_pfree(PCONTEXT, ctnstats);
	}

	PRESPOOL->LastCheckContainerTime = gettime_microsec();

	return response.Result;
}

void buildToReturnNotTrackedGRMContainers(RB_GRMContainerStat ctnstats, int size)
{
	for ( int i = 0 ; i < size ; ++i )
	{
		if ( ctnstats[i].isFound )
		{
			continue;
		}

		/* Here we simulate one GRM container instance for the untracked one. */
		GRMContainer ctn = rm_palloc0(PCONTEXT, sizeof(GRMContainerData));
		ctn->CalcDecPending = false;
		ctn->ID 			= ctnstats[i].ContainerID;
		ctn->HostName 		= NULL;
		ctn->MemoryMB 		= 0;
		ctn->Core 			= 0;
		ctn->Life 			= 0;
		ctn->Resource 		= NULL;

		elog(DEBUG3, "YARN mode resource broker creates dummy GRM container "
					 INT64_FORMAT".",
					 ctn->ID);

		addGRMContainerToKicked(ctn);
	}
}

void RB_LIBYARN_handleError(int errorcode)
{
	/* Pipe error raised, this needs resource broker process to be resarted. */
	if ( errorcode == RESBROK_PIPE_ERROR )
	{
		DRMGlobalInstance->ResBrokerAppTimeStamp = 0;
		elog(WARNING, "YARN mode resource broker pipe does not work well. "
					  "Try to restart it.");
		if ( RB_stop() != FUNC_RETURN_OK )
		{
			DRMGlobalInstance->ResManagerMainKeepRun = false;
			return;
		}
	}

	/*
	 * If global resource manager has error raised, resource manager has to go
	 * into clean GRM container phase.
	 */
	else if ( errorcode == RESBROK_ERROR_GRM )
	{
		DRMGlobalInstance->ResBrokerAppTimeStamp = 0;
		elog(WARNING, "YARN mode resource broker reports YARN exceptions. "
					  "Reset resource manager allocated resource.");
		setCleanGRMResourceStatus();
	}
	else
	{
		Assert(errorcode == FUNC_RETURN_OK ||
			   errorcode == RESBROK_WRONG_GLOB_MGR_QUEUEREPORT);
	}
}
