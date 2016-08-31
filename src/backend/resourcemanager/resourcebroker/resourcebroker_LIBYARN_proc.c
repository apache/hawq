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
#include "miscadmin.h"
#include "getaddrinfo.h"
#include "libpq/ip.h"

#include "utils/kvproperties.h"
#include "resourcebroker/resourcebroker_LIBYARN.h"
#include "resourcebroker/resourcebroker_RM_RB_Protocol.h"
#include "resourcemanager.h"

#include "libyarn/LibYarnClientC.h"

#include <krb5.h>
#include "cdb/cdbfilesystemcredential.h"

/*
 *------------------------------------------------------------------------------
 * Internal functions
 *------------------------------------------------------------------------------
 */
char * ExtractPrincipalFromTicketCache(const char* cache);

int ResBrokerMainInternal(void);

int loadParameters(void);

/*
 * Functions for communicating between RB process and YARN resource manager.
 */
int  RB2YARN_initializeConnection(void);
int  RB2YARN_connectToYARN(void);
int  RB2YARN_registerYARNApplication(void);
int  RB2YARN_getClusterReport(DQueue hosts);
int  RB2YARN_getQueueReport(char 	*queuename,
							double 	*cap,
							double 	*curcap,
							double 	*maxcap,
							bool	*haschildren);
int  RB2YARN_acquireResource(uint32_t memorymb,
							 uint32_t core,
							 uint32_t count,
							 LibYarnNodeInfo_t *preferredArray,
							 uint32_t preferredSize,
							 DQueue	 containerids,
							 DQueue	 containerhosts);
int  RB2YARN_returnResource(int64_t *contids, int contcount);
int  RB2YARN_getContainerReport(RB_GRMContainerStat *ctnstats, int *size);
int  RB2YARN_finishYARNApplication(void);
int  RB2YARN_disconnectFromYARN(void);
void RB2YARN_freeContainersInMemory(DQueue containerids, DQueue containerhosts);
const char *RB2YARN_getErrorMessage(void);

/*
 * Functions for handling request from HAWQ resource manager.
 */
int handleRM2RB_GetClusterReport(void);
int handleRM2RB_AllocateResource(void);
int handleRM2RB_ReturnResource(void);
int handleRM2RB_GetContainerReport(void);

int sendRBGetClusterReportErrorData(int errorcode);

int sendRBAllocateResourceErrorData(int 								   errorcode,
									RPCRequestRBAllocateResourceContainers request);

int sendRBReturnResourceErrorData(int errorcode);
int sendRBGetContainerReportErrorData(int errorcode);

void quitResBroker(SIGNAL_ARGS);

uint64_t                 ResBrokerStartTime;

/* The user who submits hawq application to Hadoop Yarn,
 * default is postgres, if Kerberos is enable, should be principal name.
 * */
char*				 	 YARNUser;
bool					 YARNUserShouldFree;

SimpString				 YARNServer;
SimpString				 YARNPort;
SimpString				 YARNSchedulerServer;
SimpString				 YARNSchedulerPort;

char 					 YARNAMServer[] = "0.0.0.0";
int32_t					 YARNAMPort	    = 0;
char					 YARNTRKUrl[]	= "url";

SimpString				 YARNAppName;
SimpString				 YARNQueueName;
char 					*YARNJobID;
LibYarnClient_t			*LIBYARNClient;

bool					 YARNResourceTight;
uint32_t				 YARNResourceTightTestMemoryMB;
uint32_t				 YARNResourceTightTestCore;

/**
 * Handler of SIGINT which will be sent from HAWQ RM.
 */
void quitResBroker(SIGNAL_ARGS)
{
    ResBrokerKeepRun = false;
}

int ResBrokerMain(void)
{
	/* Change process ps display. */
	if( ResourceManagerIsForked ) {
		int oldid = gp_session_id;
		gp_session_id = -1;
		init_ps_display("yarn resource broker", "", "", "");
		gp_session_id = oldid;
	}

	MyProcPid = getpid();

	ResBrokerKeepRun = true;

	/* Load parameters */
	int res = loadParameters();
	if ( res != FUNC_RETURN_OK ) {
		elog(WARNING, "Resource broker loads invalid yarn parameters");
	}

	/* Set signal behavior */
	PG_SETMASK(&BlockSig);
	pqsignal(SIGHUP , SIG_IGN);
	pqsignal(SIGINT , quitResBroker);
	pqsignal(SIGTERM, quitResBroker);
	pqsignal(SIGQUIT, SIG_DFL);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, SIG_IGN);
	/* call system() needs set SIG_DFL for SIGCHLD */
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_IGN);
	pqsignal(SIGTTOU, SIG_IGN);
	PG_SETMASK(&UnBlockSig);

	res = ResBrokerMainInternal();

	pqsignal(SIGCHLD, SIG_IGN);
	elog(LOG, "YARN mode resource broker goes into exit phase.");
	return res;
}

/**
 * Main entry of resource broker process.
 */
int ResBrokerMainInternal(void)
{
	uint32_t 		messagehead[2];
	uint32_t		messageid;
	fd_set      	rfds;
	struct timeval 	timeout;
	int				res		= FUNC_RETURN_OK;
	int				yarnres	= FUNCTION_SUCCEEDED;
	int				fd 		= ResBrokerRequestPipe[0];

	YARNJobID 						= NULL;
	LIBYARNClient 					= NULL;
	ResBrokerStartTime 				= 0;
	YARNResourceTight				= false;
	YARNResourceTightTestMemoryMB 	= 0;
	YARNResourceTightTestCore		= 0;

	while( ResBrokerKeepRun )
	{
		/*
		 * If the parent process (HAWQ RM process) does not exist, no need to
		 * run anymore. The process goes to the exit phase directly.
		 */
		if ( getppid() != ResBrokerParentPID ) {
			elog(WARNING, "Parent process of YARN mode resource broker quit. "
					  	  "Resource broker process will actively close.");
			ResBrokerKeepRun = false;
			continue;
		}

		/* refresh kerberos ticket */
		if (enable_secure_filesystem && !login())
		{
			elog(WARNING, "Resource broker failed to refresh kerberos ticket.");
		}

		/*
		 * If the connection between YARN and YARN resource broker is not
		 * created, try to build up connection and register application.
		 */
		if ( LIBYARNClient == NULL && YARNJobID == NULL ) {
			/* Get ready to connect to global resource manager. */
			yarnres = RB2YARN_initializeConnection();
			if ( yarnres != FUNCTION_SUCCEEDED ) {
				elog(WARNING, "YARN mode resource broker failed to register YARN "
							  "application. Resource broker will retry soon.");
			}
		}

		/*
		 * Resource broker always handle requests from resource manager process.
		 * If the connection between resource broker and YARN is not created,
		 * error information is sent back as the response.
		 */

		/* fd to select. */
		FD_ZERO(&rfds);
		FD_SET(fd, &rfds);

		/* select timeout setting. */
		timeout.tv_sec  = 0;
		timeout.tv_usec = 100000;

		res = select(fd + 1, &rfds, NULL, NULL, &timeout);

		/* Something passed from HAWQ RM process. */
		if ( res > 0 && FD_ISSET(fd, &rfds) )
		{

			/* read request from ResBrokerRequestPipe */
			int readres = readPipe(fd, messagehead, sizeof(messagehead));
			if (readres == -1) {
				elog(WARNING, "YARN mode resource broker pipe has error raised.");
				ResBrokerKeepRun = false;
				continue;
			}
			else if ( readres != sizeof(messagehead) ) {
				elog(WARNING, "YARN mode resource broker pipe cannot read expect "
						  	  "data.");
				ResBrokerKeepRun = false;
			    continue;
			}

			messageid = messagehead[0];
            elog(DEBUG3, "YARN mode resource broker gets request %d from "
            			 "resource manager main process.",
            			 messageid);

            res = FUNC_RETURN_OK;
            switch(messageid) {
            case RM2RB_GET_CLUSTERREPORT:
				res = handleRM2RB_GetClusterReport();
				break;
            case RM2RB_ALLOC_RESOURCE:
            	res = handleRM2RB_AllocateResource();
            	break;
            case RM2RB_RETURN_RESOURCE:
            	res = handleRM2RB_ReturnResource();
            	break;
            case RM2RB_GET_CONTAINERREPORT:
            	res = handleRM2RB_GetContainerReport();
            	break;
            default:
            	elog(WARNING, "YARN mode resource broker received wrong message "
            				  "id %d",
							  messageid);
            	ResBrokerKeepRun = false;
            	res = RESBROK_WRONG_MESSAGE_ID;
			}

            if ( res != FUNC_RETURN_OK ) {
            	elog(WARNING, "YARN mode resource broker failed to process request. "
            			  	  "Message id = %d, result = %d.",
							  messageid,
							  res);
            	/* If this is a pipe error between RM and RB or YARN remove error.
            	 * Exit RB and let RM restart RB process. */
                if ( res == RESBROK_ERROR_GRM )
                {
                    if ( LIBYARNClient != NULL && YARNJobID != NULL ) {
                        forceKillJob(LIBYARNClient, YARNJobID);
                        RB2YARN_disconnectFromYARN();
                        elog(LOG, "YARN mode resource broker disconnects from YARN. "
                                  "Resource broker will retry to register soon.");
                    }
                }
            	else
            	{
            		ResBrokerKeepRun = false;
            	}
            }
		}
		else if ( res < 0 ) {
			if ( errno != EAGAIN && errno != EINTR ) {
				elog(WARNING, "YARN mode resource broker got select() error, "
							  "(errno %d).", errno);
				/* We treat this case as pipe error. */
				ResBrokerKeepRun = false;
			}
		}
	}

	elog(LOG, "YARN mode resource broker finish YARN application now.");

	if ( LIBYARNClient != NULL && YARNJobID != NULL ) {
		/*
		 * Unregister the application in YARN, this also makes all allocated
		 * resource containers for HAWQ returned. Here, we ignore the result
		 * of the action.
		 */
		int yarnres = RB2YARN_finishYARNApplication();
		elog(LOG, "YARN mode resource broker get result of finish yarn application "
				  "through libYARN %d",
				  yarnres);
		if (YARNUser != NULL && YARNUserShouldFree)
		{
			free(YARNUser);
		}
		YARNUser = NULL;
		YARNUserShouldFree = true;
		yarnres = RB2YARN_disconnectFromYARN();
		elog(LOG, "YARN mode resource broker get result of disconnecting YARN "
				  "through libYARN %d",
				  yarnres);
	}
	return FUNC_RETURN_OK;
}

/*
 * Extract principal from cache
 */
char * ExtractPrincipalFromTicketCache(const char* cache)
{
	krb5_context cxt = NULL;
	krb5_ccache ccache = NULL;
	krb5_principal principal = NULL;
	krb5_error_code ec = 0;
	char *priName = NULL, *retval = NULL;
	const char *errorMsg = NULL;

	if (cache) {
		if (0 != setenv("KRB5CCNAME", cache, 1)) {
			elog(WARNING, "Cannot set env parameter \"KRB5CCNAME\" when extract principal from cache:%s", cache);
			return NULL;
		}
	}

	do {
		if (0 != (ec = krb5_init_context(&cxt))) {
			break;
		}

		if (0 != (ec = krb5_cc_default(cxt, &ccache))) {
			break;
		}

		if (0 != (ec = krb5_cc_get_principal(cxt, ccache, &principal))) {
			break;
		}

		if (0 != (ec = krb5_unparse_name(cxt, principal, &priName))) {
			break;
		}
	} while (0);

	if (!ec) {
		retval = strdup(priName);
	} else {
		if (cxt) {
			errorMsg = krb5_get_error_message(cxt, ec);
		} else {
			errorMsg = "Cannot initialize kerberos context";
		}
	}

	if (priName != NULL) {
		krb5_free_unparsed_name(cxt, priName);
	}

	if (principal != NULL) {
		krb5_free_principal(cxt, principal);
	}

	if (ccache != NULL) {
		krb5_cc_close(cxt, ccache);
	}

	if (cxt != NULL) {
		krb5_free_context(cxt);
	}

	if (errorMsg != NULL) {
		elog(WARNING, "Fail to extract principal from cache, because : %s", errorMsg);
		return NULL;
	}

	return retval;
}

int  loadParameters(void)
{
	int	 			res 		= FUNC_RETURN_OK;

	initSimpleString(&YARNServer, 			PCONTEXT);
	initSimpleString(&YARNPort, 			PCONTEXT);
	initSimpleString(&YARNSchedulerServer, 	PCONTEXT);
	initSimpleString(&YARNSchedulerPort, 	PCONTEXT);
	initSimpleString(&YARNQueueName, 		PCONTEXT);
	initSimpleString(&YARNAppName, 			PCONTEXT);
	YARNUser = NULL;
	YARNUserShouldFree = false;

	/* Get server and port */
	char *pcolon = NULL;
	if ( rm_grm_yarn_rm_addr == NULL ||
		 (pcolon = strchr(rm_grm_yarn_rm_addr, ':')) ==NULL ) {
		res = RESBROK_WRONG_GLOB_MGR_ADDRESS;
		elog(LOG, "The format of property %s must be <address>:<port>.",
				  HAWQDRM_CONFFILE_YARN_SERVERADDR);
		goto exit;
	}

	/* YARNServer:YARNPort */
	setSimpleStringWithContent(&YARNServer,
							   rm_grm_yarn_rm_addr,
							   pcolon - rm_grm_yarn_rm_addr);
	setSimpleStringNoLen(&YARNPort, pcolon + 1);

	int32_t testport;
	res = SimpleStringToInt32(&YARNPort, &testport);
	if ( res != FUNC_RETURN_OK ) {
		res = RESBROK_WRONG_GLOB_MGR_ADDRESS;
		elog(LOG, "The port number in property %s can not be parsed.",
				  HAWQDRM_CONFFILE_YARN_SERVERADDR);
		goto exit;
	}

	/* Get scheduler server and port */
	if ( rm_grm_yarn_sched_addr == NULL ||
		 (pcolon = strchr(rm_grm_yarn_sched_addr, ':')) ==NULL ) {
		res = RESBROK_WRONG_GLOB_MGR_ADDRESS;
		elog(LOG, "The format of property %s must be <address>:<port>.",
				  HAWQDRM_CONFFILE_YARN_SCHEDULERADDR);
		goto exit;
	}

	setSimpleStringWithContent(&YARNSchedulerServer,
							   rm_grm_yarn_sched_addr,
							   pcolon - rm_grm_yarn_sched_addr);
	setSimpleStringNoLen(&YARNSchedulerPort, pcolon + 1);

	int32_t testschedport;
	res = SimpleStringToInt32(&YARNSchedulerPort, &testschedport);
	if ( res != FUNC_RETURN_OK ) {
		res = RESBROK_WRONG_GLOB_MGR_ADDRESS;
		elog(LOG, "The port number in property %s can not be parsed.",
				  HAWQDRM_CONFFILE_YARN_SCHEDULERADDR);
		goto exit;
	}

	/* Get YARN queue name. */
	if ( rm_grm_yarn_queue == NULL || rm_grm_yarn_queue[0] == '\0' ) {
		res = RESBROK_WRONG_GLOB_MGR_QUEUE;
		elog(LOG, "Can not find property %s.", HAWQDRM_CONFFILE_YARN_QUEUE);
		goto exit;
	}

	setSimpleStringNoLen(&YARNQueueName, rm_grm_yarn_queue);

	/* Get YARN HAWQ application name string. */
	if ( rm_grm_yarn_app_name == NULL || rm_grm_yarn_app_name[0] == '\0' ) {
		res = RESBROK_WRONG_GLOB_MGR_APPNAME;
		elog(LOG, "Can not find property %s.", HAWQDRM_CONFFILE_YARN_APP_NAME);
		goto exit;
	}

	setSimpleStringNoLen(&YARNAppName, rm_grm_yarn_app_name);

	/* If kerberos is enable, fetch the principal from ticket cache file. */
	if (enable_secure_filesystem)
	{
		if (!login())
		{
			elog(WARNING, "Resource broker failed to refresh kerberos ticket.");
		}
		YARNUser = ExtractPrincipalFromTicketCache(krb5_ccname);
		YARNUserShouldFree = true;
	}

	if (YARNUser == NULL)
	{
		YARNUser = "postgres";
		YARNUserShouldFree = false;
	}

	elog(LOG, "YARN mode resource broker accepted YARN connection arguments : "
			  "YARN Server %s:%s "
			  "Scheduler server %s:%s "
			  "Queue %s Application name %s, "
			  "by user:%s",
			  YARNServer.Str,
			  YARNPort.Str,
			  YARNSchedulerServer.Str,
			  YARNSchedulerPort.Str,
			  YARNQueueName.Str,
			  YARNAppName.Str,
			  YARNUser);
exit:
	if ( res != FUNC_RETURN_OK ) {
		elog(WARNING, "YARN mode resource broker failed to load YARN connection arguments.");
	}
	return res;
}

/**
 * Handle the request from RM to RB for getting cluster report.
 */
int handleRM2RB_GetClusterReport(void)
{
	int 			res 		= FUNC_RETURN_OK;
	DQueueData 		clusterreport;
	uint32_t		Reserved	= 0;
	uint32_t		MessageID   = RB2RM_CLUSTERREPORT;
	int				piperes		= 0;

	/* Get request header. */
	RPCRequestRBGetClusterReportHeadData requesthead;

	piperes = readPipe(ResBrokerRequestPipe[0], &requesthead, sizeof(requesthead));
	if ( piperes != sizeof(requesthead) ) {
		elog(WARNING, "YARN mode resource broker failed to read cluster report "
					  "request message from pipe. Read length %d, expected "
					  "length %lu",
					  piperes,
					  sizeof(requesthead));
		return RESBROK_PIPE_ERROR;
	}

	/* This is bad assumption... */
	Assert( (requesthead.QueueNameLen % 8) == 0 );

	/* Get resource queue name string. */
	SelfMaintainBufferData resqueuename;
	initializeSelfMaintainBuffer(&resqueuename, PCONTEXT);
	prepareSelfMaintainBuffer(&resqueuename, requesthead.QueueNameLen, true);
	piperes = readPipe(ResBrokerRequestPipe[0],
					   resqueuename.Buffer,
					   requesthead.QueueNameLen);
	if ( piperes != requesthead.QueueNameLen ) {
		elog(WARNING, "YARN mode resource broker failed to read cluster report "
					  "request message from pipe (queue name). Read length %d, "
					  "expected length %d",
					  piperes,
					  requesthead.QueueNameLen);
		return RESBROK_PIPE_ERROR;
	}

	if ( YARNJobID == NULL ) {
		return sendRBGetClusterReportErrorData(RESBROK_ERROR_GRM);
	}

	/* Get resource queue capacity. */
	double cap         = 0.0;
	double curcap      = 0.0;
	double maxcap      = 0.0;
	bool   haschildren = false;
	res = RB2YARN_getQueueReport(resqueuename.Buffer,
								 &cap,
								 &curcap,
								 &maxcap,
								 &haschildren);
	if ( res != FUNCTION_SUCCEEDED ) {
		return sendRBGetClusterReportErrorData(RESBROK_ERROR_GRM);
	}

	elog(LOG, "Get YARN resource queue %s report: "
			  "Capacity %lf, Current Capacity %lf, Maximum Capacity %lf.",
			  resqueuename.Buffer,
			  cap,
			  curcap,
			  maxcap);

	destroySelfMaintainBuffer(&resqueuename);

	/* Get cluster report. */
	initializeDQueue(&clusterreport, PCONTEXT);
	res = RB2YARN_getClusterReport(&clusterreport);

	if ( res != FUNCTION_SUCCEEDED )
	{
		return sendRBGetClusterReportErrorData(RESBROK_ERROR_GRM);
	}

	elog(DEBUG3, "YARN resource tight test (%d MB, %d CORE)",
				 YARNResourceTightTestMemoryMB,
				 YARNResourceTightTestCore);
	/* Test queue resource tightness. */
	if ( YARNResourceTightTestMemoryMB > 0 && YARNResourceTightTestCore > 0 )
	{
		/* Try to allocate 1 container. */
		DQueueData ctnids;
		DQueueData ctnhosts;
		initializeDQueue(&ctnids, PCONTEXT);
		initializeDQueue(&ctnhosts, PCONTEXT);
		res = RB2YARN_acquireResource(YARNResourceTightTestMemoryMB,
									  YARNResourceTightTestCore,
									  1,
									  NULL,
									  0,
									  &ctnids,
									  &ctnhosts);
		if ( res != FUNCTION_SUCCEEDED )
		{
			RB2YARN_freeContainersInMemory(&ctnids, &ctnhosts);
			return sendRBGetClusterReportErrorData(RESBROK_ERROR_GRM);
		}

		if ( ctnids.NodeCount > 0 )
		{
			/* Return it at once. */
			int64_t *ctnidarr = rm_palloc0(PCONTEXT,
										   sizeof(int64_t) * ctnids.NodeCount);
			int idx = 0;
			DQUEUE_LOOP_BEGIN(&ctnids, iter, int64_t *, pctnid)
				ctnidarr[idx] = *pctnid;
				elog(DEBUG3, "YARN mode resource broker returns container ID "
							 INT64_FORMAT".",
							 ctnidarr[idx]);
				idx++;
			DQUEUE_LOOP_END
			RB2YARN_freeContainersInMemory(&ctnids, &ctnhosts);
			res = RB2YARN_returnResource(ctnidarr, idx);
		    if ( res != FUNCTION_SUCCEEDED )
		    {
		    	rm_pfree(PCONTEXT, ctnidarr);
		    	return sendRBReturnResourceErrorData(RESBROK_ERROR_GRM);
		    }
		    YARNResourceTight = false;
		    elog(DEBUG3, "YARN mode resource broker consider the target queue not busy.");
		}
		else
		{
			YARNResourceTight = true;
			elog(DEBUG3, "YARN mode resource broker consider the target queue busy.");
		}

	}

	/*
	 * Build response message.
	 */

	/* Prepare buffer for the content to send. */
	SelfMaintainBufferData sendBuffer;
	initializeSelfMaintainBuffer(&sendBuffer, PCONTEXT);

	appendSMBVar(&sendBuffer, MessageID);
	appendSelfMaintainBufferTill64bitAligned(&sendBuffer);

	RPCResponseRBGetClusterReportHeadData responsehead;
	responsehead.Result 		  = FUNC_RETURN_OK;
	responsehead.MachineCount	  = clusterreport.NodeCount;
	responsehead.QueueCapacity    = cap;
	responsehead.QueueCurCapacity = curcap;
	responsehead.QueueMaxCapacity = maxcap;
	responsehead.Reserved		  = 0;
	responsehead.ResourceTight	  = (YARNResourceTightTestMemoryMB > 0 &&
									 YARNResourceTightTestCore > 0 &&
									 YARNResourceTight) ? 1 : 0;
	appendSMBVar(&sendBuffer, responsehead);

	elog(DEBUG3, "YARN cluster report includes %d hosts.", clusterreport.NodeCount);

	/* all machines */
	DQUEUE_LOOP_BEGIN(&clusterreport, iter, SegStat, segstat)
		/* calculate segent stat size. */
		uint32_t datasize = offsetof(SegStatData, Info) + segstat->Info.Size;

		appendSMBVar(&sendBuffer, datasize);
		appendSMBVar(&sendBuffer, Reserved);
		appendSelfMaintainBuffer(&sendBuffer, (char *)segstat, datasize);
	DQUEUE_LOOP_END

	/*
	 * Send Message.
	 */
	int buffsize = getSMBContentSize(&sendBuffer);
	piperes = writePipe(ResBrokerNotifyPipe[1], sendBuffer.Buffer, buffsize);
	if ( piperes != buffsize )
	{
		res = RESBROK_PIPE_ERROR;
		elog(WARNING, "YARN mode resource broker failed to write out cluster "
					  "report response message from pipe. "
					  "Wrote length %d, expected length %d, errno %m.",
					  piperes,
					  buffsize);
	}
	else {
		elog(DEBUG3, "YARN mode resource broker sent YARN cluster report "
					 "response to resource manager.");
	}

	/* Free send buffer. */
	destroySelfMaintainBuffer(&sendBuffer);

	/* Free the result. */
	while( clusterreport.NodeCount > 0 ) {
		SegStat tofree = removeDQueueHeadNode(&clusterreport);
		rm_pfree(PCONTEXT, tofree);
	}
	cleanDQueue(&clusterreport);

	return res;
}

int sendRBGetClusterReportErrorData(int errorcode)
{
	uint32_t MessageID = RB2RM_CLUSTERREPORT;

	/* Prepare buffer for the content to send. */
	SelfMaintainBufferData sendBuffer;
	initializeSelfMaintainBuffer(&sendBuffer, PCONTEXT);
	appendSMBVar(&sendBuffer, MessageID);
	appendSelfMaintainBufferTill64bitAligned(&sendBuffer);

	RPCResponseRBGetClusterReportHeadData responsehead;
	responsehead.Result 		  = errorcode;
	responsehead.MachineCount	  = 0;
	responsehead.QueueCapacity    = 0.0;
	responsehead.QueueCurCapacity = 0.0;
	responsehead.QueueMaxCapacity = 0.0;

	appendSMBVar(&sendBuffer, responsehead);

	elog(DEBUG3, "YARN mode resource broker sends cluster report "
				 "error response %d.", errorcode);

	int buffsize = getSMBContentSize(&sendBuffer);
	int piperes = writePipe(ResBrokerNotifyPipe[1], sendBuffer.Buffer, buffsize);
	/* Free send buffer. */
	destroySelfMaintainBuffer(&sendBuffer);

	if ( piperes != buffsize )
	{
		elog(WARNING, "YARN mode resource broker failed to write out cluster "
					  "report response message from pipe. "
					  "Wrote length %d, expected length %d, errno %m.",
					  piperes,
					  buffsize);
		return RESBROK_PIPE_ERROR;
	}

	return errorcode;
}

/**
 * Handle the request from RM to RB for allocating a group of containers as
 * allocated resource to HAWQ.
 */
int handleRM2RB_AllocateResource(void)
{
	uint32_t				MessageID   = RB2RM_ALLOCATED_RESOURCE;
	int						res			= FUNC_RETURN_OK;
	int 					libyarnres 	= FUNCTION_SUCCEEDED;
	int						piperes		= 0;
	void                    *pBuffer     = NULL;
	LibYarnNodeInfo_t       *preferredArray =  NULL;
	DQueueData 				acquiredcontids;
	DQueueData				acquiredconthosts;

	initializeDQueue(&acquiredcontids,   PCONTEXT);
	initializeDQueue(&acquiredconthosts, PCONTEXT);

	/* Get request content. */
	RPCRequestRBAllocateResourceContainersData request;
	piperes = readPipe(ResBrokerRequestPipe[0], &request, sizeof(request));
	if ( piperes != sizeof(request) )
	{
		elog(WARNING, "YARN mode resource broker failed to read resource "
					  "allocation request message from pipe. "
					  "Read length %d, expected length %lu",
					  piperes,
					  sizeof(request));
		return RESBROK_PIPE_ERROR;
	}

    elog(DEBUG3, "Resource manager acquires (%d MB, %d CORE) x %d containers "
    			 "from YARN.",
                 request.MemoryMB,
				 request.Core,
				 request.ContainerCount);

	/* build preferred host list */
	if (request.MsgLength > 0 && request.PreferredSize > 0) {

		/* read preferred host message */
		pBuffer = rm_palloc0(PCONTEXT, request.MsgLength);
		piperes = readPipe(ResBrokerRequestPipe[0], pBuffer, request.MsgLength);
		if ( piperes != request.MsgLength)
		{
			elog(WARNING, "YARN mode resource broker failed to read resource "
						  "allocation request message with preferred host list from pipe."
						  "Read length %d, expected length %d.",
						  piperes,
						  request.MsgLength);
			if (pBuffer != NULL)
				rm_pfree(PCONTEXT, pBuffer);
			return RESBROK_PIPE_ERROR;
		}

		preferredArray = (LibYarnNodeInfo_t*)rm_palloc0(PCONTEXT,
							request.PreferredSize * sizeof(LibYarnNodeInfo_t));
		uint32_t preferredOffset = 0;
		for (int i = 0; i < request.PreferredSize; i++) {
			preferredArray[i].num_containers = *(uint16_t*)((char*)pBuffer + preferredOffset + sizeof(uint32_t));
			preferredArray[i].hostname = (char*)pBuffer + preferredOffset + sizeof(uint32_t)*2;
			preferredArray[i].rackname = (char*)pBuffer + preferredOffset + sizeof(uint32_t)*2 +
										strlen(preferredArray[i].hostname) + 1;
			preferredOffset += *(uint32_t*)((char*)pBuffer + preferredOffset);
			elog(LOG, "YARN mode resource broker acquire resource from YARN with "
					  "preferred host, hostname:%s, rackname:%s, container number:%d.",
					  preferredArray[i].hostname, preferredArray[i].rackname, preferredArray[i].num_containers);
		}
	}

	elog(DEBUG3, "LIBYARN mode resource broker process read %d bytes in.",
				 request.MsgLength + sizeof(RPCRequestRBAllocateResourceContainersData));

	if ( YARNJobID == NULL )
	{
    	if (pBuffer != NULL)
    	{
    		rm_pfree(PCONTEXT, pBuffer);
    	}
    	if (preferredArray != NULL)
    	{
    		rm_pfree(PCONTEXT, preferredArray);
    	}
		return sendRBAllocateResourceErrorData(RESBROK_ERROR_GRM, &request);
	}

	/*
	 * Mark latest used YARN container resource quota for testing if YARN
	 * resource queue is busy.
	 */
	YARNResourceTightTestMemoryMB = request.MemoryMB;
	YARNResourceTightTestCore 	  = request.Core;

	/* Acquire resource from global resource manager. */
    libyarnres = RB2YARN_acquireResource(request.MemoryMB,
										 request.Core,
										 request.ContainerCount,
										 preferredArray,
										 request.PreferredSize,
										 &acquiredcontids,
										 &acquiredconthosts);

    if ( libyarnres != FUNCTION_SUCCEEDED )
    {
    	if (pBuffer != NULL)
    	{
    		rm_pfree(PCONTEXT, pBuffer);
    	}
    	if (preferredArray != NULL)
    	{
    		rm_pfree(PCONTEXT, preferredArray);
    	}
    	return sendRBAllocateResourceErrorData(RESBROK_ERROR_GRM, &request);
    }

	/*
	 * Build response message.
	 */
    SelfMaintainBufferData 	sendBuffer;
	initializeSelfMaintainBuffer(&sendBuffer, PCONTEXT);

	appendSMBVar(&sendBuffer, MessageID);
	appendSelfMaintainBufferTill64bitAligned(&sendBuffer);

	Assert( acquiredcontids.NodeCount == acquiredconthosts.NodeCount );

	/* Build response head information. */
	RPCResponseRBAllocateResourceContainersHeadData responsehead;
	responsehead.Result					= FUNC_RETURN_OK;
	responsehead.MemoryMB 				= request.MemoryMB;
	responsehead.Core					= request.Core;
	responsehead.ContainerCount 		= acquiredcontids.NodeCount;
	responsehead.ExpectedContainerCount	= request.ContainerCount;
	responsehead.HostNameStringLen		= 0;
	responsehead.SystemStartTimestamp   = ResBrokerStartTime;
	appendSMBVar(&sendBuffer, responsehead);

	/* Append each container id. */
	DQUEUE_LOOP_BEGIN(&acquiredcontids, iter, int64_t *, pcontid)
		appendSMBVar(&sendBuffer, *pcontid);
	DQUEUE_LOOP_END
	appendSelfMaintainBufferTill64bitAligned(&sendBuffer);

	int cursize = sendBuffer.Cursor + 1;
	/* Append each host name string. */
	DQUEUE_LOOP_BEGIN(&acquiredconthosts, iter, char *, hostname)
		appendSMBStr(&sendBuffer, hostname);
	DQUEUE_LOOP_END
	appendSelfMaintainBufferTill64bitAligned(&sendBuffer);

	/* Set host name string total length. */
	RPCResponseRBAllocateResourceContainersHead phead =
		(RPCResponseRBAllocateResourceContainersHead)
		(sendBuffer.Buffer + sizeof(MessageID) + sizeof(uint32_t));
	phead->HostNameStringLen = sendBuffer.Cursor + 1 - cursize;

	/*
	 * Send message.
	 */
	int buffsize = getSMBContentSize(&sendBuffer);
	piperes = writePipe(ResBrokerNotifyPipe[1], sendBuffer.Buffer, buffsize);
	if ( piperes != buffsize )
	{
		res = RESBROK_PIPE_ERROR;
		elog(WARNING, "YARN mode resource broker failed to write out resource "
					  "allocation response message from pipe. "
					  "Wrote length %d, expected length %d.",
					  piperes,
					  buffsize);
	}
	else
	{
		elog(DEBUG3, "Sent YARN resource allocation response to resource manager.");
	}

	if (pBuffer != NULL)
		rm_pfree(PCONTEXT, pBuffer);

	if (preferredArray != NULL)
		rm_pfree(PCONTEXT, preferredArray);

	/* Free the result. */
	RB2YARN_freeContainersInMemory(&acquiredcontids, &acquiredconthosts);

	/* Free send buffer. */
	destroySelfMaintainBuffer(&sendBuffer);
	return res;
}

int sendRBAllocateResourceErrorData(int 								   errorcode,
									RPCRequestRBAllocateResourceContainers request)
{
	uint32_t MessageID = RB2RM_ALLOCATED_RESOURCE;

	/* Prepare buffer for the content to send. */
	SelfMaintainBufferData sendBuffer;
	initializeSelfMaintainBuffer(&sendBuffer, PCONTEXT);
	appendSMBVar(&sendBuffer, MessageID);
	appendSelfMaintainBufferTill64bitAligned(&sendBuffer);

	/* Build response head information. */
	RPCResponseRBAllocateResourceContainersHeadData responsehead;
	responsehead.Result					= errorcode;
	responsehead.MemoryMB 				= request->MemoryMB;
	responsehead.Core					= request->Core;
	responsehead.ContainerCount 		= 0;
	responsehead.ExpectedContainerCount	= request->ContainerCount;
	responsehead.HostNameStringLen		= 0;
	responsehead.SystemStartTimestamp   = ResBrokerStartTime;
	appendSMBVar(&sendBuffer, responsehead);

	elog(DEBUG3, "YARN mode resource broker sends resource allocation "
				 "error response %d.", errorcode);
	int buffsize = getSMBContentSize(&sendBuffer);
	int piperes = writePipe(ResBrokerNotifyPipe[1], sendBuffer.Buffer, buffsize);
	/* Free send buffer. */
	destroySelfMaintainBuffer(&sendBuffer);

	if ( piperes != buffsize )
	{
		elog(WARNING, "YARN mode resource broker failed to write out resource "
					  "allocation response error message from pipe. "
					  "Wrote length %d, expected length %d.",
					  piperes,
					  buffsize);
		return RESBROK_PIPE_ERROR;
	}
	return errorcode;
}

int handleRM2RB_ReturnResource(void)
{
	uint32_t				 MessageID    = RB2RM_RETURNED_RESOURCE;
	int						 res		  = FUNC_RETURN_OK;
	int 					 libyarnres   = FUNCTION_SUCCEEDED;
	int						 piperes	  = 0;
	int 					 actualsize   = 0;
	int64_t    				*containerids = NULL;

	/* Read request content. */
	RPCRequestRBReturnResourceContainersHeadData request;
	piperes = readPipe(ResBrokerRequestPipe[0], &request, sizeof(request));
	if ( piperes != sizeof(request) ) {
		elog(WARNING, "YARN mode resource broker failed to read resource return "
					  "request message from pipe. "
					  "Read length %d, expected length %lu, errno %m.",
					  piperes,
					  sizeof(request));
		return RESBROK_PIPE_ERROR;
	}

	/* Read the container id list. */
	actualsize = request.ContainerCount;
	containerids = rm_palloc(PCONTEXT, sizeof(int64_t) * actualsize);

	piperes = readPipe(ResBrokerRequestPipe[0],
					   containerids,
					   sizeof(int64_t) * actualsize);
	if ( piperes != sizeof(int64_t) * actualsize )
	{
		elog(WARNING, "YARN mode resource broker failed to read resource return "
					  "request message (container ids) from pipe. "
					  "Read length %d, expected length %lu, errno %m.",
					  piperes,
					  sizeof(request));
		if( containerids != NULL )
		{
			rm_pfree(PCONTEXT, containerids);
		}
		return RESBROK_PIPE_ERROR;
	}

    elog(LOG, "YARN mode resource broker returns %d Containers to YARN.",
    		  request.ContainerCount);

    for ( int i = 0 ; i < request.ContainerCount ; ++i )
    {
    	elog(RMLOG, "YARN mode resource broker tries to return container of id "
    			    INT64_FORMAT,
					containerids[i]);
    }

	if ( YARNJobID == NULL )
	{
		if( containerids != NULL ) {
			rm_pfree(PCONTEXT, containerids);
		}
		return sendRBReturnResourceErrorData(RESBROK_ERROR_GRM);
	}

	/* Return resource to global resource manager. */
    libyarnres = RB2YARN_returnResource(containerids, request.ContainerCount);
    if ( libyarnres != FUNCTION_SUCCEEDED )
    {
    	if( containerids != NULL ) {
    		rm_pfree(PCONTEXT, containerids);
    	}
    	return sendRBReturnResourceErrorData(RESBROK_ERROR_GRM);
    }

	/*
	 * Build response message.
	 */

	/* Prepare buffer for the content to send. */
	SelfMaintainBufferData sendBuffer;
	initializeSelfMaintainBuffer(&sendBuffer, PCONTEXT);

	appendSMBVar(&sendBuffer, MessageID);
	appendSelfMaintainBufferTill64bitAligned(&sendBuffer);

	RPCResponseRBReturnResourceContainersData response;
	response.Result 			  = FUNC_RETURN_OK;
	response.Reserved			  = 0;
	response.SystemStartTimestamp = ResBrokerStartTime;
	appendSMBVar(&sendBuffer, response);

	/*
	 * Send Message.
	 */
	int buffsize = getSMBContentSize(&sendBuffer);
	piperes = writePipe(ResBrokerNotifyPipe[1], sendBuffer.Buffer, buffsize);
	if ( piperes != buffsize )
	{
		res = RESBROK_PIPE_ERROR;
		elog(WARNING, "YARN mode resource broker failed to write out resource "
					  "return response message from pipe. "
					  "Wrote length %d, expected length %d, errno %m.",
					  piperes,
					  buffsize);
	}
	else
	{
		elog(DEBUG3, "Sent YARN resource return response to resource manager.");
	}

	/* Free send buffer. */
	destroySelfMaintainBuffer(&sendBuffer);

	if( containerids != NULL )
	{
		rm_pfree(PCONTEXT, containerids);
	}
	return res;
}

int sendRBReturnResourceErrorData(int errorcode)
{
	uint32_t MessageID = RB2RM_RETURNED_RESOURCE;

	/* Prepare buffer for the content to send. */
	SelfMaintainBufferData sendBuffer;
	initializeSelfMaintainBuffer(&sendBuffer, PCONTEXT);
	appendSMBVar(&sendBuffer, MessageID);
	appendSelfMaintainBufferTill64bitAligned(&sendBuffer);

	RPCResponseRBReturnResourceContainersData response;
	response.Result 			  = errorcode;
	response.Reserved			  = 0;
	response.SystemStartTimestamp = ResBrokerStartTime;
	appendSMBVar(&sendBuffer, response);

	elog(DEBUG3, "YARN mode resource broker sends resource return "
				 "error response %d.", errorcode);

	int buffsize = getSMBContentSize(&sendBuffer);
	int piperes = writePipe(ResBrokerNotifyPipe[1], sendBuffer.Buffer, buffsize);
	/* Free send buffer. */
	destroySelfMaintainBuffer(&sendBuffer);

	if ( piperes != buffsize )
	{
		elog(WARNING, "YARN mode resource broker failed to write out resource "
					  "return response error message from pipe. "
					  "Wrote length %d, expected length %d.",
					  piperes,
					  buffsize);
		return RESBROK_PIPE_ERROR;
	}
	return errorcode;
}

int handleRM2RB_GetContainerReport(void)
{
	uint32_t MessageID  = RB2RM_CONTAINERREPORT;
	int		 res		= FUNC_RETURN_OK;
	int 	 libyarnres = FUNCTION_SUCCEEDED;
	int		 piperes	= 0;

	if ( YARNJobID == NULL )
	{
		return sendRBGetContainerReportErrorData(RESBROK_ERROR_GRM);
	}

	/* Get container report from yarn resource manager. */
	RB_GRMContainerStat  ctnstats = NULL;
	int 				 size	  = 0;
    libyarnres = RB2YARN_getContainerReport(&ctnstats, &size);
    if ( libyarnres != FUNCTION_SUCCEEDED )
    {
    	if( ctnstats != NULL )
    	{
    		rm_pfree(PCONTEXT, ctnstats);
    	}
    	return sendRBGetContainerReportErrorData(RESBROK_ERROR_GRM);
    }

    elog(LOG, "YARN mode resource broker got total %d containers", size);

    for( int i = 0 ; i < size ; ++i )
    {
    	elog(RMLOG, "Container report ID:"INT64_FORMAT", isActive:%d",
    			    ctnstats[i].ContainerID,
					ctnstats[i].isActive);
    }

	/* Build response message. */
	SelfMaintainBufferData sendBuffer;
	initializeSelfMaintainBuffer(&sendBuffer, PCONTEXT);

	appendSMBVar(&sendBuffer, MessageID);
	appendSelfMaintainBufferTill64bitAligned(&sendBuffer);

	RPCResponseRBGetContainerReportHeadData response;
	response.Result				  = FUNC_RETURN_OK;
	response.ContainerCount 	  = size;
	response.SystemStartTimestamp = ResBrokerStartTime;

	appendSMBVar(&sendBuffer, response);
	appendSelfMaintainBuffer(&sendBuffer,
							 (char *)ctnstats,
							 size * sizeof(RB_GRMContainerStatData));
	/* Send Message. */
	int buffsize = getSMBContentSize(&sendBuffer);
	piperes = writePipe(ResBrokerNotifyPipe[1], sendBuffer.Buffer, buffsize);
	if ( piperes != getSMBContentSize(&sendBuffer) )
	{
		res = RESBROK_PIPE_ERROR;
		elog(WARNING, "YARN mode resource broker failed to write out container "
					  "report response message to pipe. "
					  "Wrote length %d, expected length %d, errno %m.",
					  piperes,
					  buffsize);
	}
	else
	{
		elog(DEBUG3, "Sent YARN container report response to resource manager.");
	}

	/* Free send buffer. */
	destroySelfMaintainBuffer(&sendBuffer);

	if( ctnstats != NULL )
	{
		rm_pfree(PCONTEXT, ctnstats);
	}
	return res;
}

int sendRBGetContainerReportErrorData(int errorcode)
{
	uint32_t MessageID = RB2RM_CONTAINERREPORT;

	/* Prepare buffer for the content to send. */
	SelfMaintainBufferData sendBuffer;
	initializeSelfMaintainBuffer(&sendBuffer, PCONTEXT);
	appendSMBVar(&sendBuffer, MessageID);
	appendSelfMaintainBufferTill64bitAligned(&sendBuffer);

	RPCResponseRBGetContainerReportHeadData response;
	response.Result 			  = errorcode;
	response.ContainerCount 	  = 0;
	response.SystemStartTimestamp = ResBrokerStartTime;
	appendSMBVar(&sendBuffer, response);

	elog(DEBUG3, "YARN mode resource broker sends container report error "
				 "response %d.",
				 errorcode);

	int buffsize = getSMBContentSize(&sendBuffer);
	int piperes = writePipe(ResBrokerNotifyPipe[1], sendBuffer.Buffer, buffsize);
	/* Free send buffer. */
	destroySelfMaintainBuffer(&sendBuffer);

	if ( piperes != buffsize )
	{
		elog(WARNING, "YARN mode resource broker failed to write out container "
					  "report response error message from pipe. "
					  "Wrote length %d, expected length %d.",
					  piperes,
					  buffsize);
		return RESBROK_PIPE_ERROR;
	}
	return errorcode;
}
/*******************************************************************************
 * Wrapper of libyarn APIs to negotiate resource with YARN.
 *
 * RETURN VALUES follow the definition of libyarn APIs.
 ******************************************************************************/

/*
 * Initialize connection to global resource manager before providing resource
 * negotiation services.
 */
int RB2YARN_initializeConnection(void)
{
	int yarnres = FUNCTION_SUCCEEDED;

	/* Connect to YARN. */
	yarnres = RB2YARN_connectToYARN();
	if ( yarnres != FUNCTION_SUCCEEDED ) { return yarnres; }

	/* Register in global resource manager. */
	yarnres = RB2YARN_registerYARNApplication();
	if ( yarnres != FUNCTION_SUCCEEDED ) {
		/* Free libYARN client instance. */
		RB2YARN_disconnectFromYARN();
		return yarnres;
	}

	elog(LOG, "YARN mode resource broker is ready to access YARN resource manager.");

	YARNResourceTight = false;

	return FUNCTION_SUCCEEDED;
}

#define HAWQ_YARN_AM_HEARTBEAT_INTERVAL 5

/* Connect and disconnect to the global resource manager. */
int RB2YARN_connectToYARN(void)
{
	int yarnres = FUNCTION_SUCCEEDED;

    /* Setup YARN client. */
	yarnres = newLibYarnClient(YARNUser,
							   YARNServer.Str,
						       YARNPort.Str,
						       YARNSchedulerServer.Str,
						       YARNSchedulerPort.Str,
						       YARNAMServer,
						       YARNAMPort,
						       YARNTRKUrl,
						       &LIBYARNClient,
						       HAWQ_YARN_AM_HEARTBEAT_INTERVAL*1000 /* Hard code 5 sec */);
    return yarnres;
}

int RB2YARN_registerYARNApplication(void)
{
	int retry = 5;
	int yarnres = FUNCTION_SUCCEEDED, result = FUNCTION_SUCCEEDED;

	yarnres = createJob(LIBYARNClient,
					    YARNAppName.Str,
						YARNQueueName.Str,
						&YARNJobID);
	if ( yarnres != FUNCTION_SUCCEEDED )
	{
		elog(WARNING, "YARN mode resource broker failed to create application "
					  "in YARN resource manager. %s",
					  getErrorMessage());
		return yarnres;
	}

	elog(LOG, "YARN mode resource broker created job in YARN resource "
			  "manager %s as new application %s assigned to queue %s.",
			  YARNJobID,
			  YARNAppName.Str,
			  YARNQueueName.Str);

	/* check if hawq is registered successfully in Hadoop Yarn.
	 * if not, kill application from Hadoop Yarn.
	 */
	LibYarnApplicationReport_t *applicationReport = NULL;
	while (retry > 0)
	{
		retry--;
		result = getApplicationReport(LIBYARNClient, YARNJobID, &applicationReport);
		if (result != FUNCTION_SUCCEEDED || applicationReport == NULL)
		{
			if (retry > 0) {
				usleep(HAWQ_YARN_AM_HEARTBEAT_INTERVAL*1000*1000L);
				continue;
			} else {
				elog(WARNING, "YARN mode resource broker failed to get application report, "
							  "so kill it from Hadoop Yarn.");
				result = forceKillJob(LIBYARNClient, YARNJobID);
				if (result != FUNCTION_SUCCEEDED)
					elog(WARNING, "YARN mode resource broker kill job failed.");
				return FUNCTION_FAILED;
			}
		}

		if (applicationReport->progress < 0.5)
		{
			if (retry > 0) {
				usleep(HAWQ_YARN_AM_HEARTBEAT_INTERVAL*1000*1000L);
				continue;
			} else {
				elog(WARNING, "YARN mode resource broker failed to register itself in Hadoop Yarn."
							  "Got progress:%f, and try to kill application from Hadoop Yarn",
							  applicationReport->progress);
				result = forceKillJob(LIBYARNClient, YARNJobID);
				if (result != FUNCTION_SUCCEEDED)
					elog(WARNING, "YARN mode resource broker kill job failed.");
				return FUNCTION_FAILED;
			}
		} else {
			break;
		}
	}

	ResBrokerStartTime = gettime_microsec();

	elog(LOG, "YARN mode resource broker registered new "
			  "YARN application. Progress:%f, Start time stamp "UINT64_FORMAT,
			  applicationReport->progress, ResBrokerStartTime);

	return yarnres;
}

/**
 * Get YARN cluster report.
 */
int RB2YARN_getClusterReport(DQueue hosts)
{
	int yarnres = FUNCTION_SUCCEEDED;

    LibYarnNodeReport_t *nodeReportArray;
    int nodeReportArraySize;
    yarnres = getClusterNodes(LIBYARNClient,
    						  NODE_STATE_RUNNING,
							  &nodeReportArray,
							  &nodeReportArraySize);
    if ( yarnres != FUNCTION_SUCCEEDED )
    {
    	elog(WARNING, "YARN mode resource broker failed to get cluster "
    				  "information from YARN. %s",
					  getErrorMessage());
    }
    else
    {
    	elog(LOG, "YARN mode resource broker got information of %d YARN cluster "
    			  "nodes.",
    			  nodeReportArraySize);

    	/* Build result. For LIBYARN implementation, we only get the host name. */
    	for ( int i = 0 ; i < nodeReportArraySize ; ++i )
    	{
    		SimpString  			 ohostname;
    		LibYarnNodeReport_t 	*pnodereport 	= &(nodeReportArray[i]);
			List 	   				*gottenaddr 	= NULL;
			ListCell				*addrcell		= NULL;
			AddressString			 addr			= NULL;
			uint16_t 				 addroffset 	= 0;
			uint16_t 				 addrattr 		= HOST_ADDRESS_CONTENT_STRING;
			int 					 addrcount 		= 0;

			SelfMaintainBufferData	 AddrAttribute;
			SelfMaintainBufferData 	 AddrContent;
    		initializeSelfMaintainBuffer(&AddrAttribute, PCONTEXT);
			initializeSelfMaintainBuffer(&AddrContent,   PCONTEXT);

			/* Expect to get running nodes only. */
			Assert( pnodereport->nodeState == NODE_STATE_RUNNING );

    		/* Get host addresses based on the  */
			initSimpleString(&ohostname, PCONTEXT);
			getHostIPV4AddressesByHostNameAsString(PCONTEXT,
												   pnodereport->host,
												   &ohostname,
												   &gottenaddr);
			freeSimpleStringContent(&ohostname);

			addrcount = list_length(gottenaddr);
			if ( addrcount > 0 )
			{
				/*
				 * Calculate the offset containing only SegInfo head and address
				 * attributes. All addresses are now passed by string format.
				 */
    			addroffset = sizeof(SegInfoData) +
    						 __SIZE_ALIGN64(sizeof(uint32_t) * addrcount);

    			/* Build address offset, attribute and content. */
    			foreach(addrcell, gottenaddr)
    			{
    				addr = (AddressString)(lfirst(addrcell));

    				elog(DEBUG3, "YARN mode resource broker gets host address %s.",
    							addr->Address);

    				/* Build address attributes */
    				appendSMBVar(&AddrAttribute, addroffset);
    				appendSMBVar(&AddrAttribute, addrattr);

    				/* Build address content */
    				appendSelfMaintainBuffer(&AddrContent,
    										 (char *)addr,
											 ADDRESS_STRING_ALL_SIZE(addr));
    				appendSelfMaintainBufferTill64bitAligned(&AddrContent);

    				/* Adjust address offset data. */
    				addroffset = sizeof(SegInfoData) +
    							 __SIZE_ALIGN64(sizeof(uint32_t) * addrcount) +
								 getSMBContentSize(&AddrContent);
    			}
    			appendSelfMaintainBufferTill64bitAligned(&AddrAttribute);
    		}

			int hostnamelen = strlen(pnodereport->host);
			int racknamelen = strlen(pnodereport->rackName);
    		int segsize = sizeof(SegInfoData) +		    		/* machine id head    */
    					  getSMBContentSize(&AddrAttribute) +	/* addr attribute size*/
						  getSMBContentSize(&AddrContent) +	    /* addr content size  */
						  __SIZE_ALIGN64(hostnamelen+1) +		/* host name		  */
						  __SIZE_ALIGN64(racknamelen+1);		/* rack name		  */

    		SegStat segstat = (SegStat)rm_palloc0(PCONTEXT,
    											  offsetof(SegStatData, Info) +
												  segsize);

    		segstat->ID     					 = SEGSTAT_ID_INVALID;
    		segstat->FTSAvailable 				 = RESOURCE_SEG_STATUS_UNSET;
    		segstat->GRMTotalMemoryMB  	   		 = pnodereport->memoryCapability;
    		segstat->GRMTotalCore	  	   		 = pnodereport->vcoresCapability;
    		segstat->FTSTotalMemoryMB	   		 = 0;
    		segstat->FTSTotalCore		   		 = 0;
    		segstat->Info.HostAddrCount 		 = addrcount;
    		segstat->Info.AddressAttributeOffset = sizeof(SegInfoData);
    		segstat->Info.AddressContentOffset   = sizeof(SegInfoData) +
    											   getSMBContentSize(&AddrAttribute);
    		segstat->Info.HostNameLen			 = 0;
    		segstat->Info.HostNameOffset		 = 0;
    		segstat->Info.GRMHostNameLen 	 	 = hostnamelen;
    		segstat->Info.GRMHostNameOffset 	 = sizeof(SegInfoData) +
    											   getSMBContentSize(&AddrAttribute) +
												   getSMBContentSize(&AddrContent);
    		segstat->Info.GRMRackNameLen         = racknamelen;
    		segstat->Info.GRMRackNameOffset 	 = segstat->Info.GRMHostNameOffset +
    											   __SIZE_ALIGN64(hostnamelen+1);
    		segstat->Info.FailedTmpDirOffset	 = 0;
    		segstat->Info.FailedTmpDirLen		 = 0;
    		segstat->Info.Size 		 		 	 = segsize;

    		memcpy((char *)&(segstat->Info) + sizeof(SegInfoData),
				   AddrAttribute.Buffer,
				   getSMBContentSize(&AddrAttribute));

    		memcpy((char *)&(segstat->Info) + sizeof(SegInfoData) +
    			   getSMBContentSize(&AddrAttribute),
				   AddrContent.Buffer,
				   getSMBContentSize(&AddrContent));

    		destroySelfMaintainBuffer(&AddrAttribute);
    		destroySelfMaintainBuffer(&AddrContent);

    		freeHostIPV4AddressesAsString(PCONTEXT, &gottenaddr);
    		Assert( gottenaddr == NULL );

    		strcpy(GET_SEGINFO_GRMHOSTNAME(&(segstat->Info)), pnodereport->host);
    		strcpy(GET_SEGINFO_GRMRACKNAME(&(segstat->Info)), pnodereport->rackName);

    		insertDQueueTailNode(hosts, segstat);

        	elog(RMLOG, "YARN mode resource broker got YARN cluster host \n"
        			    "\thost:%s\n"
					    "\tport:%d\n"
					    "\thttpAddress:%s\n"
        			    "\trackName:%s\n"
        			    "\tmemoryUsed:%d\n"
        			    "\tvcoresUsed:%d\n"
        			    "\tmemoryCapability:%d\n"
        			    "\tvcoresCapability:%d\n"
        			    "\tnumContainers:%d\n"
        			    "\tnodeState:%d\n"
        			    "\thealthReport:%s\n"
        			    "\tlastHealthReportTime:"INT64_FORMAT"\n"
					    "\tmachineidsize:%d",
    				    pnodereport->host,
					    pnodereport->port,
					    pnodereport->httpAddress,
					    pnodereport->rackName,
					    pnodereport->memoryUsed,
					    pnodereport->vcoresUsed,
					    pnodereport->memoryCapability,
					    pnodereport->vcoresCapability,
					    pnodereport->numContainers,
					    pnodereport->nodeState,
					    pnodereport->healthReport,
					    pnodereport->lastHealthReportTime,
					    segstat->Info.Size);

        	elog(RMLOG, "YARN mode reosurce broker built cluster segment %s at rack %s",
        			    GET_SEGINFO_GRMHOSTNAME(&(segstat->Info)),
        			    GET_SEGINFO_GRMRACKNAME(&(segstat->Info)));
    	}
    	freeMemNodeReportArray(nodeReportArray, nodeReportArraySize);
    }
	return yarnres;
}

int RB2YARN_acquireResource(uint32_t memorymb,
							uint32_t core,
							uint32_t count,
							LibYarnNodeInfo_t *preferredArray,
							uint32_t preferredSize,
							DQueue	 containerids,
							DQueue	 containerhosts)
{
	int yarnres = FUNCTION_SUCCEEDED;

    char *blackListAdditions[0];
    char *blackListRemovals[0];

    LibYarnResource_t *allocatedResourcesArray = NULL;
    int allocatedResourcesArraySize;
    yarnres = allocateResources(LIBYARNClient,
    							YARNJobID,
    							1, 	      /* priority */
    							core, 	  /* core */
    							memorymb, /* memory */
    							count,
								blackListAdditions,
								0,
								blackListRemovals,
								0,
								preferredArray,
								preferredSize,
								&allocatedResourcesArray,
								&allocatedResourcesArraySize);
    if( yarnres != FUNCTION_SUCCEEDED ) {
    	elog(WARNING, "YARN mode resource broker failed to allocate "
    			  	  "containers from YARN. %s",
					  getErrorMessage());
    	return yarnres;
    }
    else if ( allocatedResourcesArraySize == 0 ) {
    	elog(LOG, "YARN mode resource broker temporarily cannot acquire "
    			  "container from YARN.");
    	return yarnres;
    }

    elog(LOG, "YARN mode resource broker allocated %d containers from YARN.",
    		  allocatedResourcesArraySize);

    HASHTABLEData FailedIDIndex;
	initializeHASHTABLE(&FailedIDIndex,
						PCONTEXT,
						HASHTABLE_SLOT_VOLUME_DEFAULT,
						HASHTABLE_SLOT_VOLUME_DEFAULT_MAX,
						HASHTABLE_KEYTYPE_CHARARRAY,
						NULL);

    /* Activate containers. */
    int64_t activeContainerIds[allocatedResourcesArraySize];
    for ( int i = 0 ; i < allocatedResourcesArraySize ; ++i ) {
    	activeContainerIds[i]  = allocatedResourcesArray[i].containerId;
    }

    yarnres = activeResources(LIBYARNClient,
    						  YARNJobID,
							  activeContainerIds,
							  allocatedResourcesArraySize);
    if( yarnres != FUNCTION_SUCCEEDED ) {
    	elog(WARNING, "YARN mode resource broker failed to activate containers. %s",
    			      getErrorMessage());
    	goto exit;
    }

    elog(LOG, "YARN mode resource broker submitted to activate %d containers.",
    		  allocatedResourcesArraySize);

    /* Return the containers fail to activate. */
    int64_t *activeFailIds = NULL;
    int  activeFailSize = 0;
    yarnres = getActiveFailContainerIds(LIBYARNClient,
    									&activeFailIds,
										&activeFailSize);
    if( yarnres != FUNCTION_SUCCEEDED ) {
    	elog(WARNING, "YARN mode resource broker failed to get active-fail "
    			  	  "containers. %s",
					  getErrorMessage());
    	goto exit;
    }

    /* Build temporary failed container ids in hash table for fast retrieving.*/
    if ( activeFailSize > 0 ) {
    	for (int i = 0 ; i < activeFailSize ; ++i) {
    		elog(WARNING, "YARN mode resource broker failed to activate "
    					  "container "INT64_FORMAT,
						  activeFailIds[i]);

			SimpArray key;
			setSimpleArrayRef(&key, (void *)&(activeFailIds[i]), sizeof(int64_t));
			setHASHTABLENode(&FailedIDIndex, &key, TYPCONVERT(void *, &activeFailIds[i]), false);
		}

    	yarnres = releaseResources(LIBYARNClient,
    							   YARNJobID,
								   activeFailIds,
								   activeFailSize);
    	if ( yarnres != FUNCTION_SUCCEEDED ) {
    		elog(WARNING, "YARN mode resource broker failed to return active-fail "
    					  "containers. %s",
						  getErrorMessage());
    	}
    }

    /* Build result. */
    for ( int i = 0 ; i < allocatedResourcesArraySize ; ++i )
    {
    	int64_t *ctnid = (int64_t *)rm_palloc0(PCONTEXT, sizeof(int64_t));
    	*ctnid = allocatedResourcesArray[i].containerId;
    	insertDQueueTailNode(containerids, ctnid);

    	char *hostnamestr =
    			(char *)rm_palloc0(PCONTEXT,
    							   strlen(allocatedResourcesArray[i].host) + 1);
    	strcpy(hostnamestr, allocatedResourcesArray[i].host);
    	insertDQueueTailNode(containerhosts, hostnamestr);

    	elog(RMLOG, "YARN mode resource broker allocated and activated container. "
    			    "ID : "INT64_FORMAT"(%d MB, %d CORE) at %s.",
					allocatedResourcesArray[i].containerId,
					allocatedResourcesArray[i].memory,
					allocatedResourcesArray[i].vCores,
					allocatedResourcesArray[i].host);
    }

exit:
	/* Free allocated */
	freeMemAllocatedResourcesArray(allocatedResourcesArray,
								   allocatedResourcesArraySize);
    /* Free list of active fail id list. */
    if ( activeFailIds != NULL ) {
    	free(activeFailIds);
    }

    clearHASHTABLE(&FailedIDIndex);

	return FUNCTION_SUCCEEDED;
}

int RB2YARN_returnResource(int64_t *contids, int contcount)
{
	if( contcount == 0 )
		return FUNCTION_SUCCEEDED;

	Assert(contids != NULL);
	Assert(contcount > 0);
	int yarnres = FUNCTION_SUCCEEDED;
	yarnres = releaseResources(LIBYARNClient,
	    					   YARNJobID,
							   contids,
							   contcount);
	if ( yarnres != FUNCTION_SUCCEEDED ) {
		elog(WARNING, "YARN mode resource broker failed to return containers. %s",
				  	  getErrorMessage());
	}

	for ( int i = 0 ; i < contcount ; ++i ) {
		elog(LOG, "YARN mode resource broker returned container of id "INT64_FORMAT,
				  contids[i]);
	}

	return yarnres;
}

int RB2YARN_getContainerReport(RB_GRMContainerStat *ctnstats, int *size)
{
	Assert( ctnstats != NULL );
	int 					  yarnres     = FUNCTION_SUCCEEDED;
	LibYarnContainerReport_t *ctnrparr    = NULL;
	int 					  arrsize     = 0;
	LibYarnContainerStatus_t *ctnstatarr  = NULL;
	int						  ctnstatsize = 0;
	int64_t 				 *ctnidarr    = NULL;

	*ctnstats = NULL;
	*size     = 0;

	yarnres = getContainerReports(LIBYARNClient, YARNJobID, &ctnrparr, &arrsize);
	if ( yarnres != FUNCTION_SUCCEEDED )
	{
		elog(WARNING, "YARN mode resource broker failed to get container "
					  "report. %s",
					  getErrorMessage());
	}
	else if ( arrsize > 0 )
	{
		/*
		 * TODO:
		 * There is a problem here that container report does not get correct
		 * container status. The work round here is to call container status API
		 * to get final container statuses.
		 */
		ctnidarr = (int64_t *)rm_palloc(PCONTEXT, sizeof(int64_t) * arrsize);
		for ( int i = 0 ; i < arrsize ; ++i )
		{
			ctnidarr[i] = ctnrparr[i].containerId;
		}

        yarnres = getContainerStatuses(LIBYARNClient,
        							   YARNJobID,
									   ctnidarr,
									   arrsize,
									   &ctnstatarr,
									   &ctnstatsize);
        if ( yarnres != FUNCTION_SUCCEEDED )
        {
        	elog(WARNING, "YARN mode resource broker failed to get container "
        				  "status. %s",
        			  	  getErrorMessage());
        }

        rm_pfree(PCONTEXT, ctnidarr);

        if( ctnstatsize > 0 )
        {
			*size = ctnstatsize;
			*ctnstats = rm_palloc0(PCONTEXT,
								   sizeof(RB_GRMContainerStatData) * ctnstatsize);
			for ( int i = 0 ; i < ctnstatsize ; ++i )
			{
				(*ctnstats)[i].ContainerID = ctnstatarr[i].containerId;
				(*ctnstats)[i].isActive    = ctnstatarr[i].state == C_RUNNING ? 1 : 0;
				(*ctnstats)[i].isFound     = 0;
			}
        }
		freeContainerStatusArray(ctnstatarr, ctnstatsize);

	}
	if(ctnrparr != NULL && arrsize > 0)
		freeContainerReportArray(ctnrparr, arrsize);
	return yarnres;
}

void RB2YARN_freeContainersInMemory(DQueue containerids, DQueue containerhosts)
{
	if ( containerids != NULL )
	{
		while( containerids->NodeCount > 0 )
		{
			int64_t *pctnid = removeDQueueHeadNode(containerids);
			rm_pfree(PCONTEXT, pctnid);
		}
		cleanDQueue(containerids);
	}

	if ( containerhosts != NULL )
	{
		while( containerhosts->NodeCount > 0 )
		{
			char *hostname = removeDQueueHeadNode(containerhosts);
			rm_pfree(PCONTEXT, hostname);
		}
		cleanDQueue(containerhosts);
	}
}

const char *RB2YARN_getErrorMessage(void)
{
	return getErrorMessage();
}
int  RB2YARN_getQueueReport(char 	*queuename,
							double 	*cap,
							double 	*curcap,
							double 	*maxcap,
							bool	*haschildren)
{
	int yarnres = FUNCTION_SUCCEEDED;
	LibYarnQueueInfo_t *queueInfo = NULL;

	Assert( queuename != NULL );
	yarnres = getQueueInfo(LIBYARNClient, queuename, true, true, true, &queueInfo);
	if ( yarnres != FUNCTION_SUCCEEDED ) {
		elog(WARNING, "YARN mode resource broker failed to get YARN queue report "
					  "of queue %s. %s",
					  queuename,
					  RB2YARN_getErrorMessage());
		return yarnres;
	}

	Assert( queueInfo != NULL );
	*cap    = queueInfo->capacity;
	*curcap = queueInfo->currentCapacity;
	*maxcap = queueInfo->maximumCapacity;
	*haschildren = queueInfo->childQueueNameArraySize > 0;

	freeMemQueueInfo(queueInfo);

	return FUNCTION_SUCCEEDED;
}

int  RB2YARN_finishYARNApplication(void)
{
	int yarnres = FUNCTION_SUCCEEDED;
    yarnres = finishJob(LIBYARNClient, YARNJobID, APPLICATION_SUCCEEDED);
    if (yarnres != FUNCTION_SUCCEEDED) {
    	elog(WARNING, "YARN mode resource broker failed to finish job in YARN. %s",
    			  	  getErrorMessage());
    }
    else {
    	elog(LOG, "YARN mode resource broker finished job in YARN.");
    }
    return yarnres;
}

int  RB2YARN_disconnectFromYARN(void)
{
	if ( LIBYARNClient != NULL) {
		deleteLibYarnClient(LIBYARNClient);
	}
	if ( YARNJobID != NULL ) {
		free(YARNJobID);
	}
	LIBYARNClient 		= NULL;
	YARNJobID 			= NULL;
	return FUNCTION_SUCCEEDED;
}
