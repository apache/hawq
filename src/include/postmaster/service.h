/*-------------------------------------------------------------------------
 *
 * server.c
 *	  Shared code module that supports Greenplum system server processes
 *    under the postmaster that communicate with other Postgres processes
 *    with TCP/IP (same GP instance or different GP instance).
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
 *-------------------------------------------------------------------------
 */
#ifndef SERVICE_H
#define SERVICE_H

#ifndef _WIN32
#include <stdint.h>             /* uint32_t (C99) */
#else
typedef unsigned int uint32_t;
#endif 

typedef struct ServiceCtrl
{

	void    *serviceConfig;

	int 	listenerFd;

	int		listenerPort;
	
}	ServiceCtrl;


#define SERVER_EYE_CATCHER_LEN 6


/*
 * Kludge to prevent our error handling during communication with master standby by raising and
 * catching ERROR from becoming a PANIC in critical sections... 
 * 
 * The reason for kludge is to avoid postmaster reset on master due to master standby issues. 
 *
 * However PANIC needs to be raised when transaction is in commit state.
 * Justification 
 *		a) to be able to recover GPDB (i.e. avoid commit and abort for the same transaction in the XLOG)
 *		b) to have consistent behavior with other Transaction Management design/code
 *			*) During transaction commit the code is in Critical section, any failure causes PANIC
 *			*) After transaction commit there is a check in AbortTransaction. If transaction has been already 
 *				committed then PANIC is raised
 */
#define DECLARE_SAVE_SUPPRESS_PANIC()\
	volatile int SaveSuppressPanic = false; \
	bool IsCommit = IsCommitInProgress();

#define SUPPRESS_PANIC()\
{\
	if (! IsCommit) \
	{ \
		SaveSuppressPanic = SuppressPanic;\
		SuppressPanic = true;\
	} \
}

#define RESTORE_PANIC()\
{\
	if (! IsCommit) \
	{ \
		SuppressPanic = SaveSuppressPanic;\
	} \
}


typedef struct ServiceConfig
{
	char    eyeCatcher[SERVER_EYE_CATCHER_LEN];

	char		*title;
	char		*psTitle;
	char		*forkTitle;

	int			requestLen;
	int			responseLen;

	void		(*ServiceClientTimeout) (struct timeval *clientTimeout);
	void		(*ServiceRequestShutdown) (SIGNAL_ARGS);
	void		(*ServiceEarlyInit) (void);
	void		(*ServicePostgresInit) (void);
	void		(*ServiceInit) (int listenerPort);
	bool		(*ServiceShutdownRequested) (void);
	bool		(*ServiceRequest) (ServiceCtrl *serviceCtrl, int sockfd, uint8 *request);
	void		(*ServiceShutdown) (void);
	void		(*ServicePostmasterDied) (void);
	
}	ServiceConfig;

typedef struct ServiceClient
{
	ServiceConfig    *serviceConfig;

	int 	sockfd;
	
}	ServiceClient;

extern void ServiceInit(ServiceConfig *serviceConfig, ServiceCtrl *serviceCtrl);
extern void ServiceMain(ServiceCtrl *serviceCtrl);
extern void ServiceGetClientTimeout(ServiceConfig *serviceConfig, struct timeval *timeout); 
extern bool ServiceClientConnect(ServiceConfig *serviceConfig, int listenerPort, ServiceClient *serverClient, bool complain);
extern bool ServiceClientSendRequest(ServiceClient *serverClient, void* request, int requestLen);
extern bool ServiceClientReceiveResponse(ServiceClient *serverClient, void* response, int responseLen, struct timeval *timeout);
extern bool ServiceClientPollResponse(ServiceClient *serverClient, void* response, int responseLen, bool *pollResponseReceived);
extern char *ServiceGetLastClientErrorString(void);
extern bool ServiceProcessRespond(ServiceCtrl *serviceCtrl, int sockfd, uint8 *response, int responseLen);

#ifdef EXEC_BACKEND
extern pid_t ServiceForkExec(ServiceConfig *serviceConfig);
#endif

#endif   /* SERVICE_H */
