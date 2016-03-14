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

#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_ASYNCCOMM_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_ASYNCCOMM_H

#include "resourcemanager/envswitch.h"
#include "resourcemanager/utils/simplestring.h"

extern MCTYPE AsyncCommContext;

struct AsyncCommBufferData;
typedef struct AsyncCommBufferData  AsyncCommBufferData;
typedef struct AsyncCommBufferData *AsyncCommBuffer;

typedef void ( * PollReadyHandlerType)(AsyncCommBuffer);

struct AsyncCommBufferHandlersData {
	PollReadyHandlerType    InitHandle;			/* After creating this buff   */
	PollReadyHandlerType    ReadReadyHandle;	/* Before reading new data    */
	PollReadyHandlerType   	ReadPostHandle;		/* After reading new data     */
	PollReadyHandlerType   	WriteReadyHandle;	/* Before writing data out    */
	PollReadyHandlerType	WritePostHandle;	/* After writing data         */
	PollReadyHandlerType   	ErrorReadyHandle;   /* When POLLERR or POLLHUP set*/
	PollReadyHandlerType	CleanUpHandle;		/* Before freeing this buffer */
};

typedef struct AsyncCommBufferHandlersData  AsyncCommBufferHandlersData;
typedef struct AsyncCommBufferHandlersData *AsyncCommBufferHandlers;

#define ASYNCCOMM_READ 		 0X00000001 /* Handle read ready action. */
#define ASYNCCOMM_READBYTES  0X00000002 /* Should read bytes from connection and handle it. */
#define ASYNCCOMM_WRITE		 0X00000004 /* Handle write ready action. */
#define ASYNCCOMM_WRITEBYTES 0X00000008	/* Should write bytes to connection. */

struct AsyncCommBufferData {
	int						 FD;

	/* Socket connection information */
	SimpString				 ClientHostname;
	struct sockaddr_in 		 ClientAddr;
	socklen_t				 ClientAddrLen;
	char					 ClientAddrDotStr[16];
	uint16_t				 ClientAddrPort;
	uint16_t				 ServerPort;

	SelfMaintainBufferData 	 ReadBuffer;
	List 		 			*WriteBuffer;

	/* Complete content size track. */
	int						 WriteContentSize;
	int						 WriteContentOriginalSize;

	uint32_t				 ActionMask;

	bool				  	 toClose;		/* If should actively close. */
	bool					 forcedClose;	/* If should close without handling
											   left to write data. */
	void				   	*UserData;
	AsyncCommBufferHandlers	 Methods;

	/* Forced error action.   */
	int						 forceErrorAction;
};

/* Initialize the asynchronous communication. */
void initializeAsyncComm(void);

/* Register one file descriptor for a connected socket connection. */
int registerFileDesc(int 					  fd,
					 uint32_t				  actionmask,
					 AsyncCommBufferHandlers  methods,
					 void 					 *userdata,
					 AsyncCommBuffer         *newcommbuffer);

void assignFileDescClientAddressInfo(AsyncCommBuffer	 commbuffer,
									 const char			*clienthostname,
									 uint16_t			 serverport,
									 struct sockaddr_in	*clientaddr,
									 socklen_t			 clientaddrlen);

/* Register one comm buffer for asynchronous connection and communication. */
int registerAsyncConnectionFileDesc(const char				*address,
									uint16_t				 port,
									uint32_t				 actionmask,
									AsyncCommBufferHandlers  methods,
									void					*userdata,
									AsyncCommBuffer			*newcommbuffer);

/* Process all registered file descriptors. */
int processAllCommFileDescs(void);
void unresigsterFileDesc(int fd);
void closeAndRemoveAllRegisteredFileDesc(void);

void addMessageContentToCommBuffer(AsyncCommBuffer 		buffer,
								   SelfMaintainBuffer 	content);

SelfMaintainBuffer getFirstWriteBuffer(AsyncCommBuffer commbuffer);

void shiftOutFirstWriteBuffer(AsyncCommBuffer commbuffer);

void closeFileDesc(AsyncCommBuffer commbuff);
void forceCloseFileDesc(AsyncCommBuffer commbuff);
#endif /*RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_ASYNCCOMM_H*/
