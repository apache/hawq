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

#include "communication/rmcomm_AsyncComm.h"
#include "communication/rmcomm_Connect.h"
#include "utils/network_utils.h"
#include "rmcommon.h"

#define ASYNCCOMM_MEMORY_CONTEXT_NAME			"asynccomm"
#define ASYNCCOMM_CONNECTION_MAX_CAPABILITY 	0X10000
#define ASYNCCOMM_READ_WRITE_ONCE_SIZE			8192

#define ASYNCCOMM_CONN_FILEINDEX				3

MCTYPE			AsyncCommContext;
struct pollfd 	RegClients[ASYNCCOMM_CONNECTION_MAX_CAPABILITY];
AsyncCommBuffer CommBuffers[ASYNCCOMM_CONNECTION_MAX_CAPABILITY];
int				CommBufferCounter;
char			RWBuffer[ASYNCCOMM_READ_WRITE_ONCE_SIZE];

void freeCommBuffer(AsyncCommBuffer *pcommbuffer);

AsyncCommBuffer createCommBuffer(int 					  fd,
								 uint32_t				  actionmask,
								 AsyncCommBufferHandlers  methods,
								 void					 *userdata);

void closeRegisteredFileDesc(AsyncCommBuffer commbuff);
static void closeAllRegisteredFileDescs(int code, Datum arg);

void initializeAsyncComm(void)
{
	AsyncCommContext = NULL;
	AsyncCommContext = AllocSetContextCreate(TopMemoryContext,
											 ASYNCCOMM_MEMORY_CONTEXT_NAME,
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);
	CommBufferCounter = 0;

	on_proc_exit(closeAllRegisteredFileDescs, 0);
}

int registerFileDesc(int 					  fd,
					 uint32_t				  actionmask,
					 AsyncCommBufferHandlers  methods,
					 void 					 *userdata,
					 AsyncCommBuffer         *newcommbuffer)
{
	if ( CommBufferCounter >= ASYNCCOMM_CONNECTION_MAX_CAPABILITY )
	{
		elog(WARNING, "There are too many communication buffers in use. "
				  	  "Current in used buffer number %d",
					  CommBufferCounter);
		return ASYNCCOMM_BUFFER_ARRAY_FULL;
	}

	if ( setConnectionNonBlocked(fd) != FUNC_RETURN_OK )
	{
		return UTIL_NETWORK_FAIL_SETFCNTL;
	}

	CommBuffers[CommBufferCounter] = createCommBuffer(fd,
													  actionmask,
													  methods,
													  userdata);
	RegClients[CommBufferCounter].fd = fd;
	*newcommbuffer = CommBuffers[CommBufferCounter];
	CommBufferCounter++;

	elog(DEBUG3, "Resource manager registered FD %d in poll slot %d, %s%s%s%s",
				 fd,
				 CommBufferCounter - 1,
				 (actionmask & ASYNCCOMM_READ) ? "(read)" : "",
				 (actionmask & ASYNCCOMM_WRITE) ? "(write)" : "",
				 (actionmask & ASYNCCOMM_READBYTES) ? "(read bytes)" : "",
				 (actionmask & ASYNCCOMM_WRITEBYTES) ? "(write bytes)" : "");
	return FUNC_RETURN_OK;
}

void assignFileDescClientAddressInfo(AsyncCommBuffer	 commbuffer,
									 const char			*clienthostname,
									 uint16_t			 serverport,
									 struct sockaddr_in	*clientaddr,
									 socklen_t			 clientaddrlen)
{
	/* Assign connection information into comm buffer. */
	memcpy(&(commbuffer->ClientAddr), clientaddr, clientaddrlen);
	commbuffer->ClientAddrLen = clientaddrlen;
	strncpy(commbuffer->ClientAddrDotStr,
			SOCKADDR(clientaddr),
			sizeof(commbuffer->ClientAddrDotStr)-1);
	commbuffer->ClientAddrPort = SOCKPORT(clientaddr);
	commbuffer->ServerPort = serverport;

	if ( clienthostname != NULL )
	{
		setSimpleStringNoLen(&(commbuffer->ClientHostname), clienthostname);

		elog(DEBUG3, "Resource manager assigned hostname %s, port %d",
					 clienthostname,
					 commbuffer->ClientAddrPort);
	}
}

int processAllCommFileDescs(void)
{
	static int FreeIndexes[ASYNCCOMM_CONNECTION_MAX_CAPABILITY];
	static int freeidx = -1;

	/*
	 * This loop is to check if there are some FDs no need to check POLLOUT
	 * event. Because, some FDs maybe usually POLLOUT ready but no data to
	 * write, which causes CPU resource wasted.
	 */
	for ( int i = 0 ; i < CommBufferCounter ; ++i )
	{
		Assert(CommBuffers[i] != NULL);
		RegClients[i].events = POLLERR | POLLHUP | POLLNVAL;
		if ( CommBuffers[i]->ActionMask & (ASYNCCOMM_READ|ASYNCCOMM_READBYTES) )
		{
			RegClients[i].events |= POLLIN;
		}

		/*
		 * If the connection has no intention to write something or to handle
		 * write ready event, we don't care POLLOUT, otherwise, CPU resource may
		 * be wasted.
		 */
		if ( (CommBuffers[i]->ActionMask & ASYNCCOMM_WRITE) ||
			 ((CommBuffers[i]->ActionMask & ASYNCCOMM_WRITEBYTES) &&
			  list_length(CommBuffers[i]->WriteBuffer) > 0) )
		{
			RegClients[i].events |= POLLOUT;
		}
	}

	/* Check and process ready FDs. */
	int readycount = 0;
	readycount = poll(RegClients, CommBufferCounter, RESOURCE_NETWORK_POLL_TIMEOUT);
	if( readycount > 0 )
	{
		for ( int i = 0 ; i < CommBufferCounter && readycount > 0 ; ++i )
		{
			/* The corresponding comm buffer instance must have correct fd. */
			Assert(CommBuffers[i] != NULL);
			Assert(CommBuffers[i]->FD == RegClients[i].fd);

			/* Case 1. Process connection having error. */
			if ( RegClients[i].revents & (POLLERR | POLLHUP | POLLNVAL) )
			{
				bool 		erroccured	= false;
				int  		error		= 0;
				socklen_t 	errlen		= sizeof(error);

				int res = getsockopt(RegClients[i].fd,
									 SOL_SOCKET,
									 SO_ERROR,
									 (void *)&error,
									 &errlen);
				if (res < 0)
				{
					elog(WARNING, "getsocketopt() on FD %d have errors raised. "
								  "errno %d",
							  	  CommBuffers[i]->FD,
								  errno);
					/* In fact, this should not occur. */
					erroccured = true;
				}
				else if ( error > 0 )
				{
					elog(WARNING, "FD %d having errors raised. errno %d",
							  	  CommBuffers[i]->FD,
								  error);
					erroccured = true;
				}

				if ( erroccured )
				{
					Assert( CommBuffers[i]->Methods->ErrorReadyHandle != NULL );
					CommBuffers[i]->Methods->ErrorReadyHandle(CommBuffers[i]);

					/* Tell the close this connection and free the buffer. */
					forceCloseFileDesc(CommBuffers[i]);
					readycount--;
					continue;
				}

				/* Otherwise, skip this error. */
				elog(DEBUG3, "poll() detected error is skipped.");
			}

			/* Case 2. Process connection ready to send data. */
			if ( RegClients[i].revents & POLLOUT )
			{
				int wbuffsize = list_length(CommBuffers[i]->WriteBuffer);

				elog(DEBUG3, "FD %d (client) is write ready.", CommBuffers[i]->FD);

				/* Call write ready call back if necessary. */
				if ( CommBuffers[i]->Methods->WriteReadyHandle != NULL )
				{
					SelfMaintainBuffer firstbuff = getFirstWriteBuffer(CommBuffers[i]);

					elog(DEBUG3, "Write ready callback is set.");
					/*
					 * When commbuffer wants to process received bytes or it
					 * cares the write event ready only, we call write ready
					 * handle here.
					 */
					if ( ((CommBuffers[i]->ActionMask & ASYNCCOMM_WRITEBYTES) &&
						  firstbuff != NULL &&
						  CommBuffers[i]->WriteContentSize ==
							  CommBuffers[i]->WriteContentOriginalSize) ||
						 ((CommBuffers[i]->ActionMask & ASYNCCOMM_WRITE)) )
					{
						CommBuffers[i]->Methods->WriteReadyHandle(CommBuffers[i]);
					}
				}

				/*
				 * Send the content firstly. Write ready handler might change
				 * the content to send or force the connection to close without
				 * writing out content, therefore we fetch the content size
				 * again and double check the close mark.
				 */
				wbuffsize = list_length(CommBuffers[i]->WriteBuffer);
				if ( (CommBuffers[i]->ActionMask & ASYNCCOMM_WRITEBYTES) &&
					 !CommBuffers[i]->forcedClose &&
					 wbuffsize > 0 )
				{
					SelfMaintainBuffer tosendbuff = getFirstWriteBuffer(CommBuffers[i]);

					/*
					 * Get content start point, WriteContentSize save the left
					 * content size should be sent.
					 */
					char *pstart = tosendbuff->Buffer +
								   getSMBContentSize(tosendbuff) -
								   CommBuffers[i]->WriteContentSize;

					int wrsize = send(CommBuffers[i]->FD,
									  pstart,
									  CommBuffers[i]->WriteContentSize,
									  0);
					if ( wrsize > 0 )
					{
						Assert( CommBuffers[i]->WriteContentSize >= wrsize );
						/* Adjust the content size not sent yet. */
						CommBuffers[i]->WriteContentSize -= wrsize;

						if ( CommBuffers[i]->WriteContentSize == 0 )
						{
							/*
							 * Before destroy sent content, Write post handler
							 * is called to make handler able to recognize the
							 * sent content by reading the first send buffer.
							 */
							if ( CommBuffers[i]->Methods->WritePostHandle != NULL )
							{
								CommBuffers[i]->Methods->WritePostHandle(CommBuffers[i]);
							}

							/* Truly drop the sent content. */
							shiftOutFirstWriteBuffer(CommBuffers[i]);
						}

						elog(DEBUG3, "FD %d (client) wrote %d bytes out. "
									 "Current buffer has %d bytes left, total "
									 "%d buffers.",
									 CommBuffers[i]->FD,
									 wrsize,
									 CommBuffers[i]->WriteContentSize,
									 list_length(CommBuffers[i]->WriteBuffer));
					}
					else if ( wrsize == -1 			&&
							  errno != EWOULDBLOCK 	&&
							  errno != EAGAIN      	&&
							  errno != EINTR)
					{
						elog(WARNING, "FD %d failed to send message. errno %d",
									  CommBuffers[i]->FD,
									  errno);

						Assert( CommBuffers[i]->Methods->ErrorReadyHandle != NULL );
						CommBuffers[i]->Methods->ErrorReadyHandle(CommBuffers[i]);

						/* Not acceptable error, should actively force close. */
						forceCloseFileDesc(CommBuffers[i]);
					}
				}
				readycount--;
			}
			/* Case 3. Process connection ready to receive data. */
			else if ( RegClients[i].revents & POLLIN )
			{
				elog(DEBUG3, "Find FD %d is read ready.", CommBuffers[i]->FD);

				/* Call Ready ready handler call back to do possible actions. */
				if ( CommBuffers[i]->Methods->ReadReadyHandle != NULL)
				{
					CommBuffers[i]->Methods->ReadReadyHandle(CommBuffers[i]);
				}

				elog(DEBUG3, "commbuffer action mask %d, toclose %d, forced %d",
							 CommBuffers[i]->ActionMask,
							 CommBuffers[i]->toClose ? 1 : 0,
							 CommBuffers[i]->forcedClose ? 1 : 0);

				/* Read ready handler might force the connection to close. */
				if ( (CommBuffers[i]->ActionMask & ASYNCCOMM_READBYTES) &&
					 !CommBuffers[i]->toClose &&
					 !CommBuffers[i]->forcedClose )
				{
					/* Read data and append to the read buffer. */
					int rdsize = recv(CommBuffers[i]->FD,
									  RWBuffer,
									  sizeof(RWBuffer),
									  0);
					if ( rdsize > 0 )
					{
						appendSelfMaintainBuffer(&(CommBuffers[i]->ReadBuffer),
												 RWBuffer,
												 rdsize);
						/*
						 * For client connection, the read post handler is
						 * mandatory. This is for recognizing the content
						 * format and do necessary action.
						 */
						Assert(CommBuffers[i]->Methods->ReadPostHandle != NULL);
						CommBuffers[i]->Methods->ReadPostHandle(CommBuffers[i]);
						elog(DEBUG3, "FD %d read %d bytes. %d to handle",
									 CommBuffers[i]->FD,
									 rdsize,
									 getSMBContentSize(&(CommBuffers[i]->ReadBuffer)));
					}
					else if ( rdsize == 0 )
					{
						forceCloseFileDesc(CommBuffers[i]);
						elog(DEBUG3, "FD %d (client) is normally closed.",
									 CommBuffers[i]->FD);
					}
					else if ( rdsize == -1         &&
							  errno != EWOULDBLOCK &&
							  errno != EAGAIN      &&
							  errno != EINTR)
					{
						elog(WARNING, "FD %d is forced closed due to recv() error. "
								  	  "errno %d",
									  CommBuffers[i]->FD,
									  errno);

						Assert( CommBuffers[i]->Methods->ErrorReadyHandle != NULL );
						CommBuffers[i]->Methods->ErrorReadyHandle(CommBuffers[i]);

						/* Not acceptable error, should actively close. */
						forceCloseFileDesc(CommBuffers[i]);
					}
					else
					{
						elog(WARNING, "FD %d errno %d", CommBuffers[i]->FD, errno);
					}
				}
				readycount--;
			} /* End of case 3. */
		} /* End of looping each FD. */

		/* Validate that all ready FDs should be processed. */
		Assert(readycount == 0);
	}
	/* In case, poll() has error raised. */
	else if ( readycount == -1 )
	{
		/* Ignore the errors due to signal, should be fine to retry next time. */
		if ( errno == EAGAIN || errno == EINTR )
		{
			return FUNC_RETURN_OK;
		}
		return SYSTEM_CALL_ERROR; /* Fail to call poll() */
	}

	/* Actively close the connection should be actively closed. */
	freeidx = -1;
	for ( int i = 0 ; i < CommBufferCounter ; ++i )
	{
		bool shouldfree = false;
		if ( CommBuffers[i]->forcedClose )
		{
			/* Call cleanup handler if necessary to do user-defined cleanup. */
			elog(DEBUG5, "Close FD %d Index %d.", CommBuffers[i]->FD, i);

			/* Close connection and free buffer */
			closeRegisteredFileDesc(CommBuffers[i]);
			shouldfree = true;
		}
		else if ( CommBuffers[i]->toClose && CommBuffers[i]->WriteBuffer == NULL )
		{
			if ( CommBuffers[i]->ClientHostname.Str != NULL &&
				 CommBuffers[i]->ServerPort != 0 )
			{
				returnAliveConnectionRemoteByHostname(
							&(CommBuffers[i]->FD),
							CommBuffers[i]->ClientHostname.Str,
							CommBuffers[i]->ServerPort);
			}
			else
			{
				closeRegisteredFileDesc(CommBuffers[i]);
			}
			shouldfree = true;
		}

		if ( shouldfree )
		{
			Assert(CommBuffers[i]->Methods->CleanUpHandle != NULL);
			CommBuffers[i]->Methods->CleanUpHandle(CommBuffers[i]);
			freeCommBuffer(&CommBuffers[i]); /* Now CommBuffers[i] is set NULL.*/
			freeidx++;
			FreeIndexes[freeidx] = i;

			/* Clear poll() array. */
			RegClients[i].fd = -1;
		}
	}

	/*
	 * Shift to remove freed slots among in-use slots to shorten the length of
	 * poll status array.
	 */
	if ( freeidx >= 0 )
	{
		for ( int i = 0 ; i <= freeidx ; ++i )
		{
			while( CommBufferCounter-1 >= 0 &&
				   CommBuffers[CommBufferCounter-1] == NULL )
			{
				elog(DEBUG5, "Skip poll slot %d, Curr counter is %d",
							 CommBufferCounter - 1,
							 CommBufferCounter);
				CommBufferCounter--;

			}
			if ( CommBufferCounter-1 < FreeIndexes[i] )
			{
				/* No need to move any more. */
				break;
			}
			elog(DEBUG5, "Move poll slot from %d to %d. Curr counter is %d",
						 CommBufferCounter - 1,
						 FreeIndexes[i],
						 CommBufferCounter);
			RegClients[FreeIndexes[i]].fd      = RegClients[CommBufferCounter-1].fd;
			RegClients[FreeIndexes[i]].events  = RegClients[CommBufferCounter-1].events;
			RegClients[FreeIndexes[i]].revents = RegClients[CommBufferCounter-1].revents;
			CommBuffers[FreeIndexes[i]]        = CommBuffers[CommBufferCounter-1];
			/*
			 * NOTE: CommBuffers[CommBufferCounter-1] will be overwritten by new
			 * comm buffer instance. not setting NULL here.
			 */
			CommBufferCounter--;
		}
	}
	return FUNC_RETURN_OK;
}

AsyncCommBuffer createCommBuffer(int 					  fd,
								 uint32_t				  actionmask,
								 AsyncCommBufferHandlers  methods,
								 void					 *userdata)
{
	Assert(methods != NULL);
	AsyncCommBuffer result = rm_palloc0(AsyncCommContext,
										sizeof(AsyncCommBufferData));
	result->FD      		 = fd;
	result->ActionMask		 = actionmask;
	result->Methods 		 = methods;
	result->toClose			 = false;
	result->forcedClose 	 = false;
	result->UserData		 = userdata;

	result->ClientAddrLen	 = 0;
	result->ClientAddrPort	 = 0;

	result->ServerPort		 = 0;

	initSimpleString(&(result->ClientHostname), AsyncCommContext);

	initializeSelfMaintainBuffer(&(result->ReadBuffer), AsyncCommContext);
	result->WriteBuffer 	 		 = NULL;
	result->WriteContentSize 		 = -1;
	result->WriteContentOriginalSize = -1;

	if ( result->Methods->InitHandle != NULL )
	{
		result->Methods->InitHandle(result);
	}

	return result;
}

void freeCommBuffer(AsyncCommBuffer *pcommbuffer)
{
	Assert( pcommbuffer != NULL );

	freeSimpleStringContent(&((*pcommbuffer)->ClientHostname));

	destroySelfMaintainBuffer(&((*pcommbuffer)->ReadBuffer));

	MEMORY_CONTEXT_SWITCH_TO(AsyncCommContext)
	while( (*pcommbuffer)->WriteBuffer != NULL )
	{
		SelfMaintainBuffer wbuffer = getFirstWriteBuffer(*pcommbuffer);
		destroySelfMaintainBuffer(wbuffer);
		(*pcommbuffer)->WriteBuffer = list_delete_first((*pcommbuffer)->WriteBuffer);
	}
	MEMORY_CONTEXT_SWITCH_BACK

	rm_pfree(AsyncCommContext, *pcommbuffer);
	*pcommbuffer = NULL;
}

void closeRegisteredFileDesc(AsyncCommBuffer commbuff)
{
	closeConnectionRemote(&(commbuff->FD));
}

void closeAndRemoveAllRegisteredFileDesc(void)
{
	for ( int i = 0 ; i < CommBufferCounter ; ++i )
	{
		Assert(CommBuffers[i] != NULL);

		/* Call cleanup handler if necessary to do user-defined cleanup. */
		CommBuffers[i]->Methods->CleanUpHandle(CommBuffers[i]);
		elog(DEBUG5, "Close FD %d Index %d.", CommBuffers[i]->FD, i);

		closeRegisteredFileDesc(CommBuffers[i]);
		freeCommBuffer(&CommBuffers[i]);

		/* Clear poll() array. */
		RegClients[i].fd = -1;
	}
	CommBufferCounter = 0;
}

static void closeAllRegisteredFileDescs(int code, Datum arg)
{
	closeAndRemoveAllRegisteredFileDesc();
}
void unresigsterFileDesc(int fd)
{
	int pos = -1;
	for ( int i = 0 ; i < CommBufferCounter ; ++i )
	{
		Assert(CommBuffers[i] != NULL);

		if ( CommBuffers[i]->FD == fd )
		{
			/* Call cleanup handler if necessary to do user-defined cleanup. */
			CommBuffers[i]->Methods->CleanUpHandle(CommBuffers[i]);
			elog(DEBUG5, "Unregister FD %d Index %d.", CommBuffers[i]->FD, i);
			CommBuffers[i]->FD = -1;
			freeCommBuffer(&CommBuffers[i]);
			pos = i;
			break;
		}
	}

	/*
	 * Shift to remove freed slot among in-use slots to shorten the length of
	 * poll status array.
	 */
	if ( pos >= 0 )
	{
		if ( CommBufferCounter > 1 )
		{
			RegClients[pos].fd      = RegClients[CommBufferCounter-1].fd;
			RegClients[pos].events  = RegClients[CommBufferCounter-1].events;
			RegClients[pos].revents = RegClients[CommBufferCounter-1].revents;
			CommBuffers[pos]        = CommBuffers[CommBufferCounter-1];
		}
		CommBufferCounter--;
	}
}

void addMessageContentToCommBuffer(AsyncCommBuffer 		buffer,
								   SelfMaintainBuffer 	content)
{
	MEMORY_CONTEXT_SWITCH_TO(AsyncCommContext)
	buffer->WriteBuffer = lappend(buffer->WriteBuffer, content);
	MEMORY_CONTEXT_SWITCH_BACK
	if ( list_length(buffer->WriteBuffer) == 1 )
	{
		buffer->WriteContentSize 		 = getSMBContentSize(content);
		buffer->WriteContentOriginalSize = buffer->WriteContentSize;
	}
}

SelfMaintainBuffer getFirstWriteBuffer(AsyncCommBuffer commbuffer)
{
	if ( list_length(commbuffer->WriteBuffer) == 0 )
	{
		return NULL;
	}
	return (SelfMaintainBuffer)(lfirst(list_head(commbuffer->WriteBuffer)));
}

void shiftOutFirstWriteBuffer(AsyncCommBuffer commbuffer)
{
	SelfMaintainBuffer rmbuff = getFirstWriteBuffer(commbuffer);

	/* Free current buffer */
	deleteSelfMaintainBuffer(rmbuff);

	/* Shift to next buffer to get ready to send. */
	MEMORY_CONTEXT_SWITCH_TO(AsyncCommContext)
	commbuffer->WriteBuffer = list_delete_first(commbuffer->WriteBuffer);
	MEMORY_CONTEXT_SWITCH_BACK

	SelfMaintainBuffer firstbuff = getFirstWriteBuffer(commbuffer);
	commbuffer->WriteContentSize = firstbuff == NULL ?
								   0 :
								   getSMBContentSize(firstbuff);

	commbuffer->WriteContentOriginalSize = commbuffer->WriteContentSize;
}

void closeFileDesc(AsyncCommBuffer commbuff)
{
	commbuff->toClose = true;
}
void forceCloseFileDesc(AsyncCommBuffer commbuff)
{
	commbuff->toClose 	  = true;
	commbuff->forcedClose = true;
}
int registerAsyncConnectionFileDesc(const char				*address,
									uint16_t				 port,
									uint32_t				 actionmask,
									AsyncCommBufferHandlers  methods,
									void					*userdata,
									AsyncCommBuffer			*newcommbuffer)
{
	int					res				= FUNC_RETURN_OK;
	int 				fd 				= -1;
	int					sockres			= 0;
	struct sockaddr_in 	server_addr;

	/* Prepare for connecting. */
	AddressString resolvedaddr = getAddressStringByHostName(address);
	if ( resolvedaddr == NULL )
	{
		write_log("Failed to get host by name %s for async connecting a remote "
				  "socket server %s:%d",
				  address,
				  address,
				  port);
		return UTIL_NETWORK_FAIL_GETHOST;
	}

	if ( rm_enable_connpool )
	{
		/* Try to get an alive connection from connection pool. */
		fd = fetchAliveSocketConnection(address, resolvedaddr, port);
	}

	if ( fd != -1 )
	{
		res = registerFileDesc(fd,
							   actionmask,
							   methods,
							   userdata,
							   newcommbuffer);
		goto exit;
	}

	/* Create socket FD */
	fd = socket(AF_INET, SOCK_STREAM, 0);
	if ( fd < 0 )
	{
		write_log("Failed to open socket for async connecting a remote socket "
				  "(errno %d)",
				  errno);
		return UTIL_NETWORK_FAIL_CREATESOCKET;
	}

	/* Set FD unblocked. */
	if ( setConnectionNonBlocked(fd) != FUNC_RETURN_OK )
	{
		close(fd);
		return UTIL_NETWORK_FAIL_SETFCNTL;
	}

	bzero((char *)&server_addr, sizeof(server_addr));
	server_addr.sin_family = AF_INET;
	memcpy((char *)&server_addr.sin_addr.s_addr,
		   resolvedaddr->Address,
		   resolvedaddr->Length);
	server_addr.sin_port = htons(port);

	/* Asynchronous connect. Should return value at once. */
	sockres = connect(fd, (struct sockaddr *)&server_addr, sizeof(server_addr));
	if ( sockres == 0 )
	{
		/*
		 * New connection is created. Suppose domain socket and local socket
		 * connection can be done now. Register a normal client FD in poll() to
		 * perform content sending and receiving.
		 */
		res = registerFileDesc(fd,
							   actionmask,
							   methods,
							   userdata,
							   newcommbuffer);

		/* Assign connection address. */
		assignFileDescClientAddressInfo(*newcommbuffer,
										address,
										port,
										&server_addr,
										sizeof(server_addr));
		goto exit;
	}
	else if ( sockres < 0 && errno == EINPROGRESS )
	{
		/*
		 * System is building connection now. Register it into poll() to perform
		 * asynchronous check. Build asynchronous connection commbuffer.
		 */
		res = registerFileDescForAsyncConn(fd,
										   actionmask,
										   methods,
										   userdata,
										   newcommbuffer);

		/* Assign connection address. */
		assignFileDescClientAddressInfo(*newcommbuffer,
										address,
										port,
										&server_addr,
										sizeof(server_addr));

		goto exit;
	}
	else
	{
		/* Fail to build connection. */
		 write_log("registerAsyncConnectionFileDesc connect socket failed, "
				   "fd %d (errno %d)",
				   fd,
				   errno);
		 close(fd);
		 res = UTIL_NETWORK_FAIL_CONNECT;
	}

exit:
	return res;
}
