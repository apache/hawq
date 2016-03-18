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

#include "communication/rmcomm_Connect.h"
#include "utils/network_utils.h"

/*
 * This implementation of connecting to a socket server in an asynchronous way.
 */
AsyncCommBufferHandlersData AsyncCommBufferHandlersConn = {
	NULL,
	NULL,
	NULL,
	WriteReadyHandler_Connect,
	NULL,
	ErrorHandler_Connect,
	CleanUpHandler_Connect
};

struct AsyncCommBufferHandlerUserData {
	uint32_t					ActionMaskForAfterConn;
	AsyncCommBufferHandlers		MethodsForAfterConn;
	void					   *UserDataForAfterConn;
};

typedef struct AsyncCommBufferHandlerUserData  AsyncCommBufferHandlerUserData;
typedef struct AsyncCommBufferHandlerUserData *AsyncCommBufferHandlerUser;

void WriteReadyHandler_Connect(AsyncCommBuffer buffer)
{
	/* Check the error from socket FD. */
	bool 	  shouldclose = false;
	int  	  error 	  = 0;
	socklen_t errlen 	  = sizeof(error);

	elog(DEBUG3, "AsyncConn FD %d is write ready for checking connection.",
				 buffer->FD);

	int res = getsockopt(buffer->FD, SOL_SOCKET, SO_ERROR, (void *)&error, &errlen);
	if (res < 0)
	{
		elog(WARNING, "getsocketopt() on FD %d have errors raised. errno %d",
					  buffer->FD,
					  errno);
		shouldclose = true;
	}
	else if ( error > 0 )
	{
		elog(WARNING, "FD %d having errors raised. errno %d",
					  buffer->FD,
					  error);
		shouldclose = true;
	}

	if ( setConnectionLongTermNoDelay(buffer->FD) != FUNC_RETURN_OK )
	{
		shouldclose = true;
	}

	if ( shouldclose )
	{
		ErrorHandler_Connect(buffer);
		forceCloseFileDesc(buffer);
		return;
	}

	elog(DEBUG3, "AsyncConn FD %d is a good connection.", buffer->FD);

	/* Change to normal registered FD by changing callbacks and arguments. */
	AsyncCommBufferHandlerUser userdata = (AsyncCommBufferHandlerUser)(buffer->UserData);

	buffer->ActionMask = userdata->ActionMaskForAfterConn;
	buffer->UserData   = userdata->UserDataForAfterConn;
	buffer->Methods    = userdata->MethodsForAfterConn;
	Assert(getSMBContentSize(&(buffer->ReadBuffer)) == 0);

	elog(DEBUG3, "Freed AsyncComm Conn context ( connected ).");

	rm_pfree(AsyncCommContext, userdata);
}

void ErrorHandler_Connect(AsyncCommBuffer buffer)
{
	elog(WARNING, "Resource manager socket connect has error raised.");

	AsyncCommBufferHandlerUser userdata = (AsyncCommBufferHandlerUser)(buffer->UserData);
	buffer->ActionMask = userdata->ActionMaskForAfterConn;
	buffer->UserData   = userdata->UserDataForAfterConn;
	buffer->Methods    = userdata->MethodsForAfterConn;
	rm_pfree(AsyncCommContext, userdata);

	elog(DEBUG3, "Freed AsyncComm Conn context ( error ).");

	buffer->Methods->ErrorReadyHandle(buffer);
}

void CleanUpHandler_Connect(AsyncCommBuffer buffer)
{
	elog(LOG, "Clean up handler in socket connect is called.");

	AsyncCommBufferHandlerUser userdata = (AsyncCommBufferHandlerUser)(buffer->UserData);
	buffer->ActionMask = userdata->ActionMaskForAfterConn;
	buffer->UserData   = userdata->UserDataForAfterConn;
	buffer->Methods    = userdata->MethodsForAfterConn;
	rm_pfree(AsyncCommContext, userdata);

	elog(DEBUG3, "Freed AsyncComm Conn context ( clean up ).");

	buffer->Methods->CleanUpHandle(buffer);
}

int registerFileDescForAsyncConn(int 		 			  fd,
								 uint32_t				  actionmask_afterconn,
								 AsyncCommBufferHandlers  methods_afterconn,
								 void				     *userdata_afterconn,
								 AsyncCommBuffer		 *newcommbuffer_afterconn)
{
	AsyncCommBufferHandlerUser userdata = NULL;
	userdata = rm_palloc0(AsyncCommContext,
						  sizeof(AsyncCommBufferHandlerUserData));
	userdata->ActionMaskForAfterConn = actionmask_afterconn;
	userdata->UserDataForAfterConn   = userdata_afterconn;
	userdata->MethodsForAfterConn	 = methods_afterconn;

	elog(DEBUG3, "Created AsyncComm Conn context.");

	int res = registerFileDesc(fd,
							   ASYNCCOMM_WRITE,
							   &AsyncCommBufferHandlersConn,
							   userdata,
							   newcommbuffer_afterconn);
	if ( res != FUNC_RETURN_OK )
	{
		elog(WARNING, "Fail to register communication in asynchronous connection "
					  "progress.");
		rm_pfree(AsyncCommContext, userdata);
	}

	elog(DEBUG3, "Registered AsyncComm FD %d", fd);

	return res;
}
