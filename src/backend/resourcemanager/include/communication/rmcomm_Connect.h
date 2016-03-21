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

#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_CONNECT_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_CONNECT_H

/*
 * This is an async socket connection implementation.
 */

#include "communication/rmcomm_AsyncComm.h"

extern AsyncCommBufferHandlersData AsyncCommBufferHandlersConnect;

/* Callbacks registered in AsyncComm. */
void WriteReadyHandler_Connect(AsyncCommBuffer buffer);
void ErrorHandler_Connect(AsyncCommBuffer buffer);
void CleanUpHandler_Connect(AsyncCommBuffer buffer);

int registerFileDescForAsyncConn(int 		 			  fd,
								 uint32_t				  actionmask_afterconn,
								 AsyncCommBufferHandlers  methods_afterconn,
								 void				     *userdata_afterconn,
								 AsyncCommBuffer		 *newcommbuffer_afterconn);

#endif /*RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_CONNECT_H*/
