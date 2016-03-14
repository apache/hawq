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

#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_MESSAGESERVER_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_MESSAGESERVER_H

/*
 * This is a server implementation for accepting new clients and make them
 * managed by connection tracker.
 *
 *
 *                +-------------------------------------+
 *                |         AsyncComm Framework         |
 *                +-------------------------------------+
 *                      |
 *                      | Message Server builds for each client
 *                      v
 *                +-------------------------------------+
 *                |         AsyncCommBufferData         |
 *                +-------------------------------------+
 *                      |                         ^
 *                      | UserData                | CommBuffer
 *                      v                         |
 *                +-------------------------------------+
 *        .------>|         ConnectionTrackData         |
 *        |       +-------------------------------------+
 *        |             |
 *        |             |(Messageid indexed handler)
 *        --------------.
 *
 *
 */

#include "communication/rmcomm_AsyncComm.h"
#include "communication/rmcomm_Message.h"

extern AsyncCommBufferHandlersData AsyncCommBufferHandlersMsgServer;

/* Callbacks registered in AsyncComm. */
void ReadReadyHandler_MsgServer(AsyncCommBuffer buffer);
void ErrorHandler_MsgServer(AsyncCommBuffer buffer);
void CleanUpHandler_MsgServer(AsyncCommBuffer buffer);

/* Callbacks registered in AsyncComm Message instance */
void addMessageToConnTrack(AsyncCommMessageHandlerContext	context,
						   uint16_t							messageid,
						   uint8_t							mark1,
						   uint8_t							mark2,
						   char 						   *buffer,
						   uint32_t							buffersize);

void sentMessageFromConnTrack(AsyncCommMessageHandlerContext context);
void hasCommErrorInConnTrack(AsyncCommMessageHandlerContext context);
void cleanupConnTrack(AsyncCommMessageHandlerContext context);

#endif /*RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_MESSAGESERVER_H*/
