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

#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RMSEG_RM_PROTOCOL_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RMSEG_RM_PROTOCOL_H

#define RPC_PROTOCOL_STRUCT_BEGIN(protoname)								   \
	struct protoname##Data {

#define RPC_PROTOCOL_STRUCT_END(protoname)									   \
	};																		   \
	typedef struct protoname##Data		protoname##Data;					   \
	typedef struct protoname##Data	   *protoname;

/*******************************************************************************
 * Protocol of IMAlive.
 ******************************************************************************/
/*
 * Request format:
 *		uint16_t tmp dir count
 *		uint16_t tmp dir broken count
 *		uint32_t reserved
 *		MachineIdData machine info
 *
 * Response format:
 *		uint32_t result code
 *		uint32_t reserved
 */
RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestHeadIMAlive)
	uint16_t	TmpDirCount;
	uint16_t	TmpDirBrokenCount;
	uint32_t	Reserved;
	uint64_t	RMStartTimestamp;
RPC_PROTOCOL_STRUCT_END(RPCRequestHeadIMAlive)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseIMAlive)
	uint32_t	Result;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseIMAlive)


#endif /*RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RMSEG_RM_PROTOCOL_H*/
