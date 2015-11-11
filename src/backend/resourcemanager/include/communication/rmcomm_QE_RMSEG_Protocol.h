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

#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_QE_RMSEG_PROTOCOL_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_QE_RMSEG_PROTOCOL_H

#define RPC_PROTOCOL_STRUCT_BEGIN(protoname)								   \
	struct protoname##Data {

#define RPC_PROTOCOL_STRUCT_END(protoname)									   \
	};																		   \
	typedef struct protoname##Data		protoname##Data;					   \
	typedef struct protoname##Data	   *protoname;

/*******************************************************************************
 * Protocol of Get Local Temp Dir.
 ******************************************************************************/
/*
 * Request format:
 *		uint32_t session_id
 *		uint32_t command_id
 *		uint32_t qeindex
 *		uint32_t reserved
 *
 * Response format:
 *		uint32_t result code
 *		uint32_t reserved
 */
RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestGetTmpDirFromRMSEG)
	uint32_t	SessionID;
	uint16_t	CommandID;
	uint16_t    Reserved1;
	uint32_t	QEIndex;
	uint32_t	Reserved2;
RPC_PROTOCOL_STRUCT_END(RPCRequestGetTmpDirFromRMSEG)

/*******************************************************************************
 * Protocol of Move To CGROUP.
 ******************************************************************************/
RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestMoveToCGroup)
	TimestampTz	MasterStartTime;
	uint32_t	ConnID;
	uint32_t	SegmentID;
	uint32_t	ProcID;
RPC_PROTOCOL_STRUCT_END(RPCRequestMoveToCGroup)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseMoveToCGroup)
	uint32_t	Result;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseMoveToCGroup)

/*******************************************************************************
 * Protocol of Move Out CGROUP.
 ******************************************************************************/
RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestMoveOutCGroup)
	TimestampTz	MasterStartTime;
	uint32_t	ConnID;
	uint32_t	SegmentID;
	uint32_t	ProcID;
RPC_PROTOCOL_STRUCT_END(RPCRequestMoveOutCGroup)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseMoveOutCGroup)
	uint32_t	Result;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseMoveOutCGroup)


/*******************************************************************************
 * Protocol of Set Weight CGROUP.
 ******************************************************************************/
RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestSetWeightCGroup)
	TimestampTz	MasterStartTime;
	uint32_t	ConnID;
	uint32_t	SegmentID;
	uint32_t	ProcID;
	double		Weight;
RPC_PROTOCOL_STRUCT_END(RPCRequestSetWeightCGroup)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseSetWeightCGroup)
	uint32_t	Result;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseSetWeightCGroup)


#endif /*RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_QE_RMSEG_PROTOCOL_H*/
