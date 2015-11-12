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

#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RM_RB_PROTOCOL_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RM_RB_PROTOCOL_H

#define RPC_PROTOCOL_STRUCT_BEGIN(protoname)								   \
	struct protoname##Data {

#define RPC_PROTOCOL_STRUCT_END(protoname)									   \
	};																		   \
	typedef struct protoname##Data		protoname##Data;					   \
	typedef struct protoname##Data	   *protoname;

/*******************************************************************************
 * Protocol of get cluster report.
 ******************************************************************************/
/*
 * Request message format:
 * 	uint32_t messageid (RM2RB_GET_CLUSTERREPORT)
 *
 * Response message format:
 *	uint32_t messageid (RB2RM_CLUSTERREPORT)
 *	int32_t  result code
 *	uint32_t nodecount
 *  machine 1 ( in structure MachineInfoData )
 *  machine 2
 *  ...
 *  machine n
 *
 * Response message format(error):
 *  uint32_t messageid(RB2RM_CLUSTERREPORT)
 *  int32_t  result code
 */
RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestRBGetClusterReportHead)
	uint32_t	QueueNameLen;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCRequestRBGetClusterReportHead)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseRBGetClusterReportHead)
	uint32_t	Result;
	uint32_t	MachineCount;
	uint32_t	ResourceTight;
	uint32_t	Reserved;
	double      QueueCapacity;
	double      QueueCurCapacity;
	double      QueueMaxCapacity;
RPC_PROTOCOL_STRUCT_END(RPCResponseRBGetClusterReportHead)

/**
 * Handle the request from RM to RB for allocating a group of containers as
 * allocated resource to HAWQ.
 *
 * Request message format:
 * 	uint32_t messageid
 * 	uint32_t memorymb
 * 	uint32_t core
 * 	uint32_t containercount
 * 	uint32_t messagelength
 * 	uint32_t reserved
 * 	uint32_t preferredInfoLen
 * 	uint16_t num_containers
 * 	uint16_t flag
 * 	string   hostname1 + '\0'
 * 	string   rackname1
 * 	...
 *
 * Response message format:
 *  uint32_t messageid
 *  uint32_t result code
 *  uint32_t memorymb
 *  uint32_t core
 *  uint32_t nodecount
 *  uint32_t messagesize
 *  string   hostname1 + '\0'
 *  string   hostname2
 *  ...
 *
 *  Response message format(error):
 *  uint32_t messageid
 *  uint32_t result code
 */
RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestRBAllocateResourceContainers)
	uint32_t	MemoryMB;
	uint32_t	Core;
	uint32_t	ContainerCount;
	uint32_t    PreferredSize;
	uint32_t    MsgLength;
	uint32_t    Reserved;
RPC_PROTOCOL_STRUCT_END(RPCRequestRBAllocateResourceContainers)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseRBAllocateResourceContainersHead)
	uint32_t	Result;
	uint32_t	MemoryMB;
	uint32_t	Core;
	uint32_t	ContainerCount;
	uint32_t	ExpectedContainerCount; /* If ExpectedContainerCount is greater
										   than ContainerCount, not the whole
										   request is met. */
	uint32_t	HostNameStringLen;
	int64_t		SystemStartTimestamp;
RPC_PROTOCOL_STRUCT_END(RPCResponseRBAllocateResourceContainersHead)

/**
 * Handle the request from RM to RB for returning a group of containers as
 * allocated resource to HAWQ.
 *
 * Request message format:
 * 	 uint32_t messageid
 * 	 uint32_t containtercount
 * 	 int32_t  containerid1
 * 	 int32_t  containerid2
 * 	 ...
 * 	 int32_t  containeridn
 * 	 int32_t  reserved ( if containercount is odd )
 *
 * Response message format:
 * 	 uint32_t messageid
 * 	 uint32_t result
 */
RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestRBReturnResourceContainersHead)
	uint32_t	ContainerCount;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCRequestRBReturnResourceContainersHead)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseRBReturnResourceContainers)
	uint32_t	Result;
	uint32_t	Reserved;
	int64_t		SystemStartTimestamp;
RPC_PROTOCOL_STRUCT_END(RPCResponseRBReturnResourceContainers)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseRBGetContainerReportHead)
	uint32_t	Result;
	uint32_t	ContainerCount;
	int64_t		SystemStartTimestamp;
RPC_PROTOCOL_STRUCT_END(RPCResponseRBGetContainerReportHead)

#endif /*RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RM_RB_PROTOCOL_H*/
