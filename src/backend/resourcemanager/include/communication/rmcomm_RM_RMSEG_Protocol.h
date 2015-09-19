#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RM_RMSEG_PROTOCOL_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RM_RMSEG_PROTOCOL_H

#define RPC_PROTOCOL_STRUCT_BEGIN(protoname)								   \
	struct protoname##Data {

#define RPC_PROTOCOL_STRUCT_END(protoname)									   \
	};																		   \
	typedef struct protoname##Data		protoname##Data;					   \
	typedef struct protoname##Data	   *protoname;


/*******************************************************************************
 * Protocol of Update Memory Quota.
 ******************************************************************************/
RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestUpdateMemoryQuota)
	uint64_t	MemoryQuotaDelta;
	uint64_t	MemoryQuotaTotalPending;
RPC_PROTOCOL_STRUCT_END(RPCRequestUpdateMemoryQuota)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseUpdateMemoryQuota)
	uint32_t	Result;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseUpdateMemoryQuota)

/*******************************************************************************
 * Protocol of RUAlive
 *******************************************************************************/
RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestRUAlive)
	uint64_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCRequestRUAlive)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseRUAlive)
	uint32_t	Result;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseRUAlive)

#endif /*RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RM_RMSEG_PROTOCOL_H*/
