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
RPC_PROTOCOL_STRUCT_END(RPCRequestHeadIMAlive)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseIMAlive)
	uint32_t	Result;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseIMAlive)


#endif /*RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RMSEG_RM_PROTOCOL_H*/
