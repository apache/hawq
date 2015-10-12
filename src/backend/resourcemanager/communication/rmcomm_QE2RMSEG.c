#include "resourcemanager/communication/rmcomm_QE2RMSEG.h"

bool QE2RMSEG_Initialized = false;

void initializeQE2RMSEGComm(void);

void initializeQE2RMSEGComm(void)
{
	if ( !QE2RMSEG_Initialized )
	{
		initializeSyncRPCComm();
		QE2RMSEG_Initialized = true;
	}
}

/**
 * Move the QE PID to corresponding CGroup
 */
int
MoveToCGroupForQE(TimestampTz masterStartTime,
				  int connId,
				  int segId,
				  int procId)
{
#ifdef __linux
	initializeQE2RMSEGComm();

	int			res			= FUNC_RETURN_OK;

	char		*serverHost	= "127.0.0.1";
	uint16_t	serverPort	= rm_segment_port;

	SelfMaintainBuffer sendBuffer = createSelfMaintainBuffer(CurrentMemoryContext);
	SelfMaintainBuffer recvBuffer = createSelfMaintainBuffer(CurrentMemoryContext);

	/* Build request */
	RPCRequestMoveToCGroupData request;
	request.MasterStartTime = masterStartTime;
	request.ConnID          = connId;
	request.SegmentID		= segId;
	request.ProcID			= procId;
	appendSMBVar(sendBuffer, request);

	/* Send request */
	res = callSyncRPCRemote(serverHost,
	                        serverPort,
	                        sendBuffer->Buffer,
	                        sendBuffer->Cursor+1,
	                        REQUEST_QE_MOVETOCGROUP,
	                        RESPONSE_QE_MOVETOCGROUP,
	                        recvBuffer);
	deleteSelfMaintainBuffer(sendBuffer);
	deleteSelfMaintainBuffer(recvBuffer);
	return res;
#endif
	return FUNC_RETURN_OK;
}

/**
 * Move the QE PID out of corresponding CGroup
 */
int
MoveOutCGroupForQE(TimestampTz masterStartTime,
				   int connId,
				   int segId,
				   int procId)
{
#ifdef __linux
	initializeQE2RMSEGComm();

	int			res			= FUNC_RETURN_OK;

	char		*serverHost	= "127.0.0.1";
	uint16_t	serverPort	= rm_segment_port;

	SelfMaintainBuffer sendBuffer = createSelfMaintainBuffer(CurrentMemoryContext);
	SelfMaintainBuffer recvBuffer = createSelfMaintainBuffer(CurrentMemoryContext);

	/* Build request */
	RPCRequestMoveOutCGroupData request;
	request.MasterStartTime = masterStartTime;
	request.ConnID          = connId;
	request.SegmentID		= segId;
	request.ProcID			= procId;
	appendSMBVar(sendBuffer, request);

	/* Send request */
	res = callSyncRPCRemote(serverHost,
	                        serverPort,
	                        sendBuffer->Buffer,
	                        sendBuffer->Cursor+1,
	                        REQUEST_QE_MOVEOUTCGROUP,
	                        RESPONSE_QE_MOVEOUTCGROUP,
	                        recvBuffer);

	deleteSelfMaintainBuffer(sendBuffer);
	deleteSelfMaintainBuffer(recvBuffer);
	return res;
#endif
	return FUNC_RETURN_OK;
}
  
/**
 * Set CPU share weight for corresponding CGroup
 */
int
SetWeightCGroupForQE(TimestampTz masterStartTime,
					 int connId,
					 int segId,
					 QueryResource *resource,
					 int procId)
{
#ifdef __linux
	initializeQE2RMSEGComm();

	int			res			= FUNC_RETURN_OK;

	char		*serverHost	= "127.0.0.1";
	uint16_t	serverPort	= rm_segment_port;

	SelfMaintainBuffer sendBuffer = createSelfMaintainBuffer(CurrentMemoryContext);
	SelfMaintainBuffer recvBuffer = createSelfMaintainBuffer(CurrentMemoryContext);

	/* Build request */
	RPCRequestSetWeightCGroupData request;
	request.MasterStartTime = masterStartTime;
	request.ConnID			= connId;
	request.SegmentID		= segId;
	request.ProcID			= procId;
	request.Weight			= resource->segment_vcore;
	appendSMBVar(sendBuffer, request);

	/* Send request */
	res = callSyncRPCRemote(serverHost,
	                        serverPort,
	                        sendBuffer->Buffer,
	                        sendBuffer->Cursor+1,
	                        REQUEST_QE_SETWEIGHTCGROUP,
	                        RESPONSE_QE_SETWEIGHTCGROUP,
	                        recvBuffer);

	deleteSelfMaintainBuffer(sendBuffer);
	deleteSelfMaintainBuffer(recvBuffer);
	return res;
#endif
	return FUNC_RETURN_OK;
}

