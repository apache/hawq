#include "envswitch.h"
#include "communication/rmcomm_SyncComm.h"

#include <pthread.h>
#include "miscadmin.h"
#include "communication/rmcomm_AsyncComm.h"
#include "communication/rmcomm_Message.h"
#include "utils/network_utils.h"

enum SYNC_RPC_COMM_STATUS {
	SYNC_RPC_COMM_IDLE,
	SYNC_RPC_COMM_SENDING,
	SYNC_RPC_COMM_RECVING
};

struct SyncRPCContextData {
	uint16_t  					ExpMessageID;
	SelfMaintainBuffer			RecvBuffer;
	uint8_t						RecvMark1;
	uint8_t						RecvMark2;
	enum SYNC_RPC_COMM_STATUS	CommStatus;
	int							Result;
};
typedef struct SyncRPCContextData  SyncRPCContextData;
typedef struct SyncRPCContextData *SyncRPCContext;

void initializeSyncRPContent(SyncRPCContext 	content,
							 SelfMaintainBuffer smb,
							 uint16_t	 		expmsgid);

AsyncCommMessageHandlerContext createMessageHandlerContext(SyncRPCContext content);

void receivedSyncRPCResponse(AsyncCommMessageHandlerContext  context,
							 uint16_t						 messageid,
							 uint8_t						 mark1,
							 uint8_t						 mark2,
							 char 							*buffer,
							 uint32_t						 buffersize);
void sentSyncRPCRequest(AsyncCommMessageHandlerContext context);
void sentSyncRPCRequestError(AsyncCommMessageHandlerContext context);
void sentSyncRPCRequestCleanUp(AsyncCommMessageHandlerContext context);



int callSyncRPCDomain(const char     	   *sockfile,
					  const char 	 	   *sendbuff,
		        	  int   		  		sendbuffsize,
					  uint16_t		  		sendmsgid,
					  uint16_t 		  		exprecvmsgid,
					  SelfMaintainBuffer 	recvsmb)
{
	static char            			dfilename[1024];
	int 							fd 			  = -1;
	int 							res 		  = FUNC_RETURN_OK;
	AsyncCommBuffer					newcommbuffer = NULL;
	AsyncCommMessageHandlerContext 	context 	  = NULL;
	SyncRPCContextData 				userdata;

	/* Connect to the server side. */
	res = connectToServerDomain(sockfile, 0, &fd, 0, dfilename);
	if ( res != FUNC_RETURN_OK )
	{
		elog(WARNING, "Fail to connect to domain socket server %s, result %d",
				  sockfile,
				  res);
		goto exit;
	}

	initializeSyncRPContent(&userdata, recvsmb, exprecvmsgid);
	context = createMessageHandlerContext(&userdata);

	res = registerFileDesc(fd,
						   dfilename,
						   ASYNCCOMM_READBYTES | ASYNCCOMM_WRITEBYTES,
						   &AsyncCommBufferHandlersMessage,
						   context,
						   &newcommbuffer);
	if ( res != FUNC_RETURN_OK )
	{
		rm_pfree(AsyncCommContext, context);
		elog(WARNING, "Fail to register FD for synchronous communication. %d", res);
		goto exit;
	}

	buildMessageToCommBuffer(newcommbuffer,
							 sendbuff,
							 sendbuffsize,
							 sendmsgid,
							 0,
							 0);
	context->AsyncBuffer = newcommbuffer;

	InitHandler_Message(newcommbuffer);

	/* Wait for the complete of the communication. */
	while( true )
	{
		processAllCommFileDescs();
		if ( userdata.CommStatus == SYNC_RPC_COMM_IDLE )
		{
			break;
		}
		else if ( QueryCancelPending )
		{
			/*
			 * We find that this QD wants to cancel the query, we don't need
			 * to continue the communication.
			 */
			res = TRANSCANCEL_INPROGRESS;
			break;
		}
	}

	res = res == TRANSCANCEL_INPROGRESS ? res : userdata.Result;

	/* Close and cleanup */
	closeAndRemoveAllRegisteredFileDesc();

	if (res != FUNC_RETURN_OK)
	{
	  elog(WARNING, "Sync RPC framework (domain) finds exception raised.");
	}
	return res;
exit:
	closeConnectionDomain(&fd, dfilename);
	return res;
}

int callSyncRPCRemote(const char     	   *hostname,
					  uint16_t              port,
		  	  	  	  const char 	 	   *sendbuff,
					  int   		  		sendbuffsize,
					  uint16_t		  		sendmsgid,
					  uint16_t 		  		exprecvmsgid,
					  SelfMaintainBuffer 	recvsmb)
{
	int 							fd 			  = -1;
	int 							res 		  = FUNC_RETURN_OK;
	AsyncCommBuffer					newcommbuffer = NULL;
	AsyncCommMessageHandlerContext 	context 	  = NULL;
	SyncRPCContextData 				userdata;

	/* Connect to the server side. */
	res = connectToServerRemote(hostname, port, &fd);
	if ( res != FUNC_RETURN_OK ) {
		elog(WARNING, "Fail to connect to socket server %s:%d, result %d",
				  	  hostname, port,
					  res);
		goto exit;
	}

	initializeSyncRPContent(&userdata, recvsmb, exprecvmsgid);
	context = createMessageHandlerContext(&userdata);

	res = registerFileDesc(fd,
						   NULL,
						   ASYNCCOMM_READBYTES | ASYNCCOMM_WRITEBYTES,
						   &AsyncCommBufferHandlersMessage,
						   context,
						   &newcommbuffer);
	if ( res != FUNC_RETURN_OK )
	{
		rm_pfree(AsyncCommContext, context);
		elog(WARNING, "Fail to register FD for synchronous communication. %d", res);
		goto exit;
	}

	buildMessageToCommBuffer(newcommbuffer,
							 sendbuff,
							 sendbuffsize,
							 sendmsgid,
							 0,
							 0);

	context->AsyncBuffer = newcommbuffer;

	InitHandler_Message(newcommbuffer);

	/* Wait for the complete of the communication. */
	while( true )
	{
		processAllCommFileDescs();
		if ( userdata.CommStatus == SYNC_RPC_COMM_IDLE )
		{
			break;
		}
		else if ( QueryCancelPending )
		{
			/*
			 * We find that this QD wants to cancel the query, we don't need
			 * to continue the communication.
			 */
			res = TRANSCANCEL_INPROGRESS;
			break;
		}
	}

	res = res == TRANSCANCEL_INPROGRESS ? res : userdata.Result;

	/* Close and cleanup */
	closeAndRemoveAllRegisteredFileDesc();

	if (res != FUNC_RETURN_OK)
	{
	  elog(WARNING, "Sync RPC framework (inet) finds exception raised.");
	}
	return res;
exit:
	closeConnectionRemote(&fd);
	return res;
}

void receivedSyncRPCResponse(AsyncCommMessageHandlerContext  context,
							 uint16_t						 messageid,
							 uint8_t						 mark1,
							 uint8_t						 mark2,
							 char 							*buffer,
							 uint32_t						 buffersize)
{
	SyncRPCContext userdata = (SyncRPCContext)(context->UserData);
	Assert(userdata != NULL);

	userdata->CommStatus = SYNC_RPC_COMM_IDLE;

	if ( userdata->ExpMessageID == messageid ) {
		resetSelfMaintainBuffer(userdata->RecvBuffer);
		appendSelfMaintainBuffer(userdata->RecvBuffer, buffer, buffersize);
		userdata->RecvMark1 = mark1;
		userdata->RecvMark2 = mark2;
		userdata->Result    = FUNC_RETURN_OK;
	}
	else {
		userdata->Result    = REQUESTHANDLER_WRONGMESSAGE;
	}
}

void sentSyncRPCRequest(AsyncCommMessageHandlerContext context)
{
	SyncRPCContext userdata = (SyncRPCContext)(context->UserData);
	Assert(userdata != NULL);
	userdata->CommStatus = SYNC_RPC_COMM_RECVING;
	userdata->Result = COMM2RM_CLIENT_FAIL_RECV;
}

void sentSyncRPCRequestError(AsyncCommMessageHandlerContext context)
{
	elog(DEBUG5, "Sync RPC sending error is detected.");
	SyncRPCContext userdata = (SyncRPCContext)(context->UserData);
	Assert(userdata != NULL);
	userdata->CommStatus = SYNC_RPC_COMM_IDLE;
}

void sentSyncRPCRequestCleanUp(AsyncCommMessageHandlerContext context)
{
	elog(DEBUG5, "Sync RPC clean up is called.");
	SyncRPCContext userdata = (SyncRPCContext)(context->UserData);
	Assert(userdata != NULL);
	userdata->CommStatus = SYNC_RPC_COMM_IDLE;
}

void initializeSyncRPCComm(void)
{
	initializeAsyncComm();
}

void initializeSyncRPContent(SyncRPCContext 	content,
							 SelfMaintainBuffer smb,
							 uint16_t	 		expmsgid)
{
	content->ExpMessageID = expmsgid;
	content->RecvBuffer   = smb;
	content->RecvMark1    = 0;
	content->RecvMark2    = 0;
	content->CommStatus   = SYNC_RPC_COMM_SENDING;
	content->Result       = COMM2RM_CLIENT_FAIL_SEND;
}

AsyncCommMessageHandlerContext createMessageHandlerContext(SyncRPCContext content)
{
	AsyncCommMessageHandlerContext context = NULL;

	context = rm_palloc0(AsyncCommContext, sizeof(AsyncCommMessageHandlerContextData));
	context->inMessage				 = false;
	context->UserData  				 = NULL;
	context->MessageRecvReadyHandler = NULL;
	context->MessageRecvedHandler 	 = receivedSyncRPCResponse;
	context->MessageSendReadyHandler = NULL;
	context->MessageSentHandler		 = sentSyncRPCRequest;
	context->MessageErrorHandler 	 = sentSyncRPCRequestError;
	context->MessageCleanUpHandler	 = sentSyncRPCRequestCleanUp;
	context->UserData 				 = (void *)content;
	return context;
}
