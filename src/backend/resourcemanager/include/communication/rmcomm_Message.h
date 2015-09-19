#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_MESSAGE_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_MESSAGE_H

#include "communication/rmcomm_AsyncComm.h"
#include "communication/rmcomm_MessageProtocol.h"

extern AsyncCommBufferHandlersData AsyncCommBufferHandlersMessage;

struct AsyncCommMessageHandlerContextData;
typedef struct AsyncCommMessageHandlerContextData  AsyncCommMessageHandlerContextData;
typedef struct AsyncCommMessageHandlerContextData *AsyncCommMessageHandlerContext;

typedef void ( * MessageRecvReadyHandlerType)(AsyncCommMessageHandlerContext);
typedef void ( * MessageRecvedHandlerType)(AsyncCommMessageHandlerContext,
									 uint16_t,
									 uint8_t,
									 uint8_t,
									 char *,
									 uint32_t);
typedef void ( * MessageSendReadyHandlerType)(AsyncCommMessageHandlerContext);
typedef void ( * MessageSentHandlerType)(AsyncCommMessageHandlerContext);
typedef void ( * MessageErrorHandlerType)(AsyncCommMessageHandlerContext);
typedef void ( * MessageCleanUpHandlerType)(AsyncCommMessageHandlerContext);

struct AsyncCommMessageHandlerContextData {
	AsyncCommBuffer   			AsyncBuffer;
	bool 			   			inMessage;
	MessageRecvReadyHandlerType MessageRecvReadyHandler;
	MessageRecvedHandlerType 	MessageRecvedHandler;
	MessageSendReadyHandlerType MessageSendReadyHandler;
	MessageSentHandlerType		MessageSentHandler;
	MessageErrorHandlerType 	MessageErrorHandler;
	MessageCleanUpHandlerType	MessageCleanUpHandler;
	void					   *UserData;
};

void InitHandler_Message(AsyncCommBuffer buffer);
void ReadReadyHandler_Message(AsyncCommBuffer buffer);
void ReadPostHandler_Message(AsyncCommBuffer buffer);
void WriteReadyHandler_Message(AsyncCommBuffer buffer);
void WritePostHandler_Message(AsyncCommBuffer buffer);
void ErrorHandler_Message(AsyncCommBuffer buffer);
void CleanUpHandler_Message(AsyncCommBuffer buffer);

/* The API for pushing content to CommBuffer for sending out. */
void buildMessageToCommBuffer(AsyncCommBuffer   buffer,
							  const char 	   *content,
							  int   	 		contentsize,
							  uint16_t 	 		messageid,
							  uint8_t    		mark1,
							  uint8_t    		mark2);

#endif /*RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_MESSAGE_H*/
