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
void addNewMessageToConnTrack(AsyncCommMessageHandlerContext context,
							  uint16_t						 messageid,
							  uint8_t						 mark1,
							  uint8_t						 mark2,
							  char 							*buffer,
							  uint32_t						 buffersize);

void sentMessageFromConnTrack(AsyncCommMessageHandlerContext context);
void hasCommErrorInConnTrack(AsyncCommMessageHandlerContext context);
void cleanupConnTrack(AsyncCommMessageHandlerContext context);

#endif /*RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_MESSAGESERVER_H*/
