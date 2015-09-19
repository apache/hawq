#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_CONNECT_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_CONNECT_H

/*
 * This is an async socket connection implementation.
 */

#include "communication/rmcomm_AsyncComm.h"

extern AsyncCommBufferHandlersData AsyncCommBufferHandlersConnect;

/* Callbacks registered in AsyncComm. */
void WriteReadyHandler_Connect(AsyncCommBuffer buffer);
void ErrorHandler_Connect(AsyncCommBuffer buffer);
void CleanUpHandler_Connect(AsyncCommBuffer buffer);

int registerFileDescForAsyncConn(int 		 			  fd,
								 char					 *dmfilename,
								 uint32_t				  actionmask_afterconn,
								 AsyncCommBufferHandlers  methods_afterconn,
								 void				     *userdata_afterconn,
								 AsyncCommBuffer		 *newcommbuffer_afterconn);

#endif /*RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_CONNECT_H*/
