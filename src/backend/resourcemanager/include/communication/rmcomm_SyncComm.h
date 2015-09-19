#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_SYNCCOMM_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_SYNCCOMM_H

#include "resourcemanager/utils/memutilities.h"

typedef void (* UserDefinedBackgroundLogicHandler)(void);

void initializeSyncRPCComm(void);
void setUserDefinedBackgroundLogic(UserDefinedBackgroundLogicHandler handler);

int callSyncRPCDomain(const char     	   *sockfile,
					  const char 	 	   *sendbuff,
		        	  int   		  		sendbuffsize,
					  uint16_t		  		sendmsgid,
					  uint16_t 		  		exprecvmsgid,
					  SelfMaintainBuffer 	recvsmb);

int callSyncRPCRemote(const char     	   *hostname,
					  uint16_t              port,
		  	  	  	  const char 	 	   *sendbuff,
					  int   		  		sendbuffsize,
					  uint16_t		  		sendmsgid,
					  uint16_t 		  		exprecvmsgid,
					  SelfMaintainBuffer 	recvsmb);

#endif /*RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_SYNCCOMM_H*/
