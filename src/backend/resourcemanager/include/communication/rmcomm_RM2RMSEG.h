#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RM2RMSEG_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RM2RMSEG_H
#include "envswitch.h"
#include "utils/linkedlist.h"
#include "utils/simplestring.h"
#include "dynrm.h"

extern MemoryContext RM2RMSEG_CommContext;
/******************************************************************************
 * This header file contains the APIs for resource manager master process.
 ******************************************************************************/

struct RM2RMSEGContextData
{
	int						RM2RMSEG_Conn_FD;

	/* Reusable buffer to build request messages */
	SelfMaintainBufferData	SendBuffer;

	/* Reusable buffer to build response messages */
	SelfMaintainBufferData	RecvBuffer;
};

typedef struct RM2RMSEGContextData  RM2RMSEGContextData;
typedef struct RM2RMSEGContextData *RM2RMSEGContext;

/* Initialize environment to HAWQ RM */
void initializeRM2RMSEGComm(void);

/* Clean up and reset resource information and do necessary return to HAWQ RM */
int cleanupRM2RMSEGComm(void);

int sendRUAlive(char *seghostname);

/* Update memory quota on specific machine */
int increaseMemoryQuota(char *seghostname, GRMContainerSet containerset);
int decreaseMemoryQuota(char *seghostname, GRMContainerSet containerset);

#endif /* RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RM2RMSEG_H */
