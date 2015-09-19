#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RMSEG2RM_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RMSEG2RM_H
#include "envswitch.h"
#include "utils/linkedlist.h"
#include "utils/simplestring.h"
#include "dynrm.h"

extern MemoryContext RMSEG2RM_CommContext;
/******************************************************************************
 * This header file contains the APIs for resource manager segment process.
 ******************************************************************************/

struct RMSEG2RMContextData {
	int				RMSEG2RM_Conn_FD;

	/* Reusable buffer for building request messages & receiving response
	 * messages.*/
	SelfMaintainBufferData 	SendBuffer;
	SelfMaintainBufferData  RecvBuffer;
};

typedef struct RMSEG2RMContextData  RMSEG2RMContextData;
typedef struct RMSEG2RMContextData *RMSEG2RMContext;

/* Initialize environment to HAWQ RM */
void initializeRMSEG2RMComm(void);

/* Clean up all reset resource information and do necessary return to HAWQ RM.*/
int cleanupRMSEG2RMComm(void);

int sendIMAlive(int  *errorcode,
				char *errorbuf,
				int	  errorbufsize);

int sendIShutdown(void);
#endif /* RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RMSEG2RM_H */
