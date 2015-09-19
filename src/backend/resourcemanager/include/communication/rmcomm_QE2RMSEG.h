#include "postgres.h"
#include "resourcemanager/communication/rmcomm_SyncComm.h"
#include "resourcemanager/communication/rmcomm_MessageHandler.h"
#include "resourcemanager/communication/rmcomm_QE_RMSEG_Protocol.h"


int
MoveToCGroupForQE(TimestampTz masterStartTime,
				  int connId,
				  int segId,
				  int procId);

int
MoveOutCGroupForQE(TimestampTz masterStartTime,
				   int connId,
				   int segId,
				   int procId);

int
SetWeightCGroupForQE(TimestampTz masterStartTime,
					 int connId,
					 int segId,
					 QueryResource *resource,
					 int procId);

