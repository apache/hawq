#ifndef HAWQ_RESOURCE_MANAGER_RESOURCE_BROKER_LIBYARN_H
#define HAWQ_RESOURCE_MANAGER_RESOURCE_BROKER_LIBYARN_H
#include "envswitch.h"
#include "resourcebroker_API.h"
#include "dynrm.h"

/* Start resource broker service. */
int RB_LIBYARN_start(bool isforked);
/* Stop resource broker service. */
int RB_LIBYARN_stop(void);
/* Get information. */
int RB_LIBYARN_getClusterReport(const char  *quename,
								List 	   **machines,
								double 	    *maxcapacity);

/* Acquire and return resource. */
int RB_LIBYARN_acquireResource(uint32_t memorymb, uint32_t core, List *preferred);
int RB_LIBYARN_returnResource(List **ctnl);
int RB_LIBYARN_getContainerReport(List **ctnstatl);
int RB_LIBYARN_handleNotification(void);
void RB_LIBYARN_handleSignalSIGCHLD(void);
void RB_LIBYARN_handleError(int errorcode);
void RB_LIBYARN_createEntries(RB_FunctionEntries entries);

extern int				ResBrokerRequestPipe[2];
extern int				ResBrokerNotifyPipe[2];
extern volatile bool	ResBrokerKeepRun;
extern volatile bool	ResBrokerExits;
extern volatile pid_t	ResBrokerPID;
extern pid_t			ResBrokerParentPID;

int ResBrokerMain(void);

#endif /* HAWQ_RESOURCE_MANAGER_RESOURCE_BROKER_LIBYARN_H */
