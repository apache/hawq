#ifndef HAWQ_RESOURCE_MANAGER_COMMUNICATION_TO_GLOBAL_RESOURCE_MANAGER_LIBYARN_H
#define HAWQ_RESOURCE_MANAGER_COMMUNICATION_TO_GLOBAL_RESOURCE_MANAGER_LIBYARN_H

#include "../../../backend/resourcemanager/include/communication/rmcomm_RM2GRM.h"
#include "envswitch.h"


/* Load parameters from file system. */
int RM2GRM_LIBYARN_loadParameters(void);

/* Connect and disconnect to the global resource manager. */
int RM2GRM_LIBYARN_connect(void);
int RM2GRM_LIBYARN_disconnect(void);

/* Register and unregister this application. */
int RM2GRM_LIBYARN_register(void);
int RM2GRM_LIBYARN_unregister(void);

/* Get information. */
int RM2GRM_LIBYARN_getConnectReport(DQueue report);
int RM2GRM_LIBYARN_getClusterReport(DQueue report);
int RM2GRM_LIBYARN_getResQueueReport(DQueue report);

/* Acquire and return resource. */
int RM2GRM_LIBYARN_acquireResource(uint32_t memorymb,
						   	   	   uint32_t core,
								   uint32_t contcount,
								   DQueue   containers);
int RM2GRM_LIBYARN_returnResource(DQueue containers);

/* Clean all used memory and connections */
int RM2GRM_LIBYARN_cleanup(void);

#define HAWQDRM_COMMANDLINE_YARNSERVER  "-yarn"	// -yarn + ip + port + quename


#endif /* HAWQ_RESOURCE_MANAGER_COMMUNICATION_TO_GLOBAL_RESOURCE_MANAGER_LIBYARN_H */
