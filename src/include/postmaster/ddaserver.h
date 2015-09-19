/*-------------------------------------------------------------------------
 *
 * ddaserver.h
 *	  Under the QD postmaster the ddaserver combines lock information 
 *    from QEs to detect deadlocks.  Each QE has a local ddaserver that
 *    communicates with the master ddaserver at the QD when it finds
 *    a lock waiter.
 *
 * Copyright (c) 2007-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef DDASERVER_H
#define DDASERVER_H

#ifndef _WIN32
#include <stdint.h>             /* uint32_t (C99) */
#else
typedef unsigned int uint32_t;
#endif 


extern void DdaserverShmemInit(void);
extern int DdaserverShmemSize(void);
extern int ddaserver_start(void);





#endif   /* DDASERVER_H */
