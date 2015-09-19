#ifndef _PXF_COMUTILS_H_
#define _PXF_COMUTILS_H_

#include "postgres.h"

/*
 * Test that a server app on host:port is up.
 */
bool ping(char* host, char *port);

#endif	// _PXF_COMUTILS_H_
