#ifndef _PXF_UTILS_H_
#define _PXF_UTILS_H_

#include "postgres.h"

/* checks if two ip strings are equal */
bool are_ips_equal(char *ip1, char *ip2);

/* override port str with given new port int */
void port_to_str(char* port, int new_port);

#endif	// _PXF_UTILS_H_
