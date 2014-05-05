#include "access/pxfutils.h"

/* checks if two ip strings are equal */
bool are_ips_equal(char *ip1, char *ip2)
{
	if ((ip1 == NULL) || (ip2 == NULL))
		return false;
	return (strcmp(ip1, ip2) == 0);
}
