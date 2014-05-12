#include "access/pxfutils.h"
#include "utils/builtins.h"

/* checks if two ip strings are equal */
bool are_ips_equal(char *ip1, char *ip2)
{
	if ((ip1 == NULL) || (ip2 == NULL))
		return false;
	return (strcmp(ip1, ip2) == 0);
}

/* override port str with given new port int */
void port_to_str(char* port, int new_port)
{
	char tmp[10];
	if (port)
		pfree(port);

	Assert((new_port <= 65535) && (new_port >= 1)); /* legal port range */
	pg_ltoa(new_port, tmp);
	port = pstrdup(tmp);
}
