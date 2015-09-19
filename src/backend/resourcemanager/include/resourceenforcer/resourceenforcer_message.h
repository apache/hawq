#ifndef _HAWQ_RESOURCEENFORCER_MESSAGE_H
#define _HAWQ_RESOURCEENFORCER_MESSAGE_H

#include "resourceenforcer/resourceenforcer.h"
#include "pg_config_manual.h"

enum
{
	MOVETO,
	MOVEOUT,
	SETWEIGHT
};

typedef struct ResourceEnforcementRequest
{
	int32				type;
	int32				pid;
	char				cgroup_name[MAXPGPATH];
	SegmentResource		query_resource;
} ResourceEnforcementRequest;

#endif /* _HAWQ_RESOURCEENFORCER_MESSAGE_H */
