#ifndef _HAWQ_RESOURCE_ENFORCER_H
#define _HAWQ_RESOURCE_ENFORCER_H

#include "resourcemanager/utils/memutilities.h"
#include "resourceenforcer_list.h"

/* Per physical segment resource requirement of a query */
typedef struct SegmentResource
{
	double		vcore;
	double		iopercent;
	uint32		memory;
} SegmentResource;

/* Per physical segment CGroup information of a query */
typedef struct CGroupInfo
{
	char		name[MAXPGPATH];
	uint64		creation_time;
	llist		*pids;
	int32		vcore_current;
	int32		vdisk_current;
	int			to_be_deleted;
} CGroupInfo;

void freeCGroupInfo(void *cgroup_info);
bool isCGroupEnabled(const char *sub_system);
bool isCGroupSetup(const char *sub_system);
void ShowCGroupEnablementInformation(const char *sub_system);

int MoveToCGroup(uint32 pid, const char *cgroup_name);
int MoveOutCGroup(uint32 pid, const char *cgroup_name);
int SetupWeightCGroup(uint32 pid, const char *cgroup_name, SegmentResource *resource);
int CleanUpCGroupAtStartup(const char *sub_system);
int CleanUpCGroupAtRuntime(void);

int HAWQMemoryEnforcementIncreaseQuota(uint64 mem_limit_delta);
int HAWQMemoryEnforcementDecreaseQuota(uint64 mem_limit_delta);

void initCGroupThreads(void);

#endif /* _HAWQ_RESOURCE_ENFORCER_H */
