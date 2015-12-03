/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * resourceenforcer.c
 *     CPU usage enforcement for HAWQ.
 *
 * We leverage CGroup so as to make sure a query will not go
 * beyond its allowed CPU usage quota.
 */

#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <pthread.h>
#include "postgres.h"
#include "storage/fd.h"
#include "postmaster/postmaster.h"
#include <sys/time.h>
#include "utils/hashtable.h"
#include "utils/simplestring.h"
#include "dynrm.h"
#include "resourcemanager.h"
#include "resourceenforcer/resourceenforcer.h"
#include "resourceenforcer/resourceenforcer_message.h"
#include "resourceenforcer/resourceenforcer_queue.h"
#include "resourceenforcer/resourceenforcer_list.h"
#include "resourceenforcer/resourceenforcer_hash.h"


#define	ENFORCER_MESSAGE_HEAD "Resource enforcer"
/* #define DEBUG_GHASH 1 */

static char *getCGroupPath(const char *cgroup_name, const char *sub_system);
static int createCGroup(const char *cgroup_name, const char *sub_system);
static int deleteCGroup(const char *cgroup_name, const char *sub_system);

static char *getCGroupFilePath(const char *cgroup_name,
                               const char *sub_system,
                               const char *cgroup_file);

static int writeCGroupFileInt32(const char *cgroup_path,
		                        int32 value,
                                bool append);

static int setCGroupProcess(const char *cgroup_name,
                            const char *sub_system,
                            uint32 pid);

static int setCGroupWeightInt32(const char *cgroup_name,
                                const char *sub_system,
                                const char *cgroup_file,
                                int32 weight);

static uint64 getCGroupCleanupThreshold(void);

static uint64 getCGroupLastCleanupTime(void);
static void setCGroupLastCleanupTime(uint64 time_microsecond);

static GSimpStringPtr stringToGSimpString(const char *str);

static int CGroupPidCmp(void *p, void *q);

static void *cgroupService(void *arg);

/**
 * Move QE process into CGroup for resource enforcement purpose.
 */
int MoveToCGroup(uint32 pid, const char *cgroup_name)
{
	int			res = FUNC_RETURN_OK;

	CGroupInfo	*cgi = NULL;
	uint32		*pid_add = NULL;

	GSimpStringPtr pkey = stringToGSimpString(cgroup_name);
	if (pkey == NULL)
	{
		write_log("%s fails to prepare CGroup name %s due to out of memory",
		          ENFORCER_MESSAGE_HEAD,
		          cgroup_name);

		return RESENFORCER_ERROR_INSUFFICIENT_MEMORY;
	}

	Pair cgroup = getGHashNode(g_ghash_cgroup, (void *)pkey);
	if (cgroup == NULL)
	{
		if (rm_enforce_cpu_enable)
		{
			res = createCGroup(cgroup_name, "cpu");

			if (res != FUNC_RETURN_OK)
			{
				write_log("%s fails to create CGroup %s",
				          ENFORCER_MESSAGE_HEAD,
				          cgroup_name);

				return res;
			}
		}

		cgi = (CGroupInfo *)malloc(sizeof(CGroupInfo));
		if (cgi == NULL)
		{
			write_log("%s fails to create CGroup %s due to out of memory",
			          ENFORCER_MESSAGE_HEAD,
			          cgroup_name);

			return RESENFORCER_ERROR_INSUFFICIENT_MEMORY;
		}

		Assert(strlen(cgroup_name) < sizeof(cgi->name));
		strncpy(cgi->name, cgroup_name, strlen(cgroup_name)+1);
		cgi->creation_time = gettime_microsec();
		cgi->pids = llist_create();
		if (cgi->pids == NULL)
		{
			write_log("%s fails to add PID %d to CGroup %s",
			          ENFORCER_MESSAGE_HEAD,
			          pid,
			          cgroup_name);

			res = RESENFORCER_ERROR_INSUFFICIENT_MEMORY;

			goto exit;
		}
		cgi->vcore_current = 0;
		cgi->vdisk_current = 0;
		cgi->to_be_deleted = 0;

		void *oldvalue = NULL;

	#ifdef DEBUG_GHASH
		write_log("%s: before add CGroup %s in hash in MoveToCGroup",
		          ENFORCER_MESSAGE_HEAD,
		          cgroup_name);
		dumpGHash(g_ghash_cgroup);
	#endif

		if (setGHashNode(g_ghash_cgroup,
						 (void *)pkey,
						 (void *)cgi,
						 false,
						 &oldvalue) != FUNC_RETURN_OK)
		{
			write_log("%s fails to add CGroup to hash due to out of memory",
			          ENFORCER_MESSAGE_HEAD);
			res = RESENFORCER_ERROR_INSUFFICIENT_MEMORY;
			goto exit;
		}

	#ifdef DEBUG_GHASH
		write_log("%s: after add CGroup %s in hash in MoveToCGroup",
		          ENFORCER_MESSAGE_HEAD,
		          cgroup_name);
		dumpGHash(g_ghash_cgroup);
	#endif
	}
	else
	{
		cgi = (CGroupInfo *)(cgroup->Value);
		/* revert the delete operation */
		if (cgi == NULL)
		{
			write_log("%s finds CGroup %s is already in hash, "
			          "but its content is inaccessible",
			          ENFORCER_MESSAGE_HEAD,
			          cgroup_name);

			goto exit;
		}
		else if (cgi->to_be_deleted > 0)
		{
			cgi->to_be_deleted = 0;
		}
	}

#ifdef DEBUG_GHASH
	write_log("%s: before add PID %d in CGroup %s in hash in MoveToCGroup",
	          ENFORCER_MESSAGE_HEAD,
	          pid,
	          cgroup_name);
	dumpGHash(g_ghash_cgroup);
#endif

	pid_add = (uint32 *)malloc(sizeof(uint32));

	if (pid_add == NULL)
	{
		write_log("%s fails to create PID %d due to out of memory",
		          ENFORCER_MESSAGE_HEAD,
		          pid);

		res = RESENFORCER_ERROR_INSUFFICIENT_MEMORY;

		goto exit;
	}
	*pid_add = pid;
	llist_insert(cgi->pids, pid_add);

#ifdef DEBUG_GHASH
	write_log("%s: after add PID %d in CGroup %s in hash in MoveToCGroup",
	          ENFORCER_MESSAGE_HEAD,
	          pid,
	          cgroup_name);
	dumpGHash(g_ghash_cgroup);
#endif

	/* Process CGroup for cpu sub-system */
	if (rm_enforce_cpu_enable)
	{
		res = setCGroupProcess(cgroup_name, "cpu", pid);

		if (res != FUNC_RETURN_OK)
		{
			write_log("%s fails to add PID %d to CPU CGroup %s",
			          ENFORCER_MESSAGE_HEAD,
			          pid,
			          cgroup_name);
			goto exit;
		}
	}

	return FUNC_RETURN_OK;

exit:
	if (pkey)
	{
		free(pkey);
	}

	if (pid_add)
	{
		free(pid_add);
	}

	if (cgi)
	{
		freeCGroupInfo(cgi);
	}

	return res;
}


/**
 * Move QE process out from CGroup after resource enforcement.
 */
int MoveOutCGroup(uint32 find_pid, const char *cgroup_name)
{
	Assert(cgroup_name);

	GSimpStringPtr pkey = stringToGSimpString(cgroup_name);
	if (pkey == NULL)
	{
		write_log("%s fails to prepare CGroup name %s due to out of memory",
		          ENFORCER_MESSAGE_HEAD,
		          cgroup_name);
		return RESENFORCER_ERROR_INSUFFICIENT_MEMORY;
	}

	Pair cgroup = getGHashNode(g_ghash_cgroup, (void *)pkey);
	if (cgroup == NULL)
	{
		write_log("%s fails to move PID %u out from CGroup %s "
		          "since the CGroup does not exist",
		          ENFORCER_MESSAGE_HEAD,
		          find_pid,
		          cgroup_name);

		return RESENFORCER_FAIL_FIND_CGROUP_HASH_ENTRY;
	}
	else
	{
	#ifdef DEBUG_GHASH
		write_log("%s: before remove PID %d from CGroup %s in hash in MoveOutCGroup",
		          ENFORCER_MESSAGE_HEAD,
		          find_pid,
		          cgroup_name);
		dumpGHash(g_ghash_cgroup);
	#endif

		CGroupInfo *cgi = (CGroupInfo *)(cgroup->Value);
		lnode *pid = llist_delete(cgi->pids, &find_pid, CGroupPidCmp);

	#ifdef DEBUG_GHASH
		write_log("%s: after remove PID %d from CGroup %s in hash in MoveOutCGroup",
		          ENFORCER_MESSAGE_HEAD,
		          find_pid,
		          cgroup_name);
		dumpGHash(g_ghash_cgroup);
	#endif

		if (pid)
		{
			free((int *)pid->data);
			free(pid);

			if (cgi->pids->size == 0)
			{
				cgi->to_be_deleted = 1;
			}
		}
		else
		{
			write_log("%s fails to move PID %u out from CGroup %s "
			          "since the PID does not exist",
			          ENFORCER_MESSAGE_HEAD,
			          find_pid,
			          cgroup_name);

			return RESENFORCER_FAIL_FIND_CGROUP_HASH_ENTRY;
		}
	}

	return FUNC_RETURN_OK;
}

/**
 * Set CGroup weight for QE process for resource enforcement purpose.
 */
int SetupWeightCGroup(uint32 pid, const char *cgroup_name, SegmentResource *resource)
{
	Assert(cgroup_name);
	Assert(resource);
	Assert(resource->vcore > 0.0);

	int32		cpu_weight = -1;

	int			res = FUNC_RETURN_OK;

	GSimpStringPtr pkey = stringToGSimpString(cgroup_name);
	if (pkey == NULL)
	{
		write_log("%s fails to prepare CGroup name %s due to out of memory",
		          ENFORCER_MESSAGE_HEAD,
		          cgroup_name);

		return RESENFORCER_ERROR_INSUFFICIENT_MEMORY;
	}

	Pair cgroup = getGHashNode(g_ghash_cgroup, (void *)pkey);
	if (cgroup == NULL)
	{
		write_log("%s fails to set weight for CGroup %s since it does not exist",
		          ENFORCER_MESSAGE_HEAD,
		          cgroup_name);

		return RESENFORCER_FAIL_FIND_CGROUP_HASH_ENTRY;
	}

	/* Process CGroup for cpu sub-system */
	if (rm_enforce_cpu_enable)
	{
		cpu_weight = (int32)(resource->vcore * rm_enforce_cpu_weight) + 1;

	#ifdef DEBUG_GHASH
		write_log("%s: before set CPU weight %d for CGroup %s in hash in SetupWeightCGroup",
		          ENFORCER_MESSAGE_HEAD,
		          cpu_weight,
		          cgroup_name);
		dumpGHash(g_ghash_cgroup);
	#endif

		CGroupInfo *cgi = (CGroupInfo *)(cgroup->Value);
		if (cpu_weight != cgi->vcore_current)
		{
			res = setCGroupWeightInt32(cgroup_name,
						   "cpu",
						   "cpu.shares",
						   cpu_weight);

			if (res == FUNC_RETURN_OK)
			{
				cgi->vcore_current = cpu_weight;
			}
			else
			{
				write_log("%s fails to set weight %d for CPU CGroup %s",
				          ENFORCER_MESSAGE_HEAD,
				          cpu_weight,
				          cgroup_name);

				return res;
			}
		}

	#ifdef DEBUG_GHASH
		write_log("%s: after set CPU weight %d for CGroup %s in hash in SetupWeightCGroup",
		          ENFORCER_MESSAGE_HEAD,
		          cpu_weight,
		          cgroup_name);
		dumpGHash(g_ghash_cgroup);
	#endif
	}

	return FUNC_RETURN_OK;
}

/**
 * Clean up CGroup directories in a periodical fashion when HAWQ is up and running.
 */
int CleanUpCGroupAtRuntime(void)
{
	int		res = FUNC_RETURN_OK;

	if (gettime_microsec() - getCGroupLastCleanupTime() < getCGroupCleanupThreshold())
	{
		return FUNC_RETURN_OK;
	}

	for (int i = 0; i < g_ghash_cgroup->SlotVolume; ++i)
	{
		if (g_ghash_cgroup->Slots[i] == NULL)
		{
			continue;
		}

		lnode *pairnode = (g_ghash_cgroup->Slots[i])->head;
		lnode *nextnode = NULL;
		while (pairnode)
		{
			nextnode = pairnode->next;

			Pair pair = (Pair)llist_lfirst(pairnode);
			GSimpString *key = (GSimpString *)(pair->Key);
			CGroupInfo *cgi = (CGroupInfo *)(pair->Value);

			/* Delay remove CGroup directory to workaround CGroup panic bug */
			if (cgi->to_be_deleted == 1)
			{
				cgi->to_be_deleted++;
			}
			else if (cgi->to_be_deleted > 1)
			{
				res = deleteCGroup(cgi->name, "cpu");

				if (res == RESENFORCER_ERROR_INSUFFICIENT_MEMORY)
				{
					write_log("%s fails to remove CPU CGroup directory %s "
					          "due to out of memory",
					          ENFORCER_MESSAGE_HEAD,
					          cgi->name);

					return RESENFORCER_ERROR_INSUFFICIENT_MEMORY;
				}

				if (res == FUNC_RETURN_OK)
				{
					if (pairnode->prev)
					{
						pairnode->prev->next = pairnode->next;

						if (pairnode->next)
						{
							pairnode->next->prev = pairnode->prev;
						}
					}
					else
					{
						(g_ghash_cgroup->Slots[i])->head = pairnode->next;

						if (pairnode->next)
						{
							pairnode->next->prev = NULL;
						}
					}

					(g_ghash_cgroup->Slots[i])->size--;

					if (key)
					{
						g_ghash_cgroup->KeyFreeFunction((void *)key);
					}

					if (g_ghash_cgroup->ValFreeFunction)
					{
						g_ghash_cgroup->ValFreeFunction((void *)cgi);
					}

					free(pairnode->data);
					free(pairnode);
				}
				else
				{
					write_log("%s fails to remove CGroup %s with errno %d",
					          ENFORCER_MESSAGE_HEAD,
					          cgi->name,
					          res);
				}
			}

			pairnode = nextnode;
		}
	}

	setCGroupLastCleanupTime(gettime_microsec());

	return FUNC_RETURN_OK;
}

/**
 * Clean up CGroup directories when HAWQ is starting up.
 */
int CleanUpCGroupAtStartup(const char *sub_system)
{
	char			*cgroup_path = NULL;
	DIR				*cgroup_dir = NULL;
	struct dirent	*cgroup_ent = NULL;

	int				res = FUNC_RETURN_OK;

	cgroup_path = getCGroupPath("", sub_system);
	if (cgroup_path == NULL)
	{
		write_log("%s fails to get CGroup root directory %s "
		          "for cleanup at startup due to out of memory",
		          ENFORCER_MESSAGE_HEAD,
		          cgroup_path);

		return RESENFORCER_ERROR_INSUFFICIENT_MEMORY;
	}

	cgroup_dir = opendir(cgroup_path);
	if (cgroup_dir == NULL)
	{
		write_log("%s fails to open CGroup root directory %s "
		          "for cleanup at startup",
		          ENFORCER_MESSAGE_HEAD,
		          cgroup_path);

		free(cgroup_path);
		return RESENFORCER_FAIL_READ_CGROUP_FILE;
	}
	free(cgroup_path);

	while ((cgroup_ent = readdir(cgroup_dir)) != NULL)
	{
		if (strncmp(cgroup_ent->d_name, "hawq", 4) == 0)
		{
			res = deleteCGroup(cgroup_ent->d_name, sub_system);

			if (res != FUNC_RETURN_OK)
			{
				write_log("%s fails to remove CGroup directory %s "
				          "for cleanup at startup",
				          ENFORCER_MESSAGE_HEAD,
				          cgroup_ent->d_name);

				closedir(cgroup_dir);
				return RESENFORCER_FAIL_DELETE_CGROUP;
			}
		}
	}

	closedir(cgroup_dir);

	return FUNC_RETURN_OK;
}

uint64 getCGroupCleanupThreshold(void)
{
	return ((uint64)(rm_enforce_cleanup_period)) * 1000000ULL;
}

uint64 getCGroupLastCleanupTime(void)
{
	return rm_enforce_last_cleanup_time;
}

void setCGroupLastCleanupTime(uint64 time_microsecond)
{
	rm_enforce_last_cleanup_time = time_microsecond;
}

char *getCGroupPath(const char *cgroup_name, const char *sub_system)
{
	char	*mount_point = rm_enforce_cgrp_mnt_pnt;
	char	*hierarchy = rm_enforce_cgrp_hier_name;

	char	*cgroup_path = NULL;
	int		cgroup_len = 0;

	cgroup_len = strlen(mount_point) +
			     strlen("/") + strlen(sub_system) +
			     strlen("/") + strlen(hierarchy) +
			     strlen("/") + strlen(cgroup_name) + 1;

	cgroup_path = (char *)malloc(cgroup_len);

	if (cgroup_path == NULL)
	{
		write_log("%s fails to get CGroup path due to out of memroy",
		          ENFORCER_MESSAGE_HEAD);

		return NULL;
	}

	snprintf(cgroup_path, cgroup_len, "%s/%s/%s/%s", mount_point,
                                                     sub_system,
									                 hierarchy,
									                 cgroup_name);
	cgroup_path[cgroup_len-1] = '\0';

	return cgroup_path;
}

/**
 * Create directory for specific CGroup
 */
int createCGroup(const char *cgroup_name, const char *sub_system)
{
	Assert(cgroup_name);
	Assert(sub_system);

	int		 res		 = FUNC_RETURN_OK;
	char	*cgroup_path = getCGroupPath(cgroup_name, sub_system);

	if (cgroup_path == NULL)
	{
		write_log("%s fails to create CGroup %s due to out of memory",
		          ENFORCER_MESSAGE_HEAD,
		          cgroup_name);

		return RESENFORCER_ERROR_INSUFFICIENT_MEMORY;
	}

	/*  create the CGroup directory directly */
	if (mkdir(cgroup_path, S_IRWXU | S_IRGRP | S_IXGRP) < 0)
	{
		write_log("%s fails to create CGroup directory %s with errno %d",
		          ENFORCER_MESSAGE_HEAD,
		          cgroup_path,
		          errno);

		res = RESENFORCER_FAIL_CREATE_CGROUP;
	}

	free(cgroup_path);

	return res;
}

/**
 * Delete directory of specific CGroup
 */
int deleteCGroup(const char *cgroup_name, const char *sub_system)
{
	Assert(cgroup_name);
	Assert(sub_system);

	char	*cgroup_path = getCGroupPath(cgroup_name, sub_system);

	if (cgroup_path == NULL)
	{
		write_log("%s fails to delete CGroup %s due to out of memory",
		          ENFORCER_MESSAGE_HEAD,
		          cgroup_name);

		return RESENFORCER_ERROR_INSUFFICIENT_MEMORY;
	}

	int		res = FUNC_RETURN_OK;

	/* Delete the CGroup directory directly */
	if (rmdir(cgroup_path) < 0)
	{
		write_log("%s fails to delete CGroup directory %s with errno %d",
		          ENFORCER_MESSAGE_HEAD,
		          cgroup_path,
		          errno);

		res = RESENFORCER_FAIL_DELETE_CGROUP;
	}

	free(cgroup_path);

	return res;
}

char *getCGroupFilePath(const char *cgroup_name,
                        const char *sub_system,
                        const char *cgroup_file)
{
	char	*mount_point = rm_enforce_cgrp_mnt_pnt;
	char	*hierarchy = rm_enforce_cgrp_hier_name;

	char	*cgroup_path = NULL;
	int		cgroup_len = 0;

	cgroup_len = strlen(mount_point) +
			     strlen("/") + strlen(sub_system) +
			     strlen("/") + strlen(hierarchy) +
			     strlen("/") + strlen(cgroup_name) +
			     strlen("/") + strlen(cgroup_file) + 1;

	cgroup_path = (char *)malloc(cgroup_len);

	if (cgroup_path == NULL)
	{
		write_log("%s fails to get CGroup path due to out of memroy",
		          ENFORCER_MESSAGE_HEAD);

		return NULL;
	}

	snprintf(cgroup_path, cgroup_len, "%s/%s/%s/%s/%s", mount_point,
                                                        sub_system,
									                    hierarchy,
									                    cgroup_name,
									                    cgroup_file);
	cgroup_path[cgroup_len-1] = '\0';

	return cgroup_path;
}

int writeCGroupFileInt32(const char *cgroup_path,
                         int32 value,
                         bool append)
{
	Assert(cgroup_path);

	int 	res = FUNC_RETURN_OK;
	FILE	*fp = NULL;

	fp = fopen(cgroup_path, append ? "a" : "w");

	if (!fp)
	{
		write_log("%s fails to write weight %d for CGroup %s with errno %d",
		          ENFORCER_MESSAGE_HEAD,
		          value,
		          cgroup_path,
		          errno);

		return RESENFORCER_FAIL_WRITE_CGROUP_FILE;
	}

	res = fprintf(fp, "%d\n", value);
	if ((res < 0) && (errno != ESRCH))
	{
		write_log("%s fails to write weight %d for CGroup %s with errno %d",
		          ENFORCER_MESSAGE_HEAD,
		          value,
		          cgroup_path,
		          errno);

		fclose(fp);

		return RESENFORCER_FAIL_WRITE_CGROUP_FILE;
	}

	fclose(fp);

	return FUNC_RETURN_OK;
}

int setCGroupProcess(const char *cgroup_name,
                     const char *sub_system,
                     uint32 pid)
{
	Assert(cgroup_name);
	Assert(sub_system);

	char	*cgroup_path = NULL;

	int		res = FUNC_RETURN_OK;

	cgroup_path = getCGroupFilePath(cgroup_name, sub_system, "cgroup.procs");

	if (cgroup_path == NULL)
	{
		write_log("%s fails to add PID %d to CGroup %s due to out of memory",
		          ENFORCER_MESSAGE_HEAD,
		          pid,
		          cgroup_name);

		return RESENFORCER_ERROR_INSUFFICIENT_MEMORY;
	}

	res = writeCGroupFileInt32(cgroup_path, pid, true);

	if (res != FUNC_RETURN_OK)
	{
		write_log("%s fails to add PID %u to CGroup %s",
		          ENFORCER_MESSAGE_HEAD,
		          pid,
		          cgroup_path);
	}

	free(cgroup_path);

	return res;
}

int setCGroupWeightInt32(const char *cgroup_name,
                         const char *sub_system,
                         const char *cgroup_file,
                         int32 weight)
{
	Assert(cgroup_name);
	Assert(sub_system);
	Assert(cgroup_file);
	Assert(weight > 0);

	char	*cgroup_path = NULL;

	int		res = FUNC_RETURN_OK;

	cgroup_path = getCGroupFilePath(cgroup_name, sub_system, cgroup_file);

	if (cgroup_path == NULL)
	{
		write_log("%s fails to set weight %d for CGroup %s due to out of memory",
		          ENFORCER_MESSAGE_HEAD,
		          weight,
		          cgroup_name);

		return RESENFORCER_ERROR_INSUFFICIENT_MEMORY;
	}

	res = writeCGroupFileInt32(cgroup_path, weight, false);

	if (res != FUNC_RETURN_OK)
	{
		write_log("%s fails to set weight %d for CGroup %s",
		          ENFORCER_MESSAGE_HEAD,
		          weight,
		          cgroup_path);
	}

	free(cgroup_path);

	return res;
}

bool isCGroupEnabled(const char *sub_system)
{
	if (strcasecmp(sub_system, "cpu") == 0)
	{
		return rm_enforce_cpu_enable;
	}
	else
	{
		write_log("%s fails to check CGroup enablement "
		          "due to invalid sub-system name %s",
		          ENFORCER_MESSAGE_HEAD,
		          sub_system);

		return false;
	}
}

bool isCGroupSetup(const char *sub_system)
{
	bool enabled = false;

	char *cgroup_path = getCGroupPath("", sub_system);

	if (access(cgroup_path, F_OK) == 0)
	{
		enabled = true;
	}

	free(cgroup_path);

	return enabled;
}

void ShowCGroupEnablementInformation(const char *sub_system)
{
#ifdef __linux

	if (isCGroupEnabled(sub_system))
	{
		if (isCGroupSetup(sub_system))
		{
			elog(RMLOG, "%s finds %s sub-system is enabled and setup",
			            ENFORCER_MESSAGE_HEAD,
			            sub_system);
		}
		else
		{
			elog(WARNING, "%s finds %s sub-system is enabled but not setup",
			              ENFORCER_MESSAGE_HEAD,
			              sub_system);
		}
	}
	else
	{
		elog(RMLOG, "%s finds %s sub-system is disabled",
		            ENFORCER_MESSAGE_HEAD,
		            sub_system);
	}

#endif
}

void *cgroupService(void *arg)
{
	int res	= FUNC_RETURN_OK;

	gp_set_thread_sigmasks();

	res = CleanUpCGroupAtStartup("cpu");
	if (res != FUNC_RETURN_OK)
	{
		write_log("%s fails to CleanUpCGroupAtStartup, "
		          "cgroupService thread will quit",
		          ENFORCER_MESSAGE_HEAD);
		return NULL;
	}

	g_ghash_cgroup = createGHash(GHASH_SLOT_VOLUME_DEFAULT,
								 GHASH_SLOT_VOLUME_DEFAULT_MAX,
								 GHASH_KEYTYPE_SIMPSTR,
								 NULL);
	if (g_ghash_cgroup == NULL)
	{
		write_log("%s fails to createGHash due to out of memory, "
		          "cgroupService thread will quit",
		          ENFORCER_MESSAGE_HEAD);
		return NULL;
	}

    while (true)
    {
    	/* Timeout in 1 second (1000 ms) */
    	ResourceEnforcementRequest *task = NULL;
		task = (ResourceEnforcementRequest *)dequeue(g_queue_cgroup, 1*1000);

    	if (!task)
    	{
			CleanUpCGroupAtRuntime();
			continue;
    	}

    	if (task->type == MOVETO)
    	{
    		res = MoveToCGroup(task->pid, task->cgroup_name);

    		if (res != FUNC_RETURN_OK)
    		{
				write_log("%s fails to move PID %d to CGroup %s with error %d",
				          ENFORCER_MESSAGE_HEAD,
				          task->pid,
				          task->cgroup_name,
				          res);
    			free(task);
    			break;
    		}
    	}
    	else if (task->type == MOVEOUT)
    	{
    		res = MoveOutCGroup(task->pid, task->cgroup_name);

    		if (res != FUNC_RETURN_OK)
			{
				write_log("%s fails to move PID %d out from CGroup %s "
				          "with error %d",
				          ENFORCER_MESSAGE_HEAD,
				          task->pid,
				          task->cgroup_name,
				          res);
				free(task);
				break;
			}
    	}
    	else if (task->type == SETWEIGHT)
    	{
    		res = SetupWeightCGroup(task->pid, task->cgroup_name, &(task->query_resource));

    		if (res != FUNC_RETURN_OK)
			{
				write_log("%s fails to set weight %lf for CGroup %s with error %d",
				          ENFORCER_MESSAGE_HEAD,
				          task->query_resource.vcore,
				          task->cgroup_name,
				          res);
				free(task);
				break;
			}
    	}
    	else
    	{
			write_log("%s receives invalid CGroup task type",
			          ENFORCER_MESSAGE_HEAD);
    		free(task);
    		break;
    	}

    	free(task);

    	CleanUpCGroupAtRuntime();
    }

	write_log("%s marks the indicator that cgroupService thread will quit",
	          ENFORCER_MESSAGE_HEAD);

	g_enforcement_thread_quited = true;

    return NULL;
}

void initCGroupThreads(void)
{
	/* We don't initialize CGroup thread if CPU enforcement is not enabled*/
	if (!rm_enforce_cpu_enable)
	{
		return;
	}

	/* Initialize queue for CPU enforcement tasks */
	g_queue_cgroup = queue_create();

	if (g_queue_cgroup == NULL)
	{
		elog(ERROR, "%s fail to initCGroupThreads due to out of memory",
		            ENFORCER_MESSAGE_HEAD);
	}

	/* Create thread to handle CPU enforcement tasks in queue */
	if (pthread_create(&t_move_cgroup, NULL, cgroupService, NULL))
	{
		elog(FATAL, "%s fails to initCGroupThreads "
		            "due to failure to create cgroupService thread",
		            ENFORCER_MESSAGE_HEAD);
	}

	/* Set current time as latest cleanup time for CGroup */
	setCGroupLastCleanupTime(gettime_microsec());
}

int CGroupPidCmp(void *p, void *q)
{
	Assert(p);
	Assert(q);

	uint32 *pc = (uint32 *)p;
	uint32 *qc = (uint32 *)q;

	return (*pc == *qc) ? 0 : 1;
}

void freeCGroupInfo(void *cgroup_info)
{
	Assert(cgroup_info);

	CGroupInfo *cgi = (CGroupInfo *)cgroup_info;

	if (cgi->pids)
	{
		llist_destroy(cgi->pids);
	}

	free(cgi);
}

static GSimpStringPtr stringToGSimpString(const char *str)
{
	char *tempStr = (char *)malloc(strlen(str) + 1);
	if ( tempStr == NULL )
	{
		write_log("%s fails to malloc in stringToGSimpleString "
		          "due to out of memory",
		          ENFORCER_MESSAGE_HEAD);
		return NULL;
	}
	strncpy(tempStr, str, strlen(str)+1);

	GSimpStringPtr simpStr = createGSimpString();
	if ( simpStr == NULL )
	{
		free(tempStr);
		write_log("%s fails to createGSimpString in stringToGSimpleString "
		          "due to out of memory",
		          ENFORCER_MESSAGE_HEAD);
		return NULL;
	}

	if ( setGSimpStringWithContent(simpStr, tempStr, strlen(str)) != FUNC_RETURN_OK )
	{
		free(tempStr);
		free(simpStr);
		write_log("%s fails to setGSimpStringWithContent in stringToGSimpleString "
		          "due to out of memory",
		          ENFORCER_MESSAGE_HEAD);
		return NULL;
	}

	free(tempStr);

	return simpStr;
}
