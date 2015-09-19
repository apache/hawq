/*-------------------------------------------------------------------------
 *
 * mdver.h
 *	  Interface for metadata versioning
 *
 * Copyright (c) 2014, Pivotal, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef __MDVER_H__
#define __MDVER_H__

#include "postgres.h"
#include "utils/relcache.h"
#include "utils/sharedcache.h"

#define INVALID_MD_VERSION 0

typedef struct mdver_entry
{
	Oid key; /* Key of the versioned entry */
	uint64 ddl_version; /* The ddl version of the entry */
	uint64 dml_version; /* The dml version of the entry */
} mdver_entry;

typedef struct mdver_event
{
	Oid key; /* Key of the versioned entry */
#ifdef MD_VERSIONING_INSTRUMENTATION
	int backend_pid; /* The PID of the originating backend */
#endif
	uint64 old_ddl_version; /* The ddl version of the entry before this update */
	uint64 old_dml_version; /* The dml version of the entry before this update */
	uint64 new_ddl_version; /* The ddl version of the entry after this update */
	uint64 new_dml_version; /* The dml version of the entry after this update */
} mdver_event;

typedef struct mdver_local_mdvsn
{
	HTAB *htable;
	bool nuke_happened;
} mdver_local_mdvsn;

/* Pointer to the shared memory global version counter (GVC) */
extern int64 *mdver_global_version_counter;

/* MD Versioning shared memory initialization */
void mdver_shmem_init(void);
Size mdver_shmem_size(void);

/* MD Versioning Global MDVSN operations */
Cache *mdver_get_glob_mdvsn(void);
mdver_entry *mdver_glob_mdvsn_find(Oid oid);
void mdver_glob_mdvsn_nuke(void);

/* MD Versioning Local MDVSN operations */
void mdver_init_session_mdvsn(void);
mdver_local_mdvsn *mdver_create_local_mdvsn(int nesting_level);
void mdver_destroy_local_mdvsn(mdver_local_mdvsn *local_mdvsn, int nesting_level);
mdver_entry *mdver_local_mdvsn_find(mdver_local_mdvsn *local_mdvsn, Oid key);
void mdver_local_mdvsn_add(mdver_local_mdvsn *local_mdvsn, mdver_entry *entry, bool local);
void mdver_local_mdvsn_nuke(mdver_local_mdvsn *local_mdvsn);

/* MD Versioning Dependency Translator operations */
void mdver_dt_catcache_inval(Relation relation, HeapTuple tuple, SysCacheInvalidateAction action);
bool mdver_is_nuke_event(const mdver_event *event);

/* MD Version operations */
uint64 mdver_next_global_version(void);
void mdver_request_version(Oid key, uint64 *ddl_version, uint64 *dml_version);
bool mdver_enabled(void);

/* inval.c */
extern mdver_local_mdvsn *GetCurrentLocalMDVSN(void);

/* Debugging functions */

/* Maximum length for the string representation of a mdver_event */
#define MDVER_EVENT_STR_LEN 256
char *mdver_event_str(mdver_event *ev);

#endif /* __MDVER_H__ */

/* EOF */
