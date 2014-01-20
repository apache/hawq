/*-------------------------------------------------------------------------
 *
 * cdbfilesystemcredential.c
 *	  manage local credential and file system credential
 *
 * Copyright (c) 2003-2013, Pivotal inc
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/time.h>

#include "cdb/cdbfilesystemcredential.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbvars.h"
#include "lib/stringinfo.h"
#include "libpq/auth.h"
#include "libpq/pqformat.h"
#include "storage/fd.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/portal.h"

int server_ticket_renew_interval = 43200000; //millisecond
char *krb5_ccname = "/tmp/postgres.ccname";
static int64 server_ticket_last_renew = 0;
MemoryContext CurrentFilesystemCredentialsMemoryContext = NULL;
HTAB * CurrentFilesystemCredentials = NULL;

struct FileSystemCredentialKey
{
	char protocol[64];
	char host[1024];
	int port;
};

struct FileSystemCredential
{
	struct FileSystemCredentialKey key;
	void *credential;
	void *fs;
	int credentialSize;
};

bool
login (void)
{
	struct timeval tp;

	if (0 != gettimeofday(&tp, NULL))
		elog(ERROR, "cannot get current time");

	int64 now = tp.tv_sec;
	now = (now * 1000000 + tp.tv_usec) / 1000;  //millisecond

	if (now > server_ticket_last_renew + server_ticket_renew_interval)
	{
		char cmd[1024];

		if (!pg_krb_server_keyfile || strlen(pg_krb_server_keyfile) == 0)
		{
			elog(WARNING, "pg_krb_server_keyfile is not set");
			return false;
		}

		if (!krb5_ccname || strlen(krb5_ccname) == 0)
		{
			elog(WARNING, "krb5_ccname is not set");
			return false;
		}

		if (!pg_krb_srvnam || strlen(pg_krb_srvnam) == 0)
		{
			elog(WARNING, "pg_krb_srvnam is not set");
			return false;
		}

		snprintf(cmd, sizeof(cmd), "kinit -k -t %s -c %s %s",
				pg_krb_server_keyfile, krb5_ccname, pg_krb_srvnam);

		if (system(cmd))
		{
			elog(WARNING, "failed to login to Kerberos, command line: %s", cmd);
			return false;
		}

		server_ticket_last_renew = now;
	}

	return true;
}

static bool
get_filesystem_crential(struct FileSystemCredential *entry)
{
	Assert(NULL != entry);
	struct FileSystemCredentialKey *key =
			(struct FileSystemCredentialKey*) entry;
	Insist(strcasecmp(key->protocol, "hdfs") == 0);

	char uri[1024];
	snprintf(uri, sizeof(uri), "%s://%s:%d", key->protocol, key->host,
			key->port);
	entry->credential = HdfsGetDelegationToken(uri, &entry->credentialSize, &entry->fs);

	return entry->credential ? true : false;

}

static HTAB *
create_filesystem_credentials_cache(MemoryContext mcxt)
{
	HASHCTL hash_ctl;
	MemSet(&hash_ctl, 0, sizeof(hash_ctl));

	hash_ctl.keysize = sizeof(struct FileSystemCredentialKey);
	hash_ctl.entrysize = sizeof(struct FileSystemCredential);
	hash_ctl.hcxt = mcxt;
	hash_ctl.hash = tag_hash;

	return hash_create("hdfs filesystem credential hash table",
			10, &hash_ctl,
			HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

static void
create_filesystem_credentials_internal(HTAB **credentials, MemoryContext *mcxt)
{
	if (!enable_secure_filesystem)
		return;

	Assert(NULL != credentials && NULL != mcxt);

	*credentials = NULL;
	*mcxt = AllocSetContextCreate(
			TopMemoryContext, "FileSystem Credentials Context",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

	*credentials = create_filesystem_credentials_cache(*mcxt);
}

void
create_filesystem_credentials(Portal portal)
{
	create_filesystem_credentials_internal(&portal->filesystem_credentials,
			&portal->filesystem_credentials_memory);
}

void
add_filesystem_credential(const char * uri)
{
	char *protocol;
	char *host;

	bool found = false;
	struct FileSystemCredential *entry;
	struct FileSystemCredentialKey key;

	Assert(NULL != CurrentFilesystemCredentials);
	Assert(NULL != CurrentFilesystemCredentialsMemoryContext);

	MemoryContext old = MemoryContextSwitchTo(CurrentFilesystemCredentialsMemoryContext);

	memset(&key, 0, sizeof(key));

	if (HdfsParsePath(uri, &protocol, &host, &key.port, NULL)
			|| NULL == protocol || NULL == host)
		elog(ERROR, "fail to parse uri: %s", uri);

	strncpy(key.protocol, protocol, sizeof(key.protocol));
	strncpy(key.host, host, sizeof(key.host));

	entry = (struct FileSystemCredential *) hash_search(CurrentFilesystemCredentials,
			&key, HASH_ENTER, &found);

	if (!found)
	{
		Assert(NULL != entry);

		int retry = 5;

		while (true)
		{
			if (get_filesystem_crential(entry))
				break;

			if (--retry <= 0)
			{
				hash_search(CurrentFilesystemCredentials, &key, HASH_REMOVE, &found);
				elog(ERROR, "fail to get filesystem credential for uri %s", uri);
				break;
			}

			pg_usleep(cdb_randint(0, 5) * 1000000L);
		}
	}

	MemoryContextSwitchTo(old);
}

static void
cancel_filesystem_credential(struct FileSystemCredential *entry)
{
	Assert(NULL != entry);

	struct FileSystemCredentialKey *key =
			(struct FileSystemCredentialKey*) entry;
	Insist(strcasecmp(key->protocol, "hdfs") == 0);

	HdfsCancelDelegationToken(entry->fs, entry->credential,
			entry->credentialSize);
}

static void
cancel_filesystem_credentials(HTAB *credentials, MemoryContext mcxt)
{
	HASH_SEQ_STATUS status;
	struct FileSystemCredential *entry;

	Assert(NULL != credentials);
	Assert(NULL != mcxt);

	MemoryContext old = MemoryContextSwitchTo(mcxt);

	hash_seq_init(&status, credentials);

	while (NULL != (entry = hash_seq_search(&status)))
		cancel_filesystem_credential(entry);

	MemoryContextSwitchTo(old);
}

void
cleanup_filesystem_credentials(Portal portal)
{
	if (enable_secure_filesystem)
	{
		if (Gp_role == GP_ROLE_EXECUTE)
		{
			/*
			 * we assume that there only one unnamed portal on segment.
			 */
			cleanup_lru_opened_files();
			cleanup_filesystem_handler();
		}
		else
		{
			/*
			 * Master is responsible for cancel all file system credentials
			 */
			cancel_filesystem_credentials(portal->filesystem_credentials,
					portal->filesystem_credentials_memory);
		}
	}

	if (portal->filesystem_credentials)
		hash_destroy(portal->filesystem_credentials);
	if (portal->filesystem_credentials_memory)
		MemoryContextResetAndDeleteChildren(portal->filesystem_credentials_memory);

	CurrentFilesystemCredentials = NULL;
	CurrentFilesystemCredentialsMemoryContext = NULL;
}

static void
serialize_filesystem_credential(StringInfo buffer,
		struct FileSystemCredential *entry)
{
	Assert(NULL != entry && NULL != buffer);

	pq_sendstring(buffer, entry->key.host);
	pq_sendstring(buffer, entry->key.protocol);
	pq_sendint(buffer, entry->key.port, sizeof(int));
	pq_sendint(buffer, entry->credentialSize, sizeof(int));
	pq_sendbytes(buffer, entry->credential, entry->credentialSize);
}

char *
serialize_filesystem_credentials(int *size)
{
	HASH_SEQ_STATUS status;
	struct FileSystemCredential *entry;
	StringInfoData buffer;

	Assert(NULL != CurrentFilesystemCredentials);
	Assert(NULL != CurrentFilesystemCredentialsMemoryContext);

	initStringInfo(&buffer);
	hash_seq_init(&status, CurrentFilesystemCredentials);

	while (NULL != (entry = hash_seq_search(&status)))
		serialize_filesystem_credential(&buffer, entry);

	*size = buffer.len;
	return buffer.data;
}

void
set_filesystem_credentials(Portal portal)
{
	portal->filesystem_credentials = CurrentFilesystemCredentials;
	portal->filesystem_credentials_memory = CurrentFilesystemCredentialsMemoryContext;
}

void
deserialize_filesystem_credentials(char *binary, int len)
{
	bool found = false;

	struct FileSystemCredentialKey key;
	struct FileSystemCredential *entry;

	Assert(NULL == CurrentFilesystemCredentials);
	Assert(NULL == CurrentFilesystemCredentialsMemoryContext);

	Insist(NULL != binary && Gp_role == GP_ROLE_EXECUTE && len > 0);

	create_filesystem_credentials_internal(&CurrentFilesystemCredentials, &CurrentFilesystemCredentialsMemoryContext);

	MemoryContext old = MemoryContextSwitchTo(CurrentFilesystemCredentialsMemoryContext);

	StringInfoData buffer;
	initStringInfoOfString(&buffer, binary, len);

	while (buffer.cursor < buffer.len)
	{
		memset(&key, 0, sizeof(key));

		strncpy(key.host, pq_getmsgstring(&buffer), sizeof(key.host));
		strncpy(key.protocol, pq_getmsgstring(&buffer), sizeof(key.protocol));

		key.port = pq_getmsgint(&buffer, sizeof(int));

		entry = (struct FileSystemCredential *) hash_search(
				CurrentFilesystemCredentials, &key, HASH_ENTER, &found);

		if (found)
			elog(ERROR, "dispatched duplicate file system credential");

		entry->credentialSize = pq_getmsgint(&buffer, sizeof(int));
		entry->credential = palloc(entry->credentialSize);
		memcpy(entry->credential,
				pq_getmsgbytes(&buffer, entry->credentialSize),
				entry->credentialSize);
	}

	if (buffer.cursor != buffer.len)
		elog(ERROR, "dispatched invalid file system credential");

	MemoryContextSwitchTo(old);
}

void*
find_filesystem_credential(const char *protocol, const char *host,
		int port, int *credentialSize)
{
	bool found = false;
	struct FileSystemCredentialKey key;
	struct FileSystemCredential *entry;

	if (NULL == CurrentFilesystemCredentials)
		return NULL;

	MemoryContext old = MemoryContextSwitchTo(CurrentFilesystemCredentialsMemoryContext);

	memset(&key, 0, sizeof(key));
	strncpy(key.protocol, protocol, sizeof(key.protocol));
	strncpy(key.host, host, sizeof(key.host));
	key.port = port;

	entry = (struct FileSystemCredential *) hash_search(CurrentFilesystemCredentials,
			&key, HASH_FIND, &found);

	MemoryContextSwitchTo(old);

	if (!found)
		return NULL;

	*credentialSize = entry->credentialSize;
	return entry->credential;
}

static void
renew_filesystem_credential(struct FileSystemCredential *entry)
{
	Assert(NULL != entry);
	struct FileSystemCredentialKey *key =
			(struct FileSystemCredentialKey*) entry;
	Insist(strcasecmp(key->protocol, "hdfs") == 0);

	Assert(NULL != entry->credential);
	HdfsRenewDelegationToken(entry->fs, entry->credential, entry->credentialSize);
}

void
renew_filesystem_credentials()
{
	HASH_SEQ_STATUS status;
	struct FileSystemCredential *entry;

	if (NULL == CurrentFilesystemCredentials)
		return;

	MemoryContext old = MemoryContextSwitchTo(CurrentFilesystemCredentialsMemoryContext);

	hash_seq_init(&status, CurrentFilesystemCredentials);

	while (NULL != (entry = hash_seq_search(&status)))
		renew_filesystem_credential(entry);

	MemoryContextSwitchTo(old);
}
