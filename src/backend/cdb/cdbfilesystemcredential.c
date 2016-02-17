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

/*-------------------------------------------------------------------------
 *
 * cdbfilesystemcredential.c
 *	  manage local credential and file system credential
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
#include "miscadmin.h"
#include "storage/fd.h"
#include "tcop/pquery.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/portal.h"

int server_ticket_renew_interval = 43200000; /* millisecond */
char *krb5_ccname = "/tmp/postgres.ccname";
static volatile int64 *server_ticket_last_renew = NULL;

struct FileSystemCredentialKey
{
	char protocol[64];
	char host[1024];
	int port;
};

struct FileSystemCredential
{
	struct FileSystemCredentialKey key;
	char *credential;
	void *fs;
};

void
FSCredShmemInit(void)
{
    bool found;

    server_ticket_last_renew = (int64 *)ShmemInitStruct(
        "FileSystem credentials server ticket last renew time", sizeof(int64),
        &found);

    if (!server_ticket_last_renew)
        elog(FATAL, "could not initialize server kerberos ticket share memory");

    if (IsUnderPostmaster)
        *server_ticket_last_renew = 0;
}

int
FSCredShmemSize(void)
{
	return sizeof(int64);
}

bool
login (void)
{
	struct timeval tp;

	if (0 != gettimeofday(&tp, NULL))
		elog(ERROR, "cannot get current time");

	int64 now = tp.tv_sec;
	now = (now * 1000000 + tp.tv_usec) / 1000;  /* millisecond */

	if (now > *server_ticket_last_renew + server_ticket_renew_interval)
	{
		char cmd[1024];

		elog(LOG, "applying the kerberos ticket for %s", pg_krb_srvnam);

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

		*server_ticket_last_renew = now;
	}

	return true;
}

static bool
get_filesystem_credential_internal(struct FileSystemCredential *entry)
{
	Assert(NULL != entry);
	struct FileSystemCredentialKey *key =
			(struct FileSystemCredentialKey*) entry;
	Insist(strcasecmp(key->protocol, "hdfs") == 0);

	char uri[1024];
	snprintf(uri, sizeof(uri), "%s://%s:%d", key->protocol, key->host,
			key->port);
	entry->credential = HdfsGetDelegationToken(uri, &entry->fs);

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

static void
get_current_credential_cache_and_memcxt(HTAB ** currentFilesystemCredentials,
        MemoryContext *currentFilesystemCredentialsMemoryContext)
{
    if (!ActivePortal)
        elog(ERROR, "cannot find ActivePortal");

    Insist(NULL != currentFilesystemCredentials);
    Insist(NULL != currentFilesystemCredentialsMemoryContext);

    *currentFilesystemCredentials = ActivePortal->filesystem_credentials;
    *currentFilesystemCredentialsMemoryContext = ActivePortal->filesystem_credentials_memory;
}

void
create_filesystem_credentials(Portal portal)
{
	create_filesystem_credentials_internal(&portal->filesystem_credentials,
			&portal->filesystem_credentials_memory);
}

void
add_filesystem_credential_to_cache(const char * uri, char *token)
{
	char *protocol;
	char *host;

	bool found = false;
	struct FileSystemCredential *entry;
	struct FileSystemCredentialKey key;
    HTAB * currentFilesystemCredentials;
    MemoryContext currentFilesystemCredentialsMemoryContext;

	Assert(NULL != token);

	get_current_credential_cache_and_memcxt(&currentFilesystemCredentials,
	        &currentFilesystemCredentialsMemoryContext);

	Insist(NULL != currentFilesystemCredentials);
	Insist(NULL != currentFilesystemCredentialsMemoryContext);

	MemoryContext old = MemoryContextSwitchTo(
			currentFilesystemCredentialsMemoryContext);

	memset(&key, 0, sizeof(key));

	if (HdfsParsePath(uri, &protocol, &host, &key.port, NULL)
			|| NULL == protocol || NULL == host)
		elog(ERROR, "fail to parse uri: %s", uri);

	StrNCpy(key.protocol, protocol, sizeof(key.protocol));
	StrNCpy(key.host, host, sizeof(key.host));

	entry = (struct FileSystemCredential *) hash_search(
			currentFilesystemCredentials, &key, HASH_ENTER, &found);

	if (!found) {
		entry->credential = pstrdup(token);
	}

	MemoryContextSwitchTo(old);
}

void
add_filesystem_credential(const char * uri)
{
	char *protocol;
	char *host;

    bool found = false;
	struct FileSystemCredentialKey key;

	HTAB * currentFilesystemCredentials;
    MemoryContext currentFilesystemCredentialsMemoryContext;

    get_current_credential_cache_and_memcxt(&currentFilesystemCredentials,
            &currentFilesystemCredentialsMemoryContext);

    Insist(NULL != currentFilesystemCredentials);
    Insist(NULL != currentFilesystemCredentialsMemoryContext);

	MemoryContext old = MemoryContextSwitchTo(currentFilesystemCredentialsMemoryContext);

	memset(&key, 0, sizeof(key));

	if (HdfsParsePath(uri, &protocol, &host, &key.port, NULL)
			|| NULL == protocol || NULL == host)
		elog(ERROR, "fail to parse uri: %s", uri);

	StrNCpy(key.protocol, protocol, sizeof(key.protocol));
	StrNCpy(key.host, host, sizeof(key.host));

	hash_search(currentFilesystemCredentials, &key, HASH_FIND, &found);

	if (!found)
	{
		int retry = 5;
		bool success = false;

		/*
		 * Canceling the query at this point will result in an exception.
		 * We have to make sure we clean the hash table from the new entry,
		 * because all credentials are being removed from HDFS when
		 * we end the transaction, and this entry doesn't have a valid
		 * credential yet.
		 *
		 * In some case fatal error will happen and NO exception will be thrown.
		 * Transaction will abort immediate and we do not know the status of delegation
		 * token. So initialize token in the hash entry to NULL and check it when canceling
		 * token.
		 */
		PG_TRY();
		{
			struct FileSystemCredential *entry =
					(struct FileSystemCredential *) hash_search(
							currentFilesystemCredentials, &key, HASH_ENTER, NULL);

			Assert(NULL != entry);

			entry->credential = NULL;
			entry->fs = NULL;

			while (true)
			{
				success = get_filesystem_credential_internal(entry);

				if (success)
				{
					break;
				}

				if (--retry <= 0)
				{
					hash_search(currentFilesystemCredentials, &key, HASH_REMOVE, NULL);
					break;
				}

				elog(DEBUG5, "failed to getting credentials for %s://%s:%d, retrying...",
					 key.protocol, key.host, key.port);

				CHECK_FOR_INTERRUPTS();
				pg_usleep(cdb_randint(0, 5) * 1000000L);
			}
		}
		PG_CATCH();
		{
			if (!success)
			{
				hash_search(currentFilesystemCredentials, &key, HASH_REMOVE, NULL);
			}
			PG_RE_THROW();
		}
		PG_END_TRY();

		if (retry <= 0)
		{
			elog(ERROR, "fail to get filesystem credential for uri %s", uri);
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

	if (NULL != entry->credential)
		HdfsCancelDelegationToken(entry->fs, entry->credential);
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
			/**
			 * this case happened for the command "set enable_secure_filesystem to true;"
			 * enable_secure_filesystem is true but credentials did not allocated"
			 */
			if (portal->filesystem_credentials == NULL &&
				portal->filesystem_credentials_memory == NULL)
				return;

			Assert(portal->filesystem_credentials != NULL &&
				portal->filesystem_credentials_memory != NULL);

			/*
			 * Master is responsible for cancel all file system credentials
			 */
			cancel_filesystem_credentials(portal->filesystem_credentials,
					portal->filesystem_credentials_memory);
		}
	}

	if (portal->filesystem_credentials)
	{
		hash_destroy(portal->filesystem_credentials);
		portal->filesystem_credentials = NULL;
	}
	if (portal->filesystem_credentials_memory)
	{
	    MemoryContextDelete(portal->filesystem_credentials_memory);
	    portal->filesystem_credentials_memory = NULL;
	}
}

static void
serialize_filesystem_credential(StringInfo buffer,
		struct FileSystemCredential *entry)
{
	Assert(NULL != entry && NULL != buffer);

	pq_sendstring(buffer, entry->key.host);
	pq_sendstring(buffer, entry->key.protocol);
	pq_sendint(buffer, entry->key.port, sizeof(int));
	pq_sendstring(buffer, entry->credential);
}

char *
serialize_filesystem_credentials(int *size)
{
	HASH_SEQ_STATUS status;
	struct FileSystemCredential *entry;
	StringInfoData buffer;

    HTAB * currentFilesystemCredentials;
    MemoryContext currentFilesystemCredentialsMemoryContext;

    get_current_credential_cache_and_memcxt(&currentFilesystemCredentials,
            &currentFilesystemCredentialsMemoryContext);

    Insist(NULL != currentFilesystemCredentials);
    Insist(NULL != currentFilesystemCredentialsMemoryContext);


	initStringInfo(&buffer);
	hash_seq_init(&status, currentFilesystemCredentials);

	while (NULL != (entry = hash_seq_search(&status)))
		serialize_filesystem_credential(&buffer, entry);

	*size = buffer.len;
	return buffer.data;
}

void
set_filesystem_credentials(Portal portal, HTAB * currentFilesystemCredentials,
        MemoryContext currentFilesystemCredentialsMemoryContext)
{
	portal->filesystem_credentials = currentFilesystemCredentials;
	portal->filesystem_credentials_memory = currentFilesystemCredentialsMemoryContext;
}

void
deserialize_filesystem_credentials(char *binary, int len, HTAB **currentFilesystemCredentials,
        MemoryContext *currentFilesystemCredentialsMemoryContext)
{
	bool found = false;

	struct FileSystemCredentialKey key;
	struct FileSystemCredential *entry;

	Insist(NULL != currentFilesystemCredentials);
	Insist(NULL != currentFilesystemCredentialsMemoryContext);

	Insist(NULL != binary && Gp_role == GP_ROLE_EXECUTE && len > 0);

	create_filesystem_credentials_internal(currentFilesystemCredentials,
	        currentFilesystemCredentialsMemoryContext);

	MemoryContext old = MemoryContextSwitchTo(*currentFilesystemCredentialsMemoryContext);

	StringInfoData buffer;
	initStringInfoOfString(&buffer, binary, len);

	while (buffer.cursor < buffer.len)
	{
		memset(&key, 0, sizeof(key));

		StrNCpy(key.host, pq_getmsgstring(&buffer), sizeof(key.host));
		StrNCpy(key.protocol, pq_getmsgstring(&buffer), sizeof(key.protocol));

		key.port = pq_getmsgint(&buffer, sizeof(int));

		entry = (struct FileSystemCredential *) hash_search(
				*currentFilesystemCredentials, &key, HASH_ENTER, &found);

		if (found)
			elog(ERROR, "dispatched duplicate file system credential");

		entry->credential = pstrdup(pq_getmsgstring(&buffer));
	}

	if (buffer.cursor != buffer.len)
		elog(ERROR, "dispatched invalid file system credential");

	MemoryContextSwitchTo(old);
}

char*
find_filesystem_credential_with_uri(const char *uri)
{
	char   *protocol;
	char   *host;
	int 	port;

	if (HdfsParsePath(uri, &protocol, &host, &port, NULL)
			|| NULL == protocol || NULL == host)
		elog(ERROR, "fail to parse uri: %s", uri);

	return find_filesystem_credential(protocol, host, port);
}

char*
find_filesystem_credential(const char *protocol, const char *host,
		int port)
{
	bool found = false;
	struct FileSystemCredentialKey key;
	struct FileSystemCredential *entry;

    HTAB * currentFilesystemCredentials;
    MemoryContext currentFilesystemCredentialsMemoryContext;

    get_current_credential_cache_and_memcxt(&currentFilesystemCredentials,
            &currentFilesystemCredentialsMemoryContext);

	if (NULL == currentFilesystemCredentials)
		return NULL;

	MemoryContext old = MemoryContextSwitchTo(currentFilesystemCredentialsMemoryContext);

	memset(&key, 0, sizeof(key));
	StrNCpy(key.protocol, protocol, sizeof(key.protocol));
	StrNCpy(key.host, host, sizeof(key.host));
	key.port = port;

	entry = (struct FileSystemCredential *) hash_search(currentFilesystemCredentials,
			&key, HASH_FIND, &found);

	MemoryContextSwitchTo(old);

	if (!found)
		return NULL;

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
	HdfsRenewDelegationToken(entry->fs, entry->credential);
}

void
renew_filesystem_credentials()
{
	HASH_SEQ_STATUS status;
	struct FileSystemCredential *entry;

    HTAB * currentFilesystemCredentials;
    MemoryContext currentFilesystemCredentialsMemoryContext;

    get_current_credential_cache_and_memcxt(&currentFilesystemCredentials,
            &currentFilesystemCredentialsMemoryContext);

	if (NULL == currentFilesystemCredentials)
		return;

	MemoryContext old = MemoryContextSwitchTo(currentFilesystemCredentialsMemoryContext);

	hash_seq_init(&status, currentFilesystemCredentials);

	while (NULL != (entry = hash_seq_search(&status)))
		renew_filesystem_credential(entry);

	MemoryContextSwitchTo(old);
}
