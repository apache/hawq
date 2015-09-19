/*-------------------------------------------------------------------------
 *
 * cdbfilesystemcredential.h
 *	  manage local credential and file system credential
 *
 * Copyright (c) 2003-2013, Pivotal inc
 *
 *-------------------------------------------------------------------------
 */

#ifndef _CDB_CDBFILESYSTEMCREDENTIAL_H_
#define _CDB_CDBFILESYSTEMCREDENTIAL_H_

#include "postgres.h"
#include "utils/hsearch.h"
#include "tcop/dest.h"

extern int server_ticket_renew_interval;
extern char *krb5_ccname;

bool login(void);
void FSCredShmemInit(void);
int FSCredShmemSize(void);

void create_filesystem_credentials(Portal portal);
void cleanup_filesystem_credentials(Portal portal);
void set_filesystem_credentials(Portal portal, HTAB * currentFilesystemCredentials,
        MemoryContext currentFilesystemCredentialsMemoryContext);
void add_filesystem_credential(const char * uri);
void add_filesystem_credential_to_cache(const char * uri, char *token);
char *serialize_filesystem_credentials(int *size);
void deserialize_filesystem_credentials(char *binary, int len, HTAB **currentFilesystemCredentials,
        MemoryContext *currentFilesystemCredentialsMemoryContext);
char *find_filesystem_credential_with_uri(const char *uri);
char *find_filesystem_credential(const char *protocol, const char *host, int port);
void renew_filesystem_credentials(void);

#endif /* _CDB_CDBFILESYSTEMCREDENTIAL_H_ */
