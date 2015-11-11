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
 * cdbfilesystemcredential.h
 *	  manage local credential and file system credential
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
