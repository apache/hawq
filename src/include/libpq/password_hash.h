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
 * password_hash.h
 *
 * Declarations and constants for password hashing.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PASSWORD_HASH_H
#define PASSWORD_HASH_H

#include "libpq/md5.h"
#include "libpq/pg_sha2.h"

#define isHashedPasswd(passwd) (isMD5(passwd) || isSHA256(passwd))

#define MAX_PASSWD_HASH_LEN Max(MD5_PASSWD_LEN, SHA256_PASSWD_LEN)

extern bool hash_password(const char *passwd, char *salt, size_t salt_len,
						  char *buf);

typedef enum
{
	PASSWORD_HASH_NONE = 0,
	PASSWORD_HASH_MD5,
	PASSWORD_HASH_SHA_256
} PasswdHashAlg;

extern PasswdHashAlg password_hash_algorithm;

#endif
