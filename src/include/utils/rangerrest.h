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
 * rangerrest.h
 *	routines to interact with Ranger REST API
 *
 *-------------------------------------------------------------------------
 */
#ifndef RANGERREST_H
#define RANGERREST_H

#include <json-c/json.h>
#include <curl/curl.h>
#include "postgres.h"
#include "utils/acl.h"
#include "utils/guc.h"
#include "miscadmin.h"
#include "libpq/ip.h"
#include "libpq/libpq-be.h"
#include "tcop/tcopprot.h"

typedef enum
{
  RANGERCHECK_OK = 0,
  RANGERCHECK_NO_PRIV,
  RANGERCHECK_UNKNOWN
} RangerACLResult;

/*
 * Internal buffer for libcurl context
 */
typedef struct curl_context_t
{
  CURL* curl_handle;

  char curl_error_buffer[CURL_ERROR_SIZE];

  int curl_still_running;

  struct
  {
    char* buffer;
    int size;
  } response;

  char* last_http_reponse;
} curl_context_t;

typedef curl_context_t* CURL_HANDLE;

typedef struct RangerPrivilegeArgs
{
  AclObjectKind objkind;
  Oid        object_oid;
  Oid            roleid;
  AclMode          mask;
  AclMaskHow        how;
} RangerPrivilegeArgs;

typedef struct RangerPrivilegeResults
{
  RangerACLResult result;
  Oid relOid;
} RangerPrivilegeResults;

typedef struct RangerRequestJsonArgs {
  char* user;
  AclObjectKind kind;
  char* object;
  List* actions;
  bool isAll;
} RangerRequestJsonArgs;

RangerACLResult parse_ranger_response(char *);
json_object *create_ranger_request_json(List *);
int call_ranger_rest(CURL_HANDLE curl_handle, const char *request);
extern int check_privilege_from_ranger(List *);
void getClientIP(char *remote_host);

#endif
