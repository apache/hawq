///////////////////////////////////////////////////////////////////////////////
// Copyright 2016, Oushu Inc.
// All rights reserved.
//
// Author:
///////////////////////////////////////////////////////////////////////////////
#ifndef SRC_INCLUDE_UTILS_CLOUDREST_H_
#define SRC_INCLUDE_UTILS_CLOUDREST_H_

#include <json-c/json.h>
#include <curl/curl.h>

#include "postgres.h"
#include "utils/guc.h"
#include "miscadmin.h"
#include "libpq/auth.h"
#include "libpq/libpq-be.h"
#include "tcop/tcopprot.h"

#define HOST_BUFFER_SIZE 1025
#define CURL_RES_BUFFER_SIZE 1024

typedef enum
{
	AUTHENTICATION_CHECK = 0,
	USER_SYNC,
	USER_EXIST
} CouldAuthAction;

typedef enum
{
	CLOUDCHECK_OK = 0,
	CLOUDCHECK_NO_PRIV,
	CLOUDCHECK_UNKNOWN
} CouldAuthResult;

typedef enum
{
	CLOUDSYNC_OK = 0,
	CLOUDSYNC_USEREXIST,
	CLOUDSYNC_FAIL,
	CLOUDSYNC_UNKNOWN
} CouldSyncResult;

typedef enum
{
	CLOUDUSER_EXIST = 0,
	CLOUDUSER_NOTEXIST,
} CouldExistResult;

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
    int response_size;
    int buffer_size;
  } response;

  char* last_http_reponse;

  bool hasInited;
} curl_context_t;

typedef curl_context_t* CURL_HANDLE;

extern struct curl_context_t curl_context_cloud;

extern void init_cloud_curl();
extern int check_authentication_from_cloud(char *username, char *password,
		bool *createuser, CouldAuthAction authAction, char * action,
		char **errmsg);

extern char	   *pg_cloud_server;
extern char	   *pg_cloud_token;
extern bool		pg_cloud_createrole;
extern bool		pg_cloud_auth;

#endif /* SRC_INCLUDE_UTILS_CLOUDREST_H_ */
