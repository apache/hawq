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

#include "utils/cloudrest.h"

#include <string.h>

#include "utils/elog.h"

char	   *pg_cloud_server = NULL;
char	   *pg_cloud_token = NULL;
bool		pg_cloud_createrole;
bool		pg_cloud_auth = false;

static void finalize_cloud_curl();

static json_object *create_cloud_authentication_request_json(char *username, char *password)
{
	json_object *jrequest = json_object_new_object();

	json_object *jusername = json_object_new_string(username);
	json_object_object_add(jrequest, "username", jusername);
	json_object *jpassword = json_object_new_string(password);
	json_object_object_add(jrequest, "password", jpassword);
	json_object *jclustername = json_object_new_string(pg_cloud_clustername);
	json_object_object_add(jrequest, "clustername", jclustername);

	return jrequest;
}

static json_object *create_cloud_usersync_request_json(char *username, char *password, bool *createuser, char *action)
{
	json_object *jrequest = json_object_new_object();

	json_object *jaction = json_object_new_string(action);
	json_object_object_add(jrequest, "action", jaction);
	json_object *jtoken = json_object_new_string(pg_cloud_token);
	json_object_object_add(jrequest, "token", jtoken);
	json_object *jusername = json_object_new_string(username);
	json_object_object_add(jrequest, "username", jusername);
	if (password)
	{
		json_object *jpassword = json_object_new_string(password);
		json_object_object_add(jrequest, "password", jpassword);
	}
	json_object *jclustername = json_object_new_string(pg_cloud_clustername);
	json_object_object_add(jrequest, "clustername", jclustername);
	if (createuser)
	{
		json_object *jcreateuser = json_object_new_boolean(*createuser);
		json_object_object_add(jrequest, "cancreateuser", jcreateuser);
	}

	return jrequest;
}

static json_object *create_cloud_userexist_request_json(char *username)
{
	json_object *jrequest = json_object_new_object();

	json_object *jusername = json_object_new_string(username);
	json_object_object_add(jrequest, "username", jusername);
	json_object *jclustername = json_object_new_string(pg_cloud_clustername);
	json_object_object_add(jrequest, "clustername", jclustername);

	return jrequest;
}

static size_t write_callback(char *contents, size_t size, size_t nitems,
	void *userp)
{
	size_t realsize = size * nitems;
	CURL_HANDLE curl = (CURL_HANDLE) userp;
	Assert(curl != NULL);

	elog(DEBUG3, "cloud restful response size is %d. response buffer size is %d.", curl->response.response_size, curl->response.buffer_size);
	int original_size = curl->response.buffer_size;
	while(curl->response.response_size + realsize >= curl->response.buffer_size)
	{
		/* double the buffer size if the buffer is not enough.*/
		curl->response.buffer_size = curl->response.buffer_size * 2;
	}
	if(original_size < curl->response.buffer_size)
	{
		/* repalloc is not same as realloc, repalloc's first parameter cannot be NULL */
		curl->response.buffer = repalloc(curl->response.buffer, curl->response.buffer_size);
	}
	elog(DEBUG3, "cloud restful response size is %d. response buffer size is %d.", curl->response.response_size, curl->response.buffer_size);
	if (curl->response.buffer == NULL)
	{
		/* allocate memory failed. probably out of memory */
		elog(WARNING, "cannot allocate memory for cloud response");
		return 0;
	}
	memcpy(curl->response.buffer + curl->response.response_size, contents, realsize);
	curl->response.response_size += realsize;
	curl->response.buffer[curl->response.response_size] = '\0';
	elog(DEBUG3, "read from cloud restful response: %s", curl->response.buffer);
	return realsize;
}

/**
 * @return	0 curl success; -1 curl failed
 */
static int call_cloud_rest(CURL_HANDLE curl_handle, const char* request, char *action)
{
	int ret = -1;
	CURLcode res;
	Assert(request != NULL);

	/*
	 * Re-initializes all options previously set on a specified CURL handle
	 * to the default values. This puts back the handle to the same state as
	 * it was in when it was just created with curl_easy_init.It does not
	 * change the following information kept in the handle: live connections,
	 * the Session ID cache, the DNS cache, the cookies and shares.
	 */
	curl_easy_reset(curl_handle->curl_handle);
	/* timeout: hard-coded temporarily and maybe should be a guc in future */
	curl_easy_setopt(curl_handle->curl_handle, CURLOPT_TIMEOUT, 30L);

	/* specify URL to get */
	StringInfoData tname;
	initStringInfo(&tname);
	appendStringInfo(&tname, "%s", pg_cloud_server);
	appendStringInfo(&tname, "/");
	appendStringInfo(&tname, "%s", action);
	curl_easy_setopt(curl_handle->curl_handle, CURLOPT_URL, tname.data);
	elog(DEBUG3, "in call_cloud_rest: %s", tname.data);
	pfree(tname.data);

	struct curl_slist *headers = NULL;
	headers = curl_slist_append(headers, "Content-Type:application/json");
	if (pg_cloud_token) {
		char buf[512];
		memset(buf, 0, sizeof(buf));
		sprintf(buf, "token:%s", pg_cloud_token);
		elog(DEBUG3, "in call_cloud_rest: %s", buf);
		headers = curl_slist_append(headers, buf);
	}
	headers = curl_slist_append(headers, "connection:close");
	curl_easy_setopt(curl_handle->curl_handle, CURLOPT_HTTPHEADER, headers);

	curl_easy_setopt(curl_handle->curl_handle, CURLOPT_POSTFIELDS, request);
	/* send all data to this function  */
	curl_easy_setopt(curl_handle->curl_handle, CURLOPT_WRITEFUNCTION,
			write_callback);
	curl_easy_setopt(curl_handle->curl_handle, CURLOPT_WRITEDATA,
			(void * )curl_handle);

	res = curl_easy_perform(curl_handle->curl_handle);
	/* check for errors */
	if (res != CURLE_OK)
	{
		elog(ERROR, "cloud server from %s/%s is unavailable : %s.\n",
		pg_cloud_server, action, curl_easy_strerror(res));
	}
	else
	{
		ret = 0;
		elog(DEBUG3, "retrieved %d bytes data from cloud restful response.",
		curl_handle->response.response_size);
	}

	return ret;
}

static int parse_cloud_auth_response(char* buffer, int *result, char **errormsg)
{
	if (buffer == NULL || strlen(buffer) == 0)
		return -1;

	elog(DEBUG3, "parse cloud restful response content : %s", buffer);

	struct json_object *response = json_tokener_parse(buffer);
	if (response == NULL)
	{
		elog(WARNING, "failed to parse json tokener.");
		return -1;
	}

	struct json_object *jtoken = NULL;
	if (!json_object_object_get_ex(response, "token", &jtoken))
	{
		elog(WARNING, "failed to get json \"token\" field.");
		return -1;
	}
	char *token = json_object_get_string(jtoken);
	size_t len = strlen(token);
	MemoryContext old;
	old = MemoryContextSwitchTo(TopMemoryContext);
	pg_cloud_token = (char *)palloc0(len + 1);
	memcpy(pg_cloud_token, token, len);
	MemoryContextSwitchTo(old);
	elog(DEBUG3, "in parse_cloud_auth_response, token(%p): %s", pg_cloud_token, pg_cloud_token);

	struct json_object *jcreaterole = NULL;
	if (!json_object_object_get_ex(response, "cancreateuser", &jcreaterole))
	{
		elog(WARNING, "failed to get json \"cancreateuser\" field.");
		return -1;
	}
	pg_cloud_createrole = json_object_get_boolean(jcreaterole);
	elog(DEBUG3, "pg_cloud_createrole=%d", pg_cloud_createrole);

	struct json_object *jresult = NULL;
	if (!json_object_object_get_ex(response, "result", &jresult))
	{
		elog(WARNING, "failed to get json \"result\" field.");
		return -1;
	}

	json_bool ok = json_object_get_int(jresult);
	if (ok == 1)
	{
		*result = CLOUDCHECK_OK;
	}
	else
	{
		struct json_object *jerror = NULL;
		if (!json_object_object_get_ex(response, "error", &jerror))
		{
			elog(WARNING, "failed to get json \"token\" field.");
			return -1;
		}
		char *err = json_object_get_string(jerror);
		size_t len = strlen(err);
		*errormsg = (char *)palloc0(len + 1);
		memcpy(*errormsg, err, len);
		(*errormsg)[len] = '\0';
		*result = CLOUDCHECK_NO_PRIV;
		elog(INFO, "errmsg=%s, size=%d", *errormsg, strlen(*errormsg));
	}

	return 0;
}

static int parse_cloud_sync_response(char* buffer, int *result, char **errormsg)
{
	if (buffer == NULL || strlen(buffer) == 0)
		return -1;

	elog(DEBUG3, "parse cloud restful response content : %s", buffer);

	struct json_object *response = json_tokener_parse(buffer);
	if (response == NULL)
	{
		elog(WARNING, "failed to parse json tokener.");
		return -1;
	}

	struct json_object *jresult = NULL;
	if (!json_object_object_get_ex(response, "result", &jresult))
	{
		elog(WARNING, "failed to get json \"result\" field.");
		return -1;
	}

	int ok = json_object_get_boolean(jresult);
	elog(DEBUG3, "in parse_cloud_sync_response, ret=%d", ok);
	if (ok == 0)
	{
		*result = CLOUDSYNC_OK;
	}
	else
	{
		struct json_object *jerror = NULL;
		if (!json_object_object_get_ex(response, "error", &jerror))
		{
			elog(WARNING, "failed to get json \"token\" field.");
			return -1;
		}
		char *err = json_object_get_string(jerror);
		size_t len = strlen(err);
		*errormsg = (char *)palloc0(len + 1);
		memcpy(*errormsg, err, len);
		(*errormsg)[len] = '\0';
		if (ok == 1)
			*result = CLOUDSYNC_USEREXIST;
		else
			*result = CLOUDSYNC_FAIL;
	}

	return 0;
}

static int parse_cloud_exist_response(char* buffer, int *result, char **errormsg)
{
	if (buffer == NULL || strlen(buffer) == 0)
		return -1;

	elog(DEBUG3, "parse cloud restful response content : %s", buffer);

	struct json_object *response = json_tokener_parse(buffer);
	if (response == NULL)
	{
		elog(WARNING, "failed to parse json tokener.");
		return -1;
	}

	struct json_object *jresult = NULL;
	if (!json_object_object_get_ex(response, "result", &jresult))
	{
		elog(WARNING, "failed to get json \"result\" field.");
		return -1;
	}

	json_bool ok = json_object_get_boolean(jresult);
	if (ok == 1)
	{
		*result = CLOUDUSER_EXIST;
	}
	else
	{
		*result = CLOUDUSER_NOTEXIST;
	}

	return 0;
}

void init_cloud_curl() {
	memset(&curl_context_cloud, 0, sizeof(curl_context_t));
	curl_global_init(CURL_GLOBAL_ALL);
	/* init the curl session */
	curl_context_cloud.curl_handle = curl_easy_init();
	if (curl_context_cloud.curl_handle == NULL) {
		/* cleanup curl stuff */
		/* no need to cleanup curl_handle since it's null. just cleanup curl global.*/
		curl_global_cleanup();
		elog(ERROR, "initialize global curl context failed.");
	}
	curl_context_cloud.hasInited = true;
	MemoryContext old;
	old = MemoryContextSwitchTo(TopMemoryContext);
	curl_context_cloud.response.buffer = palloc0(CURL_RES_BUFFER_SIZE);
	MemoryContextSwitchTo(old);
	curl_context_cloud.response.buffer_size = CURL_RES_BUFFER_SIZE;
	elog(DEBUG3, "initialize global curl context for privileges check.");
	on_proc_exit(finalize_cloud_curl, 0);
}

void finalize_cloud_curl() {
	if (curl_context_cloud.hasInited) {
		if (curl_context_cloud.response.buffer != NULL) {
			pfree(curl_context_cloud.response.buffer);
		}
		/* cleanup curl stuff */
		if (curl_context_cloud.curl_handle) {
			curl_easy_cleanup(curl_context_cloud.curl_handle);
		}
		/* we're done with libcurl, so clean it up */
		curl_global_cleanup();
		curl_context_cloud.hasInited = false;
		elog(DEBUG3, "finalize the global struct for curl handle context.");
	}
}

int check_authentication_from_cloud(char *username, char *password,
		bool *createuser, CouldAuthAction authAction, char * action,
		char **errormsg)
{
	json_object* jrequest;
	switch (authAction)
	{
		case AUTHENTICATION_CHECK:
			jrequest = create_cloud_authentication_request_json(username, password);
			break;
		case USER_SYNC:
			jrequest = create_cloud_usersync_request_json(username, password,
					createuser, action);
			break;
		case USER_EXIST:
			jrequest = create_cloud_userexist_request_json(username);
			break;
		default:
			elog(ERROR, "Invalid cloud authentication action:%d", authAction);
	}
	Assert(jrequest != NULL);

	const char *request = json_object_to_json_string(jrequest);
	Assert(request != NULL);
	elog(
			DEBUG3, "send json request to cloud : %s", request);

			/* call GET method to send request*/
			Assert(curl_context_cloud.hasInited);
			switch (authAction)
			{
				case AUTHENTICATION_CHECK:
					if (call_cloud_rest(&curl_context_cloud, request, "cloudauthenticate") < 0)
					{
						return -1;
					}
					break;
				case USER_SYNC:
					if (call_cloud_rest(&curl_context_cloud, request, "syncuser") < 0)
					{
						return -1;
					}
					break;
				case USER_EXIST:
					if (call_cloud_rest(&curl_context_cloud, request, "userexist") < 0)
					{
						return -1;
					}
					break;
				default:
					elog(ERROR, "Invalid cloud authentication action:%d", authAction);
			}

			/* free the JSON object */
			json_object_put(jrequest);

			/* parse the JSON-format result */
			int result,
	ret;
	switch (authAction)
	{
		case AUTHENTICATION_CHECK:
			ret = parse_cloud_auth_response(curl_context_cloud.response.buffer,
					&result, errormsg);
			break;
		case USER_SYNC:
			ret = parse_cloud_sync_response(curl_context_cloud.response.buffer,
					&result, errormsg);
			break;
		case USER_EXIST:
			ret = parse_cloud_exist_response(curl_context_cloud.response.buffer,
					&result, errormsg);
			break;
		default:
			elog(ERROR, "Invalid cloud authentication action:%d", authAction);
	}
	if (ret < 0)
	{
		elog(
				ERROR, "parse cloud response failed, cloud response content is %s",
				curl_context_cloud.response.buffer == NULL? "empty.":curl_context_cloud.response.buffer);
	}
	if (curl_context_cloud.response.buffer != NULL)
	{
		/* reset response size to reuse the buffer. */
		curl_context_cloud.response.response_size = 0;
	}

	elog(DEBUG3, "in check_authentication_from_cloud: ret=%d", result);
	return result;
}
