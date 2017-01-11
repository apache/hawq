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
 * rangerrest.c
 *	routines to interact with Ranger REST API
 *
 *-------------------------------------------------------------------------
 */
#include "utils/rangerrest.h"
/*
 * A mapping from AclObjectKind to string
 */
char* AclObjectKindStr[] =
{
	"table",             /* pg_class */
	"sequence",          /* pg_sequence */
	"database",          /* pg_database */
	"function",          /* pg_proc */
	"operator",          /* pg_operator */
	"type",              /* pg_type */
	"language",          /* pg_language */
	"namespace",         /* pg_namespace */
	"oplass",            /* pg_opclass */
	"conversion",        /* pg_conversion */
	"tablespace",        /* pg_tablespace */
	"filespace",         /* pg_filespace */
	"filesystem",        /* pg_filesystem */
	"fdw",               /* pg_foreign_data_wrapper */
	"foreign_server",    /* pg_foreign_server */
	"protocol",          /* pg_extprotocol */
	"none"               /* MUST BE LAST */
};

static int request_id = 1;

static void getClientIP(char *remote_host)
{
	if( MyProcPort->remote_host == NULL || strlen(MyProcPort->remote_host) == 0 )
	{
		snprintf(remote_host, HOST_BUFFER_SIZE, "%s", "UNKNOWN");
		return;
	}
	if (strcmp(MyProcPort->remote_host, "[local]") == 0)
	{
		snprintf(remote_host, HOST_BUFFER_SIZE, "%s", "127.0.0.1");
	}
	else
	{
		snprintf(remote_host, HOST_BUFFER_SIZE, "%s", MyProcPort->remote_host);
	}
}

RangerACLResult parse_ranger_response(char* buffer)
{
	if (buffer == NULL || strlen(buffer) == 0)
	{
		return RANGERCHECK_UNKNOWN;
	}

	elog(DEBUG3, "parse ranger restful response content : %s", buffer);

	struct json_object *response = json_tokener_parse(buffer);
	if (response == NULL) 
	{
		elog(WARNING, "json_tokener_parse failed");
		return RANGERCHECK_NO_PRIV;
	}

	struct json_object *accessObj = NULL;
	if (!json_object_object_get_ex(response, "access", &accessObj))
	{
		elog(WARNING, "get json access field failed");
		return RANGERCHECK_NO_PRIV;
	}

	int arraylen = json_object_array_length(accessObj);
	elog(DEBUG3, "parse ranger response result array length: %d",arraylen);

	// here should return which table's acl check failed in future.
	for (int i=0; i< arraylen; i++){
		struct json_object *jvalue = NULL;
		struct json_object *jallow = NULL;

		jvalue = json_object_array_get_idx(accessObj, i);
		if (!json_object_object_get_ex(jvalue, "allowed", &jallow))
		{
			return RANGERCHECK_NO_PRIV;
		}
		json_bool result = json_object_get_boolean(jallow);
		if(result != 1){
			return RANGERCHECK_NO_PRIV;
		}
	}
	return RANGERCHECK_OK;

}
/**
 * Create a JSON object for Ranger request given some parameters.
 *
 *   {
 *     "requestId": 1,
 *     "user": "joe",
 *     "groups": ["admin","us"],
 *     "clientIp": "123.0.0.21",
 *     "context": "SELECT * FROM sales",
 *     "access":
 *       [
 *         {
 *           "resource":
 *           {
 *             "database": "finance"
 *           },
 *           "privileges": ["connect"]
 *         },
 *         {
 *           "resource":
 *           {
 *             "database": "finance",
 *             "schema": "us",
 *             "table": "sales"
 *           },
 *           "privileges": ["select", "insert"]
 *         }
 *       ]
 *   }
 *
 *   args: List of RangerRequestJsonArgs
 */
json_object *create_ranger_request_json(List *args)
{
	json_object *jrequest = json_object_new_object();
	json_object *juser = NULL;
	json_object *jaccess = json_object_new_array();
	char *user = NULL;
	ListCell *arg;

	foreach(arg, args)
	{
		RangerRequestJsonArgs *arg_ptr = (RangerRequestJsonArgs *) lfirst(arg);
		if (user == NULL)
		{
			user = arg_ptr->user;
			juser = json_object_new_string(user);
		}
		AclObjectKind kind = arg_ptr->kind;
		char* object = arg_ptr->object;
		Assert(user != NULL && object != NULL && privilege != NULL && arg_ptr->isAll);
		elog(DEBUG3, "build json for ranger restful request, user:%s, kind:%s, object:%s",
				user, AclObjectKindStr[kind], object);

		json_object *jelement = json_object_new_object();
		json_object *jresource = json_object_new_object();
		json_object *jactions = json_object_new_array();
		switch(kind)
		{
			case ACL_KIND_CLASS:
			case ACL_KIND_SEQUENCE:
			case ACL_KIND_PROC:
			case ACL_KIND_NAMESPACE:
			case ACL_KIND_LANGUAGE:
			{
				char *ptr = NULL;
				char *name = NULL;
				char *first = NULL; // could be a database or protocol or tablespace
				char *second = NULL; // could be a schema or language
				char *third = NULL; // could be a table or sequence or function
				int idx = 0;
				for (name = strtok_r(object, ".", &ptr); name;
						name = strtok_r(NULL, ".", &ptr), idx++)
				{
					if (idx == 0)
					{
						first = pstrdup(name);
					}
					else if (idx == 1)
					{
						second = pstrdup(name);
					}
					else
					{
						third = pstrdup(name);
					}
				}

				if (first != NULL)
				{
					json_object *jfirst = json_object_new_string(first);
					json_object_object_add(jresource, "database", jfirst);
				}
				if (second != NULL)
				{
					json_object *jsecond = json_object_new_string(second);
					json_object_object_add(jresource,
						(kind == ACL_KIND_LANGUAGE) ? "language" : "schema", jsecond);
				}
				if (third != NULL)
				{
					json_object *jthird = json_object_new_string(third);
					json_object_object_add(jresource,
						(kind == ACL_KIND_CLASS) ? "table" :
						(kind == ACL_KIND_SEQUENCE) ? "sequence" : "function", jthird);
				}

				if (first != NULL)
					pfree(first);
				if (second != NULL)
					pfree(second);
				if (third != NULL)
					pfree(third);
				break;
			}
			case ACL_KIND_OPER:
			case ACL_KIND_CONVERSION:
			case ACL_KIND_DATABASE:
			case ACL_KIND_TABLESPACE:
			case ACL_KIND_TYPE:
			case ACL_KIND_FILESYSTEM:
			case ACL_KIND_FDW:
			case ACL_KIND_FOREIGN_SERVER:
			case ACL_KIND_EXTPROTOCOL:
			{
				json_object *jobject = json_object_new_string(object);
				json_object_object_add(jresource, AclObjectKindStr[kind], jobject);
				break;
			}
			default:
				elog(ERROR, "unrecognized objkind: %d", (int) kind);
		} // switch
		json_object_object_add(jelement, "resource", jresource);

		ListCell *cell;
		foreach(cell, arg_ptr->actions)
		{
		    json_object* jaction = json_object_new_string((char *)cell->data.ptr_value);
		    json_object_array_add(jactions, jaction);
		}
		json_object_object_add(jelement, "privileges", jactions);
		json_object_array_add(jaccess, jelement);

	} // foreach
	char str[32];
	sprintf(str,"%d",request_id);
	json_object *jreqid = json_object_new_string(str);
	json_object_object_add(jrequest, "requestId", jreqid);
	json_object_object_add(jrequest, "user", juser);

	char remote_host[HOST_BUFFER_SIZE];
	getClientIP(remote_host);
	json_object *jclientip = json_object_new_string(remote_host);
	json_object_object_add(jrequest, "clientIp", jclientip);

	json_object *jcontext = json_object_new_string(
			(debug_query_string == NULL || strlen(debug_query_string) == 0)
				? "connect to db" : debug_query_string);
	json_object_object_add(jrequest, "context", jcontext);
	json_object_object_add(jrequest, "access", jaccess);

	return jrequest;
}

static size_t write_callback(char *contents, size_t size, size_t nitems,
	void *userp)
{
	size_t realsize = size * nitems;
	CURL_HANDLE curl = (CURL_HANDLE) userp;
	Assert(curl != NULL);

	elog(DEBUG3, "ranger restful response size is %d. response buffer size is %d.", curl->response.response_size, curl->response.buffer_size);
	int original_size = curl->response.buffer_size;
	while(curl->response.response_size + realsize >= curl->response.buffer_size)
	{
		/*double the buffer size if the buffer is not enough.*/
		curl->response.buffer_size = curl->response.buffer_size * 2;
	}
	if(original_size < curl->response.buffer_size)
	{
		/* our repalloc is not same as realloc, repalloc's first param(buffer) can not be NULL */
		curl->response.buffer = repalloc(curl->response.buffer, curl->response.buffer_size);
	}
	elog(DEBUG3, "ranger restful response size is %d. response buffer size is %d.", curl->response.response_size, curl->response.buffer_size);
	if (curl->response.buffer == NULL)
	{
		/* out of memory! */
		elog(WARNING, "not enough memory for Ranger response");
		return 0;
	}
	memcpy(curl->response.buffer + curl->response.response_size, contents, realsize);
	elog(DEBUG3, "read from ranger restful response: %s", curl->response.buffer);
	curl->response.response_size += realsize;
	curl->response.buffer[curl->response.response_size] = '\0';
	return realsize;
}

/**
 * @returns: 0 curl success; -1 curl failed
 */
int call_ranger_rest(CURL_HANDLE curl_handle, const char* request)
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
	appendStringInfo(&tname, "http://");
	appendStringInfo(&tname, "%s", rps_addr_host);
	appendStringInfo(&tname, ":");
	appendStringInfo(&tname, "%d", rps_addr_port);
	appendStringInfo(&tname, "/");
	appendStringInfo(&tname, "%s", rps_addr_suffix);
	curl_easy_setopt(curl_handle->curl_handle, CURLOPT_URL, tname.data);

	struct curl_slist *headers = NULL;
	headers = curl_slist_append(headers, "Content-Type:application/json");
	curl_easy_setopt(curl_handle->curl_handle, CURLOPT_HTTPHEADER, headers);

	curl_easy_setopt(curl_handle->curl_handle, CURLOPT_POSTFIELDS,request);
	//"{\"requestId\": 1,\"user\": \"hubert\",\"clientIp\":\"123.0.0.21\",\"context\": \"SELECT * FROM sales\",\"access\":[{\"resource\":{\"database\":\"a-database\",\"schema\":\"a-schema\",\"table\":\"sales\"},\"privileges\": [\"select\"]}]}");
	/* send all data to this function  */
	curl_easy_setopt(curl_handle->curl_handle, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(curl_handle->curl_handle, CURLOPT_WRITEDATA, (void *)curl_handle);

	res = curl_easy_perform(curl_handle->curl_handle);
	if(request_id == INT_MAX)
	{
		request_id = 0;
	}
	request_id++;
	/* check for errors */
	if(res != CURLE_OK)
	{
		elog(WARNING, "curl_easy_perform() failed: %s\n",
			curl_easy_strerror(res));
	}
	else
	{
		ret = 0;
		elog(DEBUG3, "retrieved %d bytes from ranger restful response.",
			curl_handle->response.response_size);
	}

	return ret;
}

/*
 * arg_list: List of RangerRequestJsonArgs
 */
int check_privilege_from_ranger(List *arg_list)
{
	json_object* jrequest = create_ranger_request_json(arg_list);
	Assert(jrequest != NULL);
	const char *request = json_object_to_json_string(jrequest);
	elog(DEBUG3, "send json request to ranger : %s", request);
	Assert(request != NULL);

	/* call GET method to send request*/
	if (call_ranger_rest(&curl_context_ranger, request) < 0)
	{
		return RANGERCHECK_NO_PRIV;
	}

	/* free the JSON object */
	json_object_put(jrequest);

	/* parse the JSON-format result */
	RangerACLResult ret = parse_ranger_response(curl_context_ranger.response.buffer);
	if (curl_context_ranger.response.buffer != NULL)
	{
		/* reset response size to reuse the buffer. */
		curl_context_ranger.response.response_size = 0;
	}

	return ret;
}
