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
#include "postgres.h"

#include <json-c/json.h>

#include "utils/acl.h"
#include "utils/guc.h"
#include "utils/rangerrest.h"

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

RangerACLResult parse_ranger_response(char* buffer)
{
    Assert(buffer != NULL);
    if (strlen(buffer) == 0)
        return RANGERCHECK_UNKNOWN;

    elog(LOG, "read from Ranger Restful API: %s", buffer);

    struct json_object *response = json_tokener_parse(buffer);
    struct json_object *accessObj = json_object_object_get(response, "access");

    int arraylen = json_object_array_length(accessObj);
    elog(LOG, "Array Length: %dn",arraylen);

    json_object * jvalue;
    json_object * jallow;
    json_bool result;
    // here should return which table's acl check failed in future.
    for (int i=0; i< arraylen; i++){
      jvalue = json_object_array_get_idx(accessObj, i);
      jallow = json_object_object_get(jvalue, "allowed");
      result = json_object_get_boolean(jallow);
      if(result != 1){
        return RANGERCHECK_NO_PRIV;
      }
    }
    return RANGERCHECK_OK;

}

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

/*
 * args: List of RangerRequestJsonArgs
 */
json_object *create_ranger_request_json_batch(List *args)
{
  json_object *juser = NULL;
  json_object *jaccess = json_object_new_array();
  json_object *jrequest = json_object_new_object();
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
    char* how = arg_ptr->how;
    Assert(user != NULL && object != NULL && privilege != NULL && how != NULL);
    elog(LOG, "build json for ranger request, user:%s, kind:%s, object:%s",
         user, AclObjectKindStr[kind], object);
    
    json_object *jresource = json_object_new_object();
    json_object *jelement = json_object_new_object();
    json_object *jactions = json_object_new_array();

    switch(kind)
    {
        case ACL_KIND_CLASS:
        case ACL_KIND_SEQUENCE:
        case ACL_KIND_PROC:
        case ACL_KIND_NAMESPACE:
        case ACL_KIND_LANGUAGE:
        {
            char *ptr = NULL; char *name = NULL;
            char *first = NULL; // could be a database or protocol or tablespace
            char *second = NULL; // could be a schema or language
            char *third = NULL; // could be a table or sequence or function
            int idx = 0;
            for (name = strtok_r(object, ".", &ptr);
                 name;
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
                elog(LOG, "JTHIRD %s\n", jthird);
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
    
    //ListCell *cell;
    //foreach(cell, arg_ptr->actions)
    //{
      char tmp[7] = "select";
      json_object* jaction = json_object_new_string((char *)tmp);
      //json_object* jaction = json_object_new_string((char *)cell->data.ptr_value);
      json_object_array_add(jactions, jaction);
    //}
    json_object_object_add(jelement, "privileges", jactions);
    json_object_array_add(jaccess, jelement);

  } // foreach

  json_object_object_add(jrequest, "user", juser);
  json_object_object_add(jrequest, "access", jaccess);

  json_object *jreqid = json_object_new_string("1");
  json_object_object_add(jrequest, "requestId", jreqid);
  json_object *jclientip = json_object_new_string("123.0.0.21");
  json_object_object_add(jrequest, "clientIp", jclientip);
  json_object *jcontext = json_object_new_string("SELECT * FROM DDDDDDD");
  json_object_object_add(jrequest, "context", jcontext);

  return jrequest;
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
 *           "privileges": ["select, insert"]
 *         }
 *       ]
 *   }
 */
json_object* create_ranger_request_json(char* user, AclObjectKind kind, char* object,
        List* actions, char* how)
{
    Assert(user != NULL && object != NULL && privilege != NULL
                    && how != NULL);
    ListCell *cell;

    elog(LOG, "build json for ranger request, user:%s, kind:%s, object:%s",
              user, AclObjectKindStr[kind], object);
    json_object *jrequest = json_object_new_object();
    json_object *juser = json_object_new_string(user);

    json_object *jaccess = json_object_new_array();
    json_object *jelement = json_object_new_object();

    json_object *jresource = json_object_new_object();
    switch(kind)
    {
        case ACL_KIND_CLASS:
        case ACL_KIND_SEQUENCE:
        case ACL_KIND_PROC:
        case ACL_KIND_NAMESPACE:
        case ACL_KIND_LANGUAGE:
        {
            char *ptr = NULL; char *name = NULL;
            char *first = NULL; // could be a database or protocol or tablespace
            char *second = NULL; // could be a schema or language
            char *third = NULL; // could be a table or sequence or function
            int idx = 0;
            for (name = strtok_r(object, ".", &ptr);
                 name;
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
    }

    json_object *jactions = json_object_new_array();
    foreach(cell, actions)
    {
        json_object* jaction = json_object_new_string((char *)cell->data.ptr_value);
        json_object_array_add(jactions, jaction);
    }
    json_object_object_add(jelement, "resource", jresource);
    json_object_object_add(jelement, "privileges", jactions);
    json_object_array_add(jaccess, jelement);

    json_object_object_add(jrequest, "user", juser);
    json_object_object_add(jrequest, "access", jaccess);
    json_object *jreqid = json_object_new_string("1");
    json_object_object_add(jrequest, "requestId", jreqid);
    json_object *jclientip = json_object_new_string("123.0.0.21");
    json_object_object_add(jrequest, "clientIp", jclientip);
    json_object *jcontext = json_object_new_string("SELECT * FROM DDDDDDD");
    json_object_object_add(jrequest, "context", jcontext);


    return jrequest;
}

static size_t write_callback(char *contents, size_t size, size_t nitems,
        void *userp)
{
    size_t realsize = size * nitems;
    CURL_HANDLE curl = (struct curl_context *) userp;

    curl->response.buffer = palloc0(realsize + 1);
    memset(curl->response.buffer, 0, realsize + 1);
    if (curl->response.buffer == NULL)
    {
        /* out of memory! */
        elog(WARNING, "not enough memory for Ranger response");
        return 0;
    }

    memcpy(curl->response.buffer, contents, realsize);
    curl->response.size = realsize + 1;
    elog(LOG, "read from Ranger Restful API: %s", curl->response.buffer);

    return realsize;
}

void call_ranger_rest(CURL_HANDLE curl_handle, char* request)
{
    CURLcode res;
    Assert(request != NULL);

    curl_global_init(CURL_GLOBAL_ALL);

    /* init the curl session */
    curl_handle->curl_handle = curl_easy_init();
    if (curl_handle->curl_handle == NULL)
    {
        goto _exit;
    }

    /* timeout */
    // curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 1);

    /* specify URL to get */
    //curl_easy_setopt(curl_handle->curl_handle, CURLOPT_URL, "http://localhost:8089/checkprivilege");
    StringInfoData tname;
    initStringInfo(&tname);
    appendStringInfo(&tname, "http://");
    appendStringInfo(&tname, rps_addr_host);
    appendStringInfo(&tname, ":");
    appendStringInfo(&tname, "%d", rps_addr_port);
    appendStringInfo(&tname, "/rps");
    curl_easy_setopt(curl_handle->curl_handle, CURLOPT_URL, tname.data);

    /* specify format */
    // struct curl_slist *plist = curl_slist_append(NULL, "Content-Type:application/json;charset=UTF-8");
    // curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, plist);


    //curl_easy_setopt(curl_handle->curl_handle, CURLOPT_POSTFIELDSIZE_LARGE, 1000);
    //curl_easy_setopt(curl_handle->curl_handle, CURLOPT_HTTPGET, 0);
    //curl_easy_setopt(curl_handle->curl_handle, CURLOPT_CUSTOMREQUEST, "POST");

    struct curl_slist *headers = NULL;
    //curl_slist_append(headers, "Accept: application/json");
    headers = curl_slist_append(headers, "Content-Type:application/json");
    curl_easy_setopt(curl_handle->curl_handle, CURLOPT_HTTPHEADER, headers);

    //curl_easy_setopt(curl_handle->curl_handle, CURLOPT_POST, 1L);
    curl_easy_setopt(curl_handle->curl_handle, CURLOPT_POSTFIELDS,request);
    //"{\"requestId\": 1,\"user\": \"hubert\",\"clientIp\":\"123.0.0.21\",\"context\": \"SELECT * FROM sales\",\"access\":[{\"resource\":{\"database\":\"a-database\",\"schema\":\"a-schema\",\"table\":\"sales\"},\"privileges\": [\"select\"]}]}");
    /* send all data to this function  */
    curl_easy_setopt(curl_handle->curl_handle, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl_handle->curl_handle, CURLOPT_WRITEDATA, (void *)curl_handle);

    res = curl_easy_perform(curl_handle->curl_handle);

    /* check for errors */
    if(res != CURLE_OK)
    {
        elog(WARNING, "curl_easy_perform() failed: %s\n",
                curl_easy_strerror(res));
    }
    else
    {
        elog(LOG, "%lu bytes retrieved from Ranger Restful API.",
                curl_handle->response.size);
    }

_exit:
    /* cleanup curl stuff */
    if (curl_handle->curl_handle)
    {
        curl_easy_cleanup(curl_handle->curl_handle);
    }

    /* we're done with libcurl, so clean it up */
    curl_global_cleanup();
}

/*
 * arg_list: List of RangerRequestJsonArgs
 */
int check_privilege_from_ranger_batch(List *arg_list)
{
  json_object* jrequest = create_ranger_request_json_batch(arg_list);
  Assert(jrequest != NULL);
  char *request = json_object_to_json_string(jrequest);
  elog(LOG, "Send JSON request to Ranger: %s", request);
  Assert(request != NULL);
  struct curl_context_t curl_context;
  memset(&curl_context, 0, sizeof(struct curl_context_t));

  /* call GET method to send request*/
  call_ranger_rest(&curl_context, request);
  
  /* free the JSON object */
  json_object_put(jrequest);
  
  /* parse the JSON-format result */
  RangerACLResult ret = parse_ranger_response(curl_context.response.buffer);
  
  /* free response buffer */
  if (curl_context.response.buffer != NULL)
  {
    pfree(curl_context.response.buffer);
  }

  return ret;
}

/*
 * Check the privilege from Ranger for one role
 */
int check_privilege_from_ranger(char* user, AclObjectKind kind, char* object,
        List* actions, char* how)
{
    json_object* jrequest = create_ranger_request_json(user, kind, object,
                                                       actions, how);

    Assert(jrequest != NULL);
    char* request = json_object_to_json_string(jrequest);
    elog(LOG, "send JSON request to Ranger: %s", request);
    Assert(request != NULL);

    struct curl_context_t curl_context;
    memset(&curl_context, 0, sizeof(struct curl_context_t));

    /* call GET method to send request*/
    call_ranger_rest(&curl_context, request);

    /* free the JSON object */
    json_object_put(jrequest);

    /* parse the JSON-format result */
    RangerACLResult ret = parse_ranger_response(curl_context.response.buffer);

    /* free response buffer */
    if (curl_context.response.buffer != NULL)
    {
        pfree(curl_context.response.buffer);
    }

    return ret;
}

