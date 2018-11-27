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
#ifndef _PXF_UTILS_H_
#define _PXF_UTILS_H_

#include "postgres.h"
#include "access/libchurl.h"
#include "access/pxfuriparser.h"
#include "commands/copy.h"

typedef struct sClientContext
{
	CHURL_HEADERS http_headers;
	CHURL_HANDLE handle;
									/* part of the HTTP response - received	*/
									/* from one call to churl_read 			*/
	StringInfoData the_rest_buf; 	/* contains the complete HTTP response 	*/
} ClientContext;

/* checks if two ip strings are equal */
bool are_ips_equal(char *ip1, char *ip2);

/* override port str with given new port int */
void port_to_str(char** port, int new_port);

/* get hdfs location from current session's filespace entry */
void get_hdfs_location_from_filespace(char** path);

/* parse the REST message and issue the libchurl call */
void call_rest(GPHDUri *hadoop_uri, ClientContext *client_context, char* rest_msg);

/* get ip address of loopback interface */
char* get_loopback_ip_addr(void);

/* replace first occurrence of replace in string with replacement*/
char* replace_string(const char* string, const char* replace, const char* replacement);

#endif	// _PXF_UTILS_H_
