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

#include "errorcode.h"
#include "envswitch.h"
#include <string.h>

ErrorDetailData ErrorDetailsPreset[] = {
		{COMM2RM_CLIENT_FAIL_CONN, 				"failing to connect"},
		{COMM2RM_CLIENT_FAIL_SEND, 				"failing to send content"},
		{COMM2RM_CLIENT_FAIL_RECV, 				"failing to receive content"},
		{COMM2RM_CLIENT_WRONG_INPUT, 			"wrong local resource context index"},
		{COMM2RM_CLIENT_FULL_RESOURCECONTEXT, 	"too many resource contexts"},
		{RESQUEMGR_DEADLOCK_DETECTED,			"session resource deadlock is detected"},
		{RESQUEMGR_NOCLUSTER_TIMEOUT,			"no available cluster to run query"},
		{RESQUEMGR_NORESOURCE_TIMEOUT,			"no available resource to run query"},
		{RESQUEMGR_TOO_MANY_FIXED_SEGNUM,		"expecting too many virtual segments"},
		{REQUESTHANDLER_WRONG_CONNSTAT,			"that resource context maybe recycled due to timeout"},
		{CONNTRACK_NO_CONNID,					"that resource context does not exist"},
		{RESOURCEPOOL_TOO_MANY_UAVAILABLE_HOST, "too many unavailable segments"},

		{-1, ""}
};

bool initializedErrorDetails = false;
ErrorDetailData ErrorDetails[ERROR_COUNT_MAX];

const char *getErrorCodeExplain(int errcode)
{
	if ( !initializedErrorDetails )
	{
		for ( int i = 0 ; i < ERROR_COUNT_MAX ; ++i )
		{
			ErrorDetails[i].Code = -1;
			ErrorDetails[i].Message[0] = '\0';
		}

		for ( int i = 0 ; ErrorDetailsPreset[i].Code >= 0 ; ++i )
		{
			ErrorDetails[ErrorDetailsPreset[i].Code].Code = ErrorDetailsPreset[i].Code;
			strncpy(ErrorDetails[ErrorDetailsPreset[i].Code].Message,
					ErrorDetailsPreset[i].Message,
					sizeof(ErrorDetailsPreset[i].Message)-1);
		}

		initializedErrorDetails = true;
	}

	static char message[256];

	if ( errcode < 0 || errcode >= ERROR_COUNT_MAX )
	{
		snprintf(message, sizeof(message), "Invalid error code %d", errcode);
	}
	else if ( ErrorDetails[errcode].Code == -1 )
	{
		snprintf(message, sizeof(message), "Unexplained %d", errcode);
	}
	else {
		return ErrorDetails[errcode].Message;
	}
	return message;
}
