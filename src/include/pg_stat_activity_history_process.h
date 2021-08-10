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
#ifndef SRC_INCLUDE_PG_STAT_ACTIVITY_HISTORY_PROCESS_H_
#define SRC_INCLUDE_PG_STAT_ACTIVITY_HISTORY_PROCESS_H_

#include "utils/timestamp.h"
#include "postgres_ext.h"

#define MAXTIMELENGTH 100
#define MAXDATABASENAME 100
#define MAXUSERNAME 100
#define MAXAPPNAMELENGTH 100
#define MAXERRORINFOLENGTH 1000
#define MAXCLIENTADDRLENGTH 1025
#define MAXSTATUSLENGTH 10

typedef struct queryHistoryInfo
{
	Oid databaseId;
	Oid userId;
	int processId;
	Oid sessionId;
	int client_port;
	uint32_t memoryUsage;
	double cpuUsage;
	char database_name[MAXDATABASENAME];
	char user_name[MAXUSERNAME];
	char creation_time[MAXTIMELENGTH];
	char end_time[MAXTIMELENGTH];
	char client_addr[MAXCLIENTADDRLENGTH];
	char application_name[MAXAPPNAMELENGTH];
	char status[MAXSTATUSLENGTH];
	char errorInfo[MAXERRORINFOLENGTH];
	uint32_t queryLen;
}queryHistoryInfo;

extern void pgStatActivityHistory_send(Oid databaseId, Oid userId, int processId,
                 Oid sessionId, const char *creation_time, const char *end_time,
                 struct Port *tmpProcPort, char *application_name, double cpuUsage,
                 uint32_t memoryUsage, int status, char *errorInfo,const char *query);

extern int pgStatActivityHistorySock;

extern void pgstatactivityhistory_init(void);
extern int pgstatactivityhistory_start(void);
extern void allow_immediate_pgStatActivityHistory_restart(void);

#endif /* SRC_INCLUDE_PG_STAT_ACTIVITY_HISTORY_PROCESS_H_ */
