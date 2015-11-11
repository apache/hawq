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

#ifndef _HAWQ_RESOURCE_MANAGER_H
#define _HAWQ_RESOURCE_MANAGER_H

#include "resourcemanager/envswitch.h"
#include "utils/linkedlist.h"

#define WITHLISTSTART_TAG		"withliststart"
#define WITHOUTLISTSTART_TAG	"withoutliststart"

#define HAWQSITE_HEAD_STRING    "hawq."

int ResManagerMain(int argc, char *argv[]);

int ResManagerProcessStartup(void);

List *getHawqSiteConfigurationList(const char *hawqsitepath, MCTYPE context);
void freeHawqSiteConfigurationList(MCTYPE context, List **conf);

int  loadDynamicResourceManagerConfigure(void);
#endif /* _HAWQ_RESOURCE_MANAGER_H */
