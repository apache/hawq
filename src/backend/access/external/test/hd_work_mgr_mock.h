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

#ifndef HD_WORK_MGR_MOCK_
#define HD_WORK_MGR_MOCK_

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "../hd_work_mgr.c"


static QueryResource * resource = NULL;

/*
 * Helper functions to create and free QueryResource element
 * used by hd_work_mgr
 */

/*
 * Builds a mock QueryResource which is used when
 * GetActiveQueryResource is called to get the
 * list of available segments.
 */
void buildQueryResource(int segs_num,
                        char * segs_hostips[]);
void freeQueryResource();
                                  
#endif //HD_WORK_MGR_MOCK_
