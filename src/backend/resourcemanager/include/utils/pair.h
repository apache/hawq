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

#ifndef _PAIR_VALUES_H
#define _PAIR_VALUES_H

#include "resourcemanager/envswitch.h"

struct PAIRData {
	MCTYPE Context;
	void * Key;
	void * Value;
};

typedef struct PAIRData *PAIR;
typedef struct PAIRData  PAIRData;

//#define PAIR_KEYASUINT32(pair) 		((uint32_t)((pair)->Key))
//#define PAIR_KEYASVOIDPT(pair) 		((void *)  ((pair)->Key))
//#define PAIR_KEYASSTRING(pair) ((void *)  ((pair)->Key))

//#define PAIR_VALUEASVOIDPT(pair) 	((void *)  ((pair)->Value))

PAIR createPAIR(MCTYPE context, void *key, void *value);
void freePAIR(PAIR pair);

#endif /* _PAIR_VALUES_H */
