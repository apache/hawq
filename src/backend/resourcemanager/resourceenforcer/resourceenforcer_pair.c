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

#include <stdlib.h>
#include "postgres.h"
#include "cdb/cdbvars.h"
#include "resourceenforcer/resourceenforcer_pair.h"

Pair createPair(void *key, void *value)
{
	Pair p = (Pair)malloc(sizeof(PairData));

	if (p == NULL)
	{
		write_log("Function createPair out of memory");
		return NULL;
	}

	p->Key = key;
	p->Value = value;

	return p;
}

void freePair(Pair p)
{
	if (p != NULL)
	{
		free(p);
	}
}
