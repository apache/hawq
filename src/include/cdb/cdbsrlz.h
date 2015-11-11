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
 * cdbsrlz.h
 *	  definitions for paln serialization utilities
 *
 *
 * NOTES
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBSRLZ_H
#define CDBSRLZ_H

#include "lib/stringinfo.h"
#include "nodes/pg_list.h"

extern char *WriteBackCatalogs;
extern int32 WriteBackCatalogLen;

extern char *serializeNode(Node *node, int *size, int *uncompressed_size);
extern Node *deserializeNode(const char *strNode, int size);

#endif   /* CDBSRLZ_H */
