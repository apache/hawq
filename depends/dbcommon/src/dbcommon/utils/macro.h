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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_MACRO_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_MACRO_H_

#define force_inline __attribute__((always_inline))
// default block size is 128 MB
#define DEFAULT_BLOCK_SIZE (1 << 27)
// default number of tuples per batch is 2048
#define DEFAULT_NUMBER_TUPLES_PER_BATCH 2048
#define DEFAULT_RESERVED_SIZE_OF_STRING 16
#define DEFAULT_CACHE_LINE_SIZE 64
#define DEFAULT_SIZE_PER_AGG_COUNTER_BLK (1 << 24)
#define DEFAULT_SIZE_PER_HASH_CHAIN_BLK (1 << 27)
#define DEFAULT_SIZE_PER_HASHKEY_BLK (1 << 26)
#define DEFAULT_SIZE_PER_HASHJOIN_BLK (1 << 26)

#define INNER_VAR 65000  // reference to inner subplan
#define OUTER_VAR 65001  // reference to outer subplan

// msb for char */
#define HIGHBIT (0x80)
#define IS_HIGHBIT_SET(ch) ((unsigned char)(ch)&HIGHBIT)

static const char* INSERT_HIDDEN_DIR = "/.tmp";

#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_MACRO_H_
