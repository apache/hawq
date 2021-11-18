/*-------------------------------------------------------------------------
 *
 * hawq_type_mapping.h
 *     Definitions for hawq type and its mapping
 *
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
 *
 *-------------------------------------------------------------------------
 */
#ifndef HAWQTYPE_H
#define HAWQTYPE_H

#include "postgres.h"

#include "dbcommon/type/type-kind.h"

// primitive type
#define HAWQ_TYPE_BOOL 16
#define HAWQ_TYPE_CHAR 18
#define HAWQ_TYPE_NAME 19
#define HAWQ_TYPE_INT8 20
#define HAWQ_TYPE_INT2 21
#define HAWQ_TYPE_INT4 23
#define HAWQ_TYPE_TID 27
#define HAWQ_TYPE_FLOAT4 700
#define HAWQ_TYPE_FLOAT8 701
#define HAWQ_TYPE_MONEY 790
#define HAWQ_TYPE_NUMERIC 1700
#define HAWQ_TYPE_BYTE 17
#define HAWQ_TYPE_TEXT 25
#define HAWQ_TYPE_JSON 193
#define HAWQ_TYPE_JSONB 3802
#define HAWQ_TYPE_XML 142
#define HAWQ_TYPE_MACADDR 829
#define HAWQ_TYPE_INET 869
#define HAWQ_TYPE_CIDR 650
#define HAWQ_TYPE_BPCHAR 1042
#define HAWQ_TYPE_VARCHAR 1043
#define HAWQ_TYPE_DATE 1082
#define HAWQ_TYPE_TIME 1083
#define HAWQ_TYPE_TIMESTAMP 1114
#define HAWQ_TYPE_TIMETZ 1266
#define HAWQ_TYPE_TIMESTAMPTZ 1184
#define HAWQ_TYPE_INTERVAL 1186
#define HAWQ_TYPE_BIT 1560
#define HAWQ_TYPE_VARBIT 1562

// group type
#define HAWQ_TYPE_POINT 600
#define HAWQ_TYPE_LSEG 601
#define HAWQ_TYPE_PATH 602
#define HAWQ_TYPE_BOX 603
#define HAWQ_TYPE_POLYGON 604
#define HAWQ_TYPE_CIRCLE 718
#define HAWQ_TYPE_INT2_ARRAY 1005
#define HAWQ_TYPE_INT4_ARRAY 1007
#define HAWQ_TYPE_INT8_ARRAY 1016
#define HAWQ_TYPE_FLOAT4_ARRAY 1021
#define HAWQ_TYPE_FLOAT8_ARRAY 1022
#define HAWQ_TYPE_TEXT_ARRAY 1009
#define HAWQ_TYPE_BPCHAR_ARRAY 1014
#define HAWQ_TYPE_NUMERIC_ARRAY 1231
#define HAWQ_TYPE_DATE_ARRAY 1182

#define HAWQ_TYPE_INVALID -1

#define HAWQ_TYPE_UNKNOWN 705

#define HAEQ_TYPE_UDT(x) ( x > FirstNormalObjectId)

extern int32_t map_hawq_type_to_common_plan(int32_t hawqTypeID);

// if hawq type unsupported, return true
extern bool checkUnsupportedDataType(int32_t hawqTypeID, int32_t dateStyle);

extern bool checkORCUnsupportedDataType(int32_t hawqTypeID);

#endif /* HAWQTYPE_H */
