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

#ifndef DBCOMMON_SRC_DBCOMMON_TYPE_TYPE_KIND_H_
#define DBCOMMON_SRC_DBCOMMON_TYPE_TYPE_KIND_H_

enum TypeKind {
  /* Predefined types begin(0~999) */
  // 0~9 Special Types
  INVALIDTYPEID = 0,
  UNKNOWNID = 1,  // unknown yet
  ANYID = 2,      // can be any type

  // 10~99 reserved

  // 100~149 integer
  TINYINTID = 100,
  SMALLINTID = 101,
  INTID = 102,
  BIGINTID = 103,

  // 150~199 float
  FLOATID = 150,
  DOUBLEID = 151,
  DECIMALID = 152,
  DECIMALNEWID = 153,  // used by new QE only

  // 200~249 date/time
  TIMESTAMPID = 200,
  TIMESTAMPTZID = 201,
  DATEID = 202,
  TIMEID = 203,
  TIMETZID = 204,
  INTERVALID = 205,

  // 250~299 string
  STRINGID = 250,
  VARCHARID = 251,
  CHARID = 252,
  JSONID = 253,
  JSONBID = 254,
  // 300~349 misc
  BOOLEANID = 300,
  BINARYID = 301,

  // 350~999 reserved

  /* Predefined types end */

  /* Constructed types begin(1000~1999) */
  // 1000~1199 array
  SMALLINTARRAYID = 1000,
  INTARRAYID = 1001,
  BIGINTARRAYID = 1002,
  FLOATARRAYID = 1003,
  DOUBLEARRAYID = 1004,
  STRINGARRAYID = 1005,
  BPCHARARRAYID = 1006,
  DECIMAL128ARRAYID = 1007,
  DATEARRAYID = 1008,

  // 1200~1299 reference

  // 1300~1399 row types

  /* Constructed types end */

  /* User defined types begin(2000~5999) */

  // 2000~2099 complex
  ARRAYID = 2000,
  MAPID = 2001,
  STRUCTID = 2002,
  UNIONID = 2003,
  IOBASETYPEID = 2004,  // base type
  STRUCTEXID = 2005,    // struct extension
  MAGMATID = 2006,      // magma tid
  MAGMATXSID = 2007,    // magma transaction id
  AVG_DOUBLE_TRANS_DATA_ID = 2008,
  AVG_DECIMAL_TRANS_DATA_ID = 2009,
  INT_128_ID = 2010,
  STDDEV_DOUBLE_TRANS_DATA_ID = 2011,
  STDDEV_DECIMAL_TRANS_DATA_ID = 2012,
  MAGMAHLCID = 2013,  // magma hlc id

  // 2100~2199 format(ps:xml not support yet)

  // 2200~2799 reserved

  // 2800~2899 pesudo types(not support yet)

  // 2900~2999 net(not supported yet)

  // 3000~3099 postgis(not support yet)

  /* User defined types end   */

  /* Catalog types begin(6000~)   */

  // 6000~6099 catalog type(not support yet)
  /* Catalog types end   */
};

#endif  // DBCOMMON_SRC_DBCOMMON_TYPE_TYPE_KIND_H_
