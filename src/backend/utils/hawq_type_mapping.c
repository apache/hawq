/*-------------------------------------------------------------------------
 *
 * hawq_type_mapping.c
 *     Definitions for hawq type mapping function
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

#include "catalog/pg_magic_oid.h"
#include "utils/hawq_type_mapping.h"
#include "miscadmin.h"

int32_t map_hawq_type_to_common_plan(int32_t hawqTypeID) {
  switch (hawqTypeID) {
    case HAWQ_TYPE_BOOL:
      return BOOLEANID;

    case HAWQ_TYPE_INT2:
      return SMALLINTID;

    case HAWQ_TYPE_INT4:
      return INTID;

    case HAWQ_TYPE_INT8:
    case HAWQ_TYPE_TID:
      return BIGINTID;

    case HAWQ_TYPE_FLOAT4:
      return FLOATID;

    case HAWQ_TYPE_FLOAT8:
      return DOUBLEID;

    case HAWQ_TYPE_NUMERIC:
      return DECIMALID;

    case HAWQ_TYPE_DATE:
      return DATEID;

    case HAWQ_TYPE_BPCHAR:
      return CHARID;

    case HAWQ_TYPE_VARCHAR:
      return VARCHARID;

    case HAWQ_TYPE_NAME:
    case HAWQ_TYPE_TEXT:
      return STRINGID;

    case HAWQ_TYPE_JSON:
      return JSONID;

    case HAWQ_TYPE_JSONB:
      return JSONBID;

    case HAWQ_TYPE_TIME:
      return TIMEID;

    case HAWQ_TYPE_TIMESTAMP:
    case HAWQ_TYPE_TIMETZ:
      return TIMESTAMPID;

    case HAWQ_TYPE_TIMESTAMPTZ:
      return TIMESTAMPTZID;

    case HAWQ_TYPE_INTERVAL:
      return INTERVALID;

    case HAWQ_TYPE_MONEY:
    case HAWQ_TYPE_BIT:
    case HAWQ_TYPE_VARBIT:
    case HAWQ_TYPE_BYTE:
    case HAWQ_TYPE_XML:
    case HAWQ_TYPE_MACADDR:
    case HAWQ_TYPE_INET:
    case HAWQ_TYPE_CIDR:
      return BINARYID;

    case HAWQ_TYPE_INT2_ARRAY:
      return SMALLINTARRAYID;

    case HAWQ_TYPE_INT4_ARRAY:
      return INTARRAYID;

    case HAWQ_TYPE_INT8_ARRAY:
      return BIGINTARRAYID;

    case HAWQ_TYPE_FLOAT4_ARRAY:
      return FLOATARRAYID;

    case HAWQ_TYPE_FLOAT8_ARRAY:
      return DOUBLEARRAYID;

    case HAWQ_TYPE_TEXT_ARRAY:
      return STRINGARRAYID;

    case HAWQ_TYPE_BPCHAR_ARRAY:
      return BPCHARARRAYID;

    case HAWQ_TYPE_NUMERIC_ARRAY:
      return DECIMAL128ARRAYID;

    case HAWQ_TYPE_DATE_ARRAY:
      return DATEARRAYID;

    case HAWQ_TYPE_UNKNOWN:
      return STRINGID;

    case HAWQ_TYPE_POINT:
    case HAWQ_TYPE_LSEG:
    case HAWQ_TYPE_PATH:
    case HAWQ_TYPE_BOX:
    case HAWQ_TYPE_POLYGON:
    case HAWQ_TYPE_CIRCLE:
    default:
      if (HAEQ_TYPE_UDT(hawqTypeID))
        return BINARYID;
      else
        return type_is_rowtype(hawqTypeID)
                   ? (STRUCTEXID)
                   : (type_is_basetype(hawqTypeID) ? IOBASETYPEID
                                                   : INVALIDTYPEID);
  }
}

bool checkUnsupportedDataType(int32_t hawqTypeID, int32_t dateStyle) {
  switch (hawqTypeID) {
    case HAWQ_TYPE_BOOL:
    case HAWQ_TYPE_INT2:
    case HAWQ_TYPE_INT4:
    case HAWQ_TYPE_INT8:
    case HAWQ_TYPE_TID:
    case HAWQ_TYPE_FLOAT4:
    case HAWQ_TYPE_FLOAT8:
    case HAWQ_TYPE_CHAR:
    case HAWQ_TYPE_TEXT:
    case HAWQ_TYPE_BYTE:
    case HAWQ_TYPE_BPCHAR:
    case HAWQ_TYPE_VARCHAR:
    case HAWQ_TYPE_DATE:
    case HAWQ_TYPE_TIME:
    case HAWQ_TYPE_TIMESTAMP:
    case HAWQ_TYPE_INTERVAL:
    case HAWQ_TYPE_INT2_ARRAY:
    case HAWQ_TYPE_INT4_ARRAY:
    case HAWQ_TYPE_INT8_ARRAY:
    case HAWQ_TYPE_FLOAT4_ARRAY:
    case HAWQ_TYPE_FLOAT8_ARRAY:
    case HAWQ_TYPE_TEXT_ARRAY:
    case HAWQ_TYPE_BPCHAR_ARRAY:
    case HAWQ_TYPE_NUMERIC_ARRAY:
    case HAWQ_TYPE_DATE_ARRAY:
    case HAWQ_TYPE_NUMERIC:
    case HAWQ_TYPE_JSON:
    case HAWQ_TYPE_JSONB:
    case HAWQ_TYPE_UNKNOWN:
      return false;
    case HAWQ_TYPE_TIMESTAMPTZ:
      if (dateStyle == USE_ISO_DATES)
        return false;
      else
        return true;
    default:
      return true;
  }
}

/*
 * Type checking used by ORC format.
 * Some of the included types are enabled in old executor
 * while not supported in new executor.
 */
bool checkORCUnsupportedDataType(int32_t hawqTypeID) {
  switch (hawqTypeID) {
    case HAWQ_TYPE_BOOL:
    case HAWQ_TYPE_INT2:
    case HAWQ_TYPE_INT4:
    case HAWQ_TYPE_INT8:
    case HAWQ_TYPE_FLOAT4:
    case HAWQ_TYPE_FLOAT8:
    case HAWQ_TYPE_TEXT:
    case HAWQ_TYPE_BYTE:
    case HAWQ_TYPE_BPCHAR:
    case HAWQ_TYPE_VARCHAR:
    case HAWQ_TYPE_DATE:
    case HAWQ_TYPE_TIME:
    case HAWQ_TYPE_TIMESTAMP:
    case HAWQ_TYPE_TIMESTAMPTZ:
    case HAWQ_TYPE_INT2_ARRAY:
    case HAWQ_TYPE_INT4_ARRAY:
    case HAWQ_TYPE_INT8_ARRAY:
    case HAWQ_TYPE_FLOAT4_ARRAY:
    case HAWQ_TYPE_FLOAT8_ARRAY:
    case HAWQ_TYPE_TEXT_ARRAY:
    case HAWQ_TYPE_BPCHAR_ARRAY:
    case HAWQ_TYPE_NUMERIC:
    case HAWQ_TYPE_UNKNOWN:
      return false;
    default:
      return !HAEQ_TYPE_UDT(hawqTypeID);
  }
}
