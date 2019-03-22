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

#include "dbcommon/nodes/datum.h"

#include <cassert>
#include <memory>

#include "dbcommon/function/decimal-function.h"
#include "dbcommon/type/date.h"
#include "dbcommon/type/type-kind.h"
#include "dbcommon/type/type-util.h"
#include "dbcommon/type/typebase.h"

namespace dbcommon {

Datum CreateDatum(const char *str, int typeId) {
  return TypeUtil::instance()
      ->getTypeEntryById(static_cast<TypeKind>(typeId))
      ->type->getDatum(str);
}

Datum CreateDatum(const char *str, dbcommon::Timestamp *timestamp, int typeId) {
  std::shared_ptr<TypeBase> base =
      TypeUtil::instance()
          ->getTypeEntryById(static_cast<TypeKind>(typeId))
          ->type;
  if (auto *tz = dynamic_cast<dbcommon::TimestamptzType *>(&*base))
    return tz->getDatum(str, timestamp);
  dbcommon::TimestampType *ts = dynamic_cast<dbcommon::TimestampType *>(&*base);
  if (ts)
    return ts->getDatum(str, timestamp);
  else
    return base->getDatum(str);
}

std::string DatumToString(const Datum &d, int typeId) {
  return TypeUtil::instance()
      ->getTypeEntryById(static_cast<TypeKind>(typeId))
      ->type->DatumToString(d);
}

std::string DatumToBinary(const Datum &d, int typeId) {
  return TypeUtil::instance()
      ->getTypeEntryById(static_cast<TypeKind>(typeId))
      ->type->DatumToBinary(d);
}

}  // end of namespace dbcommon
