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

#ifndef UNIVPLAN_SRC_UNIVPLAN_COMMON_STATISTICS_H_
#define UNIVPLAN_SRC_UNIVPLAN_COMMON_STATISTICS_H_

namespace univplan {
// Statistics that are available for all types of columns.
class ColumnStatistics {
 public:
  virtual ~ColumnStatistics() {}

  // Get the number of values in this column. It will differ from the number
  // of rows because of NULL values and repeated values.
  // @return the number of values
  uint64_t getNumberOfValues() const { return valueCount; }

  // Returns true if there are nulls in the scope of column statistics
  bool hasNull() const { return hasNullValue; }

  virtual void reset() {
    valueCount = 0;
    hasNullValue = true;
  }

  void increment(uint64_t count) { valueCount += count; }

  void unsetNull() { hasNullValue = false; }

  virtual void merge(const ColumnStatistics& stats) {
    valueCount += stats.getNumberOfValues();
    hasNullValue |= stats.hasNull();
  }

 protected:
  uint64_t valueCount;
  bool hasNullValue;
};

class Statistics {
 public:
  virtual ~Statistics() {}

  // Get the statistics of colId column.
  // @return one column's statistics
  virtual const ColumnStatistics* getColumnStatistics(uint32_t colId) const = 0;

  // Get the number of columns
  // @return the number of columns
  virtual uint32_t getNumberOfColumns() const = 0;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_COMMON_STATISTICS_H_
