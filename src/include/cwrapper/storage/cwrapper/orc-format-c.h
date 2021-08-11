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

#ifndef STORAGE_SRC_STORAGE_CWRAPPER_ORC_FORMAT_C_H_
#define STORAGE_SRC_STORAGE_CWRAPPER_ORC_FORMAT_C_H_

#include <stdint.h>

#ifndef ERROR_MESSAGE_BUFFER_SIZE
#define ERROR_MESSAGE_BUFFER_SIZE 4096
#endif
typedef struct ORCFormatCatchedError {
  int errCode;
  char errMessage[ERROR_MESSAGE_BUFFER_SIZE];
} ORCFormatCatchedError;

#ifdef __cplusplus

#include <memory>
#include <string>
#include <vector>

#include "dbcommon/type/type-kind.h"
#include "dbcommon/utils/url.h"
#include "storage/format/orc/orc-format.h"

typedef struct OrcColumnReader {
  dbcommon::TypeKind type;
  const char *value;
  const bool *nulls;
  const uint64_t *lens;
  std::unique_ptr<dbcommon::ByteBuffer> valBuffer;
} OrcColumnReader;

struct ORCFormatC {
  std::unique_ptr<storage::ORCFormat> orcFormat;  // NOLINT
  dbcommon::URL::uptr url;
  dbcommon::Parameters params;
  dbcommon::TupleDesc desc;
  ORCFormatCatchedError error;
  std::vector<bool> columnsToRead;
  univplan::UnivPlanScanFileSplitListList splits;
  dbcommon::TupleBatch::uptr tb;
  std::string insertFileName;

  bool needNewTupleBatch;
  uint64_t rowRead;
  uint64_t rowCount;
  std::vector<std::unique_ptr<OrcColumnReader>> columnReaders;
  std::vector<uint32_t> colToReadIds;
};

extern "C" {
#endif

struct ORCFormatC;

typedef struct ORCFormatC ORCFormatC;

typedef struct ORCFormatFileSplit {
  char *fileName;
  int64_t start;
  int64_t len;
  int64_t eof;
} ORCFormatFileSplit;

#define ORCFormatType 'o'

__attribute__((weak)) void ORCFormatSetErrorORCFormatC(ORCFormatCatchedError *ce, int errCode,
                                 const char *errMsg) {}

// tableOptions in json format
__attribute__((weak)) ORCFormatC *ORCFormatNewORCFormatC(const char *tableOptions, int segno) {}
__attribute__((weak)) void ORCFormatFreeORCFormatC(ORCFormatC **fmt) {}

__attribute__((weak)) void ORCFormatAddSplitsC(ORCFormatC *fmt, ORCFormatFileSplit *splits,
                         int numSplits) {}

__attribute__((weak)) void ORCFormatBeginORCFormatC(ORCFormatC *fmt, ORCFormatFileSplit *splits,
                              int numSplits, bool *columnsToRead,
                              char **columnName, int *columnDatatype,
                              uint64_t *columnDatatypeMod, int numColumns,
                              const void *qualList) {}

__attribute__((weak)) bool ORCFormatNextORCFormatC(ORCFormatC *fmt, const char **values,
                             uint64_t *lens, bool *nulls) {}

__attribute__((weak)) bool ORCFormatNextORCFormatWithRowIdC(ORCFormatC *fmt, const char **values,
                                      uint64_t *lens, bool *nulls,
                                      uint64_t *rowId) {}

__attribute__((weak)) void ORCFormatRescanORCFormatC(ORCFormatC *fmt) {}

__attribute__((weak)) void ORCFormatEndORCFormatC(ORCFormatC *fmt) {}

__attribute__((weak)) void ORCFormatBeginInsertORCFormatFileC(ORCFormatC *fmt, const char *filePath,
                                        char **columnName, int *columnDatatype,
                                        uint64_t *columnDatatypeMod,
                                        int numColumns) {}
__attribute__((weak)) void ORCFormatBeginInsertORCFormatC(ORCFormatC *fmt, const char *dirFullPath,
                                    char **columnName, int *columnDatatype,
                                    uint64_t *columnDatatypeMod,
                                    int numColumns) {}
__attribute__((weak)) void ORCFormatInsertORCFormatBufferC(ORCFormatC *fmt, int *datatypes,
                                     char **values, uint64_t *lens,
                                     unsigned char **nullBitmap, int32_t **dims,
                                     bool *isNull) {}
__attribute__((weak)) void ORCFormatInsertORCFormatC(ORCFormatC *fmt, int *datatypes, char **values,
                               uint64_t *lens, unsigned char **nullBitmap,
                               int32_t **dims, bool *isNull) {}
__attribute__((weak)) void ORCFormatEndInsertORCFormatC(ORCFormatC *fmt) {}
__attribute__((weak)) void ORCFormatEndInsertORCFormatFileC(ORCFormatC *fmt, int64_t *eof,
                                      int64_t *uncompressedEof) {}

__attribute__((weak)) ORCFormatCatchedError *ORCFormatGetErrorORCFormatC(ORCFormatC *fmt) {}

#ifdef __cplusplus
}
#endif

#endif  // STORAGE_SRC_STORAGE_CWRAPPER_ORC_FORMAT_C_H_
