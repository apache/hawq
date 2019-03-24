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

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ERROR_MESSAGE_BUFFER_SIZE
#define ERROR_MESSAGE_BUFFER_SIZE 4096
#endif

struct ORCFormatC;

typedef struct ORCFormatC ORCFormatC;

typedef struct ORCFormatCatchedError {
  int errCode;
  char errMessage[ERROR_MESSAGE_BUFFER_SIZE];
} ORCFormatCatchedError;

typedef struct ORCFormatFileSplit {
  char *fileName;
  int64_t start;
  int64_t len;
} ORCFormatFileSplit;

#define ORCFormatType 'o'

// tableOptions in json format
ORCFormatC *ORCFormatNewORCFormatC(const char *tableOptions, int segno);
void ORCFormatFreeORCFormatC(ORCFormatC **fmt);

void ORCFormatBeginORCFormatC(ORCFormatC *fmt, ORCFormatFileSplit *splits,
                              int numSplits, bool *columnsToRead,
                              char **columnName, int *columnDatatype,
                              uint64_t *columnDatatypeMod, int numColumns);

bool ORCFormatNextORCFormatC(ORCFormatC *fmt, const char **values,
                             uint64_t *lens, bool *nulls);

void ORCFormatRescanORCFormatC(ORCFormatC *fmt);

void ORCFormatEndORCFormatC(ORCFormatC *fmt);

void ORCFormatBeginInsertORCFormatC(ORCFormatC *fmt, const char *dirFullPath,
                                    char **columnName, int *columnDatatype,
                                    uint64_t *columnDatatypeMod,
                                    int numColumns);
void ORCFormatInsertORCFormatC(ORCFormatC *fmt, int *datatypes, char **values,
                               uint64_t *lens, unsigned char **nullBitmap,
                               int32_t **dims, bool *isNull);
void ORCFormatEndInsertORCFormatC(ORCFormatC *fmt);

ORCFormatCatchedError *ORCFormatGetErrorORCFormatC(ORCFormatC *fmt);

#ifdef __cplusplus
}
#endif

#endif  // STORAGE_SRC_STORAGE_CWRAPPER_ORC_FORMAT_C_H_
