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

#ifndef STORAGE_MAGMA_FORMAT_SRC_STORAGE_CWRAPPER_MAGMA_FORMAT_C_H_
#define STORAGE_MAGMA_FORMAT_SRC_STORAGE_CWRAPPER_MAGMA_FORMAT_C_H_

#include <stdint.h>
#include <stdio.h>

#include "magma/cwrapper/magma-client-c.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ERROR_MESSAGE_BUFFER_SIZE
#define ERROR_MESSAGE_BUFFER_SIZE 4096
#endif

#define MAGMAFORMATC_BT_SIZE 2048

struct MagmaFormatC;

typedef struct MagmaFormatC MagmaFormatC;

typedef struct MagmaFormatCatchedError {
  int errCode;
  char errMessage[ERROR_MESSAGE_BUFFER_SIZE];
} MagmaFormatCatchedError;

// This is for parameterized index scan.
// We use the attnoold to find the runtime key in FormatContext's indexExpr
// during rescan, and then use it and the value to form an expression (Var op
// Const), so that magma can do index scan.
typedef struct MagmaRuntimeKey {
  int flag;          // It is from ScanKey's sk_flag in PG.
  int16_t attnoold;  // Actually column's attribute number of the inner table.
  char *value;       // The value of this runtime key.
} MagmaRuntimeKey;

typedef struct MagmaRuntimeKeys {
  MagmaRuntimeKey *keys;
  uint32_t num;
} MagmaRuntimeKeys;

// tableOptions in json format contains magma for and serializeschema
MagmaFormatC *MagmaFormatNewMagmaInsertFormatC(uint32_t relOid,
                                               MagmaSnapshot *snapshot,
                                               bool *newFormat);
MagmaFormatC *MagmaFormatNewMagmaFormatC(const char *tableOptions);
void MagmaFormatFreeMagmaFormatC(MagmaFormatC **fmt);

void MagmaFormatC_CancelMagmaClient();

void MagmaFormatC_ClearInsertClientCache();
void MagmaFormatC_CleanupClients();

void MagmaFormatC_SetupSchema(MagmaFormatC *fmt, uint32_t relOid,
                              const char *serializeSchema,
                              int serializeSchemaLen, int rangeNum,
                              bool tpformat);

void MagmaFormatC_SetupTarget(MagmaFormatC *fmt, const char *dbname,
                              const char *schemaname, const char *tablename);
void MagmaFormatC_SetupTupDesc(MagmaFormatC *fmt, int ncols, char **colnames,
                               int *coldatatypes, int64_t *columnDatatypeMod,
                               bool *isnull);
void MagmaFormatC_SetupHasher(MagmaFormatC *fmt, int32_t nDistKeyIndex,
                              int16_t *distKeyIndex, int32_t nRanges,
                              uint32_t *rangeToRgMap, int16_t nRg,
                              uint16_t *rgIds, const char **rgUrls,
                              int32_t *jumpHashMap, int32_t jumpHashMapNum);
void MagmaFormatC_SetupSnapshot(MagmaFormatC *fmt, MagmaSnapshot *snapshot);
void MagmaFormatC_SetupTransactionState(
    MagmaFormatC *fmt, MagmaTransactionState *transactionState);
void MagmaFormatC_SetTransactionAutoCommit(MagmaFormatC *fmt, bool autoCommit);

// @param shmBlockSize count in Byte
void MagmaFormatBeginScanMagmaFormatC(MagmaFormatC *fmt, bool *columnsToRead,
                                      const char *planstr, int32_t size,
                                      bool enableShm, bool skipTid,
                                      size_t shmBlockSize);
bool MagmaFormatNextMagmaFormatC(MagmaFormatC *fmt, const char **values,
                                 uint64_t *lens, bool *nulls, const char **tid);

void MagmaFormatReScanMagmaFormatC(MagmaFormatC *fmt,
                                   MagmaRuntimeKeys *runTimeKeys);

void MagmaFormatEndScanMagmaFormatC(MagmaFormatC *fmt);

void MagmaFormatStopScanMagmaFormatC(MagmaFormatC *fmt);

// table insert operation
void MagmaFormatBeginInsertMagmaFormatC(MagmaFormatC *fmt);
void MagmaFormatInsertMagmaFormatC(MagmaFormatC *fmt, char **values,
                                   uint64_t *lens, bool *isnulls);
void MagmaFormatEndInsertMagmaFormatC(MagmaFormatC *fmt);

// table update operation
void MagmaFormatBeginUpdateMagmaFormatC(MagmaFormatC *fmt);
int MagmaFormatUpdateMagmaFormatC(MagmaFormatC *fmt, char *rowid, char **values,
                                  uint64_t *lens, bool *isnulls);
int MagmaFormatEndUpdateMagmaFormatC(MagmaFormatC *fmt);

// table delete operation
void MagmaFormatBeginDeleteMagmaFormatC(MagmaFormatC *fmt);
void MagmaFormatDeleteMagmaFormatC(MagmaFormatC *fmt, char *rowid,
                                   char **values, uint64_t *lens,
                                   bool *isnulls);
void MagmaFormatEndDeleteMagmaFormatC(MagmaFormatC *fmt);

MagmaFormatCatchedError *MagmaFormatGetErrorMagmaFormatC(MagmaFormatC *fmt);

#ifdef __cplusplus
}
#endif

#endif  // STORAGE_MAGMA_FORMAT_SRC_STORAGE_CWRAPPER_MAGMA_FORMAT_C_H_"
