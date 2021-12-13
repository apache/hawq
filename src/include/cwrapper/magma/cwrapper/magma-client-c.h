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

#ifndef MAGMA_SRC_MAGMA_CWRAPPER_MAGMA_CLIENT_C_H_
#define MAGMA_SRC_MAGMA_CWRAPPER_MAGMA_CLIENT_C_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef MAGMA_MESSAGE_BUFFER_SIZE
#define MAGMA_MESSAGE_BUFFER_SIZE 4096
#endif

#define MAGMA_TP_FORMAT "magmatp"
#define MAGMA_AP_FORMAT "magmaap"
#define MAGMA_RG_PER_RANGE_MAX 3
#define MAGMA_ADDRESS_SIZE_MAX 128
#define MAGMA_UUID_SIZE_MAX 128

#define MAGMA_STORAGE_TYPE_TP "magmatp"
#define MAGMA_STORAGE_TYPE_AP "magmaap"
#define MAGMA_STORAGE_TYPE_TP_LEN (sizeof(MAGMA_STORAGE_TYPE_TP) - 1)
#define MAGMA_STORAGE_TYPE_AP_LEN (sizeof(MAGMA_STORAGE_TYPE_AP) - 1)

__attribute__((weak)) enum MAGMACLIENTC_TABLETYPE {
  MAGMACLIENTC_TABLETYPE_TP = 0,
  MAGMACLIENTC_TABLETYPE_AP = 1,
  MAGMACLIENTC_TABLETYPE_INVALID
};

__attribute__((weak)) enum ELEVEL {
  MAGMA_DEBUG,
  MAGMA_LOG,
  MAGMA_INFO,
  MAGMA_NOTICE,
  MAGMA_WARNING,
  MAGMA_ERROR
};

__attribute__((weak)) struct MagmaClientC;

__attribute__((weak)) typedef struct MagmaClientC MagmaClientC;

__attribute__((weak)) typedef uint64_t TxnIdType;

__attribute__((weak)) typedef struct MagmaResult {
  int level;
  char message[MAGMA_MESSAGE_BUFFER_SIZE];
} MagmaResult;

__attribute__((weak)) typedef struct MagmaHLC {
  uint64_t logicaltime;
  uint32_t logicalcount;
} MagmaHLC;

__attribute__((weak)) typedef struct MagmaTxnAction {
  TxnIdType txnId;
  uint8_t txnStatus;
} MagmaTxnAction;

__attribute__((weak)) typedef struct MagmaTxnActionList {
  uint64_t txnActionStartOffset;
  uint32_t txnActionSize;
  MagmaTxnAction *txnActions;
} MagmaTxnActionList;

__attribute__((weak)) typedef struct MagmaNodeC {
  char *node;
  int compactJobRunning;
  int compactJob;
  int compactActionJobRunning;
  int compactActionJob;
  char *dirs;
} MagmaNodeC;

/* magma index info */
__attribute__((weak)) typedef struct MagmaIndex {
  char *indexName;
  int *indkey;      // key columns [ + include columns]
  int colCount;     // index key columns num [ + include columns num]
  int keynums;      // index key columns num
  char *indexType;  // index type, default is btree
  bool unique;
  bool primary;
} MagmaIndex;

__attribute__((weak)) typedef struct MagmaSnapshot {
  MagmaTxnActionList txnActions;
  MagmaTxnAction currentTransaction;

  // command id in one transaction, this value should be increasing in one
  // transaction, as this is critical to the visibility logic in magma
  uint32_t cmdIdInTransaction;
} MagmaSnapshot;

__attribute__((weak)) typedef struct MagmaColumn {
  const char *name;
  int datatype;
  int64_t rawTypeMod;
  int32_t scale1;
  int32_t scale2;
  bool isnull;
  const char *defaultValue;
  bool dropped;
  int32_t primaryKeyIndex;
  int32_t distKeyIndex;
  int32_t sortKeyIndex;
  int32_t id;
} MagmaColumn;

__attribute__((weak)) typedef int BackendId;

__attribute__((weak)) typedef uint32_t LocalTransactionId;

__attribute__((weak)) typedef struct VirtualTransactionId {
  BackendId backendId;
  LocalTransactionId localTransactionId;
} VirtualTransactionId;

__attribute__((weak)) typedef uint64_t MagmaTransactionId;

__attribute__((weak)) typedef uint8_t TransactionStatus;

__attribute__((weak)) struct MagmaRgIds;

__attribute__((weak)) typedef struct MagmaRgIds MagmaRgIds;

__attribute__((weak)) typedef struct MagmaTransactionState {
  VirtualTransactionId
      virtualTransactionId;  // used for 'magma lock', generated on magma
  // client startTransaction
  MagmaTransactionId transactionId;  // useless for read only transaction
  uint32_t commandId;                // useless for read only transaction
  TransactionStatus state;
  MagmaRgIds *relatedRgIds;
  MagmaTxnAction currentTransaction;
} MagmaTransactionState;

__attribute__((weak)) typedef struct MagmaReplicaGroup {
  uint32_t id;
  uint16_t port;
  char address[MAGMA_ADDRESS_SIZE_MAX];
  char role;
} MagmaReplicaGroup;

__attribute__((weak)) typedef struct MagmaTableFullName {
  char *databaseName;
  char *schemaName;
  char *tableName;
} MagmaTableFullName;

__attribute__((weak)) typedef void *MagmaTablePtr;
__attribute__((weak)) typedef void *MagmaRangeDistPtr;
__attribute__((weak)) typedef void *MagmaRangePtr;

#ifdef __cplusplus
}
#endif

#endif  // MAGMA_SRC_MAGMA_CWRAPPER_MAGMA_CLIENT_C_H_
