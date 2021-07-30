////////////////////////////////////////////////////////////////////////////
// Copyright 2018, Oushu Inc.
// All rights reserved.
//
// Author:
////////////////////////////////////////////////////////////////////////////

#ifndef STORAGE_SRC_STORAGE_CWRAPPER_HIVE_FILE_SYSTEM_C_H_
#define STORAGE_SRC_STORAGE_CWRAPPER_HIVE_FILE_SYSTEM_C_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int errorCode;
  const char* errorMessage;
} statusHiveC;

__attribute__((weak)) void getHiveDataDirectoryC(const char* host, int port, const char* dbname,
                           const char* tblname, const char** hiveUrl,
                           statusHiveC* status);

#ifdef __cplusplus
}
#endif

#endif  // STORAGE_SRC_STORAGE_CWRAPPER_HIVE_FILE_SYSTEM_C_H_
