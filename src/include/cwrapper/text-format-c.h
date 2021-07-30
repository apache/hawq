///////////////////////////////////////////////////////////////////////////////
// Copyright 2016, Oushu Inc.
// All rights reserved.
//
// Author:
///////////////////////////////////////////////////////////////////////////////

#ifndef STORAGE_SRC_STORAGE_CWRAPPER_TEXT_FORMAT_C_H_
#define STORAGE_SRC_STORAGE_CWRAPPER_TEXT_FORMAT_C_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ERROR_MESSAGE_BUFFER_SIZE
#define ERROR_MESSAGE_BUFFER_SIZE 4096
#endif

struct TextFormatC;

typedef struct TextFormatC TextFormatC;

typedef struct TextFormatCatchedError {
  int errCode;
  char errMessage[ERROR_MESSAGE_BUFFER_SIZE];
} TextFormatCatchedError;

typedef struct TextFormatFileSplit {
  char *fileName;
  int64_t start;
  int64_t len;
} TextFormatFileSplit;

#define TextFormatTypeCSV 'c'
#define TextFormatTypeTXT 't'

// fmtType: c->csv, t->text, tableOptions in json format
__attribute__((weak)) TextFormatC *TextFormatNewTextFormatC(char fmtType, const char *tableOptions) {}
__attribute__((weak)) void TextFormatFreeTextFormatC(TextFormatC **fmt) {}

__attribute__((weak)) void TextFormatBeginTextFormatC(TextFormatC *fmt, TextFormatFileSplit *splits,
                                int numSplits, bool *columnsToRead,
                                char **columnName, int numColumns) {}
// each call returns a tuple, false means termination
// caller should free each *values: delete [] *values
__attribute__((weak)) bool TextFormatNextTextFormatC(TextFormatC *fmt, const char **values,
                               uint64_t *valueStrLen, bool *isNull,
                               bool *lastInBatch) {}
__attribute__((weak)) void TextFormatCompleteNextTextFormatC(TextFormatC *fmt) {}
__attribute__((weak)) void TextFormatEndTextFormatC(TextFormatC *fmt) {}

__attribute__((weak)) void TextFormatBeginInsertTextFormatC(TextFormatC *fmt, const char *dirFullPath,
                                      char **columnName, int numColumns) {}
__attribute__((weak)) void TextFormatInsertTextFormatC(TextFormatC *fmt, const char **values,
                                 bool *isNull) {}
__attribute__((weak)) void TextFormatEndInsertTextFormatC(TextFormatC *fmt) {}

__attribute__((weak)) TextFormatCatchedError *TextFormatGetErrorTextFormatC(TextFormatC *fmt) {}

#ifdef __cplusplus
}
#endif

#endif  // STORAGE_SRC_STORAGE_CWRAPPER_TEXT_FORMAT_C_H_
