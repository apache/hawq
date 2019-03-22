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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_MB_MB_CONVERTER_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_MB_MB_CONVERTER_H_

#include <cerrno>
#include <map>
#include <string>
#include <vector>

#include "iconv.h"  //NOLINT

namespace dbcommon {

typedef enum {
  ENCODING_SQL_ASCII = 0, /* SQL/ASCII */
  ENCODING_EUC_JP,        /* EUC for Japanese */
  ENCODING_EUC_CN,        /* EUC for Chinese */
  ENCODING_EUC_KR,        /* EUC for Korean */
  ENCODING_EUC_TW,        /* EUC for Taiwan */
  // ENCODING_EUC_JIS_2004,  /* EUC-JIS-2004 */
  ENCODING_UTF8, /* Unicode UTF8 */
  // ENCODING_MULE_INTERNAL, /* Mule internal code */
  ENCODING_LATIN1,     /* ISO-8859-1 Latin 1 */
  ENCODING_LATIN2,     /* ISO-8859-2 Latin 2 */
  ENCODING_LATIN3,     /* ISO-8859-3 Latin 3 */
  ENCODING_LATIN4,     /* ISO-8859-4 Latin 4 */
  ENCODING_LATIN5,     /* ISO-8859-9 Latin 5 */
  ENCODING_LATIN6,     /* ISO-8859-10 Latin6 */
  ENCODING_LATIN7,     /* ISO-8859-13 Latin7 */
  ENCODING_LATIN8,     /* ISO-8859-14 Latin8 */
  ENCODING_LATIN9,     /* ISO-8859-15 Latin9 */
  ENCODING_LATIN10,    /* ISO-8859-16 Latin10 */
  ENCODING_WIN1256,    /* windows-1256 */
  ENCODING_WIN1258,    /* Windows-1258 */
  ENCODING_WIN866,     /* (MS-DOS CP866) */
  ENCODING_WIN874,     /* windows-874 */
  ENCODING_KOI8R,      /* KOI8-R */
  ENCODING_WIN1251,    /* windows-1251 */
  ENCODING_WIN1252,    /* windows-1252 */
  ENCODING_ISO_8859_5, /* ISO-8859-5 */
  ENCODING_ISO_8859_6, /* ISO-8859-6 */
  ENCODING_ISO_8859_7, /* ISO-8859-7 */
  ENCODING_ISO_8859_8, /* ISO-8859-8 */
  ENCODING_WIN1250,    /* windows-1250 */
  ENCODING_WIN1253,    /* windows-1253 */
  ENCODING_WIN1254,    /* windows-1254 */
  ENCODING_WIN1255,    /* windows-1255 */
  ENCODING_WIN1257,    /* windows-1257 */
  ENCODING_KOI8U,      /* KOI8-U */
  /* ENCODING_ENCODING_BE_LAST points to the above entry */

  /* followings are for client encoding only */
  ENCODING_SJIS,    /* Shift JIS (Winindows-932) */
  ENCODING_BIG5,    /* Big5 (Windows-950) */
  ENCODING_GBK,     /* GBK (Windows-936) */
  ENCODING_UHC,     /* UHC (Windows-949) */
  ENCODING_GB18030, /* GB18030 */
  ENCODING_JOHAB,   /* EUC for Korean JOHAB */
  // ENCODING_SHIFT_JIS_2004, /* Shift-JIS-2004 */
  _ENCODING_LAST_ENCODING_ /* mark only */
} SupportedEncodingSet;

class MbConverter {
 public:
  MbConverter(SupportedEncodingSet inputEncoding,
              SupportedEncodingSet outputEncoding, bool ignoreError_ = false,
              uint64_t bufSize_ = 1024);
  ~MbConverter();

  std::string convert(const std::string &input) const;

  static void printInHex(char *src);

  static std::string canonicalizeEncodingName(const std::string &encoding);

  static std::string getEncodingName(const SupportedEncodingSet encoding);

  static SupportedEncodingSet checkSupportedEncoding(
      const std::string &encoding);

  static bool encodingEmbedsAscii(const SupportedEncodingSet encoding);

  static int encodingMbLen(const SupportedEncodingSet encoding,
                           const char *mbStr);

 private:
  void checkConvertError() const;
  iconv_t iconv;
  bool ignoreError;
  uint64_t bufSize;
  bool needTransCoding = false;
  static const std::map<std::string, SupportedEncodingSet> SupportedEncodingMap;
  static const std::map<SupportedEncodingSet, const char *>
      SupporteEncodingName;
};

}  // namespace dbcommon
#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_MB_MB_CONVERTER_H_
