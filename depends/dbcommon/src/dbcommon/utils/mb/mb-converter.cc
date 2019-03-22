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

#include "dbcommon/utils/mb/mb-converter.h"

#include "dbcommon/log/logger.h"
#include "dbcommon/utils/macro.h"

namespace dbcommon {

MbConverter::MbConverter(SupportedEncodingSet inputEncoding,
                         SupportedEncodingSet outputEncoding, bool ignoreError_,
                         uint64_t bufSize_)
    : ignoreError(ignoreError_), bufSize(bufSize_) {
  if (bufSize < 1024) {
    bufSize = 1024;
  }

  if (inputEncoding != outputEncoding) needTransCoding = true;

  iconv_t conv_ =
      ::iconv_open(SupporteEncodingName.find(outputEncoding)->second,
                   SupporteEncodingName.find(inputEncoding)->second);
  if (conv_ == reinterpret_cast<iconv_t>(-1)) {
    if (errno == EINVAL)
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "MbConverter: not supported converter from %s to %s",
                SupporteEncodingName.find(inputEncoding)->second,
                SupporteEncodingName.find(outputEncoding)->second);
    else
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "MbConverter: unknown error on construct");
  }
  iconv = conv_;
}

MbConverter::~MbConverter() { ::iconv_close(iconv); }

std::string MbConverter::convert(const std::string &input) const {
  if (!needTransCoding) return std::move(input);

  char *srcPtr = const_cast<char *>(input.data());
  size_t srcSize = input.size();

  std::vector<char> buf(bufSize);
  std::string dst;
  while (0 < srcSize) {
    char *dstPtr = &buf[0];
    size_t dstSize = buf.size();
    size_t res = ::iconv(iconv, &srcPtr, &srcSize, &dstPtr, &dstSize);
    if (res == static_cast<size_t>(-1)) {
      if (errno == E2BIG) {
        // ignore this error, it is caused by input is too large
      } else if (ignoreError) {
        // skip character
        ++srcPtr;
        --srcSize;
      } else {
        checkConvertError();
      }
    }
    dst.append(&buf[0], buf.size() - dstSize);
  }
  return dst;
}

void MbConverter::printInHex(char *src) {
  for (int i = 0; i < strlen(src); i++)
    printf("\\x%02x", *reinterpret_cast<unsigned char *>(&src[i]));
  printf("\n");
}

std::string MbConverter::canonicalizeEncodingName(const std::string &encoding) {
  return static_cast<std::string>(iconv_canonicalize(encoding.c_str()));
}
std::string MbConverter::getEncodingName(const SupportedEncodingSet encoding) {
  return static_cast<std::string>(SupporteEncodingName.find(encoding)->second);
}

SupportedEncodingSet MbConverter::checkSupportedEncoding(
    const std::string &encoding) {
  std::map<std::string, SupportedEncodingSet>::const_iterator res;
  res = SupportedEncodingMap.find(canonicalizeEncodingName(encoding));

  if (res == SupportedEncodingMap.end())
    LOG_ERROR(ERRCODE_CASE_NOT_FOUND, "encoding value \"%s\" is invalid",
              encoding.c_str());
  return res->second;
}

bool MbConverter::encodingEmbedsAscii(const SupportedEncodingSet encoding) {
  if (encoding == ENCODING_GBK || encoding == ENCODING_BIG5 ||
      encoding == ENCODING_GB18030)
    return true;
  return false;
}

int MbConverter::encodingMbLen(const SupportedEncodingSet encoding,
                               const char *mbStr) {
  int len;
  if (encoding == ENCODING_BIG5) {
    if (IS_HIGHBIT_SET(*mbStr))
      len = 2; /* kanji? */
    else
      len = 1; /* should be ASCII */
  } else if (encoding == ENCODING_GBK) {
    if (IS_HIGHBIT_SET(*mbStr))
      len = 2; /* kanji? */
    else
      len = 1; /* should be ASCII */
  } else if (encoding == ENCODING_GB18030) {
    if (!IS_HIGHBIT_SET(*mbStr)) {
      len = 1; /* ASCII */
    } else {
      if ((*(mbStr + 1) >= 0x40 && *(mbStr + 1) <= 0x7e))
        len = 2;
      else if (*(mbStr + 1) >= 0x30 && *(mbStr + 1) <= 0x39)
        len = 4;
      else
        len = 2;
    }
  } else {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "encoding %s not supported yet",
              SupporteEncodingName.find(encoding)
                  ->second);  // this line might never be invoked?
  }
  return len;
}

void MbConverter::checkConvertError() const {
  switch (errno) {
    case EILSEQ:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "MbConverter: invalid multibyte chars sequence on converting");
      break;
    case EINVAL:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "MbConverter: invalid multibyte char on converting");
      break;
    default:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "MbConverter: unknown error on converting");
  }
}

// these are the canonicalized names in libiconv that match those in PG
const std::map<std::string, SupportedEncodingSet>
    MbConverter::SupportedEncodingMap = {{"sql_ascii", ENCODING_SQL_ASCII},
                                         {"EUC-JP", ENCODING_EUC_JP},
                                         {"EUC-CN", ENCODING_EUC_CN},
                                         {"EUC-KR", ENCODING_EUC_KR},
                                         {"EUC-TW", ENCODING_EUC_TW},
                                         {"UTF-8", ENCODING_UTF8},
                                         {"ISO-8859-1", ENCODING_LATIN1},
                                         {"ISO-8859-2", ENCODING_LATIN2},
                                         {"ISO-8859-3", ENCODING_LATIN3},
                                         {"ISO-8859-4", ENCODING_LATIN4},
                                         {"ISO-8859-9", ENCODING_LATIN5},
                                         {"ISO-8859-10", ENCODING_LATIN6},
                                         {"ISO-8859-13", ENCODING_LATIN7},
                                         {"ISO-8859-14", ENCODING_LATIN8},
                                         {"latin9", ENCODING_LATIN9},
                                         {"ISO-8859-16", ENCODING_LATIN10},
                                         {"CP1256", ENCODING_WIN1256},
                                         {"CP1258", ENCODING_WIN1258},
                                         {"CP866", ENCODING_WIN866},
                                         {"CP874", ENCODING_WIN874},
                                         {"KOI8-R", ENCODING_KOI8R},
                                         {"CP1251", ENCODING_WIN1251},
                                         {"CP1252", ENCODING_WIN1252},
                                         {"iso_8859_5", ENCODING_ISO_8859_5},
                                         {"iso_8859_6", ENCODING_ISO_8859_6},
                                         {"iso_8859_7", ENCODING_ISO_8859_7},
                                         {"iso_8859_8", ENCODING_ISO_8859_8},
                                         {"CP1250", ENCODING_WIN1250},
                                         {"CP1253", ENCODING_WIN1253},
                                         {"CP1254", ENCODING_WIN1254},
                                         {"CP1255", ENCODING_WIN1255},
                                         {"CP1257", ENCODING_WIN1257},
                                         {"KOI8-U", ENCODING_KOI8U},
                                         {"SHIFT_JIS", ENCODING_SJIS},
                                         {"BIG5", ENCODING_BIG5},
                                         {"GBK", ENCODING_GBK},
                                         {"CP949", ENCODING_UHC},
                                         {"GB18030", ENCODING_GB18030},
                                         {"JOHAB", ENCODING_JOHAB}};
const std::map<SupportedEncodingSet, const char *>
    MbConverter::SupporteEncodingName = {{ENCODING_SQL_ASCII, "US-ASCII"},
                                         {ENCODING_UTF8, "UTF-8"},
                                         {ENCODING_LATIN1, "LATIN1"},
                                         {ENCODING_LATIN2, "LATIN2"},
                                         {ENCODING_LATIN3, "LATIN3"},
                                         {ENCODING_LATIN4, "LATIN4"},
                                         {ENCODING_ISO_8859_5, "ISO-8859-5"},
                                         {ENCODING_ISO_8859_6, "ISO_8859-6"},
                                         {ENCODING_ISO_8859_7, "ISO-8859-7"},
                                         {ENCODING_ISO_8859_8, "ISO-8859-8"},
                                         {ENCODING_LATIN5, "LATIN5"},
                                         {ENCODING_LATIN6, "LATIN6"},
                                         {ENCODING_LATIN7, "LATIN7"},
                                         {ENCODING_LATIN8, "LATIN8"},
                                         {ENCODING_LATIN9, "LATIN9"},
                                         {ENCODING_LATIN10, "LATIN10"},
                                         {ENCODING_KOI8R, "KOI8-R"},
                                         {ENCODING_KOI8U, "KOI8-U"},
                                         {ENCODING_WIN1250, "CP1250"},
                                         {ENCODING_WIN1251, "CP1251"},
                                         {ENCODING_WIN1252, "CP1252"},
                                         {ENCODING_WIN1253, "CP1253"},
                                         {ENCODING_WIN1254, "CP1254"},
                                         {ENCODING_WIN1255, "CP1255"},
                                         {ENCODING_WIN1256, "CP1256"},
                                         {ENCODING_WIN1257, "CP1257"},
                                         {ENCODING_WIN1258, "CP1258"},
                                         {ENCODING_WIN866, "CP866"},
                                         {ENCODING_WIN874, "CP874"},
                                         {ENCODING_EUC_CN, "EUC-CN"},
                                         {ENCODING_EUC_JP, "EUC-JP"},
                                         {ENCODING_EUC_KR, "EUC-KR"},
                                         {ENCODING_EUC_TW, "EUC-TW"},
                                         {ENCODING_SJIS, "SHIFT_JIS"},
                                         {ENCODING_BIG5, "BIG5"},
                                         {ENCODING_GBK, "GBK"},
                                         {ENCODING_UHC, "CP949"},
                                         {ENCODING_GB18030, "GB18030"},
                                         {ENCODING_JOHAB, "JOHAB"},
                                         {_ENCODING_LAST_ENCODING_, ""}};
// these are the canonicalized names in libiconv that match those in PG

}  // namespace dbcommon
