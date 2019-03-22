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

#include <cstring>

#include "dbcommon/common/vector-transformer.h"
#include "dbcommon/common/vector/variable-length-vector.h"
#include "dbcommon/function/function.h"
#include "dbcommon/function/string-binary-function.h"
#include "dbcommon/utils/string-util.h"

namespace dbcommon {

template <bool expetedMatch>
Datum string_like_proto(Datum *params, uint64_t size) {
  assert(size == 3);

  Scalar *scalarPattern = DatumGetValue<Scalar *>(params[2]);
  auto patternSrc = DatumGetValue<char *>(scalarPattern->value);
  auto patternLen = scalarPattern->length;

  Object *para = DatumGetValue<Object *>(params[1]);
  Scalar *scalar = dynamic_cast<Scalar *>(para);

  bool noUnderScore = true;  // intend for optimization
  for (auto i = 0; i < patternLen; i++)
    if (patternSrc[i] == '_') noUnderScore = false;

  if (StringUtil::isAsciiEncoding(patternSrc, patternLen) && noUnderScore) {
    // ASCII pattern
    if (scalar) {
      Scalar *ret = DatumGetValue<Scalar *>(params[0]);
      if (scalar->isnull) {
        ret->isnull = true;
      } else {
        ret->isnull = false;
        auto str = DatumGetValue<char *>(scalar->value);
        bool matched = StringUtil::MatchAsciiPattern(str, scalar->length,
                                                     patternSrc, patternLen);
        ret->value = CreateDatum(static_cast<bool>(matched == expetedMatch));
      }
      return CreateDatum(ret);
    } else {
      SelectList *ret = DatumGetValue<SelectList *>(params[0]);
      ret->resize(0);

      Vector *vec = dynamic_cast<Vector *>(para);
      assert(vec->getTypeKind() == VARCHARID || vec->getTypeKind() == STRINGID);

      auto valPtrs = vec->getValPtrs();
      auto lens = vec->getLengths();
      if (vec->hasNullValue()) {
        auto nulls = vec->getNullBuffer()->getBools();
        if (vec->getSelected()) {
          auto sel = vec->getSelected();
          for (auto i = 0; i < vec->getNumOfRows(); i++) {
            auto idx = (*sel)[i];
            if (nulls[idx]) continue;

            bool matched = StringUtil::MatchAsciiPattern(
                valPtrs[idx], lens[idx], patternSrc, patternLen);
            if (matched == expetedMatch) ret->push_back(idx);
          }
        } else {
          for (auto i = 0; i < vec->getNumOfRowsPlain(); i++) {
            auto idx = i;
            if (nulls[idx]) continue;

            bool matched = StringUtil::MatchAsciiPattern(
                valPtrs[idx], lens[idx], patternSrc, patternLen);
            if (matched == expetedMatch) ret->push_back(idx);
          }
        }
      } else {
        if (vec->getSelected()) {
          auto sel = vec->getSelected();
          for (auto i = 0; i < vec->getNumOfRows(); i++) {
            auto idx = (*sel)[i];
            bool matched = StringUtil::MatchAsciiPattern(
                valPtrs[idx], lens[idx], patternSrc, patternLen);
            if (matched == expetedMatch) ret->push_back(idx);
          }
        } else {
          for (auto i = 0; i < vec->getNumOfRowsPlain(); i++) {
            auto idx = i;
            bool matched = StringUtil::MatchAsciiPattern(
                valPtrs[idx], lens[idx], patternSrc, patternLen);
            if (matched == expetedMatch) ret->push_back(idx);
          }
        }
      }

      return CreateDatum(ret);
    }
  } else {
    // UTF8 pattern
    if (scalar) {
      Scalar *ret = DatumGetValue<Scalar *>(params[0]);
      if (scalar->isnull) {
        ret->isnull = true;
      } else {
        ret->isnull = false;
        auto str = DatumGetValue<char *>(scalar->value);
        bool matched = StringUtil::MatchUtf8Pattern(str, scalar->length,
                                                    patternSrc, patternLen);
        ret->value = CreateDatum(static_cast<bool>(matched == expetedMatch));
      }
      return CreateDatum(ret);
    } else {
      SelectList *ret = DatumGetValue<SelectList *>(params[0]);
      ret->resize(0);

      Vector *vec = dynamic_cast<Vector *>(para);
      assert(vec->getTypeKind() == VARCHARID || vec->getTypeKind() == STRINGID);

      auto valPtrs = vec->getValPtrs();
      auto lens = vec->getLengths();
      if (vec->hasNullValue()) {
        auto nulls = vec->getNullBuffer()->getBools();
        if (vec->getSelected()) {
          auto sel = vec->getSelected();
          for (auto i = 0; i < vec->getNumOfRows(); i++) {
            auto idx = (*sel)[i];
            if (nulls[idx]) continue;

            bool matched = StringUtil::MatchUtf8Pattern(valPtrs[idx], lens[idx],
                                                        patternSrc, patternLen);
            if (matched == expetedMatch) ret->push_back(idx);
          }
        } else {
          for (auto i = 0; i < vec->getNumOfRowsPlain(); i++) {
            auto idx = i;
            if (nulls[idx]) continue;

            bool matched = StringUtil::MatchUtf8Pattern(valPtrs[idx], lens[idx],
                                                        patternSrc, patternLen);
            if (matched == expetedMatch) ret->push_back(idx);
          }
        }
      } else {
        if (vec->getSelected()) {
          auto sel = vec->getSelected();
          for (auto i = 0; i < vec->getNumOfRows(); i++) {
            auto idx = (*sel)[i];
            bool matched = StringUtil::MatchUtf8Pattern(valPtrs[idx], lens[idx],
                                                        patternSrc, patternLen);
            if (matched == expetedMatch) ret->push_back(idx);
          }
        } else {
          for (auto i = 0; i < vec->getNumOfRowsPlain(); i++) {
            auto idx = i;
            bool matched = StringUtil::MatchUtf8Pattern(valPtrs[idx], lens[idx],
                                                        patternSrc, patternLen);
            if (matched == expetedMatch) ret->push_back(idx);
          }
        }
      }

      return CreateDatum(ret);
    }
  }
}

template <bool expetedMatch>
Datum bpchar_like_proto(Datum *params, uint64_t size) {
  assert(size == 3);

  Scalar *scalarPattern = DatumGetValue<Scalar *>(params[2]);
  auto patternSrc = DatumGetValue<char *>(scalarPattern->value);
  auto patternLen = scalarPattern->length;

  Object *para = DatumGetValue<Object *>(params[1]);
  Scalar *scalar = dynamic_cast<Scalar *>(para);

  bool noUnderScore = true;  // intend for optimization
  for (auto i = 0; i < patternLen; i++)
    if (patternSrc[i] == '_') noUnderScore = false;

  if (StringUtil::isAsciiEncoding(patternSrc, patternLen) && noUnderScore) {
    // ASCII pattern
    if (scalar) {
      Scalar *ret = DatumGetValue<Scalar *>(params[0]);
      if (scalar->isnull) {
        ret->isnull = true;
      } else {
        ret->isnull = false;
        auto str = DatumGetValue<char *>(scalar->value);
        auto strLen = scalar->length;
        bool matched =
            StringUtil::MatchAsciiPattern(str, strLen, patternSrc, patternLen);
        ret->value = CreateDatum(static_cast<bool>(matched == expetedMatch));
      }
      return CreateDatum(ret);
    } else {
      SelectList *ret = DatumGetValue<SelectList *>(params[0]);
      ret->resize(0);

      BlankPaddedCharVector *vec = dynamic_cast<BlankPaddedCharVector *>(para);
      assert(vec);

      auto len = vec->getMaxLenModifier();
      auto valPtrs = vec->getValPtrs();
      if (vec->hasNullValue()) {
        auto nulls = vec->getNullBuffer()->getBools();
        if (vec->getSelected()) {
          auto sel = vec->getSelected();
          for (auto i = 0; i < vec->getNumOfRows(); i++) {
            auto idx = (*sel)[i];
            if (nulls[idx]) continue;
            bool matched = StringUtil::MatchAsciiPattern(
                valPtrs[idx], len, patternSrc, patternLen);
            if (matched == expetedMatch) ret->push_back(idx);
          }
        } else {
          for (auto i = 0; i < vec->getNumOfRowsPlain(); i++) {
            auto idx = i;
            if (nulls[idx]) continue;
            bool matched = StringUtil::MatchAsciiPattern(
                valPtrs[idx], len, patternSrc, patternLen);
            if (matched == expetedMatch) ret->push_back(idx);
          }
        }
      } else {
        if (vec->getSelected()) {
          auto sel = vec->getSelected();
          for (auto i = 0; i < vec->getNumOfRows(); i++) {
            auto idx = (*sel)[i];
            bool matched = StringUtil::MatchAsciiPattern(
                valPtrs[idx], len, patternSrc, patternLen);
            if (matched == expetedMatch) ret->push_back(idx);
          }
        } else {
          for (auto i = 0; i < vec->getNumOfRowsPlain(); i++) {
            auto idx = i;
            bool matched = StringUtil::MatchAsciiPattern(
                valPtrs[idx], len, patternSrc, patternLen);
            if (matched == expetedMatch) ret->push_back(idx);
          }
        }
      }

      return CreateDatum(ret);
    }
  } else {
    // UTF8 pattern
    if (scalar) {
      Scalar *ret = DatumGetValue<Scalar *>(params[0]);
      if (scalar->isnull) {
        ret->isnull = true;
      } else {
        ret->isnull = false;
        auto str = DatumGetValue<char *>(scalar->value);
        auto strLen = scalar->length;
        bool matched =
            StringUtil::MatchUtf8Pattern(str, strLen, patternSrc, patternLen);
        ret->value = CreateDatum(static_cast<bool>(matched == expetedMatch));
      }
      return CreateDatum(ret);
    } else {
      SelectList *ret = DatumGetValue<SelectList *>(params[0]);
      ret->resize(0);

      BlankPaddedCharVector *vec = dynamic_cast<BlankPaddedCharVector *>(para);
      assert(vec);

      auto len = vec->getMaxLenModifier();
      auto valPtrs = vec->getValPtrs();
      if (vec->hasNullValue()) {
        auto nulls = vec->getNullBuffer()->getBools();
        if (vec->getSelected()) {
          auto sel = vec->getSelected();
          for (auto i = 0; i < vec->getNumOfRows(); i++) {
            auto idx = (*sel)[i];
            if (nulls[idx]) continue;
            bool matched = StringUtil::MatchUtf8Pattern(valPtrs[idx], len,
                                                        patternSrc, patternLen);
            if (matched == expetedMatch) ret->push_back(idx);
          }
        } else {
          for (auto i = 0; i < vec->getNumOfRowsPlain(); i++) {
            auto idx = i;
            if (nulls[idx]) continue;
            bool matched = StringUtil::MatchUtf8Pattern(valPtrs[idx], len,
                                                        patternSrc, patternLen);
            if (matched == expetedMatch) ret->push_back(idx);
          }
        }
      } else {
        if (vec->getSelected()) {
          auto sel = vec->getSelected();
          for (auto i = 0; i < vec->getNumOfRows(); i++) {
            auto idx = (*sel)[i];
            bool matched = StringUtil::MatchUtf8Pattern(valPtrs[idx], len,
                                                        patternSrc, patternLen);
            if (matched == expetedMatch) ret->push_back(idx);
          }
        } else {
          for (auto i = 0; i < vec->getNumOfRowsPlain(); i++) {
            auto idx = i;
            bool matched = StringUtil::MatchUtf8Pattern(valPtrs[idx], len,
                                                        patternSrc, patternLen);
            if (matched == expetedMatch) ret->push_back(idx);
          }
        }
      }

      return CreateDatum(ret);
    }
  }
}

Datum string_like(Datum *params, uint64_t size) {
  return string_like_proto<true>(params, size);
}

Datum bpchar_like(Datum *params, uint64_t size) {
  return bpchar_like_proto<true>(params, size);
}

Datum string_not_like(Datum *params, uint64_t size) {
  return string_like_proto<false>(params, size);
}

Datum bpchar_not_like(Datum *params, uint64_t size) {
  return bpchar_like_proto<false>(params, size);
}

class utf8ptr {
 public:
  explicit utf8ptr(const char *p) : p_(p) {}
  operator const char *() { return p_; }

  utf8ptr &operator++() {
    p_ += utf8_mblen(p_);
    return *this;
  }

  utf8ptr &operator+=(const int &len) {
    int times = len;
    while (times--) p_ += utf8_mblen(p_);
    return *this;
  }

  char *get() { return const_cast<char *>(p_); }

  int characterLength(const char *p) {
    int len = 0;
    const char *tmp = p_;
    while (tmp != p) {
      tmp += utf8_mblen(tmp);
      len++;
    }
    return len;
  }

  int characterLength(const int &len) {
    int ret = 0, lenth = len;
    const char *tmp = p_;
    while (lenth > 0) {
      int tLen = utf8_mblen(tmp);
      lenth -= tLen;
      tmp += tLen;
      ret++;
    }
    return ret;
  }

  int byteLength(const int &len) {
    int ret = 0;
    int times = len;
    const char *tmp = p_;
    while (times--) {
      int tLen = utf8_mblen(tmp);
      tmp += tLen;
      ret += tLen;
    }
    return ret;
  }

 private:
  const char *p_;
};

Datum string_char_length(Datum *params, uint64_t size) {
  auto charLength = [](ByteBuffer &buf, text str) -> int32_t {
    utf8ptr p(str.val);
    uint64_t len = 0;
    while (p < str.val + str.length) {
      ++p;
      ++len;
    }
    return len;
  };
  return one_param_bind<int32_t, text>(params, size, charLength);
}

Datum bpchar_char_length(Datum *params, uint64_t size) {
  return string_char_length(params, size);
}

Datum string_lower(Datum *params, uint64_t size) {
  auto lower = [](ByteBuffer &buf, text str) {
    buf.resize(buf.size() + str.length);
    std::transform(str.val, str.val + str.length,
                   const_cast<char *>(buf.tail()) - str.length, [](char c) {
                     return 'A' <= c && c <= 'Z' ? c - ('A' - 'a') : c;
                   });
    return text(nullptr, str.length);
  };
  return one_param_bind<text, text>(params, size, lower);
}

Datum string_upper(Datum *params, uint64_t size) {
  auto upper = [](ByteBuffer &buf, text str) {
    buf.resize(buf.size() + str.length);
    std::transform(str.val, str.val + str.length,
                   const_cast<char *>(buf.tail()) - str.length, [](char c) {
                     return 'a' <= c && c <= 'z' ? c + ('A' - 'a') : c;
                   });
    return text(nullptr, str.length);
  };
  return one_param_bind<text, text>(params, size, upper);
}

Datum string_concat(Datum *params, uint64_t size) {
  auto concat = [](ByteBuffer &buf, text str1, text str2) {
    buf.resize(buf.size() + str1.length + str2.length);
    char *ret = const_cast<char *>(buf.tail() - str1.length - str2.length);
    std::transform(str1.val, str1.val + str1.length, ret,
                   [](char tmp) { return tmp; });
    std::transform(str2.val, str2.val + str2.length, ret + str1.length,
                   [](char tmp) { return tmp; });
    return text(nullptr, str1.length + str2.length);
  };
  return two_params_bind<text, text, text>(params, size, concat);
}

int32_t kmpPos(const char *str, const char *subStr, uint64_t len,
               uint64_t subLen, dbcommon::ByteBuffer *kmpPosBuf) {
  if (len < subLen) return 0;
  kmpPosBuf->resize(subLen * sizeof(int32_t));

  int32_t *__restrict__ next = reinterpret_cast<int32_t *>(kmpPosBuf->data());

  next[0] = -1;
  int i = 0, j = -1;
  while (i < subLen - 1) {
    if (j == -1 || subStr[i] == subStr[j])
      next[++i] = ++j;
    else
      j = next[j];
  }

  i = 0;
  j = 0;
  int lLen = len, sLen = subLen;
  while (i < lLen && j < sLen) {
    if (j == -1 || subStr[j] == str[i]) {
      i++;
      j++;
    } else {
      j = next[j];
    }
  }
  if (j == sLen)
    return i - j + 1;
  else
    return 0;
}

int32_t naivePos(const char *str, const char *subStr, uint64_t len,
                 uint64_t subLen) {
  if (len < subLen) return 0;

  int times = len - subLen;
  for (int i = 0; i <= times; i++) {
    bool flag = true;
    for (int j = 0; j < subLen; j++)
      if (str[i + j] != subStr[j]) {
        flag = false;
        break;
      }
    if (flag) return i + 1;
  }
  return 0;
}

Datum string_position(Datum *params, uint64_t size) {
  auto subpos = [](ByteBuffer &buf, text src, text sub) -> int32_t {
    int32_t byteLen = 0;
    if (sub.length < 15) {
      byteLen = naivePos(src.val, sub.val, src.length, sub.length);
    } else {
      dbcommon::ByteBuffer kmpPosBuf(true);
      byteLen = kmpPos(src.val, sub.val, src.length, sub.length, &kmpPosBuf);
    }
    utf8ptr utfStrPtr(src.val);
    return byteLen ? utfStrPtr.characterLength(byteLen - 1) + 1 : 0;
  };
  return two_params_bind<int32_t, text, text>(params, size, subpos);
}

Datum string_initcap(Datum *params, uint64_t size) {
  auto initcap = [](ByteBuffer &buf, text str) {
    buf.resize(buf.size() + str.length);
    char *ret = const_cast<char *>(buf.tail() - str.length);

    char last = ' ';
    int times = str.length;
    while (times--) {
      if (((unsigned int)((last | 0x20) - 'a') >= 26u &&
           (unsigned int)(last - '0') >= 10u) &&
          !(last & ~0x7F)) {
        auto low2up = [](char c) {
          return 'a' <= c && c <= 'z' ? c + ('A' - 'a') : c;
        };
        *ret++ = low2up(*str.val);
        last = *str.val++;
      } else {
        auto up2low = [](char c) {
          return 'A' <= c && c <= 'Z' ? c - ('A' - 'a') : c;
        };
        *ret++ = up2low(*str.val);
        last = *str.val++;
      }
    }
    return text(nullptr, str.length);
  };
  return one_param_bind<text, text>(params, size, initcap);
}

inline void fixRange(int32_t strlen, int32_t *subpos, int32_t *sublen) {
  int32_t zero = 0;
  *subpos = *subpos - 1;
  if (*subpos < zero) {
    *subpos += *sublen;
    if (*subpos < zero) {
      *subpos = zero;
      *sublen = zero;
    } else {
      *sublen = *subpos;
      *subpos = zero;
    }
  }
  if (*subpos + *sublen > strlen) *sublen = strlen - *subpos;
  if (*sublen < zero) *sublen = zero;
}

//  only support utf-8 now
Datum string_substring(Datum *params, uint64_t size) {
  auto substr = [](ByteBuffer &buf, text src, int32_t pos, int32_t len) {
    if (len < 0) {
      LOG_ERROR(ERRCODE_SUBSTRING_ERROR,
                "negative substring length not allowed");
    }
    utf8ptr utfStrPtr(src.val);
    int32_t utfStrLen = utfStrPtr.characterLength(src.val + src.length);
    fixRange(utfStrLen, &pos, &len);
    utfStrPtr += pos;
    char *srcBegin = utfStrPtr.get();
    utfStrPtr += len;
    int32_t retByteLen = utfStrPtr.get() - srcBegin;
    buf.resize(buf.size() + retByteLen);
    char *ret = const_cast<char *>(buf.tail() - retByteLen);
    std::transform(srcBegin, srcBegin + retByteLen, ret,
                   [](char tmp) { return tmp; });
    return text(nullptr, retByteLen);
  };
  return three_params_bind<text, text, int32_t, int32_t>(params, size, substr);
}

Datum string_substring_nolen(Datum *params, uint64_t size) {
  auto substrnolen = [](ByteBuffer &buf, text str, int32_t pos) {
    if (--pos < 0) pos = 0;
    utf8ptr utfStrPtr(str.val);
    utfStrPtr += pos;
    char *strBegin = utfStrPtr.get();
    int len = str.val + str.length - strBegin;
    if (len < 0) len = 0;
    buf.resize(buf.size() + len);
    char *ret = const_cast<char *>(buf.tail() - len);
    std::transform(strBegin, strBegin + len, ret, [](char tmp) { return tmp; });
    return text(nullptr, len);
  };
  return two_params_bind<text, text, int32_t>(params, size, substrnolen);
}

inline int32_t myAscii(const unsigned char *data) {
  int32_t retval = 0;
  if (*data > 0x7F) {
    int tsize = 0;
    if (*data >= 0xF0) {
      retval = *data & 0x07;
      tsize = 3;
    } else if (*data >= 0xE0) {
      retval = *data & 0x0F;
      tsize = 2;
    } else {
      assert(*data > 0xC0);
      retval = *data & 0x1F;
      tsize = 1;
    }
    while (tsize--) {
      retval = (retval << 6) + (*++data & 0x3F);
    }
  } else {
    retval = (int32_t)*data;
  }
  return retval;
}

Datum string_ascii(Datum *params, uint64_t size) {
  auto ascii = [](ByteBuffer &buf, text str) -> int32_t {
    unsigned char *srcval = (unsigned char *)const_cast<char *>(str.val);
    if (str.length > 0)
      return myAscii(srcval);
    else
      return 0;
  };
  return one_param_bind<int32_t, text>(params, size, ascii);
}

/*
 * Convert a VARCHAR type to the specified size.
 *
 * N.B. currently does not handle the toast string
 */
Datum string_varchar(Datum *params, uint64_t size) {
  auto varchar = [](ByteBuffer &buf, text str, int32_t len, bool exp) {
    len = TypeModifierUtil::getMaxLen(len);
    utf8ptr utfStrPtr(str.val);
    int32_t utfStrLen = utfStrPtr.characterLength(str.val + str.length);
    if (utfStrLen > len) {
      if (exp == false) {
        auto p = str.length - 1;
        int32_t retByteLen = utfStrPtr.byteLength(len);
        while (p >= retByteLen) {
          if (str.val[p--] != ' ') {
            LOG_ERROR(ERRCODE_STRING_DATA_RIGHT_TRUNCATION,
                      "value too long for type character varying(%d)", len);
          }
        }
      }
    } else {
      len = utfStrLen;
    }
    int32_t retByteLen = utfStrPtr.byteLength(len);
    buf.resize(buf.size() + retByteLen);
    char *ret = const_cast<char *>(buf.tail() - retByteLen);
    std::transform(utfStrPtr.get(), utfStrPtr.get() + retByteLen, ret,
                   [](char c) { return c; });
    return text(nullptr, retByteLen);
  };
  return three_params_bind<text, text, int32_t, bool>(params, size, varchar);
}

enum direction { left = 0, right, both };
template <direction dir>
Datum string_trim_blank(Datum *params, uint64_t size) {
  auto trim = [](ByteBuffer &buf, text str) {
    int l = 0, r = str.length - 1;
    if (dir == direction::left || dir == direction::both) {
      while (l <= r && str.val[l] == ' ') l++;
    }
    if (dir == direction::right || dir == direction::both) {
      while (l <= r && str.val[r] == ' ') r--;
    }
    int len = r - l + 1;
    if (len < 0) len = 0;
    buf.resize(buf.size() + len);
    char *ret = const_cast<char *>(buf.tail() - len);
    std::transform(str.val + l, str.val + r + 1, ret, [](char c) { return c; });

    return text(nullptr, len);
  };
  return one_param_bind<text, text>(params, size, trim);
}

template <direction dir>
Datum string_trim_chars(Datum *params, uint64_t size) {
  auto trim = [](ByteBuffer &buf, text str, text chr) {
    int l = 0, r = str.length - 1;
    if (dir == direction::left || dir == direction::both) {
      std::string s(const_cast<char *>(chr.val), chr.length);
      while (l <= r && s.find(str.val[l]) != std::string::npos) l++;
    }
    if (dir == direction::right || dir == direction::both) {
      std::string s(const_cast<char *>(chr.val), chr.length);
      while (l <= r && s.find(str.val[r]) != std::string::npos) r--;
    }
    int len = r - l + 1;
    if (len < 0) len = 0;
    buf.resize(buf.size() + len);
    char *ret = const_cast<char *>(buf.tail() - len);
    std::transform(str.val + l, str.val + r + 1, ret, [](char c) { return c; });

    return text(nullptr, len);
  };
  return two_params_bind<text, text, text>(params, size, trim);
}

// CASE 1: trim left blank
Datum string_ltrim_blank(Datum *params, uint64_t size) {
  return string_trim_blank<direction::left>(params, size);
}

// CASE 2: trim left character
Datum string_ltrim_chars(Datum *params, uint64_t size) {
  return string_trim_chars<direction::left>(params, size);
}

// CASE 3: trim right blank
Datum string_rtrim_blank(Datum *params, uint64_t size) {
  return string_trim_blank<direction::right>(params, size);
}

// CASE 4: trim right character
Datum string_rtrim_chars(Datum *params, uint64_t size) {
  return string_trim_chars<direction::right>(params, size);
}

// CASE 5: trim both blank
Datum string_btrim_blank(Datum *params, uint64_t size) {
  return string_trim_blank<direction::both>(params, size);
}

// CASE 6: trim both character
Datum string_btrim_chars(Datum *params, uint64_t size) {
  return string_trim_chars<direction::both>(params, size);
}

Datum string_repeat(Datum *params, uint64_t size) {
  auto demo_func = [](ByteBuffer &buf, text str, int32_t dupLen) {
    int32_t retLen = str.length * dupLen;
    if (dupLen && (retLen / dupLen) != str.length) {
      LOG_ERROR(ERRCODE_PROGRAM_LIMIT_EXCEEDED, "requested length too large");
    }

    buf.resize(buf.size() + retLen);
    char *retBufPtr = const_cast<char *>(buf.tail() - retLen);
    while (dupLen--) {
      std::transform(str.val, str.val + str.length, retBufPtr,
                     [](char c) { return c; });
      retBufPtr += str.length;
    }
    return text(nullptr, retLen);
  };

  return two_params_bind<text, text, int32_t>(params, size, demo_func);
}

Datum string_chr(Datum *params, uint64_t size) {
  auto chr = [](ByteBuffer &buf, int32_t val) {
    int len = 0;
    char wch[4];
    if (val > 0x7F) {
      if (val > 0x001fffff) {
        LOG_ERROR(ERRCODE_PROGRAM_LIMIT_EXCEEDED,
                  "requested character too large for encoding: %u", val);
      }

      if (val > 0xffff) {
        len = 4;
      } else if (val > 0x07ff) {
        len = 3;
      } else {
        len = 2;
      }

      if (len == 2) {
        wch[0] = 0xC0 | ((val >> 6) & 0x1F);
        wch[1] = 0x80 | (val & 0x3F);
      } else if (len == 3) {
        wch[0] = 0xE0 | ((val >> 12) & 0x0F);
        wch[1] = 0x80 | ((val >> 6) & 0x3F);
        wch[2] = 0x80 | (val & 0x3F);
      } else {
        wch[0] = 0xF0 | ((val >> 18) & 0x07);
        wch[1] = 0x80 | ((val >> 12) & 0x3F);
        wch[2] = 0x80 | ((val >> 6) & 0x3F);
        wch[3] = 0x80 | (val & 0x3F);
      }
    } else {
      if (val == 0) {
        LOG_ERROR(ERRCODE_PROGRAM_LIMIT_EXCEEDED,
                  "null character not permitted");
      }

      bool isMB = true;
      if ((isMB && (val > 127)) || (!isMB && (val > 255))) {
        // TODO(zdh): error for Multi-byte encoding except utf8
        // LOG_ERROR(ERRCODE_PROGRAM_LIMIT_EXCEEDED,
        //           "requested character too large for encoding: %d", val);
      }
      len = 1;
      wch[0] = static_cast<char>(val);
    }
    buf.resize(buf.size() + len);
    char *ret = const_cast<char *>(buf.tail() - len);
    strncpy(ret, wch, len);
    return text(nullptr, len);
  };
  return one_param_bind<text, int32_t>(params, size, chr);
}

Datum string_bpchar(Datum *params, uint64_t size) {
  auto bpchar = [](ByteBuffer &buf, text str, int32_t len, bool exp) {
    len = TypeModifierUtil::getMaxLen(len);
    utf8ptr utfStrPtr(str.val);
    int32_t characterLen = utfStrPtr.characterLength(str.val + str.length);
    if (characterLen > len) {
      if (exp == false) {
        auto p = str.length - 1;
        int32_t byteLen = utfStrPtr.byteLength(len);
        while (p >= len) {
          if (str.val[p--] != ' ') {
            LOG_ERROR(ERRCODE_STRING_DATA_RIGHT_TRUNCATION,
                      "value too long for type character(%d)", len);
          }
        }
      }
    }
    int32_t copyLen =
        characterLen > len ? utfStrPtr.byteLength(len) : str.length;
    int32_t retByteLen = characterLen > len ? utfStrPtr.byteLength(len)
                                            : str.length + len - characterLen;
    buf.resize(buf.size() + retByteLen);
    char *ret = const_cast<char *>(buf.tail() - retByteLen);
    std::transform(str.val, str.val + copyLen, ret, [](char c) { return c; });

    ret += copyLen;
    while (copyLen++ < retByteLen) *ret++ = ' ';
    return text(nullptr, retByteLen);
  };
  return three_params_bind<text, text, int32_t, bool>(params, size, bpchar);
}

template <direction dir>
Datum string_pad_blank(Datum *params, uint64_t size) {
  auto pad = [](ByteBuffer &buf, text str, int32_t len) {
    utf8ptr utfStrPtr(str.val);
    int32_t characterLen = utfStrPtr.characterLength(str.val + str.length);
    int32_t retByteLen = str.length;
    if (characterLen < len)
      retByteLen += len - characterLen;
    else
      retByteLen = utfStrPtr.byteLength(len);
    buf.resize(buf.size() + retByteLen);
    char *ret = const_cast<char *>(buf.tail() - retByteLen);

    if (dir == direction::left) {
      int32_t remainder = retByteLen - str.length;
      remainder = remainder < 0 ? 0 : remainder;
      std::memset(ret, ' ', remainder);
      ret += remainder;
    }

    int32_t writeLen = str.length < retByteLen ? str.length : retByteLen;
    for (int i = 0; i < writeLen; i++) *ret++ = str.val[i];

    if (dir == direction::right) {
      int32_t remainder = retByteLen - str.length;
      remainder = remainder < 0 ? 0 : remainder;
      std::memset(ret, ' ', remainder);
    }

    return text(nullptr, retByteLen);
  };
  return two_params_bind<text, text, int32_t>(params, size, pad);
}

Datum string_lpad_nofill(Datum *params, uint64_t size) {
  return string_pad_blank<direction::left>(params, size);
}

Datum string_rpad_nofill(Datum *params, uint64_t size) {
  return string_pad_blank<direction::right>(params, size);
}

template <direction dir>
Datum string_pad_chars(Datum *params, uint64_t size) {
  auto pad = [](ByteBuffer &buf, text str, int32_t len, text fil) {
    utf8ptr utfStrPtr(str.val);
    utf8ptr utfFilPtr(fil.val);
    int32_t strCharLen = utfStrPtr.characterLength(str.val + str.length);
    int32_t filCharLen = utfFilPtr.characterLength(fil.val + fil.length);
    int32_t retByteLen = str.length;
    if (strCharLen >= len) {
      retByteLen = utfStrPtr.byteLength(len);
    } else {
      int rem = len - strCharLen;
      while (rem >= filCharLen) {
        retByteLen += fil.length;
        rem -= filCharLen;
      }
      retByteLen += utfFilPtr.byteLength(rem);
    }
    buf.resize(buf.size() + retByteLen);
    char *ret = const_cast<char *>(buf.tail() - retByteLen);

    if (dir == direction::left) {
      int32_t remainder = len - strCharLen;
      if (fil.length == 1 && remainder > 0) {
        std::memset(ret, *fil.val, remainder);
        ret += remainder;
      } else {
        while (remainder > 0) {
          if (remainder >= filCharLen) {
            for (int i = 0; i < fil.length; i++) *ret++ = fil.val[i];
          } else {
            int32_t fillLen = utfFilPtr.byteLength(remainder);
            for (int i = 0; i < fillLen; i++) *ret++ = fil.val[i];
          }
          remainder -= filCharLen;
        }
      }
    }

    int32_t writeLen = str.length < retByteLen ? str.length : retByteLen;
    for (int i = 0; i < writeLen; i++) *ret++ = str.val[i];

    if (dir == direction::right) {
      int32_t remainder = len - strCharLen;
      if (fil.length == 1 && remainder > 0) {
        std::memset(ret, *fil.val, remainder);
        ret += remainder;
      } else {
        while (remainder > 0) {
          if (remainder >= filCharLen) {
            for (int i = 0; i < fil.length; i++) *ret++ = fil.val[i];
          } else {
            int32_t fillLen = utfFilPtr.byteLength(remainder);
            for (int i = 0; i < fillLen; i++) *ret++ = fil.val[i];
          }
          remainder -= filCharLen;
        }
      }
    }

    return text(nullptr, retByteLen);
  };
  return three_params_bind<text, text, int32_t, text>(params, size, pad);
}

Datum string_lpad(Datum *params, uint64_t size) {
  return string_pad_chars<direction::left>(params, size);
}

Datum string_rpad(Datum *params, uint64_t size) {
  return string_pad_chars<direction::right>(params, size);
}

}  // namespace dbcommon
