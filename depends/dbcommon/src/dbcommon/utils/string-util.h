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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_STRING_UTIL_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_STRING_UTIL_H_

#include <iomanip>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace dbcommon {

class StringUtil {
 public:
  StringUtil() {}
  ~StringUtil() {}

  static bool iequals(const std::string &str1, const std::string &str2);
  static void replace(std::string *subject, const std::string &search,
                      const std::string &replace);
  static std::string regexReplace(std::string *subject,
                                  const std::string &pattern,
                                  const std::string &replace);
  static void toLower(std::string *str);
  static std::string lower(const std::string &str);
  static std::string &trim(std::string &s);         // NOLINT
  static std::string &trimNewLine(std::string &s);  // NOLINT
  static std::vector<std::string> split(const std::string &s, char delimiter);
  static bool StartWith(const std::string &str, const std::string &strStart);

  template <typename T>
  static std::string toStringWithPrecision(const T value, const int n) {
    std::ostringstream out;
    out << std::setiosflags(std::ios::fixed) << std::setprecision(n) << value;
    return out.str();
  }

  static int countReplicates(const std::string &s, const std::string &sub) {
    int res = 0;
    std::size_t pos = 0;
    while ((pos = s.find(sub, pos)) != std::string::npos) {
      res++;
      pos += sub.size();
    }
    return res;
  }
  static std::string toOct(const char *srcBin, uint64_t srcLen) {
    std::string output;
    for (auto i = 0; i < srcLen; i++) {
      unsigned char byte = srcBin[i];
      if (byte == '\\') {
        output.append("\\\\");
      } else if (byte < 0x20 || byte > 0x7e) {
        output.append(1, '\\');
        output.append(1, '0' + byte / 64);
        output.append(1, '0' + byte / 8 % 8);
        output.append(1, '0' + byte % 8);
      } else {
        output.append(1, byte);
      }
    }
    return output;
  }
  static std::string toOct(const std::string &srcStr) {
    return toOct(srcStr.data(), srcStr.size());
  }

  static bool parseIpPortString(const std::string &srcStr, std::string *ip,
                                uint16_t *port) {
    size_t p = srcStr.find_last_of(':');
    if (p == std::string::npos) {
      return false;
    }
    try {
      *ip = srcStr.substr(0, p);
      *port = std::stoul(srcStr.substr(p + 1));
    } catch (...) {
      return false;
    }
    return true;
  }

  static bool isAsciiEncoding(const char *str, uint64_t len) {
    while (len != 0) {
      if (*reinterpret_cast<const uint8_t *>(str) > 0x80) return false;
      str++;
      len--;
    }
    return true;
  }

  // Check SQL LIKE matching.
  // @param t
  // @param tlen length of bytes stream
  // @param p
  // @param plen length of bytes stream
  static bool MatchUtf8Pattern(const char *t, int tlen, const char *p,
                               int plen);
  static bool MatchAsciiPattern(const char *t, int tlen, const char *p,
                                int plen);
};

inline int utf8_mblen(const char *src) {
  int len;

  auto s = reinterpret_cast<const unsigned char *>(src);
  if ((*s & 0x80) == 0)
    len = 1;
  else if ((*s & 0xe0) == 0xc0)
    len = 2;
  else if ((*s & 0xf0) == 0xe0)
    len = 3;
  else if ((*s & 0xf8) == 0xf0)
    len = 4;
#ifdef NOT_USED
  else if ((*s & 0xfc) == 0xf8)
    len = 5;
  else if ((*s & 0xfe) == 0xfc)
    len = 6;
#endif
  else
    len = 1;
  return len;
}

inline std::size_t strlen_utf8(const char *str, std::size_t len) {
  std::size_t result = 0;
  const char *ptr = str;
  const char *end = ptr + len;
  while (ptr < end) {
    int next = utf8_mblen(ptr);
    /*
    if (next == -1) {
      throw std::runtime_error("strlen_mb(): conversion error");
    }
    */
    ptr += next;
    ++result;
  }
  return result;
}

inline uint64_t bpCharTrueLen(const char *val, uint64_t len) {
  while (len != 0 && val[len - 1] == ' ') len--;
  return len;
}

inline std::string newBlankPaddedChar(const char *val, uint64_t actualLen,
                                      uint64_t expectedNChar) {
  while (actualLen != 0 && val[actualLen - 1] == ' ') --actualLen;
  uint64_t nChar = strlen_utf8(val, actualLen);
  if (nChar >= expectedNChar) {
    return std::move(std::string(val, actualLen));
  } else {
    std::string ret(val, actualLen);
    ret.append(expectedNChar - nChar, ' ');
    actualLen += expectedNChar - nChar;
    return std::move(ret);
  }
}

// todo(chiyang): the following code comes from postgres, there could be more
// optimization
inline static int wchareq(const char *p1, const char *p2) {
  int p1_len;

  /* Optimization:  quickly compare the first byte. */
  if (*p1 != *p2) return 0;

  p1_len = utf8_mblen(p1);
  if (utf8_mblen(p2) != p1_len) return 0;

  /* They are the same length */
  while (p1_len--) {
    if (*p1++ != *p2++) return 0;
  }
  return 1;
}

#define CHAREQ(p1, p2) wchareq(p1, p2)
#define LIKE_TRUE true
#define LIKE_FALSE false
#define LIKE_ABORT false

inline void NextChar(const char *&p, int &plen) {  // NOLINT
  int __l = utf8_mblen(p);
  (p) += __l;
  (plen) -= __l;
}

inline static bool MatchText(const char *t, int tlen, const char *p, int plen) {
  /* Fast path for match-everything pattern */
  if ((plen == 1) && (*p == '%')) return LIKE_TRUE;

  while ((tlen > 0) && (plen > 0)) {
    if (*p == '\\') {
      /* Next pattern char must match literally, whatever it is */
      NextChar(p, plen);
      if ((plen <= 0) || !CHAREQ(t, p)) return LIKE_FALSE;
    } else if (*p == '%') {
      /* %% is the same as % according to the SQL standard */
      /* Advance past all %'s */
      while ((plen > 0) && (*p == '%')) NextChar(p, plen);
      /* Trailing percent matches everything. */
      if (plen <= 0) return LIKE_TRUE;

      /*
       * Otherwise, scan for a text position at which we can match the
       * rest of the pattern.
       */
      while (tlen > 0) {
        /*
         * Optimization to prevent most recursion: don't recurse
         * unless first pattern char might match this text char.
         */
        if (CHAREQ(t, p) || (*p == '\\') || (*p == '_')) {
          int matched = MatchText(t, tlen, p, plen);

          if (matched != LIKE_FALSE) return matched; /* TRUE or ABORT */
        }

        NextChar(t, tlen);
      }

      /*
       * End of text with no match, so no point in trying later places
       * to start matching this pattern.
       */
      return LIKE_ABORT;
    } else if ((*p != '_') && !CHAREQ(t, p)) {
      /*
       * Not the single-character wildcard and no explicit match? Then
       * time to quit...
       */
      return LIKE_FALSE;
    }

    NextChar(t, tlen);
    NextChar(p, plen);
  }

  if (tlen > 0) return LIKE_FALSE; /* end of pattern, but not of text */

  /* End of input string.  Do we have matching pattern remaining? */
  while ((plen > 0) && (*p == '%')) /* allow multiple %'s at end of  //NOLINT
                                     * pattern */
    NextChar(p, plen);
  if (plen <= 0) return LIKE_TRUE;

  /*
   * End of text with no match, so no point in trying later places to start
   * matching this pattern.
   */
  return LIKE_ABORT;
} /* MatchText() */

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_STRING_UTIL_H_
