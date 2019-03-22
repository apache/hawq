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

#include "dbcommon/utils/string-util.h"

#include <algorithm>
#include <cassert>
#include <regex>  // NOLINT
#include <string>

namespace dbcommon {

bool StringUtil::iequals(const std::string &str1, const std::string &str2) {
  if (str1.size() != str2.size()) {
    return false;
  }
  for (std::string::const_iterator c1 = str1.begin(), c2 = str2.begin();
       c1 != str1.end(); ++c1, ++c2) {
    if (tolower(*c1) != tolower(*c2)) {
      return false;
    }
  }
  return true;
}

void StringUtil::replace(std::string *subject, const std::string &search,
                         const std::string &replace) {
  size_t pos = 0;
  while ((pos = subject->find(search, pos)) != std::string::npos) {
    subject->replace(pos, search.length(), replace);
    pos += replace.length();
  }
}

void StringUtil::toLower(std::string *str) {
  assert(str != nullptr);

  std::transform(str->begin(), str->end(), str->begin(), ::tolower);
}

std::string StringUtil::lower(const std::string &str) {
  std::string result;

  for (std::string::const_iterator iter = str.begin(); iter != str.end();
       iter++) {
    char c = tolower(*iter);
    result.append(&c, sizeof(char));
  }

  return std::move(result);
}

std::string &StringUtil::trim(std::string &s) {  // NOLINT
  if (s.empty()) {
    return s;
  }
  s.erase(0, s.find_first_not_of(" "));
  s.erase(s.find_last_not_of(" ") + 1);
  return s;
}

std::string &StringUtil::trimNewLine(std::string &s) {  // NOLINT
  s.erase(std::remove(s.begin(), s.end(), '\n'), s.end());
  return s;
}

std::vector<std::string> StringUtil::split(const std::string &s,
                                           char delimiter) {
  std::vector<std::string> v;

  std::string::size_type i = 0;
  std::string::size_type j = s.find(delimiter);
  if (j == std::string::npos) {
    v.push_back(s);
  }
  while (j != std::string::npos) {
    v.push_back(s.substr(i, j - i));
    i = ++j;
    j = s.find(delimiter, j);

    if (j == std::string::npos) v.push_back(s.substr(i, s.length()));
  }
  return v;
}

std::string StringUtil::regexReplace(std::string *subject,
                                     const std::string &pattern,
                                     const std::string &replace) {
  const std::regex regPattern(pattern);
  return std::regex_replace(*subject, regPattern, replace);
}

bool StringUtil::StartWith(const std::string &str,
                           const std::string &strStart) {
  if (str.empty() || strStart.empty()) {
    return false;
  }
  return str.compare(0, strStart.size(), strStart) == 0 ? true : false;
}

// todo(chiyang): the following code comes from postgres, there could be more
// optimization
struct Utf8LikeUtil {
  inline static int CHAREQ(const char *p1, const char *p2) {
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
  inline static void NextChar(const char *&p, int &plen) {  // NOLINT
    int __l = utf8_mblen(p);
    (p) += __l;
    (plen) -= __l;
  }
};

struct AsciiLikeUtil {
  inline static int CHAREQ(const char *p1, const char *p2) {
    return (*p1 == *p2);
  }
  inline static void NextChar(const char *&p, int &plen) {  // NOLINT
    (p) += 1;
    (plen) -= 1;
  }
};

#define LIKE_TRUE true
#define LIKE_FALSE false
#define LIKE_ABORT false

template <typename LikeUtil>
inline static bool MatchText(const char *t, int tlen, const char *p, int plen) {
  /* Fast path for match-everything pattern */
  if ((plen == 1) && (*p == '%')) return LIKE_TRUE;

  while ((tlen > 0) && (plen > 0)) {
    if (*p == '\\') {
      /* Next pattern char must match literally, whatever it is */
      LikeUtil::NextChar(p, plen);
      if ((plen <= 0) || !LikeUtil::CHAREQ(t, p)) return LIKE_FALSE;
    } else if (*p == '%') {
      /* %% is the same as % according to the SQL standard */
      /* Advance past all %'s */
      while ((plen > 0) && (*p == '%')) LikeUtil::NextChar(p, plen);
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
        if (LikeUtil::CHAREQ(t, p) || (*p == '\\') || (*p == '_')) {
          int matched = MatchText<LikeUtil>(t, tlen, p, plen);

          if (matched != LIKE_FALSE) return matched; /* TRUE or ABORT */
        }

        LikeUtil::NextChar(t, tlen);
      }

      /*
       * End of text with no match, so no point in trying later places
       * to start matching this pattern.
       */
      return LIKE_ABORT;
    } else if ((*p != '_') && !LikeUtil::CHAREQ(t, p)) {
      /*
       * Not the single-character wildcard and no explicit match? Then
       * time to quit...
       */
      return LIKE_FALSE;
    }

    LikeUtil::NextChar(t, tlen);
    LikeUtil::NextChar(p, plen);
  }

  if (tlen > 0) return LIKE_FALSE; /* end of pattern, but not of text */

  /* End of input string.  Do we have matching pattern remaining? */
  while ((plen > 0) && (*p == '%')) /* allow multiple %'s at end of  pattern */
    LikeUtil::NextChar(p, plen);
  if (plen <= 0) return LIKE_TRUE;

  /*
   * End of text with no match, so no point in trying later places to start
   * matching this pattern.
   */
  return LIKE_ABORT;
} /* MatchText() */

bool StringUtil::MatchUtf8Pattern(const char *t, int tlen, const char *p,
                                  int plen) {
  return MatchText<Utf8LikeUtil>(t, tlen, p, plen);
}
bool StringUtil::MatchAsciiPattern(const char *t, int tlen, const char *p,
                                   int plen) {
  return MatchText<AsciiLikeUtil>(t, tlen, p, plen);
}

}  // namespace dbcommon
