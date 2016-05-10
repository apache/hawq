#include "string-util.h"

#include <algorithm>
#include <cassert>
#include <regex>
#include <string>

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

