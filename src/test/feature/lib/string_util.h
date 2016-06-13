#ifndef HAWQ_SRC_TEST_FEATURE_LIB_STRING_UTIL_H_
#define HAWQ_SRC_TEST_FEATURE_LIB_STRING_UTIL_H_

#include <string>
#include <vector>
#include <cstdio>

namespace hawq {
namespace test {

bool iequals(const std::string &, const std::string &);

void replace(std::string &, const std::string &, const std::string &);

void toLower(std::string &);

std::string lower(const std::string &);

std::string &trim(std::string &);

std::string &trimNewLine(std::string &);

std::vector<std::string> split(const std::string &, char);

std::string regexReplace(std::string &, const std::string &, const std::string &);

bool startsWith(const std::string &, const std::string &);

bool endsWith(const std::string &, const std::string &);

template <typename... T>
std::string stringFormat(const std::string &fmt, T... vs) {
  char b;
  unsigned required = std::snprintf(&b, 0, fmt.c_str(), vs...) + 1;
  char bytes[required];
  std::snprintf(bytes, required, fmt.c_str(), vs...);
  return std::string(bytes);
}

} // namespace test
} // namespace hawq 

#endif
